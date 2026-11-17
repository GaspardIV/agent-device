import fs from 'node:fs';
import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import path from 'node:path';
import { readProcessStartTime } from '@agent-device/host-kit/process';
import { afterEach, expect, test, vi } from 'vitest';
import { mkdtempForTestSync, createTestHostEnvironment } from '@agent-device/test-kit';
import {
  DAEMON_CAPABILITIES,
  DAEMON_LIFECYCLE_REASONS,
  DAEMON_SHUTDOWN_COMMAND,
  resolveDaemonPaths,
  type DaemonCapability,
  type DaemonPaths,
} from '@agent-device/daemon-contracts/daemon-resolution';
import { readDaemonStopIdentity } from '@agent-device/daemon-contracts/daemon-registration';
import { resolveDaemonShutdownReportPath } from '@agent-device/daemon-contracts/daemon-shutdown-report';
import { stopRecordedDaemon } from '../daemon-client-stop.ts';
import * as transport from '../daemon-client-transport.ts';

afterEach(() => vi.restoreAllMocks());

test.each([
  ['embedded', false],
  ['embedded', true],
  ['future-owner', false],
  ['future-owner', true],
] as const)(
  'stop refuses live %s ownership without transport (start time recorded: %s)',
  async (ownership, withStartTime) => {
    const paths = resolveDaemonPaths(mkdtempForTestSync('agent-device-stop-owner-'));
    const processStartTime = withStartTime ? readProcessStartTime(process.pid) : undefined;
    if (withStartTime) expect(processStartTime).toBeTruthy();
    const record = JSON.stringify({
      pid: process.pid,
      ownership,
      processStartTime,
      port: 4310,
      token: 'secret-token',
    });
    fs.writeFileSync(paths.infoPath, record);
    const send = vi.spyOn(transport, 'sendRequest');
    const signals = vi.spyOn(process, 'kill');
    await expect(
      stopRecordedDaemon(readDaemonStopIdentity(paths.infoPath), {
        paths,
        host: createTestHostEnvironment(),
      }),
    ).rejects.toMatchObject({
      code: 'COMMAND_FAILED',
      details: {
        reason: ownership === 'embedded' ? 'daemon-owner-embedded' : 'daemon-owner-unrecognized',
        pid: process.pid,
      },
    });
    expect(send).not.toHaveBeenCalled();
    expect(signals.mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
    expect(fs.readFileSync(paths.infoPath, 'utf8')).toBe(record);
  },
);

test('stop refuses an unreadable record without a request or signal', async () => {
  const paths = resolveDaemonPaths(mkdtempForTestSync('agent-device-stop-unreadable-'));
  fs.writeFileSync(paths.infoPath, '{not json');
  const send = vi.spyOn(transport, 'sendRequest');
  const signals = vi.spyOn(process, 'kill');
  await expect(
    stopRecordedDaemon(readDaemonStopIdentity(paths.infoPath), {
      paths,
      host: createTestHostEnvironment(),
    }),
  ).rejects.toMatchObject({ details: { reason: 'daemon-record-unreadable' } });
  expect(send).not.toHaveBeenCalled();
  expect(signals.mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
  expect(fs.readFileSync(paths.infoPath, 'utf8')).toBe('{not json');
});

test.each(['embedded', 'future-owner'])(
  'stop has no process to retire for %s ownership without a PID',
  async (ownership) => {
    const paths = resolveDaemonPaths(mkdtempForTestSync('agent-device-stop-no-pid-'));
    const record = JSON.stringify({ ownership });
    fs.writeFileSync(paths.infoPath, record);
    const send = vi.spyOn(transport, 'sendRequest');
    const signals = vi.spyOn(process, 'kill');
    await expect(
      stopRecordedDaemon(readDaemonStopIdentity(paths.infoPath), {
        paths,
        host: createTestHostEnvironment(),
      }),
    ).resolves.toMatchObject({ stopped: false, mode: 'not-running' });
    expect(send).not.toHaveBeenCalled();
    expect(signals).not.toHaveBeenCalled();
    expect(fs.readFileSync(paths.infoPath, 'utf8')).toBe(record);
  },
);

test.each([
  ['embedded', true],
  ['embedded', false],
  ['future-owner', true],
  ['future-owner', false],
] as const)(
  'stop requires lifetime proof to retire an exited %s host (start time recorded: %s)',
  async (ownership, withStartTime) => {
    const paths = resolveDaemonPaths(mkdtempForTestSync('agent-device-stop-exited-host-'));
    const host = spawn(process.execPath, ['-e', "process.stdin.resume(); console.log('ready')"], {
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    try {
      await once(host.stdout!, 'data');
      const processStartTime = readProcessStartTime(host.pid!);
      expect(processStartTime).toBeTruthy();
      fs.writeFileSync(
        paths.infoPath,
        JSON.stringify({
          pid: host.pid,
          processStartTime: withStartTime ? processStartTime : undefined,
          ownership,
        }),
      );
      const exited = once(host, 'exit');
      host.stdin!.end();
      await exited;
      const send = vi.spyOn(transport, 'sendRequest');
      const signals = vi.spyOn(process, 'kill');
      const stopped = stopRecordedDaemon(readDaemonStopIdentity(paths.infoPath), {
        paths,
        host: createTestHostEnvironment(),
      });
      if (withStartTime) {
        await expect(stopped).resolves.toMatchObject({ stopped: false, mode: 'not-running' });
      } else {
        await expect(stopped).rejects.toMatchObject({
          details: {
            reason:
              ownership === 'embedded' ? 'daemon-owner-embedded' : 'daemon-owner-unrecognized',
          },
        });
      }
      expect(send).not.toHaveBeenCalled();
      expect(signals.mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
      expect(fs.existsSync(paths.infoPath)).toBe(!withStartTime);
    } finally {
      if (host.exitCode === null) host.kill();
    }
  },
);

async function withMockStopDaemon(
  prefix: string,
  transportKind: 'socket' | 'http',
  buildScript: (ctx: { infoPath: string; reportPath: string }) => string,
  run: (ctx: { paths: DaemonPaths; daemon: ChildProcess; record: string }) => Promise<void>,
  options?: { capabilities?: readonly DaemonCapability[] },
): Promise<void> {
  const baseDir = mkdtempForTestSync(prefix);
  const paths = resolveDaemonPaths(baseDir);
  const entry = path.join(baseDir, 'dist', 'src', 'internal', 'daemon.js');
  fs.mkdirSync(path.dirname(entry), { recursive: true });
  fs.writeFileSync(path.join(baseDir, 'package.json'), '{"type":"module"}');
  fs.writeFileSync(
    entry,
    buildScript({
      infoPath: paths.infoPath,
      reportPath: resolveDaemonShutdownReportPath(baseDir),
    }),
  );

  const daemon = spawn(process.execPath, [entry], { stdio: ['ignore', 'pipe', 'pipe'] });
  try {
    const [line] = await once(daemon.stdout!, 'data');
    const { port, token } = JSON.parse(String(line).trim()) as { port: number; token: string };
    const processStartTime = readProcessStartTime(daemon.pid!);
    expect(processStartTime).toBeTruthy();
    const record = JSON.stringify({
      pid: daemon.pid,
      processStartTime,
      ownership: 'detached',
      ...(transportKind === 'http' ? { httpPort: port } : { port }),
      transport: transportKind,
      token,
      capabilities: options?.capabilities ?? DAEMON_CAPABILITIES,
    });
    fs.writeFileSync(paths.infoPath, record);
    await run({ paths, daemon, record });
  } finally {
    if (daemon.exitCode === null) daemon.kill('SIGKILL');
    fs.rmSync(baseDir, { recursive: true, force: true });
  }
}

test('stopRecordedDaemon stops a detached daemon over RPC and merges its shutdown report without signalling', async () => {
  await withMockStopDaemon(
    'agent-device-stop-rpc-',
    'socket',
    ({ infoPath, reportPath }) => `import fs from 'node:fs';
import net from 'node:net';
const infoPath = ${JSON.stringify(infoPath)};
const reportPath = ${JSON.stringify(reportPath)};
const token = 'test-daemon-token';
const server = net.createServer((socket) => {
  let buf = '';
  socket.setEncoding('utf8');
  socket.on('data', (chunk) => {
    buf += chunk;
    const nl = buf.indexOf('\\n');
    if (nl === -1) return;
    const req = JSON.parse(buf.slice(0, nl));
    if (req.command === ${JSON.stringify(DAEMON_SHUTDOWN_COMMAND)} && req.token === token && req.input?.pid === process.pid) {
      socket.write(JSON.stringify({ ok: true, data: { outcome: 'draining' } }) + '\\n', () => {
        fs.writeFileSync(reportPath, JSON.stringify({
          providerReleases: { released: [{ leaseId: 'lease-1', provider: 'browserstack' }], pending: [] },
          claims: { released: [], orphaned: [], superseded: [], unattributable: [] },
        }) + '\\n');
        fs.rmSync(infoPath, { force: true });
        server.close(() => process.exit(0));
      });
      return;
    }
    socket.end(JSON.stringify({ ok: false, error: { code: 'INVALID_ARGS', message: 'unexpected request' } }) + '\\n');
  });
});
server.listen(0, '127.0.0.1', () => {
  console.log(JSON.stringify({ port: server.address().port, token }));
});`,
    async ({ paths, daemon }) => {
      const signals = vi.spyOn(process, 'kill');
      const exited = once(daemon, 'exit');
      const result = await stopRecordedDaemon(readDaemonStopIdentity(paths.infoPath), {
        paths,
        host: createTestHostEnvironment(),
      });
      const [exitCode] = await exited;

      expect(exitCode).toBe(0);
      expect(signals.mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
      expect(result).toEqual({
        stopped: true,
        mode: 'graceful',
        cleanupConfidence: 'known',
        claimsReleased: [],
        claimsOrphaned: [],
        claimsSuperseded: [],
        claimsUnattributable: [],
        providerReleases: {
          status: 'completed',
          released: [{ leaseId: 'lease-1', provider: 'browserstack' }],
          pending: [],
        },
        warnings: [],
      });
    },
  );
});

test('stopRecordedDaemon skips daemon_shutdown when the daemon lacks the shutdown capability', async () => {
  await withMockStopDaemon(
    'agent-device-stop-no-shutdown-cap-',
    'http',
    () => `import http from 'node:http';
const server = http.createServer((req, res) => {
  req.resume();
  req.on('end', () => {
    res.writeHead(500, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'unexpected request' }));
  });
});
server.listen(0, '127.0.0.1', () => {
  console.log(JSON.stringify({ port: server.address().port, token: 'legacy-token' }));
});`,
    async ({ paths, daemon }) => {
      const send = vi.spyOn(transport, 'sendRequest');
      const exited = once(daemon, 'exit');
      const result = await stopRecordedDaemon(readDaemonStopIdentity(paths.infoPath), {
        paths,
        host: createTestHostEnvironment(),
      });
      await exited;

      expect(send).not.toHaveBeenCalled();
      expect(result.stopped).toBe(true);
      expect(result.mode).toBe(process.platform === 'win32' ? 'forced' : 'graceful');
    },
    { capabilities: ['quiesce', 'script-artifacts'] },
  );
});

test('stopRecordedDaemon falls back to signal retirement when an HTTP daemon rejects daemon_shutdown with JSON-RPC error SESSION_NOT_FOUND', async () => {
  await withMockStopDaemon(
    'agent-device-stop-legacy-http-',
    'http',
    () => `import http from 'node:http';
const server = http.createServer((req, res) => {
  let body = '';
  req.setEncoding('utf8');
  req.on('data', (chunk) => { body += chunk; });
  req.on('end', () => {
    const rpc = JSON.parse(body);
    const error = { code: 'SESSION_NOT_FOUND', message: 'No active session.' };
    res.writeHead(404, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      jsonrpc: '2.0',
      id: rpc.id ?? null,
      error: { code: -32000, message: error.message, data: error },
    }));
  });
});
server.listen(0, '127.0.0.1', () => {
  console.log(JSON.stringify({ port: server.address().port, token: 'legacy-token' }));
});`,
    async ({ paths, daemon }) => {
      const exited = once(daemon, 'exit');
      const result = await stopRecordedDaemon(readDaemonStopIdentity(paths.infoPath), {
        paths,
        host: createTestHostEnvironment(),
      });
      await exited;

      expect(result.stopped).toBe(true);
      expect(result.mode).toBe(process.platform === 'win32' ? 'forced' : 'graceful');
    },
  );
});

test('stopRecordedDaemon retries HTTP JSON-RPC daemon-busy while starting up and exits gracefully without signalling', async () => {
  await withMockStopDaemon(
    'agent-device-stop-startup-retry-',
    'http',
    ({ infoPath }) => `import fs from 'node:fs';
import http from 'node:http';
const infoPath = ${JSON.stringify(infoPath)};
let attempts = 0;
const server = http.createServer((req, res) => {
  let body = '';
  req.setEncoding('utf8');
  req.on('data', (chunk) => { body += chunk; });
  req.on('end', () => {
    const rpc = JSON.parse(body);
    attempts += 1;
    if (attempts === 1) {
      const error = {
        code: 'COMMAND_FAILED',
        message: 'Daemon is starting up.',
        details: {
          reason: ${JSON.stringify(DAEMON_LIFECYCLE_REASONS.busy)},
          startingUp: true,
          inFlightRequests: 0,
          sessions: 0,
          repairSessions: 0,
          leases: 0,
        },
      };
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({
        jsonrpc: '2.0',
        id: rpc.id ?? null,
        error: { code: -32000, message: error.message, data: error },
      }));
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(
      JSON.stringify({ jsonrpc: '2.0', id: rpc.id ?? null, result: { ok: true, data: { outcome: 'draining' } } }),
      () => {
        fs.rmSync(infoPath, { force: true });
        server.close(() => process.exit(0));
      },
    );
  });
});
server.listen(0, '127.0.0.1', () => {
  console.log(JSON.stringify({ port: server.address().port, token: 'startup-token' }));
});`,
    async ({ paths, daemon }) => {
      const signals = vi.spyOn(process, 'kill');
      const exited = once(daemon, 'exit');
      const result = await stopRecordedDaemon(readDaemonStopIdentity(paths.infoPath), {
        paths,
        host: createTestHostEnvironment(),
      });
      const [exitCode] = await exited;

      expect(exitCode).toBe(0);
      expect(signals.mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
      expect(result.mode).toBe('graceful');
    },
  );
});

test('stopRecordedDaemon preserves embedded-owner refusal when HTTP JSON-RPC reports embedded ownership without signalling', async () => {
  await withMockStopDaemon(
    'agent-device-stop-embedded-rpc-',
    'http',
    () => `import http from 'node:http';
const server = http.createServer((req, res) => {
  let body = '';
  req.setEncoding('utf8');
  req.on('data', (chunk) => { body += chunk; });
  req.on('end', () => {
    const rpc = JSON.parse(body);
    const error = {
      code: 'COMMAND_FAILED',
      message: 'Owned by host.',
      details: { reason: ${JSON.stringify(DAEMON_LIFECYCLE_REASONS.ownerEmbedded)} },
    };
    res.writeHead(500, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      jsonrpc: '2.0',
      id: rpc.id ?? null,
      error: { code: -32000, message: error.message, data: error },
    }));
  });
});
server.listen(0, '127.0.0.1', () => {
  console.log(JSON.stringify({ port: server.address().port, token: 'embedded-token' }));
});`,
    async ({ paths, daemon, record }) => {
      const signals = vi.spyOn(process, 'kill');
      await expect(
        stopRecordedDaemon(readDaemonStopIdentity(paths.infoPath), {
          paths,
          host: createTestHostEnvironment(),
        }),
      ).rejects.toMatchObject({
        code: 'COMMAND_FAILED',
        details: {
          reason: DAEMON_LIFECYCLE_REASONS.ownerEmbedded,
          pid: daemon.pid,
        },
      });
      expect(signals.mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
      expect(fs.readFileSync(paths.infoPath, 'utf8')).toBe(record);
    },
  );
});
