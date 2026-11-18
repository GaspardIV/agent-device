import fs from 'node:fs';
import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import path from 'node:path';
import { readProcessStartTime } from '@agent-device/host-kit/process';
import { readVersion } from '@agent-device/host-kit/version';
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
import { resolveLocalDaemonCodeIdentity } from '../daemon-launch-spec.ts';
import { readDaemonInfo } from '../daemon-client-metadata.ts';
import { readReusableLocalDaemon, stopDaemonForTakeover } from '../daemon-client-startup.ts';
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

function mockControlHmacHelperSource(): string {
  return `import crypto from 'node:crypto';
function hmac(token, fields) {
  return crypto.createHmac('sha256', token).update(JSON.stringify(fields)).digest('hex');
}
function createControlServer(token, infoPath, onCommand) {
  let cachedStartTime = null;
  const readStartTime = () => {
    if (cachedStartTime !== null) return cachedStartTime;
    try {
      cachedStartTime = JSON.parse(fs.readFileSync(infoPath, 'utf8')).processStartTime ?? '';
    } catch {
      cachedStartTime = '';
    }
    return cachedStartTime;
  };
  return net.createServer((socket) => {
    let buffer = '';
    let challenge = null;
    socket.setEncoding('utf8');
    socket.on('data', (chunk) => {
      buffer += chunk;
      let nl = buffer.indexOf('\\n');
      while (nl !== -1) {
        const line = buffer.slice(0, nl).trim();
        buffer = buffer.slice(nl + 1);
        nl = buffer.indexOf('\\n');
        if (!line) continue;
        const msg = JSON.parse(line);
        const startTime = readStartTime();
        if (msg.type === 'challenge') {
          const serverNonce = crypto.randomBytes(16).toString('hex');
          challenge = { clientNonce: msg.nonce, serverNonce, requestId: msg.requestId };
          const proof = hmac(token, ['agent-device.control.challenge.v1', process.pid, startTime, msg.nonce, serverNonce]);
          socket.write(JSON.stringify({ type: 'challenge_response', version: 1, serverNonce, proof }) + '\\n');
          continue;
        }
        if (msg.type === 'command' && challenge) {
          const expected = hmac(token, [
            'agent-device.control.command.v1',
            msg.command,
            msg.pid,
            msg.processStartTime ?? '',
            challenge.clientNonce,
            challenge.serverNonce,
            challenge.requestId,
          ]);
          if (msg.proof !== expected) {
            socket.destroy();
            return;
          }
          onCommand({ socket, msg, challenge, startTime, respond: (payloadObj, onFlushed) => {
            const payload = JSON.stringify(payloadObj);
            const proof = hmac(token, [
              'agent-device.control.reply.v1',
              msg.pid,
              msg.processStartTime ?? '',
              challenge.clientNonce,
              challenge.serverNonce,
              challenge.requestId,
              payload,
            ]);
            socket.write(JSON.stringify({ type: 'command_response', version: 1, payload, proof }) + '\\n', onFlushed);
          }});
        }
      }
    });
  });
}`;
}

async function withMockStopDaemon(
  prefix: string,
  transportKind: 'socket' | 'http',
  buildScript: (ctx: { baseDir: string; infoPath: string; reportPath: string }) => string,
  run: (ctx: {
    baseDir: string;
    paths: DaemonPaths;
    daemon: ChildProcess;
    record: string;
  }) => Promise<void>,
  options?: {
    capabilities?: readonly DaemonCapability[];
    version?: string;
    codeOrigin?: 'installed' | 'checkout';
    codeSignature?: string;
  },
): Promise<void> {
  const baseDir = mkdtempForTestSync(prefix);
  const paths = resolveDaemonPaths(baseDir);
  const entry = path.join(baseDir, 'dist', 'src', 'internal', 'daemon.js');
  fs.mkdirSync(path.dirname(entry), { recursive: true });
  fs.writeFileSync(path.join(baseDir, 'package.json'), '{"type":"module"}');
  fs.writeFileSync(
    entry,
    buildScript({
      baseDir,
      infoPath: paths.infoPath,
      reportPath: resolveDaemonShutdownReportPath(baseDir),
    }),
  );

  const daemon = spawn(process.execPath, [entry], { stdio: ['ignore', 'pipe', 'pipe'] });
  try {
    const [line] = await once(daemon.stdout!, 'data');
    const { port, controlPort, token } = JSON.parse(String(line).trim()) as {
      port?: number;
      controlPort?: number;
      token: string;
    };
    const processStartTime = readProcessStartTime(daemon.pid!);
    expect(processStartTime).toBeTruthy();
    const record = JSON.stringify({
      pid: daemon.pid,
      processStartTime,
      ...(options?.version ? { version: options.version } : {}),
      ...(options?.codeOrigin ? { codeOrigin: options.codeOrigin } : {}),
      ...(options?.codeSignature ? { codeSignature: options.codeSignature } : {}),
      ownership: 'detached',
      ...(transportKind === 'http' ? { httpPort: port } : { port }),
      ...(controlPort !== undefined ? { controlPort } : {}),
      transport: transportKind,
      token,
      capabilities: options?.capabilities ?? DAEMON_CAPABILITIES,
    });
    fs.writeFileSync(paths.infoPath, record);
    await run({ baseDir, paths, daemon, record });
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
${mockControlHmacHelperSource()}
const infoPath = ${JSON.stringify(infoPath)};
const reportPath = ${JSON.stringify(reportPath)};
const token = 'test-daemon-token';
const server = createControlServer(token, infoPath, ({ msg, respond }) => {
  if (msg.command === ${JSON.stringify(DAEMON_SHUTDOWN_COMMAND)} && msg.pid === process.pid) {
    respond({ ok: true, data: { outcome: 'draining' } }, () => {
      fs.writeFileSync(reportPath, JSON.stringify({
        providerReleases: { released: [{ leaseId: 'lease-1', provider: 'browserstack' }], pending: [] },
        claims: { released: [], orphaned: [], superseded: [], unattributable: [] },
      }) + '\\n');
      fs.rmSync(infoPath, { force: true });
      server.close(() => process.exit(0));
    });
    return;
  }
  respond({ ok: false, error: { code: 'INVALID_ARGS', message: 'unexpected request' } });
});
server.listen(0, '127.0.0.1', () => {
  const port = server.address().port;
  console.log(JSON.stringify({ port, controlPort: port, token }));
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

test('stopRecordedDaemon falls back to signal retirement when control rejects daemon_shutdown with SESSION_NOT_FOUND', async () => {
  await withMockStopDaemon(
    'agent-device-stop-legacy-http-',
    'http',
    ({ infoPath }) => `import fs from 'node:fs';
import net from 'node:net';
${mockControlHmacHelperSource()}
const token = 'legacy-token';
const control = createControlServer(token, ${JSON.stringify(infoPath)}, ({ respond }) => {
  respond({ ok: false, error: { code: 'SESSION_NOT_FOUND', message: 'No active session.' } });
});
control.listen(0, '127.0.0.1', () => {
  const controlPort = control.address().port;
  console.log(JSON.stringify({ port: controlPort, controlPort, token }));
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

test('stopRecordedDaemon retries control daemon-busy while starting up and exits gracefully without signalling', async () => {
  await withMockStopDaemon(
    'agent-device-stop-startup-retry-',
    'http',
    ({ infoPath }) => `import fs from 'node:fs';
import net from 'node:net';
${mockControlHmacHelperSource()}
const infoPath = ${JSON.stringify(infoPath)};
const token = 'startup-token';
let attempts = 0;
const server = createControlServer(token, infoPath, ({ respond }) => {
  attempts += 1;
  if (attempts === 1) {
    respond({
      ok: false,
      error: {
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
      },
    });
    return;
  }
  respond({ ok: true, data: { outcome: 'draining' } }, () => {
    fs.rmSync(infoPath, { force: true });
    server.close(() => process.exit(0));
  });
});
server.listen(0, '127.0.0.1', () => {
  const controlPort = server.address().port;
  console.log(JSON.stringify({ port: controlPort, controlPort, token }));
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

test('stopRecordedDaemon preserves embedded-owner refusal when control reports embedded ownership without signalling', async () => {
  await withMockStopDaemon(
    'agent-device-stop-embedded-rpc-',
    'http',
    ({ infoPath }) => `import fs from 'node:fs';
import net from 'node:net';
${mockControlHmacHelperSource()}
const token = 'embedded-token';
const server = createControlServer(token, ${JSON.stringify(infoPath)}, ({ respond }) => {
  respond({
    ok: false,
    error: {
      code: 'COMMAND_FAILED',
      message: 'Owned by host.',
      details: { reason: ${JSON.stringify(DAEMON_LIFECYCLE_REASONS.ownerEmbedded)} },
    },
  });
});
server.listen(0, '127.0.0.1', () => {
  const controlPort = server.address().port;
  console.log(JSON.stringify({ port: controlPort, controlPort, token }));
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

test('stopRecordedDaemon waits the drain grace window instead of signalling when authenticated shutdown reply times out', async () => {
  await withMockStopDaemon(
    'agent-device-stop-timeout-rpc-',
    'http',
    ({ infoPath, reportPath }) => `import fs from 'node:fs';
import net from 'node:net';
${mockControlHmacHelperSource()}
const infoPath = ${JSON.stringify(infoPath)};
const reportPath = ${JSON.stringify(reportPath)};
const token = 'slow-token';
const server = createControlServer(token, infoPath, () => {
  // Simulate an authenticated daemon that received shutdown and is draining past the control read timeout.
  setTimeout(() => {
    fs.writeFileSync(reportPath, JSON.stringify({
      providerReleases: { released: [], pending: [] },
      claims: { released: [], orphaned: [], superseded: [], unattributable: [] },
    }) + '\\n');
    fs.rmSync(infoPath, { force: true });
    server.close(() => process.exit(0));
  }, 300);
});
server.listen(0, '127.0.0.1', () => {
  const controlPort = server.address().port;
  console.log(JSON.stringify({ port: controlPort, controlPort, token }));
});`,
    async ({ paths, daemon }) => {
      const signals = vi.spyOn(process, 'kill');
      const exited = once(daemon, 'exit');
      const result = await stopRecordedDaemon(
        readDaemonStopIdentity(paths.infoPath),
        {
          paths,
          host: createTestHostEnvironment(),
        },
        { shutdownTimeoutMs: 120 },
      );
      const [exitCode] = await exited;

      expect(exitCode).toBe(0);
      expect(signals.mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
      expect(result.mode).toBe('graceful');
    },
  );
});

test.each(['reset', 'truncate'] as const)(
  'stopRecordedDaemon waits for drain without signalling when authenticated shutdown socket %ss reply',
  async (mode) => {
    await withMockStopDaemon(
      `agent-device-stop-post-dispatch-${mode}-`,
      'socket',
      ({ infoPath, reportPath }) => `import fs from 'node:fs';
import net from 'node:net';
${mockControlHmacHelperSource()}
const infoPath = ${JSON.stringify(infoPath)};
const reportPath = ${JSON.stringify(reportPath)};
const token = 'post-dispatch-token';
const server = createControlServer(token, infoPath, ({ socket }) => {
  if (${JSON.stringify(mode)} === 'truncate') {
    socket.write('{"type":"command_response","version":1,"payload":"{\\"ok\\":true');
    socket.end();
  } else {
    socket.resetAndDestroy();
  }
  setTimeout(() => {
    fs.writeFileSync(reportPath, JSON.stringify({
      providerReleases: { released: [], pending: [] },
      claims: { released: [], orphaned: [], superseded: [], unattributable: [] },
    }) + '\\n');
    fs.rmSync(infoPath, { force: true });
    server.close(() => process.exit(0));
  }, 150);
});
server.listen(0, '127.0.0.1', () => {
  const port = server.address().port;
  console.log(JSON.stringify({ port, controlPort: port, token }));
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
  },
);

test('stopRecordedDaemon distinguishes pre-dispatch ECONNREFUSED during active registration from completed retirement', async () => {
  // Case A: daemon.json still matches -> do not wait drain window; escalate via signal.
  await withMockStopDaemon(
    'agent-device-stop-gone-active-info-',
    'socket',
    () => `import net from 'node:net';
const probe = net.createServer();
probe.listen(0, '127.0.0.1', () => {
  const closedPort = probe.address().port;
  probe.close(() => {
    console.log(JSON.stringify({ port: closedPort, controlPort: closedPort, token: 'gone-active' }));
  });
});
setInterval(() => {}, 1000);`,
    async ({ paths, daemon }) => {
      const signals = vi.spyOn(process, 'kill');
      const exited = once(daemon, 'exit');
      const result = await stopRecordedDaemon(readDaemonStopIdentity(paths.infoPath), {
        paths,
        host: createTestHostEnvironment(),
      });
      await exited;
      expect(result.stopped).toBe(true);
      expect(signals.mock.calls.filter(([, signal]) => signal !== 0).length).toBeGreaterThan(0);
    },
  );
});

test('stopRecordedDaemon waits without signalling on pre-dispatch ECONNREFUSED after daemon.json retires with a shutdown report', async () => {
  await withMockStopDaemon(
    'agent-device-stop-gone-finished-info-',
    'socket',
    ({ baseDir }) => `import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
const probedPath = path.join(${JSON.stringify(baseDir)}, 'probed.txt');
const probe = net.createServer();
probe.listen(0, '127.0.0.1', () => {
  const closedPort = probe.address().port;
  probe.close(() => {
    console.log(JSON.stringify({ port: closedPort, controlPort: closedPort, token: 'gone-finished' }));
  });
});
const watch = setInterval(() => {
  if (fs.existsSync(probedPath)) {
    clearInterval(watch);
    setTimeout(() => process.exit(0), 50);
  }
}, 20);`,
    async ({ baseDir, paths, daemon }) => {
      const identity = readDaemonStopIdentity(paths.infoPath);
      const reportPath = resolveDaemonShutdownReportPath(paths.baseDir);
      fs.writeFileSync(
        reportPath,
        JSON.stringify({
          providerReleases: { released: [], pending: [] },
          claims: { released: [], orphaned: [], superseded: [], unattributable: [] },
        }),
      );
      fs.rmSync(paths.infoPath, { force: true });

      const origReadFileSync = fs.readFileSync.bind(fs);
      vi.spyOn(fs, 'readFileSync').mockImplementation((...args) => {
        if (String(args[0]) === reportPath) {
          fs.writeFileSync(path.join(baseDir, 'probed.txt'), '1');
        }
        return origReadFileSync(...(args as Parameters<typeof fs.readFileSync>));
      });

      const signals = vi.spyOn(process, 'kill');
      const exited = once(daemon, 'exit');
      const result = await stopRecordedDaemon(identity, {
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

test('stopDaemonForTakeover uses graceful shutdown control in graceful mode without signalling', async () => {
  await withMockStopDaemon(
    'agent-device-takeover-graceful-rpc-',
    'http',
    ({ infoPath }) => `import fs from 'node:fs';
import net from 'node:net';
${mockControlHmacHelperSource()}
const infoPath = ${JSON.stringify(infoPath)};
const token = 'takeover-token';
const server = createControlServer(token, infoPath, ({ respond }) => {
  respond({ ok: true, data: { outcome: 'draining' } }, () => {
    fs.rmSync(infoPath, { force: true });
    server.close(() => process.exit(0));
  });
});
server.listen(0, '127.0.0.1', () => {
  const controlPort = server.address().port;
  console.log(JSON.stringify({ port: controlPort, controlPort, token }));
});`,
    async ({ paths, daemon }) => {
      const info = readDaemonInfo(paths.infoPath);
      expect(info).toBeTruthy();
      const signals = vi.spyOn(process, 'kill');
      const exited = once(daemon, 'exit');
      const retirement = await stopDaemonForTakeover(
        info!,
        paths,
        'graceful',
        createTestHostEnvironment(),
      );
      const [exitCode] = await exited;

      expect(exitCode).toBe(0);
      expect(signals.mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
      expect(retirement.status).toBe('retired');
      if (retirement.status === 'retired') {
        expect(retirement.termination.mode).toBe('graceful');
      }
    },
  );
});

test('stopDaemonForTakeover skips graceful shutdown control in force mode', async () => {
  await withMockStopDaemon(
    'agent-device-takeover-force-rpc-',
    'http',
    () => `import http from 'node:http';
const server = http.createServer(() => {});
server.listen(0, '127.0.0.1', () => {
  console.log(JSON.stringify({ port: server.address().port, token: 'force-token' }));
});
setInterval(() => {}, 1000);`,
    async ({ paths }) => {
      const info = readDaemonInfo(paths.infoPath);
      expect(info).toBeTruthy();
      const send = vi.spyOn(transport, 'sendRequest');
      const retirement = await stopDaemonForTakeover(
        info!,
        paths,
        'force',
        createTestHostEnvironment(),
      );
      expect(send).not.toHaveBeenCalled();
      expect(retirement.status).toBe('retired');
      if (retirement.status === 'retired') {
        expect(retirement.termination.mode).toBe('forced');
      }
    },
  );
});

test('readReusableLocalDaemon retires an incompatible-version daemon over control without signalling', async () => {
  await withMockStopDaemon(
    'agent-device-reusable-takeover-rpc-',
    'http',
    ({ infoPath }) => `import fs from 'node:fs';
import http from 'node:http';
import net from 'node:net';
${mockControlHmacHelperSource()}
const infoPath = ${JSON.stringify(infoPath)};
const token = 'reusable-token';
const httpServer = http.createServer((_req, res) => {
  res.writeHead(200, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ ok: true }));
});
const control = createControlServer(token, infoPath, ({ respond }) => {
  respond({ ok: true, data: { outcome: 'draining' } }, () => {
    fs.rmSync(infoPath, { force: true });
    httpServer.close();
    control.close(() => process.exit(0));
  });
});
httpServer.listen(0, '127.0.0.1', () => {
  control.listen(0, '127.0.0.1', () => {
    console.log(JSON.stringify({
      port: httpServer.address().port,
      controlPort: control.address().port,
      token,
    }));
  });
});`,
    async ({ paths, daemon }) => {
      const signals = vi.spyOn(process, 'kill');
      const exited = once(daemon, 'exit');
      const reusable = await readReusableLocalDaemon({
        host: createTestHostEnvironment(),
        paths,
        daemonLifecycle: 'managed',
        retireIncompatibleContender: true,
        transportPreference: 'auto',
        serverMode: 'http',
      });
      const [exitCode] = await exited;

      expect(reusable).toBeNull();
      expect(exitCode).toBe(0);
      expect(signals.mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
    },
    { version: '0.0.0' },
  );
});

test('readReusableLocalDaemon skips graceful control shutdown when decision cause is unreachable', async () => {
  const localIdentity = await resolveLocalDaemonCodeIdentity();
  await withMockStopDaemon(
    'agent-device-reusable-unreachable-',
    'socket',
    ({ baseDir }) => `import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
const marker = path.join(${JSON.stringify(baseDir)}, 'control-connected.txt');
const closed = net.createServer();
const control = net.createServer((socket) => {
  fs.writeFileSync(marker, 'connected');
  socket.destroy();
});
closed.listen(0, '127.0.0.1', () => {
  const closedPort = closed.address().port;
  closed.close(() => {
    control.listen(0, '127.0.0.1', () => {
      console.log(JSON.stringify({
        port: closedPort,
        controlPort: control.address().port,
        token: 'unreachable-token',
      }));
    });
  });
});`,
    async ({ baseDir, paths, daemon }) => {
      const signals = vi.spyOn(process, 'kill');
      const exited = once(daemon, 'exit');
      const reusable = await readReusableLocalDaemon({
        host: createTestHostEnvironment(),
        paths,
        daemonLifecycle: 'managed',
        retireIncompatibleContender: true,
        transportPreference: 'socket',
        serverMode: 'socket',
      });
      await exited;

      expect(reusable).toBeNull();
      expect(fs.existsSync(path.join(baseDir, 'control-connected.txt'))).toBe(false);
      expect(signals.mock.calls.filter(([, signal]) => signal !== 0).length).toBeGreaterThan(0);
    },
    {
      version: readVersion(),
      codeOrigin: localIdentity.origin,
      codeSignature: localIdentity.origin === 'checkout' ? localIdentity.codeSignature : undefined,
    },
  );
});

test('a second stopRecordedDaemon arriving mid-drain receives draining and waits without signalling', async () => {
  await withMockStopDaemon(
    'agent-device-stop-concurrent-drain-',
    'http',
    ({ baseDir, infoPath, reportPath }) => `import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
${mockControlHmacHelperSource()}
const infoPath = ${JSON.stringify(infoPath)};
const reportPath = ${JSON.stringify(reportPath)};
const drainStartedPath = path.join(${JSON.stringify(baseDir)}, 'drain-started.txt');
const token = 'concurrent-token';
let stopCalls = 0;
const server = createControlServer(token, infoPath, ({ respond }) => {
  stopCalls += 1;
  respond({ ok: true, data: { outcome: 'draining' } }, () => {
    if (stopCalls === 1) {
      fs.writeFileSync(drainStartedPath, '1');
      return;
    }
    setTimeout(() => {
      fs.writeFileSync(reportPath, JSON.stringify({
        providerReleases: { released: [{ leaseId: 'lease-concurrent', provider: 'limrun' }], pending: [] },
        claims: { released: [], orphaned: [], superseded: [], unattributable: [] },
      }) + '\\n');
      fs.rmSync(infoPath, { force: true });
      server.close(() => process.exit(0));
    }, 50);
  });
});
server.listen(0, '127.0.0.1', () => {
  const controlPort = server.address().port;
  console.log(JSON.stringify({ port: controlPort, controlPort, token }));
});`,
    async ({ baseDir, paths, daemon }) => {
      const identity = readDaemonStopIdentity(paths.infoPath);
      const signals = vi.spyOn(process, 'kill');
      const exited = once(daemon, 'exit');
      const drainStartedPath = path.join(baseDir, 'drain-started.txt');

      const stop1Promise = stopRecordedDaemon(identity, {
        paths,
        host: createTestHostEnvironment(),
      });
      await vi.waitFor(() => expect(fs.existsSync(drainStartedPath)).toBe(true), {
        timeout: 2_000,
        interval: 10,
      });
      const stop2Promise = stopRecordedDaemon(readDaemonStopIdentity(paths.infoPath), {
        paths,
        host: createTestHostEnvironment(),
      });

      const [stop1, stop2] = await Promise.all([stop1Promise, stop2Promise]);
      const [exitCode] = await exited;

      expect(exitCode).toBe(0);
      expect(signals.mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
      expect(stop1.mode).toBe('graceful');
      expect(stop2.mode).toBe('graceful');
      expect(stop1.providerReleases.released).toEqual([
        { leaseId: 'lease-concurrent', provider: 'limrun' },
      ]);
      expect(stop2.providerReleases.released).toEqual([
        { leaseId: 'lease-concurrent', provider: 'limrun' },
      ]);
    },
  );
});
