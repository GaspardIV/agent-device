import { cleanupDaemonTestState } from './support/daemon-test-cleanup.ts';
import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { DAEMON_RPC_PROTOCOL_VERSION } from '@agent-device/contracts/daemon-http';
import { skipWhenLoopbackUnavailable } from '@agent-device/test-kit';
import { formatResultDebug } from './cli-json.ts';
import { assertNoDaemonLeaks } from './support/daemon-leak-oracle.ts';
import { runCliJson } from './test-helpers.ts';

type DaemonInfo = {
  token: string;
  controlSecret?: string;
  pid: number;
  processStartTime?: string;
  transport?: string;
  httpPort?: number;
  controlPort?: number;
};

test('daemon HTTP transport starts from CLI and accepts a command RPC', async (t) => {
  if (await skipWhenLoopbackUnavailable(t)) {
    return;
  }

  const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-device-http-smoke-'));
  let info: DaemonInfo | null = null;
  try {
    const args = [
      'session',
      'list',
      '--json',
      '--daemon-transport',
      'http',
      '--state-dir',
      stateDir,
    ];
    const run = () =>
      runCliJson(args, {
        env: {
          ...process.env,
          AGENT_DEVICE_DAEMON_SERVER_MODE: 'http',
        },
      });
    const cli = run();

    assert.equal(cli.status, 0, formatResultDebug('start HTTP daemon', ['session', 'list'], cli));
    assert.equal(cli.json?.success, true, JSON.stringify(cli.json));

    info = readDaemonInfo(stateDir);
    assert.equal(info.transport, 'http');
    assert.equal(typeof info.httpPort, 'number');
    assert.ok((info.httpPort ?? 0) > 0);

    const health = await fetch(`http://127.0.0.1:${info.httpPort}/health`);
    assert.equal(health.status, 200);
    const healthPayload = (await health.json()) as Record<string, unknown>;
    assert.equal(healthPayload.ok, true);
    assert.equal(healthPayload.service, 'agent-device-daemon');
    assert.equal(typeof healthPayload.version, 'string');
    assert.equal(healthPayload.rpcProtocolVersion, DAEMON_RPC_PROTOCOL_VERSION);

    const rpc = await callCommandRpc(info, 'session_list');
    assert.equal(rpc.status, 200);
    assert.equal(rpc.body.result?.ok, true, JSON.stringify(rpc.body));

    const unauthorized = await callCommandRpc({ ...info, token: 'wrong-token' }, 'session_list');
    assert.equal(unauthorized.status, 401);
    assert.equal(unauthorized.body.error?.data?.code, 'UNAUTHORIZED');
    // #1781 B1: the HTTP-mode daemon must exit — leaving nothing it owns and no
    // unclassified state-dir residue. Asserted on the success path so the
    // oracle's settle window can never replace a primary assertion's
    // diagnostic; the `finally` below stays best-effort cleanup.
    await stopDaemon(stateDir, info);
    await assertNoDaemonLeaks({ stateDir, daemonPids: [info.pid], phase: 'after-shutdown' });
  } finally {
    await cleanupDaemonTestState(stateDir, info);
  }
});

function readDaemonInfo(stateDir: string): DaemonInfo {
  const infoPath = path.join(stateDir, 'daemon.json');
  return JSON.parse(fs.readFileSync(infoPath, 'utf8')) as DaemonInfo;
}

async function callCommandRpc(
  info: DaemonInfo,
  command: string,
  input?: Record<string, unknown>,
): Promise<{ status: number; body: any }> {
  const response = await fetch(`http://127.0.0.1:${info.httpPort}/rpc`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: `rpc-${Date.now()}`,
      method: 'agent_device.command',
      params: {
        token: info.token,
        session: 'default',
        command,
        positionals: [],
        flags: {},
        ...(input ? { input } : {}),
      },
    }),
  });
  return {
    status: response.status,
    body: await response.json(),
  };
}

test('HTTP-only daemon with AGENT_DEVICE_HTTP_AUTH_HOOK refuses /rpc and HTTP-bearer control attempts while host stop uses control route', async (t) => {
  if (await skipWhenLoopbackUnavailable(t)) {
    return;
  }

  const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-device-http-auth-stop-'));
  const hookDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-device-http-auth-hook-'));
  const hookPath = path.join(hookDir, 'tenant-auth-hook.mjs');
  fs.writeFileSync(
    hookPath,
    `export default async function ({ daemonRequest }) {
      if (daemonRequest?.command === 'session_list') {
        return { ok: true, tenantId: 'smoke-tenant' };
      }
      return { ok: false, code: 'FORBIDDEN', message: 'Denied by smoke auth hook' };
    }\n`,
  );
  let info: DaemonInfo | null = null;
  try {
    const cli = runCliJson(
      ['session', 'list', '--json', '--daemon-transport', 'http', '--state-dir', stateDir],
      {
        env: {
          ...process.env,
          AGENT_DEVICE_DAEMON_SERVER_MODE: 'http',
          AGENT_DEVICE_HTTP_AUTH_HOOK: hookPath,
        },
      },
    );
    assert.equal(
      cli.status,
      0,
      formatResultDebug('start HTTP daemon under auth hook', ['session', 'list'], cli),
    );
    assert.equal(cli.json?.success, true, JSON.stringify(cli.json));

    info = readDaemonInfo(stateDir);
    assert.equal(info.transport, 'http');
    assert.equal(typeof info.httpPort, 'number');
    assert.equal(typeof info.controlPort, 'number');
    assert.equal(typeof info.controlSecret, 'string');
    assert.notEqual(info.controlSecret, info.token);

    const tenantRpc = await callCommandRpc(info, 'session_list');
    assert.equal(tenantRpc.status, 200);
    assert.equal(tenantRpc.body.result?.ok, true, JSON.stringify(tenantRpc.body));

    const shutdownRpc = await callCommandRpc(info, 'daemon_shutdown', {
      pid: info.pid,
      processStartTime: info.processStartTime ?? null,
    });
    assert.equal(shutdownRpc.status, 401);
    assert.match(JSON.stringify(shutdownRpc.body), /Denied by smoke auth hook/);

    // Even a hook-attested HTTP tenant carrying info.token cannot stop the daemon over controlPort.
    const bearerAttack = await attemptControlShutdownWithKey(info, info.token);
    assert.equal(bearerAttack.challengeMatched, false);
    assert.equal(bearerAttack.receivedReply, false);

    const stillAliveRpc = await callCommandRpc(info, 'session_list');
    assert.equal(stillAliveRpc.status, 200);
    assert.equal(stillAliveRpc.body.result?.ok, true, JSON.stringify(stillAliveRpc.body));

    await stopDaemon(stateDir, info);
    await assertNoDaemonLeaks({ stateDir, daemonPids: [info.pid], phase: 'after-shutdown' });

    const daemonLog = fs.readFileSync(path.join(stateDir, 'daemon.log'), 'utf8');
    assert.match(daemonLog, /"phase":"daemon_control_shutdown_accepted"/);
    assert.match(daemonLog, /"phase":"daemon_shutdown_provider_release_drain"/);
  } finally {
    await cleanupDaemonTestState(stateDir, info);
    fs.rmSync(hookDir, { recursive: true, force: true });
  }
});

async function attemptControlShutdownWithKey(
  info: DaemonInfo,
  secretKey: string,
): Promise<{ challengeMatched: boolean; receivedReply: boolean }> {
  const clientNonce = crypto.randomBytes(16).toString('hex');
  const requestId = `tenant-attack-${Date.now()}`;
  const hmac = (fields: readonly (string | number)[]) =>
    crypto.createHmac('sha256', secretKey).update(JSON.stringify(fields)).digest('hex');

  return await new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: '127.0.0.1', port: info.controlPort! });
    let buffer = '';
    let challengeMatched = false;
    let receivedReply = false;
    let serverNonce = '';
    socket.setEncoding('utf8');
    socket.on('error', () => resolve({ challengeMatched, receivedReply }));
    socket.on('close', () => resolve({ challengeMatched, receivedReply }));
    socket.on('connect', () => {
      socket.write(
        `${JSON.stringify({ type: 'challenge', version: 1, nonce: clientNonce, requestId })}\n`,
      );
    });
    socket.on('data', (chunk: string) => {
      buffer += chunk;
      let nl = buffer.indexOf('\n');
      while (nl !== -1) {
        const line = buffer.slice(0, nl).trim();
        buffer = buffer.slice(nl + 1);
        nl = buffer.indexOf('\n');
        if (!line) continue;
        const msg = JSON.parse(line) as { type?: string; serverNonce?: string; proof?: string };
        if (!serverNonce && msg.type === 'challenge_response' && msg.serverNonce) {
          serverNonce = msg.serverNonce;
          const expectedChallenge = hmac([
            'agent-device.control.challenge.v1',
            info.pid,
            info.processStartTime ?? '',
            clientNonce,
            serverNonce,
          ]);
          challengeMatched = msg.proof === expectedChallenge;
          const forgedProof = hmac([
            'agent-device.control.command.v1',
            'daemon_shutdown',
            info.pid,
            info.processStartTime ?? '',
            clientNonce,
            serverNonce,
            requestId,
          ]);
          socket.write(
            `${JSON.stringify({
              type: 'command',
              version: 1,
              command: 'daemon_shutdown',
              clientNonce,
              serverNonce,
              pid: info.pid,
              processStartTime: info.processStartTime ?? null,
              requestId,
              proof: forgedProof,
            })}\n`,
          );
          continue;
        }
        if (msg.type === 'command_response') receivedReply = true;
      }
    });
    socket.setTimeout(2_000, () => {
      socket.destroy();
      reject(new Error('Timed out waiting for control server to reject HTTP bearer attempt'));
    });
  });
}

async function stopDaemon(stateDir: string, info: DaemonInfo): Promise<void> {
  if (!Number.isInteger(info.pid) || info.pid <= 0) return;
  const stopped = runCliJson(['daemon', 'stop', '--json', '--state-dir', stateDir]);
  assert.equal(
    stopped.status,
    0,
    formatResultDebug('stop HTTP daemon', ['daemon', 'stop'], stopped),
  );
  assert.equal(stopped.json?.success, true, JSON.stringify(stopped.json));
  assert.equal(stopped.json?.data?.stopped, true, JSON.stringify(stopped.json));
  assert.equal(stopped.json?.data?.mode, 'graceful', JSON.stringify(stopped.json));
  assert.equal(stopped.json?.data?.cleanupConfidence, 'known', JSON.stringify(stopped.json));
  assert.deepEqual(stopped.json?.data?.warnings, [], JSON.stringify(stopped.json));
}
