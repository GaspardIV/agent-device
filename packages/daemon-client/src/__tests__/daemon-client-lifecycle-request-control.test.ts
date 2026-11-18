import crypto from 'node:crypto';
import fs from 'node:fs';
import net from 'node:net';
import { expect, test } from 'vitest';
import { AppError } from '@agent-device/kernel/errors';
import {
  DAEMON_CONTROL_PROTOCOL_VERSION,
  parseControlChallengeRequest,
  parseControlCommandRequest,
  signDaemonControlChallenge,
  signDaemonControlReply,
  verifyDaemonControlCommand,
} from '@agent-device/daemon-contracts/daemon-control';
import {
  DAEMON_CAPABILITIES,
  DAEMON_LIFECYCLE_REASONS,
  DAEMON_QUIESCE_COMMAND,
  DAEMON_SHUTDOWN_COMMAND,
  resolveDaemonPaths,
} from '@agent-device/daemon-contracts/daemon-resolution';
import { createTestHostEnvironment, mkdtempForTestSync } from '@agent-device/test-kit';
import {
  createGracefulDaemonShutdown,
  requestDaemonLifecycle,
} from '../daemon-client-lifecycle-request.ts';

type QuiesceMode = 'draining' | 'busy' | 'mismatch' | 'hang-command' | 'hang-challenge' | 'rogue';

function buildQuiescePayload(mode: QuiesceMode): unknown {
  if (mode === 'draining') return { ok: true, data: { outcome: 'draining' } };
  if (mode === 'busy') {
    return {
      ok: false,
      error: {
        code: 'COMMAND_FAILED',
        message: 'Daemon busy',
        details: {
          reason: DAEMON_LIFECYCLE_REASONS.busy,
          startingUp: false,
          inFlightRequests: 1,
          sessions: 0,
          repairSessions: 0,
          leases: 0,
        },
      },
    };
  }
  return {
    ok: false,
    error: {
      code: 'COMMAND_FAILED',
      message: 'Mismatch',
      details: { reason: DAEMON_LIFECYCLE_REASONS.instanceMismatch },
    },
  };
}

function attachMockControlSession(params: {
  socket: net.Socket;
  token: string;
  fence: { pid: number; processStartTime: string };
  getMode: () => QuiesceMode;
  seenCommands: string[];
}): void {
  const { socket, token, fence, getMode, seenCommands } = params;
  let buffer = '';
  let pending: { clientNonce: string; serverNonce: string; requestId: string } | null = null;
  socket.setEncoding('utf8');

  const handleLine = (line: string): void => {
    const mode = getMode();
    if (mode === 'hang-challenge') return;
    if (!pending) {
      if (mode === 'rogue') {
        socket.end(
          `${JSON.stringify({
            type: 'challenge_response',
            version: DAEMON_CONTROL_PROTOCOL_VERSION,
            serverNonce: crypto.randomBytes(16).toString('hex'),
            proof: '0'.repeat(64),
          })}\n`,
        );
        return;
      }
      const challenge = parseControlChallengeRequest(line);
      expect(challenge).toBeTruthy();
      const serverNonce = crypto.randomBytes(16).toString('hex');
      pending = { clientNonce: challenge!.nonce, serverNonce, requestId: challenge!.requestId };
      socket.write(
        `${JSON.stringify({
          type: 'challenge_response',
          version: DAEMON_CONTROL_PROTOCOL_VERSION,
          serverNonce,
          proof: signDaemonControlChallenge({
            token,
            fence,
            clientNonce: pending.clientNonce,
            serverNonce,
          }),
        })}\n`,
      );
      return;
    }

    const cmd = parseControlCommandRequest(line);
    expect(cmd).toBeTruthy();
    expect(
      verifyDaemonControlCommand({
        token,
        command: cmd!.command,
        fence,
        clientNonce: pending.clientNonce,
        serverNonce: pending.serverNonce,
        requestId: pending.requestId,
        proof: cmd!.proof,
      }),
    ).toBe(true);
    seenCommands.push(cmd!.command);
    if (mode === 'hang-command') return;

    const payload = JSON.stringify(buildQuiescePayload(mode));
    socket.end(
      `${JSON.stringify({
        type: 'command_response',
        version: DAEMON_CONTROL_PROTOCOL_VERSION,
        payload,
        proof: signDaemonControlReply({
          token,
          fence,
          clientNonce: pending.clientNonce,
          serverNonce: pending.serverNonce,
          requestId: pending.requestId,
          payload,
        }),
      })}\n`,
    );
  };

  socket.on('data', (chunk) => {
    buffer += chunk;
    const lines = buffer.split('\n');
    buffer = lines.pop() ?? '';
    for (const line of lines) {
      if (line.trim()) handleLine(line);
    }
  });
}

test('createGracefulDaemonShutdown never leaks token or trusts spoofed/silent/absent-report listeners', async () => {
  const received: string[] = [];
  const spoofedReplies = [
    JSON.stringify({
      type: 'challenge_response',
      version: 1,
      serverNonce: 'deadbeef'.repeat(4),
      proof: '0'.repeat(64),
    }),
    JSON.stringify({
      type: 'command_response',
      version: 1,
      payload: JSON.stringify({
        ok: false,
        error: { details: { reason: DAEMON_LIFECYCLE_REASONS.ownerEmbedded } },
      }),
      proof: '0'.repeat(64),
    }),
    JSON.stringify({
      type: 'command_response',
      version: 1,
      payload: JSON.stringify({ ok: true, data: { outcome: 'draining' } }),
      proof: '0'.repeat(64),
    }),
  ];
  let idx = 0;
  const openSockets = new Set<net.Socket>();
  const server = net.createServer((socket) => {
    openSockets.add(socket);
    socket.on('close', () => openSockets.delete(socket));
    socket.setEncoding('utf8');
    socket.on('data', (chunk) => {
      received.push(String(chunk));
      const spoofed = spoofedReplies[idx++];
      if (spoofed !== undefined) socket.end(`${spoofed}\n`);
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const controlPort = (server.address() as { port: number }).port;
  const paths = resolveDaemonPaths(mkdtempForTestSync('agent-device-rogue-ctrl-'));
  try {
    const controlSecret = 'super-secret-control';
    // Missing controlSecret must refuse even when the HTTP bearer token is present.
    expect(
      createGracefulDaemonShutdown(
        {
          pid: process.pid,
          processStartTime: 'Mon Nov 17 12:00:00 2026',
          ownership: 'detached',
          port: controlPort,
          controlPort,
          transport: 'socket',
          token: 'super-secret-token',
          capabilities: DAEMON_CAPABILITIES,
        },
        { paths, host: createTestHostEnvironment() },
        { timeoutMs: 80 },
      ),
    ).toBeUndefined();

    const graceful = createGracefulDaemonShutdown(
      {
        pid: process.pid,
        processStartTime: 'Mon Nov 17 12:00:00 2026',
        ownership: 'detached',
        port: controlPort,
        controlPort,
        controlSecret,
        transport: 'socket',
        token: 'super-secret-token',
        capabilities: DAEMON_CAPABILITIES,
      },
      { paths, host: createTestHostEnvironment() },
      { timeoutMs: 80 },
    );
    expect(graceful).toBeDefined();
    for (let attempt = 0; attempt <= spoofedReplies.length; attempt += 1) {
      expect(await graceful!({ pid: process.pid, startTime: 'Mon Nov 17 12:00:00 2026' })).toBe(
        'unavailable',
      );
    }
    const wire = received.join('');
    expect(wire).toContain('"type":"challenge"');
    expect(wire).not.toContain('super-secret-token');
    expect(wire).not.toContain(controlSecret);
    expect(wire).not.toContain(DAEMON_SHUTDOWN_COMMAND);
    expect(wire).not.toContain('"type":"command"');

    // Absent/corrupt daemon.json without a shutdown report must also stay 'unavailable'.
    for (const socket of openSockets) socket.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    expect(await graceful!({ pid: process.pid, startTime: 'Mon Nov 17 12:00:00 2026' })).toBe(
      'unavailable',
    );
    fs.writeFileSync(paths.infoPath, '{corrupt');
    expect(await graceful!({ pid: process.pid, startTime: 'Mon Nov 17 12:00:00 2026' })).toBe(
      'unavailable',
    );
  } finally {
    for (const socket of openSockets) socket.destroy();
    if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
    fs.rmSync(paths.baseDir, { recursive: true, force: true });
  }
});

test('requestDaemonLifecycle routes quiesce over controlPort, enforces auth, and rejects timeouts', async () => {
  const controlSecret = 'quiesce-control-secret';
  const fence = { pid: process.pid, processStartTime: 'Mon Nov 17 12:00:00 2026' } as const;
  const seenCommands: string[] = [];
  let mode: QuiesceMode = 'draining';
  const openSockets = new Set<net.Socket>();

  const server = net.createServer((socket) => {
    openSockets.add(socket);
    socket.on('close', () => openSockets.delete(socket));
    attachMockControlSession({
      socket,
      token: controlSecret,
      fence,
      getMode: () => mode,
      seenCommands,
    });
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const controlPort = (server.address() as { port: number }).port;
  const paths = resolveDaemonPaths(mkdtempForTestSync('agent-device-quiesce-ctrl-'));
  const target = { paths, host: createTestHostEnvironment() };
  const info = {
    pid: fence.pid,
    processStartTime: fence.processStartTime,
    controlPort,
    controlSecret,
    port: controlPort,
    token: 'http-bearer-token',
    transport: 'socket' as const,
  };

  try {
    expect(await requestDaemonLifecycle(info, target)).toEqual({ outcome: 'draining' });
    mode = 'mismatch';
    expect(await requestDaemonLifecycle(info, target)).toEqual({ outcome: 'gone' });
    mode = 'busy';
    await expect(requestDaemonLifecycle(info, target)).rejects.toMatchObject({
      details: { reason: DAEMON_LIFECYCLE_REASONS.busy },
    });
    mode = 'rogue';
    expect(await requestDaemonLifecycle(info, target)).toEqual({ outcome: 'gone' });

    for (const timeoutMode of ['hang-challenge', 'hang-command'] as const) {
      mode = timeoutMode;
      const caught = await requestDaemonLifecycle(info, target, { timeoutMs: 80 }).catch(
        (error) => error,
      );
      expect(caught).toBeInstanceOf(AppError);
      expect((caught as AppError).details?.reason).toBe('daemon_transport_timeout');
    }
    expect(seenCommands).toEqual([
      DAEMON_QUIESCE_COMMAND,
      DAEMON_QUIESCE_COMMAND,
      DAEMON_QUIESCE_COMMAND,
      DAEMON_QUIESCE_COMMAND,
    ]);
  } finally {
    for (const socket of openSockets) socket.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    fs.rmSync(paths.baseDir, { recursive: true, force: true });
  }
});
