import assert from 'node:assert/strict';
import net from 'node:net';
import { afterEach, test, vi } from 'vitest';
import {
  DAEMON_CONTROL_MAX_LINE_BYTES,
  DAEMON_CONTROL_PROTOCOL_VERSION,
  DAEMON_SHUTDOWN_COMMAND,
  createDaemonControlNonce,
  parseControlChallengeResponse,
  parseControlCommandResponse,
  signDaemonControlCommand,
  verifyDaemonControlChallenge,
  verifyDaemonControlReply,
} from '@agent-device/daemon-contracts/daemon-control';
import { consumeTextLines } from '@agent-device/host-kit/transport';
import { closeLoopbackServer, listenOnLoopback, type LoopbackServer } from '@agent-device/test-kit';
import { createDaemonControlServer } from './control-server.ts';

const openServers: LoopbackServer[] = [];

afterEach(async () => {
  await Promise.all(openServers.splice(0).map((server) => closeLoopbackServer(server)));
});

const TOKEN = 'control-secret-token';
const IDENTITY = { pid: process.pid, processStartTime: 'Mon Nov 17 12:00:00 2026' } as const;

async function startControlServer(
  handleControl: Parameters<typeof createDaemonControlServer>[0]['handleControl'],
): Promise<number> {
  const server = createDaemonControlServer({
    token: TOKEN,
    identity: IDENTITY,
    handleControl,
  });
  openServers.push(server);
  return await listenOnLoopback(server);
}

test('control server completes 2-step authenticated handshake and signs reply', async () => {
  const handleControl = vi.fn(() => ({ ok: true as const, data: { outcome: 'draining' } }));
  const port = await startControlServer(handleControl);
  const clientNonce = createDaemonControlNonce();
  const requestId = 'ctrl-1';

  const result = await new Promise<{ payload: string; validProof: boolean }>((resolve, reject) => {
    const socket = net.createConnection({ host: '127.0.0.1', port });
    let buffer = '';
    let serverNonce = '';
    socket.setEncoding('utf8');
    socket.on('error', reject);
    socket.on('connect', () => {
      socket.write(
        `${JSON.stringify({
          type: 'challenge',
          version: DAEMON_CONTROL_PROTOCOL_VERSION,
          nonce: clientNonce,
          requestId,
        })}\n`,
      );
    });
    socket.on('data', (chunk: string) => {
      const parsed = consumeTextLines(buffer, chunk);
      buffer = parsed.buffer;
      for (const line of parsed.lines) {
        if (!serverNonce) {
          const challenge = parseControlChallengeResponse(line);
          assert.ok(challenge);
          assert.equal(
            verifyDaemonControlChallenge({
              token: TOKEN,
              fence: IDENTITY,
              clientNonce,
              serverNonce: challenge.serverNonce,
              proof: challenge.proof,
            }),
            true,
          );
          serverNonce = challenge.serverNonce;
          socket.write(
            `${JSON.stringify({
              type: 'command',
              version: DAEMON_CONTROL_PROTOCOL_VERSION,
              command: DAEMON_SHUTDOWN_COMMAND,
              clientNonce,
              serverNonce,
              pid: IDENTITY.pid,
              processStartTime: IDENTITY.processStartTime,
              requestId,
              proof: signDaemonControlCommand({
                token: TOKEN,
                command: DAEMON_SHUTDOWN_COMMAND,
                fence: IDENTITY,
                clientNonce,
                serverNonce,
                requestId,
              }),
            })}\n`,
          );
          continue;
        }
        const reply = parseControlCommandResponse(line);
        assert.ok(reply);
        const validProof = verifyDaemonControlReply({
          token: TOKEN,
          fence: IDENTITY,
          clientNonce,
          serverNonce,
          requestId,
          payload: reply.payload,
          proof: reply.proof,
        });
        socket.end();
        resolve({ payload: reply.payload, validProof });
      }
    });
  });

  assert.equal(result.validProof, true);
  assert.deepEqual(JSON.parse(result.payload), { ok: true, data: { outcome: 'draining' } });
  assert.deepEqual(handleControl.mock.calls, [[DAEMON_SHUTDOWN_COMMAND, IDENTITY]]);
});

test('control server drops unauthenticated, wrong-proof, replay, and oversized frames without calling handler', async () => {
  const handleControl = vi.fn(() => ({ ok: true as const, data: { outcome: 'draining' } }));
  const port = await startControlServer(handleControl);

  const expectClosedWithoutData = async (lines: readonly string[]) =>
    await new Promise<void>((resolve, reject) => {
      const socket = net.createConnection({ host: '127.0.0.1', port });
      let receivedAfterLast = '';
      socket.on('error', () => resolve());
      socket.on('close', () => resolve());
      socket.on('connect', () => {
        for (const line of lines) socket.write(line);
      });
      socket.on('data', (chunk) => {
        receivedAfterLast += String(chunk);
        if (receivedAfterLast.length > DAEMON_CONTROL_MAX_LINE_BYTES) {
          reject(new Error('unexpected data'));
        }
      });
    });

  await expectClosedWithoutData(['{"type":"challenge","version":999}\n']);
  await expectClosedWithoutData([`${'x'.repeat(DAEMON_CONTROL_MAX_LINE_BYTES + 8)}\n`]);

  // Send a wrong-token command after valid challenge: handler must not run and socket must close.
  const wrongProofResult = await new Promise<string>((resolve, reject) => {
    const socket = net.createConnection({ host: '127.0.0.1', port });
    const clientNonce = createDaemonControlNonce();
    let buffer = '';
    let sentCommand = false;
    socket.setEncoding('utf8');
    socket.on('error', () => {
      if (sentCommand) resolve('closed');
      else reject(new Error('closed too early'));
    });
    socket.on('close', () => {
      if (sentCommand) resolve('closed');
      else reject(new Error('closed without challenge'));
    });
    socket.on('connect', () => {
      socket.write(
        `${JSON.stringify({
          type: 'challenge',
          version: DAEMON_CONTROL_PROTOCOL_VERSION,
          nonce: clientNonce,
          requestId: 'bad-proof',
        })}\n`,
      );
    });
    socket.on('data', (chunk: string) => {
      const parsed = consumeTextLines(buffer, chunk);
      buffer = parsed.buffer;
      for (const line of parsed.lines) {
        if (!sentCommand) {
          const challenge = parseControlChallengeResponse(line);
          assert.ok(challenge);
          sentCommand = true;
          socket.write(
            `${JSON.stringify({
              type: 'command',
              version: DAEMON_CONTROL_PROTOCOL_VERSION,
              command: DAEMON_SHUTDOWN_COMMAND,
              clientNonce,
              serverNonce: challenge.serverNonce,
              pid: IDENTITY.pid,
              processStartTime: IDENTITY.processStartTime,
              requestId: 'bad-proof',
              proof: signDaemonControlCommand({
                token: 'wrong-token',
                command: DAEMON_SHUTDOWN_COMMAND,
                fence: IDENTITY,
                clientNonce,
                serverNonce: challenge.serverNonce,
                requestId: 'bad-proof',
              }),
            })}\n`,
          );
        } else {
          reject(new Error('unexpected command response'));
        }
      }
    });
  });

  assert.equal(wrongProofResult, 'closed');
  assert.equal(handleControl.mock.calls.length, 0);
});
