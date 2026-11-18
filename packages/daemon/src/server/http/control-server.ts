import { AsyncResource } from 'node:async_hooks';
import type net from 'node:net';
import {
  DAEMON_CONTROL_MAX_LINE_BYTES,
  DAEMON_CONTROL_PROTOCOL_VERSION,
  createDaemonControlNonce,
  parseControlChallengeRequest,
  parseControlCommandRequest,
  signDaemonControlChallenge,
  signDaemonControlReply,
  verifyDaemonControlCommand,
  type DaemonControlChallengeResponse,
  type DaemonControlCommand,
  type DaemonControlCommandResponse,
} from '@agent-device/daemon-contracts/daemon-control';
import type { DaemonLifecycleFence } from '@agent-device/daemon-contracts/daemon-resolution';
import { consumeTextLines } from '@agent-device/host-kit/transport';
import type { DaemonResponse } from '@agent-device/kernel/contracts';
import { createTrackedNetServer, type DaemonServer } from './transport.ts';

const CONTROL_SOCKET_IDLE_TIMEOUT_MS = 10_000;

export type DaemonControlServerOptions = Readonly<{
  token: string;
  identity: DaemonLifecycleFence;
  handleControl: (command: DaemonControlCommand, fence: DaemonLifecycleFence) => DaemonResponse;
}>;

type PendingChallenge = Readonly<{
  clientNonce: string;
  serverNonce: string;
  requestId: string;
}>;

type ControlSocketPhase = 'await-challenge' | 'await-command' | 'done';

/** Opens the dedicated loopback control listener bound to this daemon instance. */
export function createDaemonControlServer(options: DaemonControlServerOptions): DaemonServer {
  return createTrackedNetServer((socket) => {
    attachControlSocketHandler(socket, options);
  });
}

function attachControlSocketHandler(socket: net.Socket, options: DaemonControlServerOptions): void {
  let buffer = '';
  let pendingChallenge: PendingChallenge | null = null;
  let phase: ControlSocketPhase = 'await-challenge';

  socket.setEncoding('utf8');
  socket.setTimeout(CONTROL_SOCKET_IDLE_TIMEOUT_MS, () => {
    socket.destroy();
  });
  socket.on('error', () => {
    socket.destroy();
  });

  const processLine = (line: string): boolean => {
    if (phase === 'await-challenge') {
      const challenge = issueChallengeResponse(line, options);
      if (!challenge) {
        phase = 'done';
        socket.destroy();
        return true;
      }
      pendingChallenge = challenge.pending;
      phase = 'await-command';
      if (!socket.destroyed) socket.write(challenge.replyLine);
      return false;
    }
    if (phase !== 'await-command' || !pendingChallenge) {
      phase = 'done';
      socket.destroy();
      return true;
    }
    const pending = pendingChallenge;
    pendingChallenge = null;
    phase = 'done';
    const replyLine = executeCommandLine(line, pending, options);
    if (!replyLine) {
      socket.destroy();
      return true;
    }
    if (!socket.destroyed) socket.end(replyLine);
    return true;
  };

  socket.on(
    'data',
    AsyncResource.bind((chunk: string) => {
      if (phase === 'done' || socket.destroyed) return;
      if (
        Buffer.byteLength(buffer, 'utf8') + Buffer.byteLength(chunk, 'utf8') >
        DAEMON_CONTROL_MAX_LINE_BYTES
      ) {
        phase = 'done';
        socket.destroy();
        return;
      }
      const next = consumeTextLines(buffer, chunk);
      buffer = next.buffer;
      for (const line of next.lines) {
        if (processLine(line)) return;
      }
    }),
  );
}

function issueChallengeResponse(
  line: string,
  options: DaemonControlServerOptions,
): { pending: PendingChallenge; replyLine: string } | null {
  const challenge = parseControlChallengeRequest(line);
  if (!challenge) return null;
  const serverNonce = createDaemonControlNonce();
  const proof = signDaemonControlChallenge({
    token: options.token,
    fence: options.identity,
    clientNonce: challenge.nonce,
    serverNonce,
  });
  const reply: DaemonControlChallengeResponse = {
    type: 'challenge_response',
    version: DAEMON_CONTROL_PROTOCOL_VERSION,
    serverNonce,
    proof,
  };
  return {
    pending: {
      clientNonce: challenge.nonce,
      serverNonce,
      requestId: challenge.requestId,
    },
    replyLine: `${JSON.stringify(reply)}\n`,
  };
}

function executeCommandLine(
  line: string,
  pending: PendingChallenge,
  options: DaemonControlServerOptions,
): string | null {
  const parsed = parseControlCommandRequest(line);
  if (
    !parsed ||
    parsed.clientNonce !== pending.clientNonce ||
    parsed.serverNonce !== pending.serverNonce ||
    parsed.requestId !== pending.requestId
  ) {
    return null;
  }
  const fence: DaemonLifecycleFence = {
    pid: parsed.pid,
    processStartTime: parsed.processStartTime,
  };
  if (
    !verifyDaemonControlCommand({
      token: options.token,
      command: parsed.command,
      fence,
      clientNonce: pending.clientNonce,
      serverNonce: pending.serverNonce,
      requestId: pending.requestId,
      proof: parsed.proof,
    })
  ) {
    return null;
  }
  let response: DaemonResponse;
  try {
    response = options.handleControl(parsed.command, fence);
  } catch {
    return null;
  }
  const payload = JSON.stringify(response);
  const proof = signDaemonControlReply({
    token: options.token,
    fence,
    clientNonce: pending.clientNonce,
    serverNonce: pending.serverNonce,
    requestId: pending.requestId,
    payload,
  });
  const reply: DaemonControlCommandResponse = {
    type: 'command_response',
    version: DAEMON_CONTROL_PROTOCOL_VERSION,
    payload,
    proof,
  };
  return `${JSON.stringify(reply)}\n`;
}
