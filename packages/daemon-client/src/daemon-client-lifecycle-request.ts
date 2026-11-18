import net from 'node:net';
import {
  DAEMON_CONTROL_MAX_LINE_BYTES,
  DAEMON_CONTROL_PROTOCOL_VERSION,
  DAEMON_QUIESCE_COMMAND,
  DAEMON_SHUTDOWN_COMMAND,
  createDaemonControlNonce,
  parseControlChallengeResponse,
  parseControlCommandResponse,
  signDaemonControlCommand,
  verifyDaemonControlChallenge,
  verifyDaemonControlReply,
  type DaemonControlCommand,
  type DaemonControlCommandRequest,
} from '@agent-device/daemon-contracts/daemon-control';
import { readRegisteredDaemonOwnership } from '@agent-device/daemon-contracts/daemon-registration';
import {
  DAEMON_LIFECYCLE_REASONS,
  DAEMON_SHUTTING_DOWN_REASON,
  buildDaemonLifecycleRequest,
  readDaemonLifecycleCensus,
  type DaemonLifecycleFence,
  type DaemonPaths,
} from '@agent-device/daemon-contracts/daemon-resolution';
import type {
  DaemonGracefulShutdown,
  DaemonProcessIdentity,
} from '@agent-device/daemon-contracts/daemon-process';
import { readDaemonShutdownReport } from '@agent-device/daemon-contracts/daemon-shutdown-report';
import { createRequestId } from '@agent-device/host-kit/diagnostics';
import { sleep } from '@agent-device/host-kit/retry';
import { consumeTextLines } from '@agent-device/host-kit/transport';
import {
  AppError,
  throwDaemonError,
  toAppErrorCode,
  type NormalizedError,
} from '@agent-device/kernel/errors';
import { isRecord } from '@agent-device/kernel/record';
import type { HostEnvironment } from '@agent-device/kernel/source-value';
import type { DaemonInfo } from './daemon-client-metadata.ts';
import { isDaemonTransportUnavailableError, sendRequest } from './daemon-client-transport.ts';
import { daemonBusyError, daemonNotRetirableError } from './daemon-lifecycle-errors.ts';

const LIFECYCLE_REQUEST_TIMEOUT_MS = 10_000;
const SHUTDOWN_REQUEST_TIMEOUT_MS = 2_000;
const SHUTDOWN_STARTUP_RETRY_BUDGET_MS = 500;
const SHUTDOWN_STARTUP_RETRY_DELAY_MS = 50;

/** Maximum wait for an idle detached daemon to finish closing during replacement. */
export const DAEMON_LEAVE_WAIT_MS = 60_000;

/** What a lifecycle request needs besides the daemon it addresses: where and how to reach it. */
export type DaemonLifecycleTarget = Readonly<{
  paths: DaemonPaths;
  host: HostEnvironment;
}>;

// A refused or reset connection: the daemon left, is leaving, or was never there. Nothing was
// accepted, so judging again is safe.
function isConnectionFailure(error: unknown): boolean {
  return error instanceof AppError && error.details?.reason === 'daemon_transport_failure';
}

type DaemonLifecycleOutcome = Readonly<{ outcome: 'draining' | 'gone' }>;

/** A daemon's refusal, as read from either transport's shape of it. */
type RefusedDaemonAnswer = Readonly<{ reason: unknown; details: unknown }>;

type AuthenticatedControlResult =
  | Readonly<{ kind: 'ok'; data: unknown }>
  | Readonly<{ kind: 'gone' }>
  | Readonly<{ kind: 'unauthenticated' }>
  | Readonly<{ kind: 'timeout'; commandSent: boolean }>
  | Readonly<{
      kind: 'post-dispatch-uncertain';
      reason: 'transport' | 'malformed';
    }>
  | Readonly<{
      kind: 'refused';
      refusal: RefusedDaemonAnswer;
      normalizedError: NormalizedError;
    }>;

/** Asks the fenced detached daemon to leave if idle, without timeout recovery. */
export async function requestDaemonLifecycle(
  info: DaemonInfo,
  target: DaemonLifecycleTarget,
  options?: { timeoutMs?: number },
): Promise<DaemonLifecycleOutcome> {
  const timeoutMs = options?.timeoutMs ?? LIFECYCLE_REQUEST_TIMEOUT_MS;
  if (
    typeof info.controlPort === 'number' &&
    typeof info.controlSecret === 'string' &&
    info.controlSecret.length > 0
  ) {
    const result = await sendAuthenticatedControlCommand(info, DAEMON_QUIESCE_COMMAND, timeoutMs);
    return handleAuthenticatedQuiesceResult(result, info, target);
  }
  const request = buildDaemonLifecycleRequest({
    token: info.token,
    fence: { pid: info.pid, processStartTime: info.processStartTime ?? null },
    requestId: createRequestId(),
  });
  let response;
  try {
    response = await sendRequest(
      info,
      request,
      // Whichever transport the daemon serves: a client that cannot reach it on its own preference
      // is exactly the one that has to ask it to leave.
      'auto',
      target.paths,
      timeoutMs,
      { host: target.host, timeoutRecovery: 'none' },
    );
  } catch (error) {
    if (isDaemonTransportUnavailableError(error) || isConnectionFailure(error)) {
      return { outcome: 'gone' };
    }
    // HTTP carries the ownership refusal as an error rather than a response.
    const refusal = readOwnerRefusal(error);
    const judged = refusal && judgeRefusedAnswer(refusal, info, target);
    if (judged) return judged;
    throw error;
  }
  if (response.ok) return { outcome: 'draining' };
  const details = response.error.details;
  const judged = judgeRefusedAnswer({ reason: details?.reason, details }, info, target);
  if (judged) return judged;
  throwDaemonError(response.error);
}

function handleAuthenticatedQuiesceResult(
  result: AuthenticatedControlResult,
  info: DaemonInfo,
  target: DaemonLifecycleTarget,
): DaemonLifecycleOutcome {
  if (result.kind === 'ok') return { outcome: 'draining' };
  if (result.kind === 'gone' || result.kind === 'unauthenticated') return { outcome: 'gone' };
  if (result.kind === 'timeout') {
    throw new AppError('COMMAND_FAILED', 'Daemon lifecycle request timed out', {
      reason: 'daemon_transport_timeout',
    });
  }
  if (result.kind === 'post-dispatch-uncertain') {
    if (result.reason === 'malformed') {
      throw new AppError('COMMAND_FAILED', 'Invalid daemon response', {
        reason: 'daemon_response_invalid',
      });
    }
    return { outcome: 'gone' };
  }
  const judged = judgeRefusedAnswer(result.refusal, info, target);
  if (judged) return judged;
  throwDaemonError(result.normalizedError);
}

/**
 * Asks a fenced detached daemon to shut down gracefully over the authenticated local control
 * channel without waiting for idle-only admission. Returns `undefined` when `info` predates
 * `shutdown`, has a non-detached owner, or lacks a local control port/secret.
 */
export function createGracefulDaemonShutdown(
  info: DaemonInfo | null | undefined,
  target: Readonly<{ paths: DaemonPaths; host?: HostEnvironment }>,
  options?: { timeoutMs?: number },
): DaemonGracefulShutdown | undefined {
  if (!info || (info.ownership && info.ownership !== 'detached')) return undefined;
  if (!info.capabilities?.includes('shutdown')) return undefined;
  if (!info.controlSecret || typeof info.controlPort !== 'number') return undefined;
  const lifecycleTarget: Readonly<{ paths: DaemonPaths }> = { paths: target.paths };
  const timeoutMs = options?.timeoutMs ?? SHUTDOWN_REQUEST_TIMEOUT_MS;
  return async (identity) => {
    const fencedInfo = fenceDaemonInfo(info, identity);
    const deadline = Date.now() + SHUTDOWN_STARTUP_RETRY_BUDGET_MS;
    for (;;) {
      const outcome = await requestDaemonShutdownAttempt(fencedInfo, lifecycleTarget, timeoutMs);
      if (outcome !== 'retry-startup') return outcome;
      const remaining = deadline - Date.now();
      if (remaining <= 0) return 'unavailable';
      await sleep(Math.min(SHUTDOWN_STARTUP_RETRY_DELAY_MS, remaining));
    }
  };
}

type ShutdownAttemptOutcome = 'accepted' | 'embedded-owner' | 'unavailable' | 'retry-startup';

function fenceDaemonInfo(info: DaemonInfo, identity: DaemonProcessIdentity): DaemonInfo {
  if (info.pid === identity.pid && info.processStartTime === identity.startTime) return info;
  return { ...info, pid: identity.pid, processStartTime: identity.startTime ?? undefined };
}

async function requestDaemonShutdownAttempt(
  fencedInfo: DaemonInfo,
  target: Readonly<{ paths: DaemonPaths }>,
  timeoutMs: number,
): Promise<ShutdownAttemptOutcome> {
  const result = await sendAuthenticatedControlCommand(
    fencedInfo,
    DAEMON_SHUTDOWN_COMMAND,
    timeoutMs,
  );
  if (result.kind === 'ok') return isDrainingOutcome(result.data) ? 'accepted' : 'unavailable';
  // Once the daemon proved its identity and received the command, a reset, truncated reply, or
  // read timeout can mean it began draining; wait the drain grace window before escalating.
  if (result.kind === 'timeout') return result.commandSent ? 'accepted' : 'unavailable';
  if (result.kind === 'post-dispatch-uncertain') return 'accepted';
  if (result.kind === 'unauthenticated') return 'unavailable';
  if (result.kind === 'gone') return judgeGoneShutdown(fencedInfo, target);
  const { reason, details } = result.refusal;
  if (reason === DAEMON_SHUTTING_DOWN_REASON) return 'accepted';
  if (reason === DAEMON_LIFECYCLE_REASONS.ownerEmbedded) return 'embedded-owner';
  if (reason === DAEMON_LIFECYCLE_REASONS.busy && isStartingUpCensus(details)) {
    return 'retry-startup';
  }
  return 'unavailable';
}

function judgeGoneShutdown(
  fencedInfo: DaemonInfo,
  target: Readonly<{ paths: DaemonPaths }>,
): ShutdownAttemptOutcome {
  const ownership = readRegisteredDaemonOwnership(target.paths.infoPath, {
    pid: fencedInfo.pid,
    startTime: fencedInfo.processStartTime ?? null,
  });
  // `controlServer` stays open until after `finishDaemonRegistration` retires `daemon.json` and
  // writes `daemon-shutdown.json`. Only a retired registration paired with that report proves a
  // completed drain; if `daemon.json` still matches or disappeared without a report, do not stall.
  if (ownership.state === 'absent' && readDaemonShutdownReport(target.paths.baseDir) !== null) {
    return 'accepted';
  }
  return 'unavailable';
}

async function sendAuthenticatedControlCommand(
  info: DaemonInfo,
  command: DaemonControlCommand,
  timeoutMs: number,
): Promise<AuthenticatedControlResult> {
  const controlPort = info.controlPort;
  if (typeof controlPort !== 'number') return { kind: 'gone' };
  const controlSecret = info.controlSecret;
  if (!controlSecret) return { kind: 'unauthenticated' };
  const fence: DaemonLifecycleFence = {
    pid: info.pid,
    processStartTime: info.processStartTime ?? null,
  };
  const clientNonce = createDaemonControlNonce();
  const requestId = createRequestId();

  return await new Promise<AuthenticatedControlResult>((resolve) => {
    const socket = net.createConnection({ host: '127.0.0.1', port: controlPort });
    let settled = false;
    let connected = false;
    let commandSent = false;
    let serverNonce: string | null = null;
    let buffer = '';

    const finish = (result: AuthenticatedControlResult): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timeoutHandle);
      socket.destroy();
      resolve(result);
    };

    const timeoutHandle = setTimeout(() => {
      finish({ kind: 'timeout', commandSent });
    }, timeoutMs);

    const failureBeforeSettle = (reason: 'transport' | 'malformed'): AuthenticatedControlResult => {
      if (commandSent) return { kind: 'post-dispatch-uncertain', reason };
      return connected ? { kind: 'unauthenticated' } : { kind: 'gone' };
    };

    socket.setEncoding('utf8');
    socket.once('connect', () => {
      connected = true;
      if (settled || socket.destroyed) return;
      socket.write(
        `${JSON.stringify({
          type: 'challenge',
          version: DAEMON_CONTROL_PROTOCOL_VERSION,
          nonce: clientNonce,
          requestId,
        })}\n`,
      );
    });

    const handleLine = (line: string): void => {
      if (serverNonce === null) {
        const challenge = parseControlChallengeResponse(line);
        if (
          !challenge ||
          !verifyDaemonControlChallenge({
            token: controlSecret,
            fence,
            clientNonce,
            serverNonce: challenge.serverNonce,
            proof: challenge.proof,
          })
        ) {
          finish({ kind: 'unauthenticated' });
          return;
        }
        if (settled || socket.destroyed) {
          finish({ kind: 'unauthenticated' });
          return;
        }
        serverNonce = challenge.serverNonce;
        const frame: DaemonControlCommandRequest = {
          type: 'command',
          version: DAEMON_CONTROL_PROTOCOL_VERSION,
          command,
          clientNonce,
          serverNonce,
          pid: fence.pid,
          processStartTime: fence.processStartTime,
          requestId,
          proof: signDaemonControlCommand({
            token: controlSecret,
            command,
            fence,
            clientNonce,
            serverNonce,
            requestId,
          }),
        };
        commandSent = true;
        socket.write(`${JSON.stringify(frame)}\n`);
        return;
      }
      const reply = parseControlCommandResponse(line);
      if (
        !reply ||
        !verifyDaemonControlReply({
          token: controlSecret,
          fence,
          clientNonce,
          serverNonce,
          requestId,
          payload: reply.payload,
          proof: reply.proof,
        })
      ) {
        finish({ kind: 'post-dispatch-uncertain', reason: 'malformed' });
        return;
      }
      finish(parseControlPayloadResult(reply.payload));
    };

    socket.on('data', (chunk: string) => {
      if (settled) return;
      if (
        Buffer.byteLength(buffer, 'utf8') + Buffer.byteLength(chunk, 'utf8') >
        DAEMON_CONTROL_MAX_LINE_BYTES
      ) {
        finish(failureBeforeSettle('malformed'));
        return;
      }
      const next = consumeTextLines(buffer, chunk);
      buffer = next.buffer;
      for (const line of next.lines) {
        if (settled) return;
        handleLine(line);
      }
    });

    socket.on('end', () => {
      if (settled) return;
      finish(failureBeforeSettle(buffer.trim() ? 'malformed' : 'transport'));
    });

    socket.on('error', () => {
      if (settled) return;
      finish(failureBeforeSettle('transport'));
    });

    socket.on('close', () => {
      if (settled) return;
      finish(failureBeforeSettle('transport'));
    });
  });
}

function parseControlPayloadResult(payload: string): AuthenticatedControlResult {
  let parsed: unknown;
  try {
    parsed = JSON.parse(payload);
  } catch {
    return { kind: 'post-dispatch-uncertain', reason: 'malformed' };
  }
  if (!isRecord(parsed) || typeof parsed.ok !== 'boolean') {
    return { kind: 'post-dispatch-uncertain', reason: 'malformed' };
  }
  if (parsed.ok) return { kind: 'ok', data: parsed.data };
  if (!isRecord(parsed.error)) {
    return { kind: 'post-dispatch-uncertain', reason: 'malformed' };
  }
  const details = isRecord(parsed.error.details) ? parsed.error.details : undefined;
  const normalizedError: NormalizedError = {
    code: toAppErrorCode(
      typeof parsed.error.code === 'string' ? parsed.error.code : undefined,
      'COMMAND_FAILED',
    ),
    message:
      typeof parsed.error.message === 'string'
        ? parsed.error.message
        : 'Daemon control request failed.',
    ...(details ? { details } : {}),
  };
  return {
    kind: 'refused',
    refusal: { reason: details?.reason, details },
    normalizedError,
  };
}

function isDrainingOutcome(data: unknown): boolean {
  return isRecord(data) && data.outcome === 'draining';
}

function isStartingUpCensus(details: unknown): boolean {
  return readDaemonLifecycleCensus(details)?.startingUp === true;
}

function readOwnerRefusal(error: unknown): RefusedDaemonAnswer | undefined {
  if (!(error instanceof AppError)) return undefined;
  const reason = error.details?.reason;
  return reason === DAEMON_LIFECYCLE_REASONS.ownerEmbedded
    ? { reason, details: error.details }
    : undefined;
}

function judgeRefusedAnswer(
  refusal: RefusedDaemonAnswer,
  info: DaemonInfo,
  target: DaemonLifecycleTarget,
): DaemonLifecycleOutcome | undefined {
  const stateDir = target.paths.baseDir;
  if (refusal.reason === DAEMON_LIFECYCLE_REASONS.busy) {
    throw daemonBusyError(info, stateDir, readDaemonLifecycleCensus(refusal.details));
  }
  if (refusal.reason === DAEMON_LIFECYCLE_REASONS.ownerEmbedded) {
    throw daemonNotRetirableError({ info, stateDir });
  }
  if (
    refusal.reason === DAEMON_LIFECYCLE_REASONS.instanceMismatch ||
    refusal.reason === DAEMON_SHUTTING_DOWN_REASON
  ) {
    return { outcome: 'gone' };
  }
}
