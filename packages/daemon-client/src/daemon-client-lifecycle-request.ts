import {
  DAEMON_LIFECYCLE_REASONS,
  DAEMON_SHUTTING_DOWN_REASON,
  buildDaemonLifecycleRequest,
  buildDaemonShutdownRequest,
  readDaemonLifecycleCensus,
  type DaemonPaths,
} from '@agent-device/daemon-contracts/daemon-resolution';
import type { DaemonGracefulShutdown } from '@agent-device/daemon-contracts/daemon-process';
import type { DaemonWireRequest } from '@agent-device/contracts/command';
import type { HostEnvironment } from '@agent-device/kernel/source-value';
import { createRequestId } from '@agent-device/host-kit/diagnostics';
import { sleep } from '@agent-device/host-kit/retry';
import { AppError, throwDaemonError, type NormalizedError } from '@agent-device/kernel/errors';
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

function isTransportTimeoutFailure(error: unknown): boolean {
  return error instanceof AppError && error.details?.reason === 'daemon_transport_timeout';
}

type DaemonLifecycleOutcome = Readonly<{ outcome: 'draining' | 'gone' }>;

/** A daemon's refusal, as read from either transport's shape of it. */
type RefusedDaemonAnswer = Readonly<{ reason: unknown; details: unknown }>;

type ControlRequestSendResult =
  | Readonly<{ kind: 'ok'; data: unknown }>
  | Readonly<{ kind: 'gone' }>
  | Readonly<{ kind: 'timeout'; error: unknown }>
  | Readonly<{
      kind: 'refused';
      refusal: RefusedDaemonAnswer;
      normalizedError?: NormalizedError;
      caughtError?: unknown;
    }>;

async function sendControlRequest(
  info: DaemonInfo,
  request: DaemonWireRequest,
  target: DaemonLifecycleTarget,
  timeoutMs: number,
  readThrownRefusal: (error: unknown) => RefusedDaemonAnswer | undefined = readOwnerRefusal,
): Promise<ControlRequestSendResult> {
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
      return { kind: 'gone' };
    }
    if (isTransportTimeoutFailure(error)) return { kind: 'timeout', error };
    // HTTP carries RPC/ownership refusals as thrown AppErrors rather than response envelopes.
    const refusal = readThrownRefusal(error);
    if (refusal) return { kind: 'refused', refusal, caughtError: error };
    throw error;
  }
  if (response.ok) return { kind: 'ok', data: response.data };
  const details = response.error.details;
  return {
    kind: 'refused',
    refusal: { reason: details?.reason, details },
    normalizedError: response.error,
  };
}

/** Asks the fenced detached daemon to leave if idle, without timeout recovery. */
export async function requestDaemonLifecycle(
  info: DaemonInfo,
  target: DaemonLifecycleTarget,
): Promise<DaemonLifecycleOutcome> {
  const request = buildDaemonLifecycleRequest({
    token: info.token,
    fence: { pid: info.pid, processStartTime: info.processStartTime ?? null },
    requestId: createRequestId(),
  });
  const result = await sendControlRequest(info, request, target, LIFECYCLE_REQUEST_TIMEOUT_MS);
  if (result.kind === 'gone') return { outcome: 'gone' };
  if (result.kind === 'ok') return { outcome: 'draining' };
  if (result.kind === 'timeout') throw result.error;
  const judged = judgeRefusedAnswer(result.refusal, info, target);
  if (judged) return judged;
  if (result.normalizedError) throwDaemonError(result.normalizedError);
  throw result.caughtError;
}

/**
 * Asks a fenced detached daemon to shut down gracefully over IPC/HTTP without waiting for
 * idle-only admission. Returns `undefined` when `info` predates `shutdown`, has a non-detached
 * owner, or lacks an endpoint/token.
 */
export function createGracefulDaemonShutdown(
  info: DaemonInfo,
  target: DaemonLifecycleTarget,
  options?: { timeoutMs?: number },
): DaemonGracefulShutdown | undefined {
  if (info.ownership && info.ownership !== 'detached') return undefined;
  if (!info.capabilities?.includes('shutdown')) return undefined;
  if (!info.token || (info.port === undefined && info.httpPort === undefined)) return undefined;
  return async (identity) => {
    const fencedInfo =
      info.pid === identity.pid && info.processStartTime === identity.startTime
        ? info
        : { ...info, pid: identity.pid, processStartTime: identity.startTime ?? undefined };
    const timeoutMs = options?.timeoutMs ?? SHUTDOWN_REQUEST_TIMEOUT_MS;
    const deadline = Date.now() + SHUTDOWN_STARTUP_RETRY_BUDGET_MS;
    for (;;) {
      const request = buildDaemonShutdownRequest({
        token: fencedInfo.token,
        fence: { pid: fencedInfo.pid, processStartTime: fencedInfo.processStartTime ?? null },
        requestId: createRequestId(),
      });
      const result = await sendControlRequest(
        fencedInfo,
        request,
        target,
        timeoutMs,
        readAppErrorRefusal,
      );
      if (result.kind === 'ok') return isDrainingOutcome(result.data) ? 'accepted' : 'unavailable';
      if (result.kind === 'gone' || result.kind === 'timeout') return 'unavailable';
      const { reason, details } = result.refusal;
      if (reason === DAEMON_SHUTTING_DOWN_REASON) return 'accepted';
      if (reason === DAEMON_LIFECYCLE_REASONS.ownerEmbedded) return 'embedded-owner';
      if (reason === DAEMON_LIFECYCLE_REASONS.busy && isStartingUpCensus(details)) {
        const remaining = deadline - Date.now();
        if (remaining > 0) {
          await sleep(Math.min(SHUTDOWN_STARTUP_RETRY_DELAY_MS, remaining));
          continue;
        }
      }
      return 'unavailable';
    }
  };
}

function isDrainingOutcome(data: unknown): boolean {
  return (
    typeof data === 'object' &&
    data !== null &&
    (data as Record<string, unknown>).outcome === 'draining'
  );
}

function isStartingUpCensus(details: unknown): boolean {
  return readDaemonLifecycleCensus(details)?.startingUp === true;
}

function readAppErrorRefusal(error: unknown): RefusedDaemonAnswer | undefined {
  if (!(error instanceof AppError)) return undefined;
  return { reason: error.details?.reason, details: error.details };
}

function readOwnerRefusal(error: unknown): RefusedDaemonAnswer | undefined {
  const refusal = readAppErrorRefusal(error);
  return refusal?.reason === DAEMON_LIFECYCLE_REASONS.ownerEmbedded ? refusal : undefined;
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
