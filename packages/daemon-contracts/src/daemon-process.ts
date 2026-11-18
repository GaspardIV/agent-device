import {
  isProcessAlive,
  processStartTimesDiffer,
  isProcessPid,
  readHostProcessIdentityObservations,
  readProcessCommand,
  readProcessStartTime,
  type OwnerIdentity,
} from '@agent-device/host-kit/process';
import { sleep } from '@agent-device/host-kit/retry';

const DAEMON_COMMAND_PATTERNS = [
  /\/dist\/src\/daemon\.js($|[\s"'])/,
  /\/dist\/src\/internal\/daemon\.js($|[\s"'])/,
  /\/src\/daemon\.ts($|[\s"'])/,
];

/**
 * Identity is the daemon entry path, never the checkout's directory name: a
 * git worktree is named after its branch, so a `agent-device` substring gate
 * classified every worktree daemon as "not ours" (#1545). The pid always
 * comes from our own daemon.json/daemon.lock alongside the processStartTime
 * recorded with it. Missing identity must fail closed so a stale PID cannot
 * be authorized by the path match alone.
 */
export function isAgentDeviceDaemonCommand(command: string): boolean {
  const normalized = command.toLowerCase().replaceAll('\\', '/');
  return DAEMON_COMMAND_PATTERNS.some((pattern) => pattern.test(normalized));
}

export function isAgentDeviceDaemonProcess(
  pid: number,
  expectedStartTime: string | undefined,
): boolean {
  if (!expectedStartTime) return false;
  if (!isProcessAlive(pid)) return false;
  const actualStartTime = readProcessStartTime(pid);
  if (!actualStartTime || actualStartTime !== expectedStartTime) return false;
  const command = readProcessCommand(pid);
  if (!command) return false;
  return isAgentDeviceDaemonCommand(command);
}

function trySignalProcess(pid: number, signal: NodeJS.Signals): boolean {
  try {
    process.kill(pid, signal);
    return true;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === 'ESRCH' || code === 'EPERM') return false;
    throw error;
  }
}

/** A daemon pinned to one process lifetime, never a bare pid. */
export type DaemonProcessIdentity = Readonly<{
  pid: number;
  startTime: string;
}>;

export type DaemonExitWait = {
  /** The process terminated, or the host handed its pid to a different process. */
  exited: boolean;
  elapsedMs: number;
};

const DAEMON_EXIT_POLL_MS = 100;

/**
 * Confirms termination from a released pid, a verified zombie, or a different
 * readable process lifetime. An unreadable process observation is not proof.
 */
export async function waitForDaemonExit(
  identity: DaemonProcessIdentity,
  options: { timeoutMs: number; pollMs?: number },
): Promise<DaemonExitWait> {
  if (!isProcessPid(identity.pid)) return { exited: false, elapsedMs: 0 };
  const startedAt = Date.now();
  const deadline = startedAt + options.timeoutMs;
  const pollMs = options.pollMs ?? DAEMON_EXIT_POLL_MS;
  const hasExited = (): boolean => {
    if (!isProcessAlive(identity.pid)) return true;
    const observed = readHostProcessIdentityObservations([identity.pid]).get(identity.pid);
    return (
      Boolean(
        observed?.state.startsWith('Z') ||
        processStartTimesDiffer(observed?.startTime, identity.startTime),
      ) || !isProcessAlive(identity.pid)
    );
  };
  let exited = hasExited();
  while (!exited && Date.now() < deadline) {
    await sleep(Math.min(pollMs, Math.max(0, deadline - Date.now())));
    exited = hasExited();
  }
  return { exited, elapsedMs: Date.now() - startedAt };
}

export type DaemonTerminationResult =
  /** A released bare PID, without lifetime proof or metadata cleanup authority. */
  | Readonly<{ status: 'not-running' }>
  | Readonly<{
      status: 'exited';
      identity: DaemonProcessIdentity;
      mode: 'already-exited' | 'graceful' | 'forced';
    }>
  | Readonly<{
      status: 'retained';
      reason:
        | 'missing-start-time'
        | 'identity-unverified'
        /**
         * A host owns this runtime and ends it. Callers that judge a registration answer this instead of
         * calling {@link stopDaemonProcess} at all (`stopAndRetireDaemon`); this function signals
         * whatever verified identity it is handed and never decides ownership itself.
         */
        | 'embedded-owner'
        /**
         * The record states an owner this build cannot read, so nothing proves this pid is a daemon of
         * its own, and signalling it is refused. The other half of the same rule as `embedded-owner`.
         */
        | 'owner-unproven'
        | 'signal-failed'
        | 'exit-timeout';
      signal?: NodeJS.Signals;
    }>;

export type DaemonGracefulShutdownOutcome = 'accepted' | 'unavailable' | 'embedded-owner';

/**
 * In-band graceful shutdown hook tried before OS process signals. Needed on platforms such as
 * Windows where cross-process `SIGTERM` terminates immediately without invoking signal handlers.
 */
export type DaemonGracefulShutdown = (
  identity: DaemonProcessIdentity,
) => Promise<DaemonGracefulShutdownOutcome>;

function canSignalGracefully(): boolean {
  // Win32 TerminateProcess handles cross-process SIGTERM immediately without invoking Node handlers.
  return process.platform !== 'win32';
}

/** Owns every signal and exit wait for one observed daemon lifetime. */
export async function stopDaemonProcess(
  observed: Readonly<OwnerIdentity>,
  options: {
    mode: 'graceful' | 'force';
    termTimeoutMs: number;
    killTimeoutMs: number;
    gracefulShutdown?: DaemonGracefulShutdown;
  },
): Promise<DaemonTerminationResult> {
  if (!isProcessPid(observed.pid)) return { status: 'retained', reason: 'identity-unverified' };
  if (!observed.startTime?.trim()) {
    if (!isProcessAlive(observed.pid)) return { status: 'not-running' };
    return { status: 'retained', reason: 'missing-start-time' };
  }
  const identity: DaemonProcessIdentity = { pid: observed.pid, startTime: observed.startTime };
  if ((await waitForDaemonExit(identity, { timeoutMs: 0 })).exited) {
    return { status: 'exited', identity, mode: 'already-exited' };
  }
  let previousGraceful = false;
  if (options.mode === 'graceful') {
    const graceful = await stopDaemonGracefully(identity, options);
    if (graceful.status !== 'survived') return graceful;
    previousGraceful = graceful.previousGraceful;
  }
  const result = await signalAndWaitForDaemonExit(identity, {
    signal: 'SIGKILL',
    timeoutMs: options.killTimeoutMs,
    previousGraceful,
  });
  return result.status === 'survived' ? { status: 'retained', reason: 'exit-timeout' } : result;
}

async function stopDaemonGracefully(
  identity: DaemonProcessIdentity,
  options: { termTimeoutMs: number; gracefulShutdown?: DaemonGracefulShutdown },
): Promise<DaemonTerminationResult | Readonly<{ status: 'survived'; previousGraceful: boolean }>> {
  if (options.gracefulShutdown) {
    const rpcResult = await tryGracefulShutdownRpc(
      identity,
      options.gracefulShutdown,
      options.termTimeoutMs,
    );
    if (rpcResult) return rpcResult;
  }
  const term = await signalAndWaitForDaemonExit(identity, {
    signal: 'SIGTERM',
    timeoutMs: options.termTimeoutMs,
  });
  return term.status === 'survived'
    ? { status: 'survived', previousGraceful: canSignalGracefully() }
    : term;
}

async function tryGracefulShutdownRpc(
  identity: DaemonProcessIdentity,
  gracefulShutdown: DaemonGracefulShutdown,
  termTimeoutMs: number,
): Promise<
  DaemonTerminationResult | Readonly<{ status: 'survived'; previousGraceful: boolean }> | null
> {
  if (!isAgentDeviceDaemonProcess(identity.pid, identity.startTime)) {
    return (await waitForDaemonExit(identity, { timeoutMs: 0 })).exited
      ? { status: 'exited', identity, mode: 'already-exited' }
      : { status: 'retained', reason: 'identity-unverified' };
  }
  const outcome = await gracefulShutdown(identity);
  if (outcome === 'embedded-owner') return { status: 'retained', reason: 'embedded-owner' };
  if (outcome === 'accepted') {
    return (await waitForDaemonExit(identity, { timeoutMs: termTimeoutMs })).exited
      ? { status: 'exited', identity, mode: 'graceful' }
      : { status: 'survived', previousGraceful: true };
  }
  return (await waitForDaemonExit(identity, { timeoutMs: 0 })).exited
    ? { status: 'exited', identity, mode: 'already-exited' }
    : null;
}

async function signalAndWaitForDaemonExit(
  identity: DaemonProcessIdentity,
  options: { signal: 'SIGTERM' | 'SIGKILL'; timeoutMs: number; previousGraceful?: boolean },
): Promise<DaemonTerminationResult | Readonly<{ status: 'survived' }>> {
  const verified = isAgentDeviceDaemonProcess(identity.pid, identity.startTime);
  const signaled = verified && trySignalProcess(identity.pid, options.signal);
  const mode = signaled
    ? options.signal === 'SIGTERM' && canSignalGracefully()
      ? 'graceful'
      : 'forced'
    : options.previousGraceful
      ? 'graceful'
      : 'already-exited';
  const wait = await waitForDaemonExit(identity, {
    timeoutMs: mode === 'already-exited' ? 0 : options.timeoutMs,
  });
  if (wait.exited) return { status: 'exited', identity, mode };
  if (signaled) return { status: 'survived' };
  return {
    status: 'retained',
    signal: options.signal,
    reason: verified ? 'signal-failed' : 'identity-unverified',
  };
}
