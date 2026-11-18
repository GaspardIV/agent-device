import { readDaemonShutdownReport } from '@agent-device/daemon-contracts/daemon-shutdown-report';
import {
  observedDaemonIdentityOfRegistration,
  type DaemonStopIdentity,
} from '@agent-device/daemon-contracts/daemon-registration';
import {
  stopDaemon,
  mergeDaemonShutdownReport,
  type DaemonStopResult,
} from '@agent-device/daemon-contracts/daemon-stop';
import { daemonRecordUnreadableError } from './daemon-lifecycle-errors.ts';
import {
  createGracefulDaemonShutdown,
  type DaemonLifecycleTarget,
} from './daemon-client-lifecycle-request.ts';
import { parseDaemonInfo } from './daemon-client-metadata.ts';

export async function stopRecordedDaemon(
  record: DaemonStopIdentity,
  target: DaemonLifecycleTarget,
  options?: { shutdownTimeoutMs?: number },
): Promise<DaemonStopResult> {
  if (record.ownership === 'unreadable') throw daemonRecordUnreadableError(target.paths);
  const gracefulShutdown = createGracefulDaemonShutdown(
    record.record ? parseDaemonInfo(record.record) : null,
    target,
    options?.shutdownTimeoutMs !== undefined ? { timeoutMs: options.shutdownTimeoutMs } : undefined,
  );
  const stopped = await stopDaemon({
    paths: target.paths,
    observed: observedDaemonIdentityOfRegistration(record),
    ...(gracefulShutdown ? { gracefulShutdown } : {}),
  });
  return mergeDaemonShutdownReport(
    stopped,
    stopped.mode === 'graceful' ? readDaemonShutdownReport(target.paths.baseDir) : null,
  );
}
