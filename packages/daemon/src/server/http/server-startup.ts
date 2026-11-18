import { withDaemonDiagnosticsScope } from '@agent-device/daemon-contracts/daemon-diagnostics-scope';
import {
  reconcileOrphanedDeviceClaims,
  type DeviceClaimReconciler,
} from '@agent-device/daemon-contracts/device-claims';
import { emitDiagnostic } from '@agent-device/host-kit/diagnostics';
import type { DaemonServerMode } from '@agent-device/kernel/contracts';
import type { EnvMap } from '@agent-device/kernel/source-value';
import { createDaemonControlServer, type DaemonControlServerOptions } from './control-server.ts';
import { createDaemonHttpServer } from './http-server.ts';
import { closeDaemonServers, type StartupRollback } from './server-shutdown.ts';
import {
  createSocketServer,
  listenHttpServer,
  listenNetServer,
  type DaemonServer,
} from './transport.ts';

/** Registers each listener for rollback before the next startup operation can fail. */
export async function openDaemonServers(
  mode: DaemonServerMode,
  httpOptions: Parameters<typeof createDaemonHttpServer>[0],
  rollback: StartupRollback,
  controlOptions?: DaemonControlServerOptions,
): Promise<{
  servers: DaemonServer[];
  controlServer?: DaemonServer;
  socketPort?: number;
  httpPort?: number;
  controlPort?: number;
}> {
  const servers: DaemonServer[] = [];
  let controlServer: DaemonServer | undefined;
  rollback.onRollback(() =>
    closeDaemonServers(controlServer ? [...servers, controlServer] : servers),
  );
  let socketPort: number | undefined;
  let httpPort: number | undefined;
  let controlPort: number | undefined;
  if (mode !== 'http') {
    const server = createSocketServer(httpOptions.handleRequest);
    servers.push(server);
    socketPort = await listenNetServer(server);
  }
  if (mode !== 'socket') {
    const server = await createDaemonHttpServer(httpOptions);
    servers.push(server);
    httpPort = await listenHttpServer(server);
  }
  if (controlOptions) {
    controlServer = createDaemonControlServer(controlOptions);
    controlPort = await listenNetServer(controlServer);
  }
  return { servers, controlServer, socketPort, httpPort, controlPort };
}

export async function reconcileDeviceClaimsForDaemonStartup(
  logPath: string,
  reconcile: DeviceClaimReconciler,
  stateDir: string,
  env: EnvMap,
): Promise<void> {
  await withDaemonDiagnosticsScope({ logPath }, async () => {
    try {
      const summary = await reconcileOrphanedDeviceClaims(reconcile, stateDir, env);
      if (summary.examined > 0) {
        emitDiagnostic({ phase: 'device_claim_reconcile', data: summary });
      }
    } catch (error) {
      emitDiagnostic({
        level: 'warn',
        phase: 'device_claim_reconcile_failed',
        data: { error: error instanceof Error ? error.message : String(error) },
      });
    }
  });
}
