import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { afterEach, expect, test, vi } from 'vitest';
import { createTestHostEnvironment, mkdtempForTestSync } from '@agent-device/test-kit';
import {
  DAEMON_CONTROL_PROTOCOL_VERSION,
  DAEMON_SHUTDOWN_COMMAND,
  createDaemonControlNonce,
  parseControlChallengeResponse,
  parseControlCommandResponse,
  signDaemonControlCommand,
  verifyDaemonControlChallenge,
  verifyDaemonControlReply,
} from '@agent-device/daemon-contracts/daemon-control';
import {
  buildDaemonLifecycleRequest,
  resolveDaemonPaths,
  type DaemonRuntimeOwnership,
} from '@agent-device/daemon-contracts/daemon-resolution';
import { consumeTextLines } from '@agent-device/host-kit/transport';
import { readDaemonShutdownReport } from '@agent-device/daemon-contracts/daemon-shutdown-report';
import { readCurrentOwnerIdentity } from '@agent-device/host-kit/process';
import type { DaemonResponse } from '@agent-device/kernel/contracts';
import * as shutdown from './server-shutdown.ts';

// A lifecycle request is the daemon's own answer to "may I leave?", so these tests boot the real
// runtime and ask over its real socket: the census, the closing of admission, the drain and the
// runtime close are one flow.
const probe = vi.hoisted(() => ({
  /** Holds daemon startup after its servers listen, until released. */
  startupGate: undefined as Promise<void> | undefined,
  store: undefined as import('../../core/session-store.ts').SessionStore | undefined,
  registry: undefined as import('../../resources/lease-registry.ts').LeaseRegistry | undefined,
  held: undefined as Promise<void> | undefined,
  entered: undefined as (() => void) | undefined,
  diagnosticFailure: undefined as Error | undefined,
  handleRequest: undefined as import('../../core/daemon-request.ts').DaemonInvokeFn | undefined,
  idleReaps: 0,
}));
vi.mock('@agent-device/host-kit/diagnostics', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@agent-device/host-kit/diagnostics')>();
  return {
    ...actual,
    emitDiagnostic: (...args: Parameters<typeof actual.emitDiagnostic>) => {
      if (args[0].phase === 'daemon_idle_reap') probe.idleReaps += 1;
      if (probe.diagnosticFailure) {
        if (args[0].phase === 'daemon_fatal') throw probe.diagnosticFailure;
        throw new Error('secondary diagnostic failure');
      }
      return actual.emitDiagnostic(...args);
    },
  };
});

vi.mock('./transport.ts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('./transport.ts')>();
  return {
    ...actual,
    createSocketServer: (handler: import('../../core/daemon-request.ts').DaemonInvokeFn) => {
      probe.handleRequest = handler;
      return actual.createSocketServer(handler);
    },
  };
});

vi.mock('@agent-device/daemon-contracts/device-claims', async (importOriginal) => {
  const actual =
    await importOriginal<typeof import('@agent-device/daemon-contracts/device-claims')>();
  return {
    ...actual,
    reconcileOrphanedDeviceClaims: async (
      ...args: Parameters<typeof actual.reconcileOrphanedDeviceClaims>
    ) => {
      await probe.startupGate;
      return await actual.reconcileOrphanedDeviceClaims(...args);
    },
  };
});
vi.mock('../../core/session-store.ts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../core/session-store.ts')>();
  return {
    ...actual,
    SessionStore: class extends actual.SessionStore {
      constructor(...args: ConstructorParameters<typeof actual.SessionStore>) {
        super(...args);
        probe.store = this;
      }
    },
  };
});
vi.mock('../../resources/lease-registry.ts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../resources/lease-registry.ts')>();
  class RecordedLeaseRegistry extends actual.LeaseRegistry {
    constructor(...args: ConstructorParameters<typeof actual.LeaseRegistry>) {
      super(...args);
      probe.registry = this;
    }
  }
  return { ...actual, LeaseRegistry: RecordedLeaseRegistry };
});
vi.mock('../request-router.ts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../request-router.ts')>();
  return {
    ...actual,
    createRequestHandler: (...args: Parameters<typeof actual.createRequestHandler>) => {
      const dispatch = actual.createRequestHandler(...args);
      return async (request: Parameters<typeof dispatch>[0]) => {
        if (probe.held) {
          probe.entered?.();
          await probe.held;
        }
        return await dispatch(request);
      };
    },
  };
});

import {
  applicationLifecycleResourceFixture,
  daemonRuntimeDepsFixture,
} from '../../__tests__/support/daemon-runtime-deps-fixture.ts';
import { lifecycleDeviceRuntimeGateway } from '../../__tests__/support/test-device-runtime-gateway.ts';
import { makeRepairArmedSession, makeSession } from '../../__tests__/support/session-factories.ts';
import { startDaemonRuntime } from './daemon-runtime.ts';

afterEach(() => {
  probe.startupGate = undefined;
  probe.held = undefined;
  probe.entered = undefined;
  probe.store = undefined;
  probe.registry = undefined;
  probe.diagnosticFailure = undefined;
  probe.handleRequest = undefined;
  probe.idleReaps = 0;
  vi.useRealTimers();
  vi.restoreAllMocks();
});

type Booted = {
  runtime: NonNullable<Awaited<ReturnType<typeof startDaemonRuntime>>>;
  stateDir: string;
  exit: Promise<number>;
  quiesce(overrides?: LifecycleAddress): Promise<DaemonResponse>;
  shutdown(overrides?: LifecycleAddress): Promise<DaemonResponse>;
  shutdownOverSocket(overrides?: LifecycleAddress): Promise<DaemonResponse>;
  sessionList(): Promise<DaemonResponse>;
};

type LifecycleAddress = {
  token?: string;
  pid?: number;
  processStartTime?: string | null;
  requestId?: string;
  skipChallengeVerify?: boolean;
  /** Sends the request with no `meta` at all, the shape a caller that tracks no id produces. */
  omitMeta?: boolean;
};

async function bootDaemon(
  options: {
    serverMode?: 'socket' | 'http' | 'dual';
    ownership?: DaemonRuntimeOwnership;
    idleTimeoutMs?: number;
    requestDrainMs?: number;
    authHookPath?: string;
    composeDeps?: ReturnType<typeof daemonRuntimeDepsFixture>;
  } = {},
): Promise<Booted> {
  const stateDir = mkdtempForTestSync('agent-device-daemon-lifecycle-');
  const paths = resolveDaemonPaths(stateDir);
  const runtime = await startDaemonRuntime({
    composeDeps: options.composeDeps ?? daemonRuntimeDepsFixture(),
    host: createTestHostEnvironment({
      env: {
        ...process.env,
        AGENT_DEVICE_STATE_DIR: stateDir,
        AGENT_DEVICE_DAEMON_SERVER_MODE: options.serverMode ?? 'socket',
        AGENT_DEVICE_DAEMON_IDLE_TIMEOUT_MS: String(options.idleTimeoutMs ?? 0),
        ...(options.authHookPath ? { AGENT_DEVICE_HTTP_AUTH_HOOK: options.authHookPath } : {}),
      },
    }),
    ownership: options.ownership,
    shutdownBudgets:
      options.requestDrainMs === undefined ? undefined : { requestDrainMs: options.requestDrainMs },
    stderr: { write: () => {} },
    stdout: { write: () => {} },
  });
  if (!runtime?.controlPort || (!runtime.socketPort && !runtime.httpPort)) {
    throw new Error('daemon did not start');
  }
  const exit = runtime.closed.then(() => 0);
  const info = JSON.parse(fs.readFileSync(paths.infoPath, 'utf8')) as {
    pid: number;
    processStartTime?: string;
    controlPort?: number;
  };
  const socketPort = runtime.socketPort;
  const controlPort = runtime.controlPort;
  const send = (request: object) => {
    if (!socketPort) throw new Error('daemon socket server not enabled');
    return sendSocketRequest(socketPort, request);
  };
  const resolveFence = (overrides: LifecycleAddress) => ({
    pid: overrides.pid ?? info.pid,
    processStartTime:
      overrides.processStartTime !== undefined
        ? overrides.processStartTime
        : (info.processStartTime ?? null),
  });
  const buildShutdownProbeRequest = (params: {
    token: string;
    fence: { pid: number; processStartTime: string | null };
    requestId: string;
  }) => ({
    ...buildDaemonLifecycleRequest(params),
    command: DAEMON_SHUTDOWN_COMMAND,
  });
  const sendLifecycle = (
    overrides: LifecycleAddress,
    build: typeof buildDaemonLifecycleRequest = buildDaemonLifecycleRequest,
  ) => {
    const request = build({
      token: overrides.token ?? runtime.token,
      fence: resolveFence(overrides),
      requestId: overrides.requestId ?? 'quiesce-test',
    });
    return overrides.omitMeta ? send({ ...request, meta: undefined }) : send(request);
  };
  return {
    runtime,
    stateDir,
    exit,
    quiesce: (overrides = {}) => sendLifecycle(overrides, buildDaemonLifecycleRequest),
    shutdown: (overrides = {}) =>
      sendControlCommand(controlPort, {
        token: overrides.token ?? runtime.controlSecret!,
        fence: resolveFence(overrides),
        command: DAEMON_SHUTDOWN_COMMAND,
        requestId: overrides.requestId ?? 'shutdown-test',
        skipChallengeVerify: overrides.skipChallengeVerify,
      }),
    shutdownOverSocket: (overrides = {}) => sendLifecycle(overrides, buildShutdownProbeRequest),
    sessionList: () =>
      send({
        token: runtime.token,
        session: 'default',
        command: 'session_list',
        positionals: [],
        meta: { requestId: 'session-list-test' },
      }),
  };
}

async function sendControlCommand(
  controlPort: number,
  options: {
    token: string;
    fence: { pid: number; processStartTime: string | null };
    command: typeof DAEMON_SHUTDOWN_COMMAND;
    requestId: string;
    skipChallengeVerify?: boolean;
  },
): Promise<DaemonResponse> {
  const clientNonce = createDaemonControlNonce();
  return await new Promise<DaemonResponse>((resolve, reject) => {
    const socket = net.createConnection({ host: '127.0.0.1', port: controlPort });
    let buffer = '';
    let serverNonce = '';
    let settled = false;
    const finish = (fn: () => void) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      fn();
    };
    socket.setEncoding('utf8');
    socket.on('error', (err) => finish(() => reject(err)));
    socket.on('close', () =>
      finish(() =>
        resolve({
          ok: false,
          error: { code: 'UNAUTHORIZED', message: 'Control connection closed before reply.' },
        }),
      ),
    );
    socket.on('connect', () => {
      socket.write(
        `${JSON.stringify({
          type: 'challenge',
          version: DAEMON_CONTROL_PROTOCOL_VERSION,
          nonce: clientNonce,
          requestId: options.requestId,
        })}\n`,
      );
    });
    socket.on('data', (chunk: string) => {
      const next = consumeTextLines(buffer, chunk);
      buffer = next.buffer;
      for (const line of next.lines) {
        if (!serverNonce) {
          const challenge = parseControlChallengeResponse(line);
          if (
            !challenge ||
            (!options.skipChallengeVerify &&
              !verifyDaemonControlChallenge({
                token: options.token,
                fence: options.fence,
                clientNonce,
                serverNonce: challenge.serverNonce,
                proof: challenge.proof,
              }))
          ) {
            finish(() =>
              resolve({
                ok: false,
                error: { code: 'UNAUTHORIZED', message: 'Control challenge rejected.' },
              }),
            );
            return;
          }
          serverNonce = challenge.serverNonce;
          socket.write(
            `${JSON.stringify({
              type: 'command',
              version: DAEMON_CONTROL_PROTOCOL_VERSION,
              command: options.command,
              clientNonce,
              serverNonce,
              pid: options.fence.pid,
              processStartTime: options.fence.processStartTime,
              requestId: options.requestId,
              proof: signDaemonControlCommand({
                token: options.token,
                command: options.command,
                fence: options.fence,
                clientNonce,
                serverNonce,
                requestId: options.requestId,
              }),
            })}\n`,
          );
          continue;
        }
        const reply = parseControlCommandResponse(line);
        if (
          !reply ||
          !verifyDaemonControlReply({
            token: options.token,
            fence: options.fence,
            clientNonce,
            serverNonce,
            requestId: options.requestId,
            payload: reply.payload,
            proof: reply.proof,
          })
        ) {
          finish(() => reject(new Error('Invalid control reply signature')));
          return;
        }
        finish(() => resolve(JSON.parse(reply.payload) as DaemonResponse));
      }
    });
  });
}

async function sendSocketRequest(port: number, request: object): Promise<DaemonResponse> {
  return await new Promise<DaemonResponse>((resolve, reject) => {
    const socket = net.createConnection({ host: '127.0.0.1', port });
    let buffer = '';
    socket.setEncoding('utf8');
    socket.on('connect', () => socket.write(`${JSON.stringify(request)}\n`));
    socket.on('data', (chunk) => {
      buffer += chunk;
      if (!buffer.includes('\n')) return;
      socket.end();
      resolve(JSON.parse(buffer) as DaemonResponse);
    });
    socket.on('error', reject);
  });
}

function reasonOf(response: DaemonResponse): unknown {
  return response.ok ? undefined : response.error.details?.reason;
}

test('an idle daemon accepts a quiesce, drains and releases its state directory', async () => {
  const daemon = await bootDaemon();
  const paths = resolveDaemonPaths(daemon.stateDir);
  const published = JSON.parse(fs.readFileSync(paths.infoPath, 'utf8')) as {
    capabilities?: unknown;
  };
  expect(published.capabilities).toEqual(['quiesce', 'script-artifacts', 'shutdown']);

  expect(await daemon.quiesce()).toEqual({ ok: true, data: { outcome: 'draining' } });

  expect(await daemon.exit).toBe(0);
  expect(fs.existsSync(paths.infoPath)).toBe(false);
  expect(fs.existsSync(paths.lockPath)).toBe(false);
});

test('a daemon with a request in flight refuses to quiesce and keeps serving', async () => {
  const daemon = await bootDaemon();
  let release: () => void = () => {};
  const entered = new Promise<void>((resolve) => {
    probe.entered = resolve;
  });
  probe.held = new Promise<void>((resolve) => {
    release = resolve;
  });
  const running = daemon.sessionList();
  await entered;

  try {
    const refusal = await daemon.quiesce();
    expect(refusal.ok).toBe(false);
    expect(reasonOf(refusal)).toBe('daemon-busy');
    expect(!refusal.ok && refusal.error.details).toMatchObject({ inFlightRequests: 1 });

    release();
    expect((await running).ok).toBe(true);
    // Admission reopened with the refusal: the daemon still serves and can still be quiesced once idle.
    probe.held = undefined;
    expect((await daemon.sessionList()).ok).toBe(true);
    expect((await daemon.quiesce()).ok).toBe(true);
    await daemon.exit;
  } finally {
    release();
    await running.catch(() => {});
    await daemon.runtime.close();
  }
});

test('invalid socket credentials cannot postpone the original idle deadline', async () => {
  vi.useFakeTimers();
  const daemon = await bootDaemon({ idleTimeoutMs: 80 });
  const request = { session: 'default', command: 'session_list', positionals: [] };
  const responses: Awaited<ReturnType<typeof sendSocketRequest>>[] = [];
  try {
    await vi.advanceTimersByTimeAsync(40);
    for (const token of ['wrong', undefined]) {
      responses.push(await sendSocketRequest(daemon.runtime.socketPort!, { ...request, token }));
    }
    await vi.advanceTimersByTimeAsync(39);
    expect(probe.idleReaps).toBe(0);
    await vi.advanceTimersByTimeAsync(1);
    expect(probe.idleReaps).toBe(1);
    for (const response of responses) {
      expect(!response.ok && response.error.code).toBe('UNAUTHORIZED');
    }
    await vi.runOnlyPendingTimersAsync();
    await daemon.exit;
  } finally {
    await daemon.runtime.close();
  }
});

test('valid socket activity restarts the idle window', async () => {
  vi.useFakeTimers();
  const daemon = await bootDaemon({ idleTimeoutMs: 80 });
  try {
    await vi.advanceTimersByTimeAsync(40);
    expect((await daemon.sessionList()).ok).toBe(true);
    await vi.advanceTimersByTimeAsync(40);
    expect(probe.idleReaps).toBe(0);
    await vi.advanceTimersByTimeAsync(40);
    expect(probe.idleReaps).toBe(1);
    await vi.runOnlyPendingTimersAsync();
    await daemon.exit;
  } finally {
    await daemon.runtime.close();
  }
});

test('request completion after close never re-arms idle reap', async () => {
  vi.useFakeTimers();
  const daemon = await bootDaemon({ idleTimeoutMs: 80 });
  let release: () => void = () => {};
  const entered = new Promise<void>((resolve) => {
    probe.entered = resolve;
  });
  probe.held = new Promise<void>((resolve) => {
    release = resolve;
  });
  const running = daemon.runtime.run(() =>
    probe.handleRequest!({
      token: daemon.runtime.token,
      session: 'default',
      command: 'session_list',
      positionals: [],
    }),
  );
  await entered;

  try {
    const closing = daemon.runtime.close();
    release();
    expect((await running).ok).toBe(true);
    await vi.runOnlyPendingTimersAsync();
    expect((await closing).requests).toEqual({ drained: 1, abandoned: 0 });
    await vi.advanceTimersByTimeAsync(81);
    expect(probe.idleReaps).toBe(0);
  } finally {
    release();
    await running;
    await vi.runOnlyPendingTimersAsync();
    await daemon.runtime.close();
  }
});

test('quiesce during an external close reports draining while a request is still running', async () => {
  const daemon = await bootDaemon();
  let release: () => void = () => {};
  const entered = new Promise<void>((resolve) => {
    probe.entered = resolve;
  });
  probe.held = new Promise<void>((resolve) => {
    release = resolve;
  });
  const running = daemon.runtime.run(() =>
    probe.handleRequest!({
      token: daemon.runtime.token,
      session: 'default',
      command: 'session_list',
      positionals: [],
    }),
  );
  await entered;
  const closing = daemon.runtime.close();

  try {
    const response = await daemon.runtime.run(() =>
      probe.handleRequest!(
        buildDaemonLifecycleRequest({
          token: daemon.runtime.token,
          fence: {
            pid: daemon.runtime.identity.pid,
            processStartTime: daemon.runtime.identity.startTime,
          },
          requestId: 'quiesce-during-external-close',
        }),
      ),
    );
    expect(response).toEqual({ ok: true, data: { outcome: 'draining' } });
  } finally {
    release();
    await running;
    await closing;
  }
});

test('a daemon holding a session, a repair session or a live lease refuses to quiesce', async () => {
  const daemon = await bootDaemon();
  try {
    const openRef = probe.store!.publish('open-session', makeSession('open-session'));
    const withSession = await daemon.quiesce();
    expect(reasonOf(withSession)).toBe('daemon-busy');
    expect(!withSession.ok && withSession.error.details).toMatchObject({
      sessions: 1,
      repairSessions: 0,
    });
    probe.store!.retire(openRef);

    const repairRef = probe.store!.publish('repairing', makeRepairArmedSession('repairing'));
    const withRepair = await daemon.quiesce();
    expect(!withRepair.ok && withRepair.error.details).toMatchObject({
      sessions: 0,
      repairSessions: 1,
    });
    probe.store!.retire(repairRef);

    const lease = probe.registry!.allocateLease({
      tenantId: 'tenant-a',
      runId: 'run-a',
      ttlMs: 60_000,
    });
    const withLease = await daemon.quiesce();
    expect(!withLease.ok && withLease.error.details).toMatchObject({ leases: 1 });
    probe.registry!.releaseLease({ leaseId: lease.leaseId });

    expect((await daemon.quiesce()).ok).toBe(true);
    expect(await daemon.exit).toBe(0);
  } finally {
    await daemon.runtime.close();
  }
});

test('a quiesce with the wrong token or for another daemon instance leaves the daemon running', async () => {
  const daemon = await bootDaemon();
  try {
    const wrongToken = await daemon.quiesce({ token: 'not-the-token' });
    expect(!wrongToken.ok && wrongToken.error.code).toBe('UNAUTHORIZED');

    const otherInstance = await daemon.quiesce({ pid: readCurrentOwnerIdentity().pid + 1 });
    expect(reasonOf(otherInstance)).toBe('daemon-instance-mismatch');
    // A recycled pid is a different instance: the start time is part of the fence.
    const recycledPid = await daemon.quiesce({ processStartTime: 'a different start time' });
    expect(reasonOf(recycledPid)).toBe('daemon-instance-mismatch');

    expect((await daemon.sessionList()).ok).toBe(true);
  } finally {
    await daemon.runtime.close();
  }
});

async function quiesceOverHttp(
  daemon: Booted,
  headers: Record<string, string>,
  build: typeof buildDaemonLifecycleRequest = buildDaemonLifecycleRequest,
): Promise<{ result?: DaemonResponse; error?: { data?: { code?: string } } }> {
  const { httpPort, token } = daemon.runtime;
  const info = JSON.parse(
    fs.readFileSync(resolveDaemonPaths(daemon.stateDir).infoPath, 'utf8'),
  ) as { pid: number; processStartTime?: string };
  const response = await fetch(`http://127.0.0.1:${httpPort}/rpc`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      authorization: `Bearer ${token}`,
      ...headers,
    },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: 'http-quiesce',
      method: 'agent_device.command',
      params: build({
        token,
        fence: { pid: info.pid, processStartTime: info.processStartTime ?? null },
        requestId: 'http-quiesce',
      }),
    }),
  });
  return (await response.json()) as never;
}

test('a quiesce that arrives as a remote-origin HTTP request is refused', async () => {
  const daemon = await bootDaemon({ serverMode: 'dual' });
  try {
    const body = await quiesceOverHttp(daemon, { 'x-agent-device-network-access': 'public-only' });
    expect(JSON.stringify(body)).toContain('"UNAUTHORIZED"');
    expect(JSON.stringify(body)).toContain('Daemon quiesce is not available to remote clients.');
    expect((await daemon.sessionList()).ok).toBe(true);
  } finally {
    await daemon.runtime.close();
  }
});

test('a local HTTP quiesce drains an idle daemon like a socket one', async () => {
  const daemon = await bootDaemon({ serverMode: 'dual' });
  const body = await quiesceOverHttp(daemon, {});
  expect(body.result).toEqual({ ok: true, data: { outcome: 'draining' } });
  expect(await daemon.exit).toBe(0);
});

test('an HTTP-only daemon with AGENT_DEVICE_HTTP_AUTH_HOOK refuses /rpc shutdown while local control stops cleanly', async () => {
  const hookDir = mkdtempForTestSync('agent-device-http-auth-shutdown-');
  const authHookPath = path.join(hookDir, 'deny-all-hook.mjs');
  fs.writeFileSync(
    authHookPath,
    'export default async function () { return { ok: false, code: "FORBIDDEN", message: "Denied by auth hook" }; }\n',
  );
  const daemon = await bootDaemon({ serverMode: 'http', authHookPath });
  const paths = resolveDaemonPaths(daemon.stateDir);
  try {
    const published = JSON.parse(fs.readFileSync(paths.infoPath, 'utf8')) as {
      port?: number;
      httpPort?: number;
      controlPort?: number;
      controlSecret?: string;
      token?: string;
    };
    expect(published.port).toBeUndefined();
    expect(published.httpPort).toBe(daemon.runtime.httpPort);
    expect(published.controlPort).toBe(daemon.runtime.controlPort);
    expect(published.controlSecret).toBe(daemon.runtime.controlSecret);
    expect(published.controlSecret).toBeTruthy();
    expect(published.controlSecret).not.toBe(published.token);

    // A tenant carrying the HTTP bearer token must NOT be able to shut down the daemon via controlPort
    // (rejected on both client challenge verification and server command HMAC verification).
    await expect(daemon.shutdown({ token: daemon.runtime.token })).resolves.toMatchObject({
      ok: false,
      error: { code: 'UNAUTHORIZED' },
    });
    await expect(
      daemon.shutdown({ token: daemon.runtime.token, skipChallengeVerify: true }),
    ).resolves.toMatchObject({
      ok: false,
      error: { code: 'UNAUTHORIZED' },
    });
    expect(fs.existsSync(paths.infoPath)).toBe(true);

    const deniedRpc = await quiesceOverHttp(daemon, {}, (params) => ({
      ...buildDaemonLifecycleRequest(params),
      command: DAEMON_SHUTDOWN_COMMAND,
    }));
    expect(JSON.stringify(deniedRpc)).toContain('Denied by auth hook');

    expect(await daemon.shutdown()).toEqual({ ok: true, data: { outcome: 'draining' } });
    expect(await daemon.exit).toBe(0);
    expect(fs.existsSync(paths.infoPath)).toBe(false);
    expect(fs.existsSync(paths.lockPath)).toBe(false);
  } finally {
    await daemon.runtime.close();
    fs.rmSync(hookDir, { recursive: true, force: true });
  }
});

test('a host close retains registration and lock while a request ignores cancellation', async () => {
  const daemon = await bootDaemon({ ownership: 'embedded', requestDrainMs: 50 });
  const paths = resolveDaemonPaths(daemon.stateDir);
  let release: () => void = () => {};
  const entered = new Promise<void>((resolve) => {
    probe.entered = resolve;
  });
  probe.held = new Promise<void>((resolve) => {
    release = resolve;
  });
  daemon.sessionList().catch(() => undefined);
  await entered;

  try {
    const report = await daemon.runtime.close();
    expect(report.requests.abandoned).toBe(1);
    expect(fs.existsSync(paths.lockPath)).toBe(true);
    expect(fs.existsSync(paths.infoPath)).toBe(true);
  } finally {
    release();
  }
});

test('a diagnostic failure starts listener close, retains unproved work and preserves the primary error', async () => {
  const daemon = await bootDaemon({ ownership: 'embedded' });
  const paths = resolveDaemonPaths(daemon.stateDir);
  const listeners = vi.spyOn(shutdown, 'closeDaemonServers');
  const primary = new Error('fatal diagnostic failed');
  probe.diagnosticFailure = primary;
  const exited = expect(daemon.exit).rejects.toBe(primary);
  await expect(daemon.runtime.close({ cause: new Error('shutdown cause') })).rejects.toBe(primary);
  await exited;
  expect(listeners).toHaveBeenCalledTimes(2);
  expect(fs.existsSync(paths.infoPath)).toBe(true);
  expect(fs.existsSync(paths.lockPath)).toBe(true);
});

test('a rejected platform close retains registration and lock', async () => {
  const stateDir = mkdtempForTestSync('agent-device-daemon-lifecycle-close-failure-');
  const paths = resolveDaemonPaths(stateDir);
  const runtime = await startDaemonRuntime({
    composeDeps: daemonRuntimeDepsFixture(() => ({
      deviceRuntimeGateway: {
        ...lifecycleDeviceRuntimeGateway,
        applicationLifecycle: applicationLifecycleResourceFixture(),
        shutdown: async () => {
          throw new Error('gateway teardown rejected');
        },
      },
    })),
    host: createTestHostEnvironment({
      env: {
        ...process.env,
        AGENT_DEVICE_STATE_DIR: stateDir,
        AGENT_DEVICE_DAEMON_SERVER_MODE: 'socket',
        AGENT_DEVICE_DAEMON_IDLE_TIMEOUT_MS: '0',
      },
    }),
    ownership: 'embedded',
    stderr: { write: () => {} },
    stdout: { write: () => {} },
  });
  const report = await runtime.close();
  expect(report.platformOwners).toBe('failed');
  expect(fs.readFileSync(paths.logPath, 'utf8')).toContain('gateway teardown rejected');
  expect(fs.existsSync(paths.infoPath)).toBe(true);
  expect(fs.existsSync(paths.lockPath)).toBe(true);
});

test('an embedded runtime outlives the idle window a detached daemon reaps itself on', async () => {
  // Booted first on purpose: both get the same window, so the embedded one's window ends no later than
  // the detached one's. Booting it second would let this assertion run while its own window still had
  // time left, which would prove nothing about the rule.
  const embedded = await bootDaemon({ ownership: 'embedded', idleTimeoutMs: 40 });
  const detached = await bootDaemon({ idleTimeoutMs: 40 });
  const detachedPaths = resolveDaemonPaths(detached.stateDir);
  try {
    // Idle reap releases the state directory it reaped, so that is the fact to wait for. By then the
    // embedded runtime has been idle at least as long, so a window that ended one daemon's life is
    // proof enough that it did not end the other's.
    await vi.waitFor(() => expect(fs.existsSync(detachedPaths.infoPath)).toBe(false), {
      timeout: 5_000,
    });
    // A gap between requests is not the owner asking to be shut down.
    expect((await embedded.sessionList()).ok).toBe(true);
  } finally {
    await detached.runtime.close();
    await embedded.runtime.close();
  }
});

test('an embedded runtime refuses a quiesce, because its host decides when it leaves', async () => {
  const daemon = await bootDaemon({ ownership: 'embedded' });
  expect(reasonOf(await daemon.quiesce())).toBe('daemon-owner-embedded');
  // Nothing was closed and no admission was left closed: the host's runtime still serves.
  expect((await daemon.sessionList()).ok).toBe(true);
  await daemon.runtime.close();
  expect(await daemon.exit).toBe(0);
});

test('a quiesce asked while the daemon is still starting up is refused as busy, then accepted', async () => {
  const stateDir = mkdtempForTestSync('agent-device-daemon-lifecycle-startup-');
  const paths = resolveDaemonPaths(stateDir);
  let openGate: () => void = () => {};
  probe.startupGate = new Promise<void>((resolve) => {
    openGate = resolve;
  });
  const starting = startDaemonRuntime({
    composeDeps: daemonRuntimeDepsFixture(),
    host: createTestHostEnvironment({
      env: {
        ...process.env,
        AGENT_DEVICE_STATE_DIR: stateDir,
        AGENT_DEVICE_DAEMON_SERVER_MODE: 'socket',
        AGENT_DEVICE_DAEMON_IDLE_TIMEOUT_MS: '0',
      },
    }),
    stderr: { write: () => {} },
    stdout: { write: () => {} },
  });
  try {
    // Published, listening, and still held inside startup.
    for (let polls = 0; !fs.existsSync(paths.infoPath); polls += 1) {
      if (polls >= 500) throw new Error('daemon did not publish daemon.json during startup');
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    const info = JSON.parse(fs.readFileSync(paths.infoPath, 'utf8')) as {
      port: number;
      token: string;
      pid: number;
      processStartTime?: string;
    };

    const quiesceRequest = buildDaemonLifecycleRequest({
      token: info.token,
      fence: { pid: info.pid, processStartTime: info.processStartTime ?? null },
      requestId: 'startup-quiesce',
    });
    const response = await sendSocketRequest(info.port, quiesceRequest);
    // The daemon is still starting up, so it refuses as busy instead of promising a drain it cannot run yet.
    expect(response.ok).toBe(false);
    expect(reasonOf(response)).toBe('daemon-busy');
    expect(!response.ok && response.error.details).toMatchObject({
      startingUp: true,
      inFlightRequests: 0,
    });

    openGate();
    const runtime = await starting;
    const exit = runtime.closed.then(() => 0);
    const again = await sendSocketRequest(info.port, quiesceRequest);
    expect(again).toEqual({ ok: true, data: { outcome: 'draining' } });
    expect(await exit).toBe(0);
    expect(fs.existsSync(paths.infoPath)).toBe(false);
  } finally {
    openGate();
    await starting.then((runtime) => runtime?.close());
  }
});

test('shutdown accepts active sessions and leases, writes the shutdown report, and preserves quiesce security fences', async () => {
  const embedded = await bootDaemon({ ownership: 'embedded' });
  try {
    expect(reasonOf(await embedded.shutdown())).toBe('daemon-owner-embedded');
    expect((await embedded.sessionList()).ok).toBe(true);
  } finally {
    await embedded.runtime.close();
  }

  const remote = await bootDaemon({ serverMode: 'dual' });
  try {
    const tenantSocket = await remote.shutdownOverSocket();
    expect(!tenantSocket.ok && tenantSocket.error.code).toBe('UNAUTHORIZED');
    expect(!tenantSocket.ok && tenantSocket.error.message).toBe(
      'Daemon shutdown is only available on the local control channel.',
    );

    const body = await quiesceOverHttp(
      remote,
      { 'x-agent-device-network-access': 'public-only' },
      (params) => ({
        ...buildDaemonLifecycleRequest(params),
        command: DAEMON_SHUTDOWN_COMMAND,
      }),
    );
    expect(JSON.stringify(body)).toContain('"UNAUTHORIZED"');
    expect(JSON.stringify(body)).toContain('Daemon shutdown is not available to remote clients.');
    expect(
      !(await remote.shutdown({ token: 'wrong-token' })).ok &&
        (await remote.shutdown({ token: 'wrong-token' })),
    ).toMatchObject({ ok: false, error: { code: 'UNAUTHORIZED' } });
    const mismatch = await remote.shutdown({ processStartTime: 'other-start' });
    expect(!mismatch.ok && mismatch.error.code).toBe('UNAUTHORIZED');
    expect((await remote.sessionList()).ok).toBe(true);
  } finally {
    await remote.runtime.close();
  }

  const daemon = await bootDaemon({ serverMode: 'dual' });
  const paths = resolveDaemonPaths(daemon.stateDir);
  try {
    probe.store!.publish('open-session', makeSession('open-session'));
    probe.registry!.allocateLease({
      tenantId: 'tenant-a',
      runId: 'run-a',
      ttlMs: 60_000,
    });
    expect(reasonOf(await daemon.quiesce())).toBe('daemon-busy');

    const response = await daemon.shutdown();
    expect(response).toEqual({ ok: true, data: { outcome: 'draining' } });
    expect(await daemon.exit).toBe(0);
    expect(fs.existsSync(paths.infoPath)).toBe(false);
    expect(fs.existsSync(paths.lockPath)).toBe(false);
    expect(readDaemonShutdownReport(paths.baseDir)).toMatchObject({
      providerReleases: { released: [], pending: [] },
      claims: { released: [], orphaned: [], superseded: [], unattributable: [] },
    });
  } finally {
    await daemon.runtime.close();
  }
});

test('control server stays bound across active teardown after tenant listeners close until daemon.json retires', async () => {
  let releaseTeardown: () => void = () => {};
  let notifyTeardownEntered: () => void = () => {};
  const teardownEntered = new Promise<void>((resolve) => {
    notifyTeardownEntered = resolve;
  });
  const teardownGate = new Promise<void>((resolve) => {
    releaseTeardown = resolve;
  });
  const daemon = await bootDaemon({
    serverMode: 'socket',
    composeDeps: daemonRuntimeDepsFixture(() => ({
      deviceRuntimeGateway: {
        ...lifecycleDeviceRuntimeGateway,
        applicationLifecycle: applicationLifecycleResourceFixture(),
        shutdown: async () => {
          notifyTeardownEntered();
          await teardownGate;
        },
      },
    })),
  });
  const paths = resolveDaemonPaths(daemon.stateDir);
  try {
    expect(await daemon.shutdown()).toEqual({ ok: true, data: { outcome: 'draining' } });
    await teardownEntered;

    // Tenant socket server has already closed at teardown start while daemon.json and controlPort remain live.
    await expect(
      sendSocketRequest(daemon.runtime.socketPort!, { command: 'session_list' }),
    ).rejects.toThrow();
    expect(fs.existsSync(paths.infoPath)).toBe(true);
    expect(await daemon.shutdown()).toEqual({ ok: true, data: { outcome: 'draining' } });

    releaseTeardown();
    expect(await daemon.exit).toBe(0);
    expect(fs.existsSync(paths.infoPath)).toBe(false);
    await expect(
      sendControlCommand(daemon.runtime.controlPort!, {
        token: daemon.runtime.controlSecret!,
        fence: {
          pid: daemon.runtime.identity.pid,
          processStartTime: daemon.runtime.identity.startTime,
        },
        command: DAEMON_SHUTDOWN_COMMAND,
        requestId: 'after-exit',
      }),
    ).rejects.toThrow();
  } finally {
    releaseTeardown();
    await daemon.runtime.close();
  }
});
