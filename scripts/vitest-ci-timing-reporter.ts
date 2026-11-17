// TEMPORARY CI PROBE. Records when each test file starts and finishes, on which worker, and the
// per-file phase durations Vitest measured. Reads AGENT_DEVICE_CI_TIMING_FILE; writes nothing when
// it is unset, so no other lane is touched. Delete with the CI artifact step that sets it.

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { Reporter, TestModule, Vitest } from 'vitest/node';

/** `Reporter` plus the hook Vitest calls once the coverage provider has finished reporting. */
type CoverageReporter = Reporter & { onFinishedReportCoverage(): void | Promise<void> };

type FileTiming = {
  project: string;
  file: string;
  worker: number;
  queuedAt: number;
  startedAt: number;
  endedAt: number;
  setup: number;
  collect: number;
  tests: number;
  environment: number;
  prepare: number;
  testsCount: number;
};

const origin = performance.now();
const now = (): number => Math.round(performance.now() - origin);

function sum(values: readonly number[]): number {
  return values.reduce((acc, value) => acc + value, 0);
}

export default function ciTimingReporter(): CoverageReporter {
  const destination = process.env.AGENT_DEVICE_CI_TIMING_FILE;
  const pending = new Map<string, FileTiming>();
  const list: FileTiming[] = [];
  let initAt = 0;
  let runStartAt = 0;
  let runEndAt = 0;
  let coverageAt = 0;
  let vitest!: Vitest;

  function write(): void {
    if (!destination) return;
    fs.mkdirSync(path.dirname(destination), { recursive: true });
    const byWorker = new Map<number, number>();
    for (const entry of list) byWorker.set(entry.worker, (byWorker.get(entry.worker) ?? 0) + 1);
    const span = (entry: FileTiming): number => entry.endedAt - entry.startedAt;
    const sorted = [...list].sort((a, b) => span(b) - span(a));
    fs.writeFileSync(
      destination,
      JSON.stringify({
        initAt,
        runStartAt,
        runEndAt,
        coverageAt,
        availableParallelism: os.availableParallelism(),
        cpus: os.cpus().length,
        maxWorkers: vitest.config.maxWorkers,
        isolate: vitest.config.isolate,
        pool: vitest.config.pool,
        coverageEnabled: vitest.config.coverage?.enabled,
        shard: vitest.config.shard,
        filesPerWorker: Object.fromEntries([...byWorker.entries()].sort((a, b) => a[0] - b[0])),
        slowest: sorted.slice(0, 40).map((entry) => ({ ...entry, span: span(entry) })),
        latestFinishers: [...list]
          .sort((a, b) => b.endedAt - a.endedAt)
          .slice(0, 20)
          .map((entry) => ({ file: entry.file, span: span(entry), endedAt: entry.endedAt })),
        totals: {
          files: list.length,
          setup: sum(list.map((entry) => entry.setup)),
          collect: sum(list.map((entry) => entry.collect)),
          tests: sum(list.map((entry) => entry.tests)),
          environment: sum(list.map((entry) => entry.environment)),
          prepare: sum(list.map((entry) => entry.prepare)),
          span: sum(list.map(span)),
        },
        files: list,
      }),
    );
  }

  function track(module: TestModule): FileTiming | undefined {
    if (!destination) return undefined;
    const key = `${module.project.name}\u0000${module.moduleId}`;
    let entry = pending.get(key);
    if (!entry) {
      entry = {
        project: module.project.name,
        file: module.relativeModuleId,
        worker: 0,
        queuedAt: now(),
        startedAt: 0,
        endedAt: 0,
        setup: 0,
        collect: 0,
        tests: 0,
        environment: 0,
        prepare: 0,
        testsCount: 0,
      };
      pending.set(key, entry);
    }
    return entry;
  }

  return {
    onInit(ctx: Vitest): void {
      vitest = ctx;
      initAt = now();
    },
    onTestRunStart(): void {
      runStartAt = now();
    },
    onTestModuleQueued(module: TestModule): void {
      track(module);
    },
    onTestModuleStart(module: TestModule): void {
      const entry = track(module);
      if (!entry || entry.startedAt !== 0) return;
      entry.startedAt = now();
      entry.worker = module.diagnostic().concurrencyId;
    },
    onTestModuleEnd(module: TestModule): void {
      const entry = track(module);
      if (!entry || entry.endedAt !== 0) return;
      const diagnostic = module.diagnostic();
      entry.endedAt = now();
      entry.worker = diagnostic.concurrencyId;
      entry.setup = Math.round(diagnostic.setupDuration);
      entry.collect = Math.round(diagnostic.collectDuration);
      entry.tests = Math.round(diagnostic.duration);
      entry.environment = Math.round(diagnostic.environmentSetupDuration);
      entry.prepare = Math.round(diagnostic.prepareDuration);
      entry.testsCount = [...module.children.allTests()].length;
      pending.delete(`${module.project.name}\u0000${module.moduleId}`);
      list.push(entry);
    },
    onTestRunEnd(): void {
      runEndAt = now();
      write();
    },
    onFinishedReportCoverage(): void {
      coverageAt = now();
      write();
    },
  };
}
