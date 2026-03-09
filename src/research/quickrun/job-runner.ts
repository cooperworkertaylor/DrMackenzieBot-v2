import { getLogger } from "../../logging/logger.js";
import { QuickrunJobStore, type QuickrunJobRecord } from "./job-store.js";

export type QuickrunJobHandler<TPayload> = (job: QuickrunJobRecord<TPayload>) => Promise<void>;

export type QuickrunJobRunner<TPayload> = {
  start: () => void;
  poke: () => void;
  stop: () => void;
  enqueue: (params: {
    id: string;
    jobType: string;
    payload: TPayload;
    runAfterMs: number;
    maxAttempts?: number;
  }) => QuickrunJobRecord<TPayload>;
};

export function createQuickrunJobRunner<TPayload>(params: {
  store: QuickrunJobStore;
  jobType: string;
  handler: QuickrunJobHandler<TPayload>;
  pollIntervalMs?: number;
  staleAfterMs?: number;
  concurrency?: number;
  workerId?: string;
}): QuickrunJobRunner<TPayload> {
  const workerId =
    params.workerId ??
    `worker-${process.pid}-${Math.random().toString(16).slice(2, 10)}`;
  const pollIntervalMs = Math.max(250, params.pollIntervalMs ?? 1_000);
  const staleAfterMs = Math.max(5_000, params.staleAfterMs ?? 60_000);
  const concurrency = Math.max(1, Math.floor(params.concurrency ?? 1));
  const logger = getLogger();

  let running = false;
  let stopped = true;
  let runningCount = 0;
  let timer: NodeJS.Timeout | null = null;

  const schedule = (ms: number) => {
    if (stopped) return;
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      timer = null;
      void pump();
    }, ms);
  };

  const runJob = async (job: QuickrunJobRecord<TPayload>) => {
    runningCount += 1;
    const heartbeatEveryMs = Math.max(5_000, Math.floor(staleAfterMs / 3));
    const heartbeatTimer = setInterval(() => {
      params.store.heartbeat({ id: job.id, workerId });
    }, heartbeatEveryMs);
    try {
      await params.handler(job);
      params.store.markCompleted({ id: job.id, workerId });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      logger.warn({ err, jobId: job.id, jobType: job.jobType }, "quickrun job failed");
      params.store.markFailed({
        id: job.id,
        workerId,
        error: message,
      });
    } finally {
      clearInterval(heartbeatTimer);
      runningCount -= 1;
      void pump();
    }
  };

  const pump = async () => {
    if (stopped || running) return;
    running = true;
    try {
      while (!stopped && runningCount < concurrency) {
        const job = params.store.claimNext<TPayload>({
          jobType: params.jobType,
          workerId,
          staleAfterMs,
        });
        if (!job) {
          schedule(pollIntervalMs);
          break;
        }
        void runJob(job);
      }
    } finally {
      running = false;
    }
  };

  return {
    start: () => {
      if (!stopped) return;
      stopped = false;
      schedule(0);
    },
    poke: () => {
      if (stopped) return;
      schedule(0);
    },
    stop: () => {
      stopped = true;
      if (timer) clearTimeout(timer);
      timer = null;
    },
    enqueue: (job) => {
      const record = params.store.enqueue(job);
      if (!stopped) schedule(0);
      return record;
    },
  };
}
