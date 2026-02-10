export type BackgroundJob = {
  id: string;
  label: string;
  createdAtMs: number;
  run: () => Promise<void>;
};

export type BackgroundQueue = {
  enqueue: (job: BackgroundJob) => void;
  size: () => number;
};

export function createBackgroundQueue(opts?: { concurrency?: number }): BackgroundQueue {
  const concurrency = Math.max(1, Math.floor(opts?.concurrency ?? 1));
  const queue: BackgroundJob[] = [];
  let running = 0;

  const pump = () => {
    while (running < concurrency) {
      const job = queue.shift();
      if (!job) {
        return;
      }
      running += 1;
      void job
        .run()
        .catch(() => {
          // Swallow: job should report its own failure to the user/channel.
        })
        .finally(() => {
          running -= 1;
          pump();
        });
    }
  };

  return {
    enqueue: (job) => {
      queue.push(job);
      pump();
    },
    size: () => queue.length + running,
  };
}
