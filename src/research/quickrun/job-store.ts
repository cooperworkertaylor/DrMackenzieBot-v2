import { openResearchDb, type ResearchDb } from "../db.js";

export type QuickrunJobStatus = "queued" | "running" | "completed" | "failed";

export type QuickrunJobRecord<TPayload = unknown> = {
  id: string;
  jobType: string;
  status: QuickrunJobStatus;
  payload: TPayload;
  progressNote?: string;
  progressUpdatedAtMs?: number;
  runAfterMs: number;
  attempts: number;
  maxAttempts: number;
  lockedBy?: string;
  lockedAtMs?: number;
  heartbeatAtMs?: number;
  completedAtMs?: number;
  lastError?: string;
  createdAtMs: number;
  updatedAtMs: number;
};

export type EnqueueQuickrunJobParams<TPayload> = {
  id: string;
  jobType: string;
  payload: TPayload;
  runAfterMs: number;
  maxAttempts?: number;
};

const parseJson = <T>(value: string): T => JSON.parse(value) as T;

const withImmediateTransaction = <T>(db: ResearchDb, fn: () => T): T => {
  db.exec("BEGIN IMMEDIATE;");
  try {
    const result = fn();
    db.exec("COMMIT;");
    return result;
  } catch (err) {
    try {
      db.exec("ROLLBACK;");
    } catch {
      // ignore rollback failures
    }
    throw err;
  }
};

const mapRow = <TPayload>(row: {
  id: string;
  job_type: string;
  status: QuickrunJobStatus;
  payload: string;
  progress_note?: string;
  progress_updated_at_ms?: number;
  run_after_ms: number;
  attempts: number;
  max_attempts: number;
  locked_by?: string;
  locked_at_ms?: number;
  heartbeat_at_ms?: number;
  completed_at_ms?: number;
  last_error?: string;
  created_at_ms: number;
  updated_at_ms: number;
}): QuickrunJobRecord<TPayload> => ({
  id: row.id,
  jobType: row.job_type,
  status: row.status,
  payload: parseJson<TPayload>(row.payload),
  progressNote: row.progress_note || undefined,
  progressUpdatedAtMs: row.progress_updated_at_ms ?? undefined,
  runAfterMs: row.run_after_ms,
  attempts: row.attempts,
  maxAttempts: row.max_attempts,
  lockedBy: row.locked_by || undefined,
  lockedAtMs: row.locked_at_ms ?? undefined,
  heartbeatAtMs: row.heartbeat_at_ms ?? undefined,
  completedAtMs: row.completed_at_ms ?? undefined,
  lastError: row.last_error || undefined,
  createdAtMs: row.created_at_ms,
  updatedAtMs: row.updated_at_ms,
});

export class QuickrunJobStore {
  constructor(private readonly db: ResearchDb) {}

  static open(dbPath?: string): QuickrunJobStore {
    return new QuickrunJobStore(openResearchDb(dbPath));
  }

  enqueue<TPayload>(params: EnqueueQuickrunJobParams<TPayload>): QuickrunJobRecord<TPayload> {
    const now = Date.now();
    this.db
      .prepare(
        `INSERT OR REPLACE INTO quickrun_jobs (
           id, job_type, status, payload, progress_note, progress_updated_at_ms, run_after_ms, attempts, max_attempts,
           locked_by, locked_at_ms, heartbeat_at_ms, completed_at_ms, last_error,
           created_at_ms, updated_at_ms
         ) VALUES (?, ?, 'queued', ?, '', NULL, ?, 0, ?, '', NULL, NULL, NULL, '', ?, ?)`,
      )
      .run(
        params.id,
        params.jobType,
        JSON.stringify(params.payload),
        params.runAfterMs,
        params.maxAttempts ?? 3,
        now,
        now,
      );
    return this.getById<TPayload>(params.id)!;
  }

  getById<TPayload>(id: string): QuickrunJobRecord<TPayload> | null {
    const row = this.db
      .prepare(
        `SELECT
           id, job_type, status, payload, progress_note, progress_updated_at_ms, run_after_ms, attempts, max_attempts,
           locked_by, locked_at_ms, heartbeat_at_ms, completed_at_ms, last_error,
           created_at_ms, updated_at_ms
         FROM quickrun_jobs
         WHERE id = ?`,
      )
      .get(id) as
      | {
          id: string;
          job_type: string;
          status: QuickrunJobStatus;
          payload: string;
          progress_note?: string;
          progress_updated_at_ms?: number;
          run_after_ms: number;
          attempts: number;
          max_attempts: number;
          locked_by?: string;
          locked_at_ms?: number;
          heartbeat_at_ms?: number;
          completed_at_ms?: number;
          last_error?: string;
          created_at_ms: number;
          updated_at_ms: number;
        }
      | undefined;
    return row ? mapRow<TPayload>(row) : null;
  }

  claimNext<TPayload>(params: {
    jobType: string;
    workerId: string;
    nowMs?: number;
    staleAfterMs?: number;
  }): QuickrunJobRecord<TPayload> | null {
    const nowMs = params.nowMs ?? Date.now();
    const staleBeforeMs = nowMs - (params.staleAfterMs ?? 60_000);
    return withImmediateTransaction(this.db, () => {
      const row = this.db
        .prepare(
          `SELECT
             id, job_type, status, payload, progress_note, progress_updated_at_ms, run_after_ms, attempts, max_attempts,
             locked_by, locked_at_ms, heartbeat_at_ms, completed_at_ms, last_error,
             created_at_ms, updated_at_ms
           FROM quickrun_jobs
           WHERE job_type = ?
             AND (
               (status = 'queued' AND run_after_ms <= ?)
               OR (status = 'running' AND coalesce(heartbeat_at_ms, locked_at_ms, 0) <= ?)
             )
           ORDER BY
             CASE status WHEN 'running' THEN 0 ELSE 1 END,
             run_after_ms ASC,
             created_at_ms ASC
           LIMIT 1`,
        )
        .get(params.jobType, nowMs, staleBeforeMs) as
        | {
            id: string;
            job_type: string;
            status: QuickrunJobStatus;
            payload: string;
            progress_note?: string;
            progress_updated_at_ms?: number;
            run_after_ms: number;
            attempts: number;
            max_attempts: number;
            locked_by?: string;
            locked_at_ms?: number;
            heartbeat_at_ms?: number;
            completed_at_ms?: number;
            last_error?: string;
            created_at_ms: number;
            updated_at_ms: number;
          }
        | undefined;
      if (!row) return null;

      const attempts = row.status === "queued" ? row.attempts + 1 : row.attempts;
      this.db
        .prepare(
          `UPDATE quickrun_jobs
           SET status = 'running',
               attempts = ?,
               locked_by = ?,
               locked_at_ms = ?,
               heartbeat_at_ms = ?,
               updated_at_ms = ?
           WHERE id = ?`,
        )
        .run(attempts, params.workerId, nowMs, nowMs, nowMs, row.id);
      return this.getById<TPayload>(row.id);
    });
  }

  heartbeat(params: { id: string; workerId: string; nowMs?: number }): void {
    const nowMs = params.nowMs ?? Date.now();
    this.db
      .prepare(
        `UPDATE quickrun_jobs
         SET heartbeat_at_ms = ?, updated_at_ms = ?
         WHERE id = ? AND status = 'running' AND locked_by = ?`,
      )
      .run(nowMs, nowMs, params.id, params.workerId);
  }

  setProgress(params: { id: string; workerId: string; note: string; nowMs?: number }): void {
    const nowMs = params.nowMs ?? Date.now();
    this.db
      .prepare(
        `UPDATE quickrun_jobs
         SET progress_note = ?, progress_updated_at_ms = ?, updated_at_ms = ?
         WHERE id = ? AND status = 'running' AND locked_by = ?`,
      )
      .run(params.note, nowMs, nowMs, params.id, params.workerId);
  }

  markCompleted(params: { id: string; workerId: string; nowMs?: number }): void {
    const nowMs = params.nowMs ?? Date.now();
    this.db
      .prepare(
        `UPDATE quickrun_jobs
         SET status = 'completed',
             locked_by = '',
             locked_at_ms = NULL,
             completed_at_ms = ?,
             heartbeat_at_ms = ?,
             progress_note = 'Completed',
             progress_updated_at_ms = ?,
             updated_at_ms = ?
         WHERE id = ? AND status = 'running' AND locked_by = ?`,
      )
      .run(nowMs, nowMs, nowMs, nowMs, params.id, params.workerId);
  }

  markFailed(params: {
    id: string;
    workerId: string;
    error: string;
    nowMs?: number;
    retryDelayMs?: number;
  }): QuickrunJobRecord | null {
    const nowMs = params.nowMs ?? Date.now();
    return withImmediateTransaction(this.db, () => {
      const row = this.db
        .prepare(
          `SELECT attempts, max_attempts
           FROM quickrun_jobs
           WHERE id = ? AND status = 'running' AND locked_by = ?`,
        )
        .get(params.id, params.workerId) as { attempts: number; max_attempts: number } | undefined;
      if (!row) return this.getById(params.id);

      if (row.attempts < row.max_attempts) {
        this.db
          .prepare(
            `UPDATE quickrun_jobs
             SET status = 'queued',
                 run_after_ms = ?,
                 locked_by = '',
                 locked_at_ms = NULL,
                 heartbeat_at_ms = NULL,
                 progress_note = '',
                 progress_updated_at_ms = NULL,
                 last_error = ?,
                 updated_at_ms = ?
             WHERE id = ?`,
          )
          .run(
            nowMs + Math.max(1_000, params.retryDelayMs ?? 15_000),
            params.error,
            nowMs,
            params.id,
          );
      } else {
        this.db
          .prepare(
            `UPDATE quickrun_jobs
             SET status = 'failed',
                 locked_by = '',
                 locked_at_ms = NULL,
                 completed_at_ms = ?,
                 heartbeat_at_ms = ?,
                 progress_note = 'Failed',
                 progress_updated_at_ms = ?,
                 last_error = ?,
                 updated_at_ms = ?
             WHERE id = ?`,
          )
          .run(nowMs, nowMs, nowMs, params.error, nowMs, params.id);
      }
      return this.getById(params.id);
    });
  }

  pendingCount(jobType?: string): number {
    const row = jobType
      ? (this.db
          .prepare(
            `SELECT count(*) AS count
             FROM quickrun_jobs
             WHERE job_type = ? AND status IN ('queued', 'running')`,
          )
          .get(jobType) as { count: number })
      : (this.db
          .prepare(
            `SELECT count(*) AS count
             FROM quickrun_jobs
             WHERE status IN ('queued', 'running')`,
          )
          .get() as { count: number });
    return Number(row.count ?? 0);
  }

  list<TPayload>(params?: {
    jobType?: string;
    status?: QuickrunJobStatus;
    limit?: number;
  }): QuickrunJobRecord<TPayload>[] {
    const clauses: string[] = [];
    const values: Array<string | number> = [];
    if (params?.jobType) {
      clauses.push("job_type = ?");
      values.push(params.jobType);
    }
    if (params?.status) {
      clauses.push("status = ?");
      values.push(params.status);
    }
    const limit = Math.max(1, Math.floor(params?.limit ?? 100));
    values.push(limit);
    const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    const rows = this.db
      .prepare(
        `SELECT
           id, job_type, status, payload, progress_note, progress_updated_at_ms, run_after_ms, attempts, max_attempts,
           locked_by, locked_at_ms, heartbeat_at_ms, completed_at_ms, last_error,
           created_at_ms, updated_at_ms
         FROM quickrun_jobs
         ${where}
         ORDER BY updated_at_ms DESC, created_at_ms DESC
         LIMIT ?`,
      )
      .all(...values) as Array<{
      id: string;
      job_type: string;
      status: QuickrunJobStatus;
      payload: string;
      progress_note?: string;
      progress_updated_at_ms?: number;
      run_after_ms: number;
      attempts: number;
      max_attempts: number;
      locked_by?: string;
      locked_at_ms?: number;
      heartbeat_at_ms?: number;
      completed_at_ms?: number;
      last_error?: string;
      created_at_ms: number;
      updated_at_ms: number;
    }>;
    return rows.map((row) => mapRow<TPayload>(row));
  }

  requeueFailed(params: { id: string; nowMs?: number }): QuickrunJobRecord | null {
    const nowMs = params.nowMs ?? Date.now();
    return withImmediateTransaction(this.db, () => {
      const row = this.db
        .prepare(
          `SELECT id
           FROM quickrun_jobs
           WHERE id = ? AND status = 'failed'`,
        )
        .get(params.id) as { id: string } | undefined;
      if (!row) {
        return this.getById(params.id);
      }
      this.db
        .prepare(
          `UPDATE quickrun_jobs
           SET status = 'queued',
               run_after_ms = ?,
               locked_by = '',
               locked_at_ms = NULL,
               heartbeat_at_ms = NULL,
               completed_at_ms = NULL,
               progress_note = '',
               progress_updated_at_ms = NULL,
               last_error = '',
               updated_at_ms = ?
           WHERE id = ?`,
        )
        .run(nowMs, nowMs, params.id);
      return this.getById(params.id);
    });
  }
}
