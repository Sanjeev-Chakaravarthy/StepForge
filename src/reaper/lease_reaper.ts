import { Redis } from 'ioredis';
import type { Pool } from '../db/client.js';
import { RedisLeaseManager } from '../redis/lease.js';
import type { Run } from '../types/domain.js';

export interface LeaseReaperOptions {
  reaperIntervalMs?: number;
  batchSize?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
}

export class LeaseReaper {
  private pool: Pool;
  private redis: Redis;
  private leaseManager: RedisLeaseManager;
  private reaperIntervalMs: number;
  private batchSize: number;
  private baseDelayMs: number;
  private maxDelayMs: number;
  private isRunning: boolean = false;
  private timer: NodeJS.Timeout | null = null;
  private isReaping: boolean = false;
  private activeReapPromise: Promise<number> | null = null;

  constructor(pool: Pool, redis: Redis, options?: LeaseReaperOptions) {
    this.pool = pool;
    this.redis = redis;
    this.leaseManager = new RedisLeaseManager(redis);
    this.reaperIntervalMs = options?.reaperIntervalMs ?? 1000;
    this.batchSize = options?.batchSize ?? 20;
    this.baseDelayMs = options?.baseDelayMs ?? 500;
    this.maxDelayMs = options?.maxDelayMs ?? 10000;
  }

  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.scheduleNext();
  }

  public async stop(): Promise<void> {
    this.isRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.activeReapPromise) {
      await this.activeReapPromise.catch(() => {});
    }
  }

  private scheduleNext(): void {
    if (!this.isRunning) return;
    this.timer = setTimeout(async () => {
      try {
        await this.reapOnce();
      } catch (err: any) {
        if (
          !this.isRunning ||
          err?.message?.includes('Connection is closed') ||
          err?.message?.includes('Cannot use a pool after calling end')
        ) {
          return;
        }
        console.error('Error in LeaseReaper loop:', err);
      } finally {
        this.scheduleNext();
      }
    }, this.reaperIntervalMs);
  }

  /**
   * Identifies stale RUNNING runs whose lease has expired,
   * purges them from Redis, and either reschedules with backoff+jitter
   * or transitions to DEAD_LETTER.
   */
  public reapOnce(): Promise<number> {
    if (this.isReaping) return Promise.resolve(0);
    this.isReaping = true;

    const promise = this.executeReapOnce().finally(() => {
      this.isReaping = false;
      if (this.activeReapPromise === promise) {
        this.activeReapPromise = null;
      }
    });
    this.activeReapPromise = promise;
    return promise;
  }

  private async executeReapOnce(): Promise<number> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      const { rows: staleRuns } = await client.query<Run>(
        `SELECT id, tenant_id, workflow_name, idempotency_key, attempt_count, max_attempts, input
         FROM runs
         WHERE status = 'RUNNING' AND lease_expires_at < NOW()
         ORDER BY lease_expires_at ASC
         LIMIT $1
         FOR UPDATE SKIP LOCKED`,
        [this.batchSize]
      );

      if (staleRuns.length === 0) {
        await client.query('COMMIT');
        return 0;
      }

      for (const run of staleRuns) {
        // 1. Evict from Redis active leases
        await this.leaseManager.releaseLease(run.tenant_id, run.id);

        // 2. Decide if retryable
        if (run.attempt_count < run.max_attempts) {
          // Exponential backoff + decorrelated full jitter
          const backoff = Math.min(
            this.maxDelayMs,
            this.baseDelayMs * Math.pow(2, run.attempt_count)
          );
          const jitteredDelay = backoff > 0 ? Math.floor(Math.random() * backoff) : 0;

          await client.query(
            `UPDATE runs
             SET status = 'RETRYING'::run_status,
                 current_lease_worker_id = NULL,
                 lease_expires_at = NULL,
                 scheduled_at = NOW() + ($1 || ' milliseconds')::interval,
                 updated_at = NOW()
             WHERE id = $2`,
            [jitteredDelay, run.id]
          );

          // Re-publish to outbox with available_at set to scheduled retry time
          const outboxPayload = {
            run_id: run.id,
            tenant_id: run.tenant_id,
            workflow_name: run.workflow_name,
            idempotency_key: run.idempotency_key,
            input: run.input,
          };

          await client.query(
            `INSERT INTO outbox_events (tenant_id, event_type, payload, status, available_at)
             VALUES ($1, 'RUN_RETRY', $2::jsonb, 'PENDING', NOW() + ($3 || ' milliseconds')::interval)`,
            [run.tenant_id, JSON.stringify(outboxPayload), jitteredDelay]
          );
        } else {
          // Max attempts exceeded -> DEAD_LETTER
          const errPayload = {
            message: 'Worker lease expired and max attempts exceeded (Worker Crash/OOM)',
            attempt_count: run.attempt_count,
            reaped_at: new Date().toISOString(),
          };

          await client.query(
            `UPDATE runs
             SET status = 'DEAD_LETTER'::run_status,
                 current_lease_worker_id = NULL,
                 lease_expires_at = NULL,
                 error = $1::jsonb,
                 updated_at = NOW()
             WHERE id = $2`,
            [JSON.stringify(errPayload), run.id]
          );
        }
      }

      await client.query('COMMIT');
      return staleRuns.length;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }
}
