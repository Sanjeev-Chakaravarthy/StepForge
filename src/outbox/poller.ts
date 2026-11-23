import { Redis } from 'ioredis';
import type { Pool } from '../db/client.js';
import type { OutboxEvent } from '../types/domain.js';
import { publishToRedisStream } from './publisher.js';

export interface OutboxPollerOptions {
  pollIntervalMs?: number;
  batchSize?: number;
  streamKey?: string;
}

export class OutboxPoller {
  private pool: Pool;
  private redis: Redis;
  private pollIntervalMs: number;
  private batchSize: number;
  private streamKey: string;
  private isRunning: boolean = false;
  private timer: NodeJS.Timeout | null = null;
  private isPolling: boolean = false;

  constructor(pool: Pool, redis: Redis, options?: OutboxPollerOptions) {
    this.pool = pool;
    this.redis = redis;
    this.pollIntervalMs = options?.pollIntervalMs ?? 100;
    this.batchSize = options?.batchSize ?? 50;
    this.streamKey = options?.streamKey ?? 'stepforge:stream:tasks';
  }

  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.scheduleNext();
  }

  public stop(): void {
    this.isRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  private scheduleNext(): void {
    if (!this.isRunning) return;
    this.timer = setTimeout(async () => {
      try {
        await this.pollOnce();
      } catch (err: any) {
        if (
          !this.isRunning ||
          err?.message?.includes('Connection is closed') ||
          err?.message?.includes('Cannot use a pool after calling end')
        ) {
          return;
        }
        console.error('Error during outbox polling:', err);
      } finally {
        this.scheduleNext();
      }
    }, this.pollIntervalMs);
  }

  /**
   * Executes a single poll cycle with FOR UPDATE SKIP LOCKED
   * Returns the count of processed outbox events.
   */
  public async pollOnce(): Promise<number> {
    if (this.isPolling) return 0;
    this.isPolling = true;

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      const { rows: events } = await client.query<OutboxEvent>(
        `SELECT id, tenant_id, event_type, payload, status, available_at, retry_count, last_error, created_at, published_at
         FROM outbox_events
         WHERE status = 'PENDING' AND available_at <= NOW()
         ORDER BY id ASC
         LIMIT $1
         FOR UPDATE SKIP LOCKED`,
        [this.batchSize]
      );

      if (events.length === 0) {
        await client.query('COMMIT');
        return 0;
      }

      const publishedIds: number[] = [];
      const runIdsToQueue: string[] = [];

      for (const event of events) {
        try {
          await publishToRedisStream(this.redis, this.streamKey, event);
          publishedIds.push(event.id);
          if (
            (event.event_type === 'RUN_CREATED' || event.event_type === 'RUN_RETRY') &&
            event.payload &&
            typeof event.payload === 'object' &&
            'run_id' in event.payload
          ) {
            runIdsToQueue.push(event.payload.run_id as string);
          }
        } catch (error: any) {
          await client.query(
            `UPDATE outbox_events
             SET retry_count = retry_count + 1,
                 last_error = $1,
                 status = CASE WHEN retry_count + 1 >= 5 THEN 'FAILED'::outbox_status ELSE 'PENDING'::outbox_status END
             WHERE id = $2`,
            [error?.message || 'Publish error', event.id]
          );
        }
      }

      if (publishedIds.length > 0) {
        await client.query(
          `UPDATE outbox_events
           SET status = 'PUBLISHED'::outbox_status,
               published_at = NOW()
           WHERE id = ANY($1::bigint[])`,
          [publishedIds]
        );

        if (runIdsToQueue.length > 0) {
          await client.query(
            `UPDATE runs
             SET status = 'QUEUED'::run_status,
                 updated_at = NOW()
             WHERE id = ANY($1::uuid[]) AND status IN ('PENDING'::run_status, 'RETRYING'::run_status)`,
            [runIdsToQueue]
          );
        }
      }

      await client.query('COMMIT');
      return events.length;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
      this.isPolling = false;
    }
  }
}
