import crypto from 'node:crypto';
import { Redis } from 'ioredis';
import type { Pool } from '../db/client.js';
import { RedisLeaseManager } from '../redis/lease.js';
import type { Run, Tenant } from '../types/domain.js';
import { DelayQueue } from './delay_queue.js';
import {
  ApprovalRejectedError,
  DurableStepContext,
  RunSuspendedError,
  type StepContext,
} from '../step/context.js';
import type { WorkflowRegistry } from '../workflow/registry.js';

export type WorkflowHandler = (
  run: Run,
  step: StepContext
) => Promise<Record<string, unknown> | void>;

export interface WorkerOptions {
  workerId?: string;
  streamKey?: string;
  groupName?: string;
  leaseTtlMs?: number;
  heartbeatIntervalMs?: number;
  batchSize?: number;
  blockTimeoutMs?: number;
  handler?: WorkflowHandler;
  registry?: WorkflowRegistry;
  delayQueue?: DelayQueue;
}

export class Worker {
  private pool: Pool;
  private redis: Redis;
  private leaseManager: RedisLeaseManager;
  private delayQueue: DelayQueue;
  private ownsDelayQueue: boolean;

  public readonly workerId: string;
  public readonly streamKey: string;
  public readonly groupName: string;
  public readonly leaseTtlMs: number;
  public readonly heartbeatIntervalMs: number;
  public readonly batchSize: number;
  public readonly blockTimeoutMs: number;
  private handler: WorkflowHandler;
  private registry?: WorkflowRegistry;

  private isRunning: boolean = false;
  private activeHeartbeats: Map<string, NodeJS.Timeout> = new Map();
  private processingPromise: Promise<void> | null = null;

  constructor(pool: Pool, redis: Redis, options?: WorkerOptions) {
    this.pool = pool;
    this.redis = redis;
    this.leaseManager = new RedisLeaseManager(redis);
    this.ownsDelayQueue = !options?.delayQueue;
    this.delayQueue = options?.delayQueue ?? new DelayQueue(redis, { streamKey: options?.streamKey });

    this.workerId = options?.workerId ?? `worker-${crypto.randomUUID().slice(0, 8)}`;
    this.streamKey = options?.streamKey ?? 'stepforge:stream:tasks';
    this.groupName = options?.groupName ?? 'stepforge:workers';
    this.leaseTtlMs = options?.leaseTtlMs ?? 10000;
    this.heartbeatIntervalMs = options?.heartbeatIntervalMs ?? 3000;
    this.batchSize = options?.batchSize ?? 10;
    this.blockTimeoutMs = options?.blockTimeoutMs ?? 500;
    this.handler = options?.handler ?? (async () => ({ success: true }));
    this.registry = options?.registry;
  }

  /**
   * Initializes the Redis Stream consumer group if it doesn't already exist.
   */
  public async initGroup(): Promise<void> {
    try {
      await this.redis.xgroup('CREATE', this.streamKey, this.groupName, '0', 'MKSTREAM');
    } catch (err: any) {
      if (!err.message?.includes('BUSYGROUP')) {
        throw err;
      }
    }
  }

  public async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;
    await this.initGroup();
    if (this.ownsDelayQueue) {
      this.delayQueue.start();
    }

    this.processingPromise = (async () => {
      while (this.isRunning) {
        try {
          await this.processNextBatch();
        } catch (err: any) {
          if (!this.isRunning || err?.message?.includes('Connection is closed')) break;
          console.error(`[Worker ${this.workerId}] Error in consumer loop:`, err);
          await new Promise((r) => setTimeout(r, 200));
        }
      }
    })();
  }

  public async stop(): Promise<void> {
    this.isRunning = false;
    if (this.ownsDelayQueue) {
      await this.delayQueue.stop();
    }

    if (this.processingPromise) {
      await this.processingPromise;
      this.processingPromise = null;
    }

    // Clear any leftover heartbeat timers after in-flight batch settles
    for (const [runId, timer] of this.activeHeartbeats.entries()) {
      clearInterval(timer);
      this.activeHeartbeats.delete(runId);
    }
  }

  /**
   * Pulls next batch of messages from consumer group, enforces tenant concurrency,
   * transitions run states in Postgres, and invokes workflow handler.
   */
  public async processNextBatch(): Promise<number> {
    const rawStreams = (await (this.redis as any).xreadgroup(
      'GROUP',
      this.groupName,
      this.workerId,
      'BLOCK',
      this.blockTimeoutMs,
      'COUNT',
      this.batchSize,
      'STREAMS',
      this.streamKey,
      '>'
    )) as [string, [string, string[]][]][] | null;

    if (!rawStreams || rawStreams.length === 0) {
      return 0;
    }

    let processedCount = 0;

    for (const [, messages] of rawStreams) {
      for (const [messageId, fields] of messages) {
        processedCount++;
        await this.handleStreamMessage(messageId, fields);
      }
    }

    return processedCount;
  }

  private async handleStreamMessage(messageId: string, fields: string[]): Promise<void> {
    const parsedFields: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) {
      const k = fields[i];
      const v = fields[i + 1];
      if (k !== undefined && v !== undefined) {
        parsedFields[k] = v;
      }
    }

    const eventId = parsedFields.eventId || '';
    const tenantId = parsedFields.tenantId || '';
    const eventType = parsedFields.eventType || '';
    let payload: Record<string, unknown> = {};

    try {
      payload = JSON.parse(parsedFields.payload || '{}');
    } catch {
      payload = {};
    }

    const runId = (payload.run_id as string) || '';
    if (!runId || !tenantId) {
      await this.redis.xack(this.streamKey, this.groupName, messageId);
      return;
    }

    // 1. Fetch tenant max_concurrency
    const { rows: tenantRows } = await this.pool.query<Tenant>(
      `SELECT max_concurrency FROM tenants WHERE id = $1`,
      [tenantId]
    );

    const maxConcurrency = tenantRows[0]?.max_concurrency ?? 5;

    // 2. Try to acquire lease in Redis
    const leaseResult = await this.leaseManager.acquireLease(
      tenantId,
      runId,
      this.leaseTtlMs,
      maxConcurrency
    );

    if (!leaseResult.acquired) {
      // Quota exceeded for this tenant!
      // Acknowledge this message in stream to clear consumer group PEL
      await this.redis.xack(this.streamKey, this.groupName, messageId);

      // Requeue via delayed sorted set with jittered backoff (100ms - 300ms)
      const backoffMs = 100 + Math.floor(Math.random() * 200);
      await this.delayQueue.schedule(
        {
          eventId,
          tenantId,
          eventType,
          payload,
        },
        backoffMs
      );
      return;
    }

    // Lease acquired! ACK message from stream
    await this.redis.xack(this.streamKey, this.groupName, messageId);

    const expireAtMs = leaseResult.expireAtMs ?? Date.now() + this.leaseTtlMs;

    // 3. Transition Postgres run status to RUNNING
    const { rows: updatedRunRows } = await this.pool.query<Run>(
      `UPDATE runs
       SET status = 'RUNNING'::run_status,
           current_lease_worker_id = $1,
           lease_expires_at = TO_TIMESTAMP($2 / 1000.0),
           attempt_count = attempt_count + 1,
           updated_at = NOW()
       WHERE id = $3 AND status IN ('PENDING', 'QUEUED', 'RETRYING')
       RETURNING *`,
      [this.workerId, expireAtMs, runId]
    );

    const run = updatedRunRows[0];
    if (!run) {
      // Run was cancelled or already in an invalid state; release lease and exit
      await this.leaseManager.releaseLease(tenantId, runId);
      return;
    }

    // 4. Start periodic lease heartbeat
    const heartbeatTimer = setInterval(async () => {
      try {
        const renewal = await this.leaseManager.acquireLease(
          tenantId,
          runId,
          this.leaseTtlMs,
          maxConcurrency
        );
        if (renewal.acquired && renewal.expireAtMs) {
          await this.pool.query(
            `UPDATE runs
             SET lease_expires_at = TO_TIMESTAMP($1 / 1000.0),
                 updated_at = NOW()
             WHERE id = $2 AND status = 'RUNNING' AND current_lease_worker_id = $3`,
            [renewal.expireAtMs, runId, this.workerId]
          );
        }
      } catch (err) {
        console.error(`[Worker ${this.workerId}] Heartbeat renewal failed for run ${runId}:`, err);
      }
    }, this.heartbeatIntervalMs);

    this.activeHeartbeats.set(runId, heartbeatTimer);

    // 5. Execute workflow handler
    try {
      const stepContext = new DurableStepContext(this.pool, run.id, {
        onSuspend: async () => {
          clearInterval(heartbeatTimer);
          this.activeHeartbeats.delete(runId);
          await this.leaseManager.releaseLease(tenantId, runId);
        },
      });
      const workflowFn = this.registry?.get(run.workflow_name) ?? this.handler;
      const output = await workflowFn(run, stepContext);

      // Stop heartbeat
      clearInterval(heartbeatTimer);
      this.activeHeartbeats.delete(runId);

      // Transition to COMPLETED
      await this.pool.query(
        `UPDATE runs
         SET status = 'COMPLETED'::run_status,
             output = $1::jsonb,
             current_lease_worker_id = NULL,
             lease_expires_at = NULL,
             updated_at = NOW()
         WHERE id = $2`,
        [JSON.stringify(output ?? { completed: true }), runId]
      );
    } catch (handlerErr: any) {
      // Stop heartbeat
      clearInterval(heartbeatTimer);
      this.activeHeartbeats.delete(runId);

      // Clean HITL suspension: gate & WAITING_HUMAN state are already committed
      if (handlerErr instanceof RunSuspendedError) {
        return;
      }

      // Explicit human rejection: move straight to DEAD_LETTER without retries
      if (handlerErr instanceof ApprovalRejectedError) {
        const rejectionPayload = {
          name: handlerErr.name,
          message: handlerErr.message,
          gateKey: handlerErr.gateKey,
          payload: handlerErr.payload,
        };
        await this.pool.query(
          `UPDATE runs
           SET status = 'DEAD_LETTER'::run_status,
               error = $1::jsonb,
               current_lease_worker_id = NULL,
               lease_expires_at = NULL,
               updated_at = NOW()
           WHERE id = $2`,
          [JSON.stringify(rejectionPayload), runId]
        );
        return;
      }

      const errPayload = {
        message: handlerErr?.message || 'Execution error',
        name: handlerErr?.name,
      };

      if (run.attempt_count < run.max_attempts) {
        // Calculate exponential backoff
        const baseDelay = 500;
        const maxDelay = 10000;
        const backoff = Math.min(maxDelay, baseDelay * Math.pow(2, run.attempt_count));
        const jittered = Math.floor(Math.random() * backoff);

        await this.pool.query(
          `UPDATE runs
           SET status = 'RETRYING'::run_status,
               error = $1::jsonb,
               current_lease_worker_id = NULL,
               lease_expires_at = NULL,
               scheduled_at = NOW() + ($2 || ' milliseconds')::interval,
               updated_at = NOW()
           WHERE id = $3`,
          [JSON.stringify(errPayload), jittered, runId]
        );

        // Schedule retry outbox event
        await this.pool.query(
          `INSERT INTO outbox_events (tenant_id, event_type, payload, status, available_at)
           VALUES ($1, 'RUN_RETRY', $2::jsonb, 'PENDING', NOW() + ($3 || ' milliseconds')::interval)`,
          [
            tenantId,
            JSON.stringify({
              run_id: runId,
              tenant_id: tenantId,
              workflow_name: run.workflow_name,
              idempotency_key: run.idempotency_key,
              input: run.input,
            }),
            jittered,
          ]
        );
      } else {
        // Mark as DEAD_LETTER
        await this.pool.query(
          `UPDATE runs
           SET status = 'DEAD_LETTER'::run_status,
               error = $1::jsonb,
               current_lease_worker_id = NULL,
               lease_expires_at = NULL,
               updated_at = NOW()
           WHERE id = $2`,
          [JSON.stringify(errPayload), runId]
        );
      }
    } finally {
      // Always release lease in Redis
      await this.leaseManager.releaseLease(tenantId, runId);
    }
  }
}
