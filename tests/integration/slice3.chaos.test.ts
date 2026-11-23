import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import { Pool, createDbPool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import { createApp } from '../../src/server/app.js';
import { OutboxPoller } from '../../src/outbox/poller.js';
import { Worker } from '../../src/worker/worker.js';
import { WorkflowRegistry } from '../../src/workflow/registry.js';
import { LeaseReaper } from '../../src/reaper/lease_reaper.js';
import type { Run, StepCheckpoint } from '../../src/types/domain.js';
import type { StepContext } from '../../src/step/context.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('StepForge - Slice 3 SIGKILL Chaos & Step Memoization Tests', () => {
  let pgContainer: StartedPostgreSqlContainer;
  let redisContainer: StartedRedisContainer;
  let pool: Pool;
  let redis: Redis;
  let app: ReturnType<typeof createApp>;

  const STREAM_KEY = 'stepforge:stream:tasks:chaos';
  const GROUP_NAME = 'stepforge:workers:chaos';

  beforeAll(async () => {
    pgContainer = await new PostgreSqlContainer('postgres:16-alpine').start();
    redisContainer = await new RedisContainer('redis:7-alpine').start();

    pool = createDbPool(pgContainer.getConnectionUri());
    redis = new Redis(redisContainer.getConnectionUrl());

    await runMigrations(pool);

    // Create side effects tracking table for chaos test verification
    await pool.query(`
      CREATE TABLE IF NOT EXISTS test_side_effects (
        id BIGSERIAL PRIMARY KEY,
        run_id UUID NOT NULL,
        step_name TEXT NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    app = createApp({ pool, redis });
  });

  afterAll(async () => {
    if (pool) await pool.end();
    if (redis) redis.disconnect();
    if (pgContainer) await pgContainer.stop();
    if (redisContainer) await redisContainer.stop();
  });

  it('should guarantee zero duplicate side effects when worker is killed via SIGKILL mid-step', async () => {
    // 1. Create tenant
    const tenantRes = await app.request('/api/v1/tenants', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: 'chaos-tenant',
        name: 'Chaos Test Tenant',
        maxConcurrency: 5,
      }),
    });
    expect(tenantRes.status).toBe(201);

    // 2. Submit a 3-step workflow run
    const runRes = await app.request('/api/v1/workflows/chaos-order-fulfillment/runs', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': 'chaos-run-key-1',
      },
      body: JSON.stringify({
        tenantId: 'chaos-tenant',
        input: { orderId: 'ORD-CHAOS-001' },
      }),
    });

    expect(runRes.status).toBe(201);
    const { run } = (await runRes.json()) as { run: Run };
    const runId = run.id;

    // 3. Dispatch outbox event to Redis stream
    const poller = new OutboxPoller(pool, redis, {
      streamKey: STREAM_KEY,
      batchSize: 10,
    });
    const dispatched = await poller.pollOnce();
    expect(dispatched).toBe(1);

    // 4. Spawn real child worker OS process executing the workflow
    const workerScriptPath = path.resolve(__dirname, '../fixtures/chaos_worker.ts');
    const child = spawn(
      'npx',
      ['tsx', workerScriptPath],
      {
        env: {
          ...process.env,
          DATABASE_URL: pgContainer.getConnectionUri(),
          REDIS_URL: redisContainer.getConnectionUrl(),
          STREAM_KEY,
          GROUP_NAME,
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      }
    );

    // 5. Wait for child worker to reach mid-Step 3
    let childStderr = '';
    child.stderr?.on('data', (chunk) => {
      childStderr += chunk.toString();
    });

    const step3StartedPromise = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(
          new Error(
            `Timed out waiting for child worker to enter Step 3. Stderr: ${childStderr}`
          )
        );
      }, 25000);

      child.stdout?.on('data', (chunk) => {
        const text = chunk.toString();
        if (text.includes('SIGNAL:STEP_3_STARTED')) {
          clearTimeout(timeout);
          resolve();
        }
      });

      child.on('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });

    await step3StartedPromise;

    // Verify Step 1 and Step 2 checkpoints exist in DB while Step 3 is in progress
    const { rows: initialCheckpoints } = await pool.query<StepCheckpoint>(
      `SELECT step_key, status FROM step_checkpoints WHERE run_id = $1 ORDER BY id ASC`,
      [runId]
    );
    expect(initialCheckpoints.map((c) => c.step_key)).toEqual([
      'step-1-inventory',
      'step-2-payment',
    ]);

    // 6. KILL WORKER MID-STEP 3 WITH SIGKILL
    const exitPromise = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
      (resolve) => {
        child.on('exit', (code, signal) => {
          resolve({ code, signal });
        });
      }
    );

    child.kill('SIGKILL');
    const exitResult = await exitPromise;
    expect(exitResult.signal).toBe('SIGKILL');

    // 7. Verify post-kill state in PostgreSQL
    // Run is still RUNNING, with orphaned worker lease
    const { rows: postKillRunRows } = await pool.query<Run>(
      `SELECT status, current_lease_worker_id, attempt_count FROM runs WHERE id = $1`,
      [runId]
    );
    const postKillRun = postKillRunRows[0]!;
    expect(postKillRun.status).toBe('RUNNING');
    expect(postKillRun.current_lease_worker_id).toBe('chaos-worker-child');
    expect(postKillRun.attempt_count).toBe(1);

    // Verify side effects recorded so far: step 1, step 2, and step 3 (started)
    const { rows: initialSideEffects } = await pool.query<{ step_name: string }>(
      `SELECT step_name FROM test_side_effects WHERE run_id = $1 ORDER BY id ASC`,
      [runId]
    );
    expect(initialSideEffects.map((s) => s.step_name)).toEqual([
      'step-1-inventory',
      'step-2-payment',
      'step-3-started',
    ]);

    // 8. Reclaim orphaned task via LeaseReaper
    // Force lease expiration to simulate elapsed time
    await pool.query(
      `UPDATE runs SET lease_expires_at = NOW() - INTERVAL '10 seconds' WHERE id = $1`,
      [runId]
    );

    const reaper = new LeaseReaper(pool, redis, {
      baseDelayMs: 0,
      maxDelayMs: 0,
    });
    const reapedCount = await reaper.reapOnce();
    expect(reapedCount).toBe(1);

    // Verify state transitioned to RETRYING
    const { rows: retryingRunRows } = await pool.query<Run>(
      `SELECT status, current_lease_worker_id, lease_expires_at FROM runs WHERE id = $1`,
      [runId]
    );
    expect(retryingRunRows[0]!.status).toBe('RETRYING');
    expect(retryingRunRows[0]!.current_lease_worker_id).toBeNull();
    expect(retryingRunRows[0]!.lease_expires_at).toBeNull();

    // 9. Process retry via Outbox Poller and a healthy second worker
    await new Promise((r) => setTimeout(r, 50));
    await poller.pollOnce();

    // Set up healthy worker with the same workflow definition
    const healthyRegistry = new WorkflowRegistry();
    healthyRegistry.register('chaos-order-fulfillment', async (r: Run, step: StepContext) => {
      // Step 1: If executed again, this would insert a duplicate side effect!
      await step.run('step-1-inventory', async () => {
        await pool.query(
          'INSERT INTO test_side_effects (run_id, step_name) VALUES ($1, $2)',
          [r.id, 'step-1-inventory']
        );
        return { inventoryReserved: true, sku: 'ITEM-99' };
      });

      // Step 2: If executed again, this would insert a duplicate side effect!
      await step.run('step-2-payment', async () => {
        await pool.query(
          'INSERT INTO test_side_effects (run_id, step_name) VALUES ($1, $2)',
          [r.id, 'step-2-payment']
        );
        return { paymentCaptured: true, amount: 99.99 };
      });

      // Step 3: Successfully completes on recovery attempt
      const step3Result = await step.run('step-3-shipment', async () => {
        await pool.query(
          'INSERT INTO test_side_effects (run_id, step_name) VALUES ($1, $2)',
          [r.id, 'step-3-completed-healthy']
        );
        return { trackingNumber: 'SHIP-HEALTHY-001' };
      });

      return {
        orderId: (r.input as any).orderId,
        shipping: step3Result,
        status: 'fulfilled',
      };
    });

    const healthyWorker = new Worker(pool, redis, {
      workerId: 'healthy-worker-node-2',
      streamKey: STREAM_KEY,
      groupName: GROUP_NAME,
      registry: healthyRegistry,
      blockTimeoutMs: 500,
      leaseTtlMs: 10000,
    });

    await healthyWorker.initGroup();
    const processed = await healthyWorker.processNextBatch();
    expect(processed).toBeGreaterThanOrEqual(1);

    // 10. FINAL VERIFICATION: Workflow Completion & Zero Duplicate Side Effects
    // Verify run transitioned to COMPLETED
    const finalRunRes = await app.request(`/api/v1/runs/${runId}`);
    expect(finalRunRes.status).toBe(200);

    const finalRunData = (await finalRunRes.json()) as {
      run: Run;
      checkpoints: StepCheckpoint[];
    };

    expect(finalRunData.run.status).toBe('COMPLETED');
    expect(finalRunData.run.attempt_count).toBe(2);
    expect(finalRunData.run.output).toEqual({
      orderId: 'ORD-CHAOS-001',
      shipping: { trackingNumber: 'SHIP-HEALTHY-001' },
      status: 'fulfilled',
    });

    // Verify all 3 checkpoints exist
    expect(finalRunData.checkpoints.length).toBe(3);
    const checkpointKeys = finalRunData.checkpoints.map((c) => c.step_key);
    expect(checkpointKeys).toContain('step-1-inventory');
    expect(checkpointKeys).toContain('step-2-payment');
    expect(checkpointKeys).toContain('step-3-shipment');

    // PROOF OF ZERO DUPLICATE SIDE EFFECTS:
    // Step 1 side effect must have executed EXACTLY 1 time across the entire lifecycle
    const { rows: step1SideEffects } = await pool.query(
      `SELECT COUNT(*) FROM test_side_effects WHERE run_id = $1 AND step_name = 'step-1-inventory'`,
      [runId]
    );
    expect(Number(step1SideEffects[0].count)).toBe(1);

    // Step 2 side effect must have executed EXACTLY 1 time across the entire lifecycle
    const { rows: step2SideEffects } = await pool.query(
      `SELECT COUNT(*) FROM test_side_effects WHERE run_id = $1 AND step_name = 'step-2-payment'`,
      [runId]
    );
    expect(Number(step2SideEffects[0].count)).toBe(1);

    // Step 3 finished
    const { rows: step3SideEffects } = await pool.query(
      `SELECT COUNT(*) FROM test_side_effects WHERE run_id = $1 AND step_name = 'step-3-completed-healthy'`,
      [runId]
    );
    expect(Number(step3SideEffects[0].count)).toBe(1);
  });
});
