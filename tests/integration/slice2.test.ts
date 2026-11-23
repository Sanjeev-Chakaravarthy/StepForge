import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import { Pool, createDbPool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import { RedisLeaseManager } from '../../src/redis/lease.js';
import { OutboxPoller } from '../../src/outbox/poller.js';
import { Worker } from '../../src/worker/worker.js';
import { LeaseReaper } from '../../src/reaper/lease_reaper.js';
import type { Run, OutboxEvent } from '../../src/types/domain.js';

describe('StepForge - Slice 2 Integration Tests', () => {
  let pgContainer: StartedPostgreSqlContainer;
  let redisContainer: StartedRedisContainer;
  let pool: Pool;
  let redis: Redis;

  beforeAll(async () => {
    pgContainer = await new PostgreSqlContainer('postgres:16-alpine').start();
    redisContainer = await new RedisContainer('redis:7-alpine').start();

    pool = createDbPool(pgContainer.getConnectionUri());
    redis = new Redis(redisContainer.getConnectionUrl());

    await runMigrations(pool);
  });

  afterAll(async () => {
    if (pool) await pool.end();
    if (redis) redis.disconnect();
    if (pgContainer) await pgContainer.stop();
    if (redisContainer) await redisContainer.stop();
  });

  describe('1. Atomic Redis Lua Lease Scripts', () => {
    it('should grant lease when under max_concurrency and reject when quota exceeded', async () => {
      const leaseManager = new RedisLeaseManager(redis);
      const tenantId = 'tenant-lua-test';
      const maxConcurrency = 2;
      const ttlMs = 5000;

      // 1. First lease
      const r1 = await leaseManager.acquireLease(tenantId, 'run-1', ttlMs, maxConcurrency);
      expect(r1.acquired).toBe(true);
      expect(r1.expireAtMs).toBeGreaterThan(Date.now());

      // 2. Second lease (reaches cap)
      const r2 = await leaseManager.acquireLease(tenantId, 'run-2', ttlMs, maxConcurrency);
      expect(r2.acquired).toBe(true);

      // 3. Third lease (exceeds cap of 2)
      const r3 = await leaseManager.acquireLease(tenantId, 'run-3', ttlMs, maxConcurrency);
      expect(r3.acquired).toBe(false);
      expect(r3.activeCount).toBe(2);

      // 4. Re-entrant renewal for existing lease should succeed
      const r1Renewal = await leaseManager.acquireLease(tenantId, 'run-1', ttlMs, maxConcurrency);
      expect(r1Renewal.acquired).toBe(true);

      // 5. Release run-1
      const released = await leaseManager.releaseLease(tenantId, 'run-1');
      expect(released).toBe(true);

      // 6. Now run-3 can acquire
      const r3Retry = await leaseManager.acquireLease(tenantId, 'run-3', ttlMs, maxConcurrency);
      expect(r3Retry.acquired).toBe(true);
    });

    it('should automatically evict expired leases during acquisition', async () => {
      const leaseManager = new RedisLeaseManager(redis);
      const tenantId = 'tenant-lua-expiry';
      const maxConcurrency = 1;
      const ttlMs = 100; // 100ms TTL

      // Acquire with short TTL
      const r1 = await leaseManager.acquireLease(tenantId, 'fast-run', ttlMs, maxConcurrency);
      expect(r1.acquired).toBe(true);

      // Wait for expiration
      await new Promise((resolve) => setTimeout(resolve, 150));

      // Attempt acquisition for new run: fast-run should be automatically purged
      const r2 = await leaseManager.acquireLease(tenantId, 'next-run', ttlMs, maxConcurrency);
      expect(r2.acquired).toBe(true);
    });
  });

  describe('2. Multi-Tenant Concurrency & Anti-Starvation (500 Jobs Flood)', () => {
    it('should ensure Tenant A flooding 500 jobs never exceeds max_concurrency cap and does not starve Tenant B', async () => {
      const streamKey = 'stepforge:stream:tasks:flood';
      const groupName = 'stepforge:workers:flood';

      // Setup tenants
      // Tenant A: max_concurrency = 2
      await pool.query(
        `INSERT INTO tenants (id, name, max_concurrency)
         VALUES ('tenant-a-flood', 'Tenant A Flood', 2)
         ON CONFLICT (id) DO UPDATE SET max_concurrency = 2`
      );

      // Tenant B: max_concurrency = 5
      await pool.query(
        `INSERT INTO tenants (id, name, max_concurrency)
         VALUES ('tenant-b-isolated', 'Tenant B Isolated', 5)
         ON CONFLICT (id) DO UPDATE SET max_concurrency = 5`
      );

      let currentActiveTenantA = 0;
      let maxActiveTenantA = 0;
      let tenantACompletedCount = 0;
      let tenantBCompletedCount = 0;

      // Custom handler to monitor active concurrency
      const workerHandler = async (run: Run) => {
        if (run.tenant_id === 'tenant-a-flood') {
          currentActiveTenantA++;
          maxActiveTenantA = Math.max(maxActiveTenantA, currentActiveTenantA);
          // 30ms work per job -> 500 jobs will take ~7.5 seconds
          await new Promise((r) => setTimeout(r, 30));
          currentActiveTenantA--;
          tenantACompletedCount++;
        } else if (run.tenant_id === 'tenant-b-isolated') {
          await new Promise((r) => setTimeout(r, 5));
          tenantBCompletedCount++;
        }
      };

      // Create outbox poller & worker pool
      const poller = new OutboxPoller(pool, redis, {
        streamKey,
        batchSize: 50,
        pollIntervalMs: 50,
      });

      const worker1 = new Worker(pool, redis, {
        streamKey,
        groupName,
        workerId: 'worker-flood-1',
        handler: workerHandler,
        batchSize: 10,
        blockTimeoutMs: 100,
        leaseTtlMs: 5000,
      });

      const worker2 = new Worker(pool, redis, {
        streamKey,
        groupName,
        workerId: 'worker-flood-2',
        handler: workerHandler,
        batchSize: 10,
        blockTimeoutMs: 100,
        leaseTtlMs: 5000,
      });

      await worker1.initGroup();
      await worker2.initGroup();

      // Flood 500 jobs for Tenant A into outbox, interleaving Tenant B's 5 jobs in the middle
      const totalJobsTenantA = 500;
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        for (let i = 1; i <= 250; i++) {
          const runId = `00000000-0000-0000-0000-a${String(i).padStart(11, '0')}`;
          await client.query(
            `INSERT INTO runs (id, tenant_id, workflow_name, idempotency_key, status)
             VALUES ($1, 'tenant-a-flood', 'flood-flow', $2, 'PENDING')`,
            [runId, `flood-key-${i}`]
          );
          await client.query(
            `INSERT INTO outbox_events (tenant_id, event_type, payload, status, available_at)
             VALUES ('tenant-a-flood', 'RUN_CREATED', $1::jsonb, 'PENDING', NOW())`,
            [JSON.stringify({ run_id: runId, tenant_id: 'tenant-a-flood' })]
          );
        }

        // Interleave 5 jobs for Tenant B right into the active pipeline
        for (let j = 1; j <= 5; j++) {
          const runId = `00000000-0000-0000-0000-b${String(j).padStart(11, '0')}`;
          await client.query(
            `INSERT INTO runs (id, tenant_id, workflow_name, idempotency_key, status)
             VALUES ($1, 'tenant-b-isolated', 'isolated-flow', $2, 'PENDING')`,
            [runId, `isolated-key-${j}`]
          );
          await client.query(
            `INSERT INTO outbox_events (tenant_id, event_type, payload, status, available_at)
             VALUES ('tenant-b-isolated', 'RUN_CREATED', $1::jsonb, 'PENDING', NOW())`,
            [JSON.stringify({ run_id: runId, tenant_id: 'tenant-b-isolated' })]
          );
        }

        // Second batch of 250 jobs for Tenant A
        for (let i = 251; i <= totalJobsTenantA; i++) {
          const runId = `00000000-0000-0000-0000-a${String(i).padStart(11, '0')}`;
          await client.query(
            `INSERT INTO runs (id, tenant_id, workflow_name, idempotency_key, status)
             VALUES ($1, 'tenant-a-flood', 'flood-flow', $2, 'PENDING')`,
            [runId, `flood-key-${i}`]
          );
          await client.query(
            `INSERT INTO outbox_events (tenant_id, event_type, payload, status, available_at)
             VALUES ('tenant-a-flood', 'RUN_CREATED', $1::jsonb, 'PENDING', NOW())`,
            [JSON.stringify({ run_id: runId, tenant_id: 'tenant-a-flood' })]
          );
        }

        await client.query('COMMIT');
      } finally {
        client.release();
      }

      // Start poller and workers
      poller.start();
      await worker1.start();
      await worker2.start();

      // Wait until all 5 Tenant B jobs are completed
      const startTime = Date.now();
      while (tenantBCompletedCount < 5 && Date.now() - startTime < 30000) {
        await new Promise((r) => setTimeout(r, 100));
      }

      // Assert Tenant B completed all 5 jobs without starvation
      expect(tenantBCompletedCount).toBe(5);

      // Verify Tenant B finished while Tenant A was still processing its flood
      // (Tenant B did not have to wait for all 500 of Tenant A's jobs!)
      expect(tenantACompletedCount).toBeLessThan(totalJobsTenantA);

      // Stop workers and poller
      await worker1.stop();
      await worker2.stop();
      poller.stop();

      // CRITICAL INVARIANT: Max concurrent executions of Tenant A must NEVER exceed 2!
      expect(maxActiveTenantA).toBeLessThanOrEqual(2);
      expect(maxActiveTenantA).toBeGreaterThan(0);
    });
  });

  describe('3. Crash Recovery & Background Lease Reaper', () => {
    it('should reclaim a stale/crashed RUNNING task, retry it, and transition to COMPLETED', async () => {
      const tenantId = 'tenant-reap-success';
      const streamKey = 'stepforge:stream:tasks:reaper1';
      const groupName = 'stepforge:workers:reaper1';

      await pool.query(
        `INSERT INTO tenants (id, name, max_concurrency)
         VALUES ($1, 'Reaper Success Tenant', 5)
         ON CONFLICT (id) DO NOTHING`,
        [tenantId]
      );

      // 1. Insert run that crashed mid-execution (RUNNING with expired lease)
      const { rows } = await pool.query<Run>(
        `INSERT INTO runs (
           tenant_id, workflow_name, idempotency_key, status, 
           attempt_count, max_attempts, current_lease_worker_id, lease_expires_at
         )
         VALUES (
           $1, 'reap-workflow', 'reap-key-1', 'RUNNING'::run_status,
           1, 3, 'crashed-worker-sigkill', NOW() - INTERVAL '10 seconds'
         )
         RETURNING *`,
        [tenantId]
      );
      const crashedRun = rows[0]!;

      // Register mock lease in Redis to verify reaper cleans it
      await redis.zadd(
        RedisLeaseManager.getTenantLeaseKey(tenantId),
        Date.now() - 5000,
        crashedRun.id
      );

      // 2. Initialize Lease Reaper with 0ms delay for testing
      const reaper = new LeaseReaper(pool, redis, {
        baseDelayMs: 0,
        maxDelayMs: 0,
      });

      // Execute reaper
      const reapedCount = await reaper.reapOnce();
      expect(reapedCount).toBe(1);

      // 3. Verify PostgreSQL run state is now RETRYING and lease is cleared
      const { rows: reapedRows } = await pool.query<Run>(
        `SELECT * FROM runs WHERE id = $1`,
        [crashedRun.id]
      );
      const reapedRun = reapedRows[0]!;
      expect(reapedRun.status).toBe('RETRYING');
      expect(reapedRun.current_lease_worker_id).toBeNull();
      expect(reapedRun.lease_expires_at).toBeNull();

      // 4. Verify retry outbox event was created
      const { rows: retryOutboxRows } = await pool.query<OutboxEvent>(
        `SELECT * FROM outbox_events WHERE tenant_id = $1 AND event_type = 'RUN_RETRY' AND status = 'PENDING'`,
        [tenantId]
      );
      expect(retryOutboxRows.length).toBe(1);

      // 5. Verify Redis lease was evicted
      const redisLeaseScore = await redis.zscore(
        RedisLeaseManager.getTenantLeaseKey(tenantId),
        crashedRun.id
      );
      expect(redisLeaseScore).toBeNull();

      // Wait 50ms so available_at (NOW()) is mature
      await new Promise((r) => setTimeout(r, 50));

      // 6. Now let Outbox Poller and a healthy Worker process the retry
      const poller = new OutboxPoller(pool, redis, {
        streamKey,
        batchSize: 10,
      });

      const worker = new Worker(pool, redis, {
        streamKey,
        groupName,
        workerId: 'healthy-worker-resumed',
        handler: async (run) => ({ resumedSuccessfully: true, previousAttempt: run.attempt_count }),
      });

      await worker.initGroup();

      // Poll outbox into stream
      const pollCount = await poller.pollOnce();
      expect(pollCount).toBeGreaterThanOrEqual(1);

      // Worker processes stream task
      const workerProcessed = await worker.processNextBatch();
      expect(workerProcessed).toBeGreaterThanOrEqual(1);

      // 7. Verify the run has cleanly transitioned to COMPLETED
      const { rows: finalRows } = await pool.query<Run>(
        `SELECT * FROM runs WHERE id = $1`,
        [crashedRun.id]
      );
      const finalRun = finalRows[0]!;
      expect(finalRun.status).toBe('COMPLETED');
      expect(finalRun.output).toEqual({ resumedSuccessfully: true, previousAttempt: 2 });
    });

    it('should transition to DEAD_LETTER when a crashed task exceeds max_attempts', async () => {
      const tenantId = 'tenant-reap-deadletter';

      await pool.query(
        `INSERT INTO tenants (id, name, max_concurrency)
         VALUES ($1, 'Reaper DLQ Tenant', 5)
         ON CONFLICT (id) DO NOTHING`,
        [tenantId]
      );

      // Run has already exhausted its attempts: attempt_count = 3, max_attempts = 3
      const { rows } = await pool.query<Run>(
        `INSERT INTO runs (
           tenant_id, workflow_name, idempotency_key, status, 
           attempt_count, max_attempts, current_lease_worker_id, lease_expires_at
         )
         VALUES (
           $1, 'fatal-workflow', 'fatal-key-1', 'RUNNING'::run_status,
           3, 3, 'dead-worker-oom', NOW() - INTERVAL '15 seconds'
         )
         RETURNING *`,
        [tenantId]
      );
      const fatalRun = rows[0]!;

      const reaper = new LeaseReaper(pool, redis);
      const reapedCount = await reaper.reapOnce();
      expect(reapedCount).toBe(1);

      // Verify status transitioned directly to DEAD_LETTER
      const { rows: dlqRows } = await pool.query<Run>(
        `SELECT * FROM runs WHERE id = $1`,
        [fatalRun.id]
      );
      const dlqRun = dlqRows[0]!;
      expect(dlqRun.status).toBe('DEAD_LETTER');
      expect(dlqRun.current_lease_worker_id).toBeNull();
      expect(dlqRun.lease_expires_at).toBeNull();
      expect(dlqRun.error).toBeDefined();
      expect((dlqRun.error as any).message).toContain('Worker lease expired and max attempts exceeded');

      // Verify NO retry outbox event was created
      const { rows: dlqOutbox } = await pool.query(
        `SELECT COUNT(*) FROM outbox_events WHERE payload->>'run_id' = $1`,
        [fatalRun.id]
      );
      expect(Number(dlqOutbox[0].count)).toBe(0);
    });
  });
});
