import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import { Pool, createDbPool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import { createApp } from '../../src/server/app.js';
import { OutboxPoller } from '../../src/outbox/poller.js';
import type { OutboxEvent, Run } from '../../src/types/domain.js';

describe('StepForge - Slice 1 Integration Tests', () => {
  let pgContainer: StartedPostgreSqlContainer;
  let redisContainer: StartedRedisContainer;
  let pool: Pool;
  let redis: Redis;
  let app: ReturnType<typeof createApp>;
  let poller: OutboxPoller;

  const STREAM_KEY = 'stepforge:stream:tasks:test';

  beforeAll(async () => {
    // 1. Start containers
    pgContainer = await new PostgreSqlContainer('postgres:16-alpine').start();
    redisContainer = await new RedisContainer('redis:7-alpine').start();

    // 2. Initialize clients
    pool = createDbPool(pgContainer.getConnectionUri());
    redis = new Redis(redisContainer.getConnectionUrl());

    // 3. Apply schema migrations
    await runMigrations(pool);

    // 4. Initialize Outbox Poller
    poller = new OutboxPoller(pool, redis, {
      streamKey: STREAM_KEY,
      batchSize: 50,
      pollIntervalMs: 100,
    });

    // 5. Initialize Hono application
    app = createApp({ pool, redis });
  });

  afterAll(async () => {
    poller?.stop();
    if (pool) {
      await pool.end();
    }
    if (redis) {
      redis.disconnect();
    }
    if (pgContainer) {
      await pgContainer.stop();
    }
    if (redisContainer) {
      await redisContainer.stop();
    }
  });

  describe('1. Migrations & Health Check', () => {
    it('should respond with healthy status from /health', async () => {
      const res = await app.request('/health');
      expect(res.status).toBe(200);

      const body = (await res.json()) as any;
      expect(body.status).toBe('ok');
      expect(body.postgres).toBe(true);
      expect(body.redis).toBe(true);
    });

    it('should have created all required tables, constraints and indexes', async () => {
      const { rows } = await pool.query<{ table_name: string }>(
        `SELECT table_name FROM information_schema.tables 
         WHERE table_schema = 'public' 
         AND table_name IN ('tenants', 'runs', 'step_checkpoints', 'outbox_events')`
      );

      const tableNames = rows.map((r) => r.table_name);
      expect(tableNames).toContain('tenants');
      expect(tableNames).toContain('runs');
      expect(tableNames).toContain('step_checkpoints');
      expect(tableNames).toContain('outbox_events');
    });
  });

  describe('2. Tenant API', () => {
    it('should create a tenant with custom max_concurrency', async () => {
      const res = await app.request('/api/v1/tenants', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: 'tenant-alpha',
          name: 'Alpha Corp',
          maxConcurrency: 10,
        }),
      });

      expect(res.status).toBe(201);
      const data = (await res.json()) as any;
      expect(data.tenant.id).toBe('tenant-alpha');
      expect(data.tenant.name).toBe('Alpha Corp');
      expect(data.tenant.max_concurrency).toBe(10);
    });

    it('should return 409 Conflict when creating a duplicate tenant id', async () => {
      const res = await app.request('/api/v1/tenants', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: 'tenant-alpha',
          name: 'Alpha Corp Duplicate',
        }),
      });

      expect(res.status).toBe(409);
    });

    it('should retrieve a created tenant by id', async () => {
      const res = await app.request('/api/v1/tenants/tenant-alpha');
      expect(res.status).toBe(200);

      const data = (await res.json()) as any;
      expect(data.tenant.id).toBe('tenant-alpha');
      expect(data.tenant.max_concurrency).toBe(10);
    });
  });

  describe('3. Workflow Run Ingestion & State Machine', () => {
    it('should reject run submission for non-existent tenant with 404', async () => {
      const res = await app.request('/api/v1/workflows/order-flow/runs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tenantId: 'non-existent-tenant',
          input: { orderId: 123 },
        }),
      });

      expect(res.status).toBe(404);
      const body = (await res.json()) as any;
      expect(body.error).toBe('Tenant not found');
    });

    it('should atomically create a run in PENDING status and an outbox_event', async () => {
      const res = await app.request('/api/v1/workflows/order-flow/runs', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': 'order-run-001',
        },
        body: JSON.stringify({
          tenantId: 'tenant-alpha',
          input: { orderId: 'ord-100', amount: 50.5 },
        }),
      });

      expect(res.status).toBe(201);
      const body = (await res.json()) as any;
      expect(body.isDuplicate).toBe(false);
      expect(body.run.status).toBe('PENDING');
      expect(body.run.tenant_id).toBe('tenant-alpha');
      expect(body.run.workflow_name).toBe('order-flow');
      expect(body.run.idempotency_key).toBe('order-run-001');

      // Verify outbox_events table
      const { rows: outboxRows } = await pool.query<OutboxEvent>(
        `SELECT * FROM outbox_events WHERE tenant_id = 'tenant-alpha' AND event_type = 'RUN_CREATED'`
      );

      expect(outboxRows.length).toBe(1);
      const outbox = outboxRows[0]!;
      expect(outbox.status).toBe('PENDING');
      expect(outbox.payload.run_id).toBe(body.run.id);
      expect(outbox.payload.workflow_name).toBe('order-flow');
      expect(outbox.available_at).toBeDefined();
    });

    it('should query a submitted run and its step checkpoints via GET /api/v1/runs/:id', async () => {
      const { rows } = await pool.query<Run>(
        `SELECT id FROM runs WHERE idempotency_key = 'order-run-001'`
      );
      const runId = rows[0]!.id;

      const res = await app.request(`/api/v1/runs/${runId}`);
      expect(res.status).toBe(200);

      const body = (await res.json()) as any;
      expect(body.run.id).toBe(runId);
      expect(body.run.status).toBe('PENDING');
      expect(body.checkpoints).toEqual([]);
    });
  });

  describe('4. Refinement 1: Idempotency Key Handling', () => {
    it('should return existing run when submitting duplicate Idempotency-Key header', async () => {
      // First submission
      const res1 = await app.request('/api/v1/workflows/payment-flow/runs', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': 'payment-run-999',
        },
        body: JSON.stringify({
          tenantId: 'tenant-alpha',
          input: { transactionId: 'txn-999' },
        }),
      });

      expect(res1.status).toBe(201);
      const body1 = (await res1.json()) as any;
      expect(body1.isDuplicate).toBe(false);
      const runId = body1.run.id;

      // Duplicate submission with same Idempotency-Key
      const res2 = await app.request('/api/v1/workflows/payment-flow/runs', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': 'payment-run-999',
        },
        body: JSON.stringify({
          tenantId: 'tenant-alpha',
          input: { transactionId: 'txn-999' },
        }),
      });

      expect(res2.status).toBe(200);
      const body2 = (await res2.json()) as any;
      expect(body2.isDuplicate).toBe(true);
      expect(body2.run.id).toBe(runId);
      expect(body2.run.idempotency_key).toBe('payment-run-999');

      // Verify no duplicate outbox event was created
      const { rows } = await pool.query(
        `SELECT COUNT(*) FROM outbox_events WHERE payload->>'idempotency_key' = 'payment-run-999'`
      );
      expect(Number(rows[0].count)).toBe(1);
    });

    it('should allow identical idempotency key across different tenants without conflict', async () => {
      // Create second tenant
      await app.request('/api/v1/tenants', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: 'tenant-beta',
          name: 'Beta Corp',
          maxConcurrency: 3,
        }),
      });

      // Submit for tenant-beta with same idempotency key as tenant-alpha
      const res = await app.request('/api/v1/workflows/payment-flow/runs', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': 'payment-run-999',
        },
        body: JSON.stringify({
          tenantId: 'tenant-beta',
          input: { transactionId: 'txn-beta-999' },
        }),
      });

      expect(res.status).toBe(201);
      const body = (await res.json()) as any;
      expect(body.run.tenant_id).toBe('tenant-beta');
      expect(body.isDuplicate).toBe(false);
    });
  });

  describe('5. Refinement 2: Outbox available_at & Redis Stream Dispatch', () => {
    it('should defer events where available_at is in the future, and dispatch available ones', async () => {
      // Create a future (backoff delay) outbox event manually
      const { rows: futureRunRows } = await pool.query<Run>(
        `INSERT INTO runs (tenant_id, workflow_name, idempotency_key, input, status)
         VALUES ('tenant-alpha', 'delayed-flow', 'delayed-key-1', '{}'::jsonb, 'PENDING')
         RETURNING id`
      );
      const futureRunId = futureRunRows[0]!.id;

      const { rows: futureEventRows } = await pool.query<OutboxEvent>(
        `INSERT INTO outbox_events (tenant_id, event_type, payload, status, available_at)
         VALUES ('tenant-alpha', 'RUN_CREATED', $1::jsonb, 'PENDING', NOW() + INTERVAL '1 hour')
         RETURNING id`,
        [JSON.stringify({ run_id: futureRunId })]
      );
      const futureEventId = futureEventRows[0]!.id;

      // Clear any prior stream messages
      await redis.del(STREAM_KEY);

      // Execute a single outbox poller cycle
      const processedCount = await poller.pollOnce();
      expect(processedCount).toBeGreaterThanOrEqual(1);

      // Verify the future event was NOT published
      const { rows: futureCheck } = await pool.query<OutboxEvent>(
        `SELECT status, published_at FROM outbox_events WHERE id = $1`,
        [futureEventId]
      );
      expect(futureCheck[0]!.status).toBe('PENDING');
      expect(futureCheck[0]!.published_at).toBeNull();

      // Verify future run is still PENDING
      const { rows: futureRunCheck } = await pool.query<Run>(
        `SELECT status FROM runs WHERE id = $1`,
        [futureRunId]
      );
      expect(futureRunCheck[0]!.status).toBe('PENDING');

      // Check Redis Stream contents
      const streamEntries = await redis.xread('STREAMS', STREAM_KEY, 0);
      expect(streamEntries).not.toBeNull();
      const [streamKey, messages] = streamEntries![0]!;
      expect(streamKey).toBe(STREAM_KEY);
      expect(messages.length).toBeGreaterThanOrEqual(1);

      // Inspect first message in stream
      const [messageId, fields] = messages[0]!;
      expect(messageId).toBeDefined();

      const parsedFields: Record<string, string> = {};
      for (let i = 0; i < fields.length; i += 2) {
        parsedFields[fields[i]!] = fields[i + 1]!;
      }

      expect(parsedFields.tenantId).toBeDefined();
      expect(parsedFields.eventType).toBe('RUN_CREATED');
      expect(parsedFields.payload).toBeDefined();

      const parsedPayload = JSON.parse(parsedFields.payload!);
      expect(parsedPayload.run_id).toBeDefined();

      // Verify that the successfully dispatched run transitioned to QUEUED
      const { rows: queuedRunCheck } = await pool.query<Run>(
        `SELECT status FROM runs WHERE id = $1`,
        [parsedPayload.run_id]
      );
      expect(queuedRunCheck[0]!.status).toBe('QUEUED');

      // Verify the corresponding outbox event transitioned to PUBLISHED
      const { rows: publishedOutboxCheck } = await pool.query<OutboxEvent>(
        `SELECT status, published_at FROM outbox_events WHERE id = $1`,
        [parsedFields.eventId]
      );
      expect(publishedOutboxCheck[0]!.status).toBe('PUBLISHED');
      expect(publishedOutboxCheck[0]!.published_at).not.toBeNull();
    });
  });
});
