import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool, createDbPool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import {
  DockerSandboxRunner,
  SandboxTimeoutError,
  SandboxExecutionError,
} from '../../src/sandbox/docker.js';
import { DurableStepContext } from '../../src/step/context.js';
import type { StepCheckpoint } from '../../src/types/domain.js';

describe('StepForge - Slice 4 Ephemeral Docker Sandbox Tests', () => {
  let pgContainer: StartedPostgreSqlContainer;
  let pool: Pool;
  const runner = new DockerSandboxRunner();

  beforeAll(async () => {
    pgContainer = await new PostgreSqlContainer('postgres:16-alpine').start();
    pool = createDbPool(pgContainer.getConnectionUri());
    await runMigrations(pool);
  });

  afterAll(async () => {
    if (pool) await pool.end();
    if (pgContainer) await pgContainer.stop();
  });

  describe('1. DockerSandboxRunner Resource & Protocol Enforcement', () => {
    it('should pass JSON input via stdin and parse JSON output from stdout', async () => {
      const code = `
        const fs = require('fs');
        const input = JSON.parse(fs.readFileSync(0, 'utf-8'));
        const output = {
          received: input,
          sum: input.a + input.b,
          computedAt: new Date().toISOString()
        };
        console.log(JSON.stringify(output));
      `;

      const result = await runner.execute<{
        received: { a: number; b: number };
        sum: number;
        computedAt: string;
      }>({
        image: 'node:20-alpine',
        command: ['node', '-e', code],
        input: { a: 40, b: 2 },
        cpus: '0.5',
        memory: '128m',
        memorySwap: '128m',
        pidsLimit: 32,
      });

      expect(result.sum).toBe(42);
      expect(result.received).toEqual({ a: 40, b: 2 });
      expect(result.computedAt).toBeDefined();
    });

    it('should enforce hard timeout and kill container on expiry', async () => {
      const infiniteSleepCode = `
        setTimeout(() => {}, 60000);
      `;

      const startTime = Date.now();
      await expect(
        runner.execute({
          image: 'node:20-alpine',
          command: ['node', '-e', infiniteSleepCode],
          timeoutMs: 1200,
        })
      ).rejects.toThrow(SandboxTimeoutError);

      const elapsed = Date.now() - startTime;
      expect(elapsed).toBeGreaterThanOrEqual(1100);
      expect(elapsed).toBeLessThan(3500);
    });

    it('should strictly isolate network with --network=none', async () => {
      // Attempting outbound HTTP connection must fail with ENETUNREACH in --network=none
      const networkCode = `
        const http = require('http');
        const req = http.get('http://1.1.1.1', () => {});
        req.on('error', (err) => {
          console.error('NETWORK_ERROR:' + err.code);
          process.exit(1);
        });
      `;

      await expect(
        runner.execute({
          image: 'node:20-alpine',
          command: ['node', '-e', networkCode],
          network: 'none',
        })
      ).rejects.toThrow(SandboxExecutionError);
    });
  });

  describe('2. Durable Memoization with step.runSandboxed', () => {
    it('should execute sandbox container on cache miss and return memoized output on cache hit', async () => {
      const tenantId = 'sandbox-tenant';
      await pool.query(
        `INSERT INTO tenants (id, name, max_concurrency)
         VALUES ($1, 'Sandbox Tenant', 5)
         ON CONFLICT (id) DO NOTHING`,
        [tenantId]
      );

      const { rows } = await pool.query<{ id: string }>(
        `INSERT INTO runs (tenant_id, workflow_name, idempotency_key, status)
         VALUES ($1, 'sandboxed-workflow', 'sbx-run-001', 'RUNNING')
         RETURNING id`,
        [tenantId]
      );
      const runId = rows[0]!.id;

      const step = new DurableStepContext(pool, runId);

      const computeScript = `
        const fs = require('fs');
        const input = JSON.parse(fs.readFileSync(0, 'utf-8'));
        console.log(JSON.stringify({ result: input.x * 10, generatedByContainer: true }));
      `;

      // 1. First invocation: Cache miss -> Container executes
      const firstResult = await step.runSandboxed<{
        result: number;
        generatedByContainer: boolean;
      }>('heavy-compute-step', {
        image: 'node:20-alpine',
        command: ['node', '-e', computeScript],
        input: { x: 7 },
      });

      expect(firstResult).toEqual({
        result: 70,
        generatedByContainer: true,
      });

      // Verify checkpoint in PostgreSQL
      const { rows: checkpoints } = await pool.query<StepCheckpoint>(
        `SELECT step_key, output, status FROM step_checkpoints WHERE run_id = $1 AND step_key = $2`,
        [runId, 'heavy-compute-step']
      );
      expect(checkpoints.length).toBe(1);
      expect(checkpoints[0]!.status).toBe('COMPLETED');
      expect(checkpoints[0]!.output).toEqual({
        result: 70,
        generatedByContainer: true,
      });

      // 2. Second invocation: Cache hit!
      // Provide an invalid command that would crash if Docker were invoked
      const secondResult = await step.runSandboxed<{
        result: number;
        generatedByContainer: boolean;
      }>('heavy-compute-step', {
        image: 'node:20-alpine',
        command: ['invalid-non-existent-binary-command-that-fails'],
      });

      // Returns the cached output instantly without running Docker
      expect(secondResult).toEqual({
        result: 70,
        generatedByContainer: true,
      });
    });
  });
});
