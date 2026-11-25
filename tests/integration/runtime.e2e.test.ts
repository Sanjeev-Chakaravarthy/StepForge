import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { spawn } from 'node:child_process';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, StartedRedisContainer } from '@testcontainers/redis';
import { startRuntime } from '../../src/server/index.js';
import type { Run, StepCheckpoint } from '../../src/types/domain.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (!address || typeof address === 'string') {
        server.close();
        return reject(new Error('Failed to acquire free port'));
      }
      const { port } = address;
      server.close(() => resolve(port));
    });
  });
}

describe('StepForge - End-to-End Runtime & Graceful Shutdown', () => {
  let pgContainer: StartedPostgreSqlContainer;
  let redisContainer: StartedRedisContainer;

  beforeAll(async () => {
    pgContainer = await new PostgreSqlContainer('postgres:16-alpine').start();
    redisContainer = await new RedisContainer('redis:7-alpine').start();
  });

  afterAll(async () => {
    if (pgContainer) await pgContainer.stop();
    if (redisContainer) await redisContainer.stop();
  });

  it('should execute a queued run end-to-end to COMPLETED via echo handler and shut down cleanly', async () => {
    const runtime = await startRuntime({
      DATABASE_URL: pgContainer.getConnectionUri(),
      REDIS_URL: redisContainer.getConnectionUrl(),
      PORT: 0,
      NODE_ENV: 'development',
      OUTBOX_POLL_INTERVAL_MS: 50,
      DELAY_QUEUE_POLL_INTERVAL_MS: 50,
      REAPER_INTERVAL_MS: 200,
      REDIS_STREAM_KEY: 'stepforge:stream:tasks:e2e-runtime',
      WORKER_GROUP_NAME: 'stepforge:workers:e2e-runtime',
    });

    try {
      const baseUrl = `http://127.0.0.1:${runtime.port}`;

      // 1. Provision tenant
      const tenantRes = await fetch(`${baseUrl}/api/v1/tenants`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: 'runtime-tenant-1',
          name: 'Runtime E2E Tenant',
          maxConcurrency: 3,
        }),
      });
      expect(tenantRes.status).toBe(201);

      // 2. Submit workflow run
      const submitRes = await fetch(`${baseUrl}/api/v1/workflows/echo-demo/runs`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': 'e2e-echo-key-1',
        },
        body: JSON.stringify({
          tenantId: 'runtime-tenant-1',
          input: { greeting: 'hello stepforge', count: 42 },
        }),
      });
      expect(submitRes.status).toBe(201);
      const { run: createdRun } = (await submitRes.json()) as { run: Run };
      expect(createdRun.status).toBe('PENDING');

      // 3. Poll until Worker finishes the run via OutboxPoller + Echo Handler
      let finalPayload: { run: Run; checkpoints: StepCheckpoint[] } | null = null;
      const deadline = Date.now() + 10000;
      while (Date.now() < deadline) {
        const res = await fetch(`${baseUrl}/api/v1/runs/${createdRun.id}`);
        expect(res.status).toBe(200);
        const body = (await res.json()) as { run: Run; checkpoints: StepCheckpoint[] };
        if (body.run.status === 'COMPLETED') {
          finalPayload = body;
          break;
        }
        await new Promise((r) => setTimeout(r, 50));
      }

      expect(finalPayload).not.toBeNull();
      expect(finalPayload!.run.status).toBe('COMPLETED');
      expect(finalPayload!.run.output).toMatchObject({
        echoed: true,
        runId: createdRun.id,
        tenantId: 'runtime-tenant-1',
        workflowName: 'echo-demo',
        input: { greeting: 'hello stepforge', count: 42 },
      });
      expect(finalPayload!.checkpoints.length).toBe(1);
      expect(finalPayload!.checkpoints[0]!.step_key).toBe('echo');
      expect(finalPayload!.checkpoints[0]!.status).toBe('COMPLETED');
    } finally {
      await runtime.shutdown('SIGTERM');
    }
  });

  it('should run server/index.ts as a dev process, complete queued runs, and exit 0 on SIGTERM', async () => {
    const port = await getFreePort();
    const entrypoint = path.resolve(__dirname, '../../src/server/index.ts');

    const child = spawn('npx', ['tsx', entrypoint], {
      env: {
        ...process.env,
        NODE_ENV: 'development',
        PORT: String(port),
        DATABASE_URL: pgContainer.getConnectionUri(),
        REDIS_URL: redisContainer.getConnectionUrl(),
        OUTBOX_POLL_INTERVAL_MS: '50',
        DELAY_QUEUE_POLL_INTERVAL_MS: '50',
        REAPER_INTERVAL_MS: '200',
        REDIS_STREAM_KEY: 'stepforge:stream:tasks:e2e-proc',
        WORKER_GROUP_NAME: 'stepforge:workers:e2e-proc',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';
    child.stdout?.on('data', (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr?.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    try {
      // Wait for HTTP server listening log
      await new Promise<void>((resolve, reject) => {
        const cleanup = () => {
          clearTimeout(timeout);
          child.stdout?.off('data', checkReady);
          child.off('exit', onEarlyExit);
        };

        const timeout = setTimeout(() => {
          cleanup();
          reject(
            new Error(
              `Timed out waiting for server startup. Stdout: ${stdout}\nStderr: ${stderr}`
            )
          );
        }, 15000);

        const checkReady = () => {
          if (stdout.includes(`HTTP Server listening on port ${port}`)) {
            cleanup();
            resolve();
          }
        };

        const onEarlyExit = (code: number | null) => {
          cleanup();
          reject(new Error(`Server exited early with code ${code}. Stderr: ${stderr}`));
        };

        child.stdout?.on('data', checkReady);
        child.on('exit', onEarlyExit);
      });

      const baseUrl = `http://127.0.0.1:${port}`;

      // Submit tenant and run
      const tenantRes = await fetch(`${baseUrl}/api/v1/tenants`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: 'proc-tenant-1',
          name: 'Process Tenant',
          maxConcurrency: 2,
        }),
      });
      expect(tenantRes.status).toBe(201);

      const runRes = await fetch(`${baseUrl}/api/v1/workflows/process-echo/runs`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': 'proc-echo-key-1',
        },
        body: JSON.stringify({
          tenantId: 'proc-tenant-1',
          input: { task: 'verify-dev-pipeline' },
        }),
      });
      expect(runRes.status).toBe(201);
      const { run } = (await runRes.json()) as { run: Run };

      let completedRun: Run | null = null;
      const deadline = Date.now() + 10000;
      while (Date.now() < deadline) {
        const res = await fetch(`${baseUrl}/api/v1/runs/${run.id}`);
        const data = (await res.json()) as { run: Run };
        if (data.run.status === 'COMPLETED') {
          completedRun = data.run;
          break;
        }
        await new Promise((r) => setTimeout(r, 50));
      }

      expect(completedRun).not.toBeNull();
      expect(completedRun!.status).toBe('COMPLETED');
      expect(completedRun!.output).toMatchObject({
        echoed: true,
        runId: run.id,
        input: { task: 'verify-dev-pipeline' },
      });

      // Send SIGTERM and verify clean exit code 0
      const exitResult = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
        (resolve) => {
          child.once('exit', (code, signal) => resolve({ code, signal }));
          child.kill('SIGTERM');
        }
      );

      expect(exitResult.code).toBe(0);
      expect(stdout).toContain('Shutdown complete.');
    } finally {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill('SIGKILL');
      }
    }
  });
});
