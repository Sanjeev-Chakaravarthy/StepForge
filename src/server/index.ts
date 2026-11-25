import type { AddressInfo } from 'node:net';
import { serve, type ServerType } from '@hono/node-server';
import { Redis } from 'ioredis';
import { loadEnv, type EnvConfig } from '../config/env.js';
import { createDbPool, type Pool } from '../db/client.js';
import { runMigrations } from '../db/migrate.js';
import { OutboxPoller } from '../outbox/poller.js';
import { LeaseReaper } from '../reaper/lease_reaper.js';
import { DelayQueue } from '../worker/delay_queue.js';
import { Worker, type WorkflowHandler } from '../worker/worker.js';
import { defaultWorkflowRegistry } from '../workflow/registry.js';
import { createApp } from './app.js';

/**
 * Default fallback workflow handler used when a submitted workflow_name is not
 * explicitly registered in the WorkflowRegistry. Executes an idempotent echo
 * step so queued runs complete end-to-end with a persisted step checkpoint.
 */
export const echoWorkflowHandler: WorkflowHandler = async (run, step) => {
  return await step.run('echo', async () => ({
    echoed: true,
    runId: run.id,
    tenantId: run.tenant_id,
    workflowName: run.workflow_name,
    input: run.input,
    executedAt: new Date().toISOString(),
  }));
};

export interface StepForgeRuntime {
  app: ReturnType<typeof createApp>;
  server: ServerType;
  port: number;
  pool: Pool;
  redis: Redis;
  workerRedis: Redis;
  poller: OutboxPoller;
  delayQueue: DelayQueue;
  worker: Worker;
  reaper: LeaseReaper;
  shutdown: (signal?: string) => Promise<void>;
}

async function closeRedisClient(client: Redis): Promise<void> {
  if (client.status === 'end') return;
  try {
    await Promise.race([
      client.quit(),
      new Promise<void>((resolve) => setTimeout(resolve, 1000)),
    ]);
  } catch {
    // Ignore quit errors on already-closing connections
  } finally {
    client.disconnect();
  }
}

export async function startRuntime(
  overrideConfig?: Partial<EnvConfig>
): Promise<StepForgeRuntime> {
  const config = loadEnv(overrideConfig);

  console.log(`[StepForge] Starting up in ${config.NODE_ENV} mode...`);

  const pool = createDbPool(config.DATABASE_URL);
  const redis = new Redis(config.REDIS_URL);
  const workerRedis = redis.duplicate();

  let isShuttingDown = false;

  const attachRedisErrorLogger = (label: string, client: Redis) => {
    client.on('error', (err) => {
      if (isShuttingDown) return;
      console.error(`[StepForge] ${label} Redis error:`, err.message);
    });
  };

  attachRedisErrorLogger('Primary', redis);
  attachRedisErrorLogger('Worker', workerRedis);

  let poller: OutboxPoller | null = null;
  let delayQueue: DelayQueue | null = null;
  let reaper: LeaseReaper | null = null;
  let worker: Worker | null = null;
  let server: ServerType | null = null;

  try {
    // 1. Run DB migrations
    console.log('[StepForge] Applying database migrations...');
    await runMigrations(pool);
    console.log('[StepForge] Migrations applied successfully.');

    // 2. Initialize and start Transactional Outbox Poller
    poller = new OutboxPoller(pool, redis, {
      pollIntervalMs: config.OUTBOX_POLL_INTERVAL_MS,
      batchSize: config.OUTBOX_BATCH_SIZE,
      streamKey: config.REDIS_STREAM_KEY,
    });
    poller.start();
    console.log('[StepForge] Transactional Outbox Poller started.');

    // 3. Initialize and start DelayQueue dispatcher (for quota-deferred requeues)
    delayQueue = new DelayQueue(redis, {
      streamKey: config.REDIS_STREAM_KEY,
      pollIntervalMs: config.DELAY_QUEUE_POLL_INTERVAL_MS,
      batchSize: config.OUTBOX_BATCH_SIZE,
    });
    delayQueue.start();
    console.log('[StepForge] DelayQueue dispatcher started.');

    // 4. Initialize and start Background Lease Reaper
    reaper = new LeaseReaper(pool, redis, {
      reaperIntervalMs: config.REAPER_INTERVAL_MS,
    });
    reaper.start();
    console.log('[StepForge] Background Lease Reaper started.');

    // 5. Initialize and start Redis Streams Worker (dedicated blocking connection)
    worker = new Worker(pool, workerRedis, {
      streamKey: config.REDIS_STREAM_KEY,
      groupName: config.WORKER_GROUP_NAME,
      leaseTtlMs: config.WORKER_LEASE_TTL_MS,
      heartbeatIntervalMs: config.WORKER_HEARTBEAT_INTERVAL_MS,
      delayQueue,
      registry: defaultWorkflowRegistry,
      handler: echoWorkflowHandler,
    });
    await worker.start();
    console.log(`[StepForge] Worker "${worker.workerId}" started.`);

    // 6. Start Hono HTTP Server
    const app = createApp({ pool, redis });

    let boundPort = config.PORT;
    server = await new Promise<ServerType>((resolve, reject) => {
      const onError = (err: Error) => reject(err);
      const srv = serve(
        {
          fetch: app.fetch,
          port: config.PORT,
        },
        (info: AddressInfo) => {
          srv.off('error', onError);
          boundPort = info.port;
          console.log(`[StepForge] HTTP Server listening on port ${info.port}`);
          resolve(srv);
        }
      );
      srv.once('error', onError);
    });

    let shutdownPromise: Promise<void> | null = null;
    const shutdown = (signal?: string): Promise<void> => {
      if (shutdownPromise) return shutdownPromise;
      isShuttingDown = true;

      shutdownPromise = (async () => {
        console.log(
          `[StepForge] Shutting down gracefully${signal ? ` (${signal})` : ''}...`
        );

        await new Promise<void>((resolve) => {
          server!.close(() => resolve());
          if ('closeIdleConnections' in server! && typeof server.closeIdleConnections === 'function') {
            server.closeIdleConnections();
          }
        });

        await poller!.stop();
        await reaper!.stop();
        await worker!.stop();
        await delayQueue!.stop();

        await pool.end();
        await closeRedisClient(workerRedis);
        await closeRedisClient(redis);

        console.log('[StepForge] Shutdown complete.');
      })();

      return shutdownPromise;
    };

    return {
      app,
      server,
      port: boundPort,
      pool,
      redis,
      workerRedis,
      poller,
      delayQueue,
      worker,
      reaper,
      shutdown,
    };
  } catch (err) {
    isShuttingDown = true;
    server?.close();
    await poller?.stop();
    await reaper?.stop();
    await worker?.stop();
    await delayQueue?.stop();
    await pool.end().catch(() => {});
    await closeRedisClient(workerRedis);
    await closeRedisClient(redis);
    throw err;
  }
}

export async function bootstrap(): Promise<void> {
  const runtime = await startRuntime();

  let exiting = false;
  const handleSignal = (signal: NodeJS.Signals) => {
    if (exiting) return;
    exiting = true;

    const forceTimer = setTimeout(() => {
      console.error('[StepForge] Graceful shutdown timed out, forcing exit.');
      process.exit(1);
    }, 10000);
    forceTimer.unref();

    runtime
      .shutdown(signal)
      .then(() => {
        clearTimeout(forceTimer);
        process.exit(0);
      })
      .catch((err) => {
        console.error('[StepForge] Error during shutdown:', err);
        clearTimeout(forceTimer);
        process.exit(1);
      });
  };

  process.once('SIGTERM', () => handleSignal('SIGTERM'));
  process.once('SIGINT', () => handleSignal('SIGINT'));
}

if (process.env.NODE_ENV !== 'test') {
  bootstrap().catch((err) => {
    console.error('[StepForge] Fatal bootstrap error:', err);
    process.exit(1);
  });
}
