import { serve } from '@hono/node-server';
import { Redis } from 'ioredis';
import { loadEnv } from '../config/env.js';
import { createDbPool } from '../db/client.js';
import { runMigrations } from '../db/migrate.js';
import { OutboxPoller } from '../outbox/poller.js';
import { createApp } from './app.js';

async function bootstrap() {
  const config = loadEnv();

  console.log(`[StepForge] Starting up in ${config.NODE_ENV} mode...`);

  const pool = createDbPool(config.DATABASE_URL);
  const redis = new Redis(config.REDIS_URL);

  // Run DB migrations
  console.log('[StepForge] Applying database migrations...');
  await runMigrations(pool);
  console.log('[StepForge] Migrations applied successfully.');

  // Initialize and start Outbox Poller
  const poller = new OutboxPoller(pool, redis, {
    pollIntervalMs: config.OUTBOX_POLL_INTERVAL_MS,
    batchSize: config.OUTBOX_BATCH_SIZE,
    streamKey: config.REDIS_STREAM_KEY,
  });
  poller.start();
  console.log('[StepForge] Transactional Outbox Poller started.');

  // Create HTTP App
  const app = createApp({ pool, redis });

  serve(
    {
      fetch: app.fetch,
      port: config.PORT,
    },
    (info) => {
      console.log(`[StepForge] HTTP Server listening on port ${info.port}`);
    }
  );

  // Graceful shutdown
  const shutdown = async () => {
    console.log('[StepForge] Shutting down gracefully...');
    poller.stop();
    await pool.end();
    redis.disconnect();
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

if (process.env.NODE_ENV !== 'test') {
  bootstrap().catch((err) => {
    console.error('[StepForge] Fatal bootstrap error:', err);
    process.exit(1);
  });
}
