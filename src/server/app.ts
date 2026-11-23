import { Hono } from 'hono';
import type { Pool } from '../db/client.js';
import { Redis } from 'ioredis';
import { createTenantRoutes } from './routes/tenants.js';
import { createRunRoutes } from './routes/runs.js';
import { createHealthRoutes } from './routes/health.js';

export interface AppDependencies {
  pool: Pool;
  redis: Redis;
}

export function createApp(deps: AppDependencies): Hono {
  const app = new Hono();

  // Root / Health
  app.route('/health', createHealthRoutes(deps.pool, deps.redis));

  // API v1
  const apiV1 = new Hono();
  apiV1.route('/tenants', createTenantRoutes(deps.pool));
  apiV1.route('/', createRunRoutes(deps.pool));

  app.route('/api/v1', apiV1);

  // Global Error Handler
  app.onError((err, c) => {
    console.error('Unhandled server error:', err);
    return c.json(
      {
        error: 'Internal Server Error',
        message: err.message,
      },
      500
    );
  });

  return app;
}
