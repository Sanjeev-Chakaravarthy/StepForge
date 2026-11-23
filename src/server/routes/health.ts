import { Hono } from 'hono';
import type { Pool } from '../../db/client.js';
import { Redis } from 'ioredis';

export function createHealthRoutes(pool: Pool, redis: Redis) {
  const router = new Hono();

  router.get('/', async (c) => {
    let postgresOk = false;
    let redisOk = false;

    try {
      await pool.query('SELECT 1');
      postgresOk = true;
    } catch {
      postgresOk = false;
    }

    try {
      const pong = await redis.ping();
      redisOk = pong === 'PONG';
    } catch {
      redisOk = false;
    }

    const isHealthy = postgresOk && redisOk;
    return c.json(
      {
        status: isHealthy ? 'ok' : 'degraded',
        postgres: postgresOk,
        redis: redisOk,
        timestamp: new Date().toISOString(),
      },
      isHealthy ? 200 : 503
    );
  });

  return router;
}
