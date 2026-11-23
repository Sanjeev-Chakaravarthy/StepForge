import { Hono } from 'hono';
import { z } from 'zod';
import type { Pool } from '../../db/client.js';
import type { Tenant } from '../../types/domain.js';

const createTenantSchema = z.object({
  id: z.string().min(1).max(64),
  name: z.string().min(1).max(255),
  maxConcurrency: z.number().int().positive().optional().default(5),
});

export function createTenantRoutes(pool: Pool) {
  const router = new Hono();

  router.post('/', async (c) => {
    const body = await c.req.json().catch(() => null);
    const parsed = createTenantSchema.safeParse(body);

    if (!parsed.success) {
      return c.json({ error: 'Validation failed', details: parsed.error.issues }, 400);
    }

    const { id, name, maxConcurrency } = parsed.data;

    try {
      const { rows } = await pool.query<Tenant>(
        `INSERT INTO tenants (id, name, max_concurrency)
         VALUES ($1, $2, $3)
         RETURNING id, name, max_concurrency, created_at, updated_at`,
        [id, name, maxConcurrency]
      );

      const tenant = rows[0];
      return c.json({ tenant }, 201);
    } catch (err: any) {
      if (err.code === '23505') {
        // Unique violation
        return c.json({ error: `Tenant with id '${id}' already exists` }, 409);
      }
      return c.json({ error: 'Failed to create tenant', message: err.message }, 500);
    }
  });

  router.get('/:id', async (c) => {
    const id = c.req.param('id');
    const { rows } = await pool.query<Tenant>(
      `SELECT id, name, max_concurrency, created_at, updated_at
       FROM tenants
       WHERE id = $1`,
      [id]
    );

    const tenant = rows[0];
    if (!tenant) {
      return c.json({ error: 'Tenant not found' }, 404);
    }

    return c.json({ tenant });
  });

  return router;
}
