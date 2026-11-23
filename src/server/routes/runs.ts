import { Hono } from 'hono';
import { z } from 'zod';
import crypto from 'node:crypto';
import type { Pool } from '../../db/client.js';
import { withTransaction } from '../../db/client.js';
import type { Run, StepCheckpoint, Tenant } from '../../types/domain.js';

const submitRunSchema = z.object({
  tenantId: z.string().min(1).max(64),
  input: z.record(z.string(), z.unknown()).optional().default({}),
  idempotencyKey: z.string().min(1).max(255).optional(),
});

export function createRunRoutes(pool: Pool) {
  const router = new Hono();

  // POST /api/v1/workflows/:workflowName/runs
  router.post('/workflows/:workflowName/runs', async (c) => {
    const workflowName = c.req.param('workflowName');
    const body = await c.req.json().catch(() => null);
    const parsed = submitRunSchema.safeParse(body);

    if (!parsed.success) {
      return c.json({ error: 'Validation failed', details: parsed.error.issues }, 400);
    }

    const { tenantId, input } = parsed.data;

    // Refinement 1: Handle Idempotency-Key header or body field or generate fallback
    const headerKey = c.req.header('idempotency-key') || c.req.header('Idempotency-Key');
    const idempotencyKey = (headerKey || parsed.data.idempotencyKey || crypto.randomUUID()).trim();

    try {
      const result = await withTransaction(pool, async (client) => {
        // 1. Verify tenant exists
        const { rows: tenantRows } = await client.query<Tenant>(
          `SELECT id, max_concurrency FROM tenants WHERE id = $1`,
          [tenantId]
        );

        if (tenantRows.length === 0) {
          return { error: 'Tenant not found', statusCode: 404 as const };
        }

        // 2. Check for duplicate run with same (tenant_id, idempotency_key)
        const { rows: existingRows } = await client.query<Run>(
          `SELECT * FROM runs WHERE tenant_id = $1 AND idempotency_key = $2`,
          [tenantId, idempotencyKey]
        );

        if (existingRows.length > 0 && existingRows[0]) {
          return {
            run: existingRows[0],
            isDuplicate: true,
            statusCode: 200 as const,
          };
        }

        // 3. Insert new run in PENDING status
        const { rows: runRows } = await client.query<Run>(
          `INSERT INTO runs (tenant_id, workflow_name, idempotency_key, input, status)
           VALUES ($1, $2, $3, $4::jsonb, 'PENDING')
           RETURNING *`,
          [tenantId, workflowName, idempotencyKey, JSON.stringify(input)]
        );

        const newRun = runRows[0];
        if (!newRun) {
          throw new Error('Failed to insert run');
        }

        // 4. Atomically insert outbox event
        const outboxPayload = {
          run_id: newRun.id,
          tenant_id: newRun.tenant_id,
          workflow_name: newRun.workflow_name,
          idempotency_key: newRun.idempotency_key,
          input: newRun.input,
        };

        await client.query(
          `INSERT INTO outbox_events (tenant_id, event_type, payload, status, available_at)
           VALUES ($1, 'RUN_CREATED', $2::jsonb, 'PENDING', NOW())`,
          [tenantId, JSON.stringify(outboxPayload)]
        );

        return {
          run: newRun,
          isDuplicate: false,
          statusCode: 201 as const,
        };
      });

      if ('error' in result) {
        return c.json({ error: result.error }, result.statusCode);
      }

      return c.json(
        {
          run: result.run,
          isDuplicate: result.isDuplicate,
        },
        result.statusCode
      );
    } catch (err: any) {
      return c.json({ error: 'Failed to submit workflow run', message: err.message }, 500);
    }
  });

  // GET /api/v1/runs/:id
  router.get('/runs/:id', async (c) => {
    const runId = c.req.param('id');

    const { rows: runRows } = await pool.query<Run>(
      `SELECT * FROM runs WHERE id = $1`,
      [runId]
    );

    const run = runRows[0];
    if (!run) {
      return c.json({ error: 'Run not found' }, 404);
    }

    const { rows: checkpointRows } = await pool.query<StepCheckpoint>(
      `SELECT id, run_id, step_key, output, status, created_at
       FROM step_checkpoints
       WHERE run_id = $1
       ORDER BY id ASC`,
      [runId]
    );

    return c.json({
      run,
      checkpoints: checkpointRows,
    });
  });

  return router;
}
