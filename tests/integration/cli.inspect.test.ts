import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool, createDbPool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import {
  inspectPendingApprovals,
  inspectRun,
  inspectRuns,
  runInspectCli,
} from '../../src/cli/inspect.js';
import type { Run } from '../../src/types/domain.js';

const execFileAsync = promisify(execFile);
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('StepForge - Read-Only CLI Inspection Tests', () => {
  let pgContainer: StartedPostgreSqlContainer;
  let pool: Pool;
  let completedRunId: string;
  let waitingRunId: string;

  beforeAll(async () => {
    pgContainer = await new PostgreSqlContainer('postgres:16-alpine').start();
    pool = createDbPool(pgContainer.getConnectionUri());
    await runMigrations(pool);

    await pool.query(
      `INSERT INTO tenants (id, name, max_concurrency)
       VALUES ('cli-tenant', 'CLI Tenant', 5)`
    );

    // Seed 22 completed runs to verify LIMIT 20 ordering
    for (let i = 1; i <= 22; i++) {
      const { rows } = await pool.query<Run>(
        `INSERT INTO runs (
           tenant_id, workflow_name, idempotency_key, status, attempt_count,
           input, output, updated_at
         )
         VALUES (
           'cli-tenant', 'echo-demo', $1, 'COMPLETED'::run_status, 1,
           $2::jsonb, $3::jsonb, NOW() + ($4 || ' seconds')::interval
         )
         RETURNING *`,
        [
          `cli-key-${i}`,
          JSON.stringify({ idx: i }),
          JSON.stringify({ echoed: true, idx: i }),
          i,
        ]
      );
      if (i === 22) {
        completedRunId = rows[0]!.id;
      }
    }

    // Seed checkpoints & resolved approval on the latest completed run
    await pool.query(
      `INSERT INTO step_checkpoints (run_id, step_key, step_type, metadata, output, status)
       VALUES
         ($1, 'plan', 'LLM', '{"model":"gemini-2.5-pro","tokenUsage":{"totalTokens":120},"latencyMs":15}'::jsonb, '{"action":"ship"}'::jsonb, 'COMPLETED'),
         ($1, 'tool:lookup:abc123', 'TOOL', '{"toolName":"lookup","args":{"id":"1"},"latencyMs":4}'::jsonb, '{"found":true}'::jsonb, 'COMPLETED')`,
      [completedRunId]
    );
    await pool.query(
      `INSERT INTO run_approvals (run_id, gate_key, status, prompt_data, payload, resolved_at)
       VALUES ($1, 'ship-gate', 'APPROVED', '{"amount":50}'::jsonb, '{"approvedBy":"alice"}'::jsonb, NOW())`,
      [completedRunId]
    );

    // Seed a run currently paused in WAITING_HUMAN
    const { rows: waitingRows } = await pool.query<Run>(
      `INSERT INTO runs (
         tenant_id, workflow_name, idempotency_key, status, attempt_count,
         input, updated_at
       )
       VALUES (
         'cli-tenant', 'ai-refund-agent', 'cli-waiting-1', 'WAITING_HUMAN'::run_status, 0,
         '{"orderId":"ORD-77"}'::jsonb, NOW() + INTERVAL '60 seconds'
       )
       RETURNING *`
    );
    waitingRunId = waitingRows[0]!.id;

    await pool.query(
      `INSERT INTO run_approvals (run_id, gate_key, status, prompt_data)
       VALUES ($1, 'confirm-refund', 'PENDING', '{"amount":75,"orderId":"ORD-77"}'::jsonb)`,
      [waitingRunId]
    );
  });

  afterAll(async () => {
    if (pool) await pool.end();
    if (pgContainer) await pgContainer.stop();
  });

  it('should list at most 20 recent runs ordered by updated_at DESC', async () => {
    const tableSpy = vi.spyOn(console, 'table').mockImplementation(() => {});
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    try {
      const rows = await inspectRuns(pool);
      expect(rows.length).toBe(20);
      expect(rows[0]!.id).toBe(waitingRunId);
      expect(rows[1]!.id).toBe(completedRunId);
      expect(tableSpy).toHaveBeenCalledTimes(1);
    } finally {
      tableSpy.mockRestore();
      logSpy.mockRestore();
    }
  });

  it('should inspect a single run with ordered step_checkpoints and run_approvals', async () => {
    const tableSpy = vi.spyOn(console, 'table').mockImplementation(() => {});
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    try {
      const details = await inspectRun(pool, completedRunId);
      expect(details.run.id).toBe(completedRunId);
      expect(details.checkpoints.map((c) => [c.step_key, c.step_type])).toEqual([
        ['plan', 'LLM'],
        ['tool:lookup:abc123', 'TOOL'],
      ]);
      expect(details.approvals.length).toBe(1);
      expect(details.approvals[0]!.gate_key).toBe('ship-gate');
      expect(details.approvals[0]!.status).toBe('APPROVED');
      expect(tableSpy).toHaveBeenCalledTimes(2);
    } finally {
      tableSpy.mockRestore();
      logSpy.mockRestore();
    }
  });

  it('should list WAITING_HUMAN pending approvals with actionable approve and reject curl commands', async () => {
    const logs: string[] = [];
    const logSpy = vi.spyOn(console, 'log').mockImplementation((...args) => {
      logs.push(args.join(' '));
    });

    try {
      const pending = await inspectPendingApprovals(pool, 'http://localhost:3000');
      expect(pending.length).toBe(1);
      expect(pending[0]!.run_id).toBe(waitingRunId);
      expect(pending[0]!.gate_key).toBe('confirm-refund');

      const output = logs.join('\n');
      expect(output).toContain(`/api/v1/runs/${waitingRunId}/approve`);
      expect(output).toContain(`/api/v1/runs/${waitingRunId}/reject`);
      expect(output).toContain('"gateKey":"confirm-refund"');
    } finally {
      logSpy.mockRestore();
    }
  });

  it('should validate unknown commands and execute cleanly as a real CLI subprocess', async () => {
    await expect(runInspectCli(['unknown'], pool, 'http://localhost:3000')).rejects.toThrow(
      'Unknown inspect command'
    );

    const cliScript = path.resolve(__dirname, '../../src/cli/inspect.ts');
    const { stdout } = await execFileAsync('npx', ['tsx', cliScript, 'pending-approvals'], {
      env: {
        ...process.env,
        DATABASE_URL: pgContainer.getConnectionUri(),
        PORT: '3000',
      },
    });

    expect(stdout).toContain(waitingRunId);
    expect(stdout).toContain('confirm-refund');
    expect(stdout).toContain(`curl -X POST http://localhost:3000/api/v1/runs/${waitingRunId}/approve`);
  });
});
