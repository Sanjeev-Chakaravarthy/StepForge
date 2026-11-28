import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { z } from 'zod';
import { loadEnv } from '../config/env.js';
import { createDbPool, type Pool } from '../db/client.js';
import type { Run, RunApproval, StepCheckpoint } from '../types/domain.js';

const uuidSchema = z.string().uuid();

function formatIso(value: Date | string | null | undefined): string | null {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toISOString();
}

function formatJsonInline(value: unknown): string {
  if (value === null || value === undefined) return 'null';
  return JSON.stringify(value);
}

function shellSingleQuoteEscape(str: string): string {
  return str.replace(/'/g, "'\\''");
}

export interface InspectRunListRow {
  id: string;
  tenant_id: string;
  workflow_name: string;
  status: string;
  attempt_count: number;
  current_lease_worker_id: string | null;
  updated_at: string | null;
}

export interface PendingApprovalRow {
  run_id: string;
  tenant_id: string;
  workflow_name: string;
  gate_key: string;
  prompt_data: Record<string, unknown>;
  created_at: Date;
}

export async function inspectRuns(pool: Pool): Promise<InspectRunListRow[]> {
  const { rows } = await pool.query<
    Pick<
      Run,
      | 'id'
      | 'tenant_id'
      | 'workflow_name'
      | 'status'
      | 'attempt_count'
      | 'current_lease_worker_id'
      | 'updated_at'
    >
  >(
    `SELECT id, tenant_id, workflow_name, status, attempt_count, current_lease_worker_id, updated_at
     FROM runs
     ORDER BY updated_at DESC, created_at DESC
     LIMIT 20`
  );

  if (rows.length === 0) {
    console.log('No runs found.');
    return [];
  }

  const formatted: InspectRunListRow[] = rows.map((row) => ({
    id: row.id,
    tenant_id: row.tenant_id,
    workflow_name: row.workflow_name,
    status: row.status,
    attempt_count: row.attempt_count,
    current_lease_worker_id: row.current_lease_worker_id,
    updated_at: formatIso(row.updated_at),
  }));

  console.log(`\nRecent Runs (${formatted.length}):`);
  console.table(formatted);
  return formatted;
}

export async function inspectRun(
  pool: Pool,
  runId: string
): Promise<{
  run: Run;
  checkpoints: StepCheckpoint[];
  approvals: RunApproval[];
}> {
  if (!uuidSchema.safeParse(runId).success) {
    throw new Error(`Invalid runId "${runId}". Expected a valid UUID.`);
  }

  const { rows: runRows } = await pool.query<Run>(`SELECT * FROM runs WHERE id = $1`, [runId]);
  const run = runRows[0];
  if (!run) {
    throw new Error(`Run "${runId}" not found.`);
  }

  const { rows: checkpoints } = await pool.query<StepCheckpoint>(
    `SELECT id, run_id, step_key, step_type, status, metadata, output, created_at
     FROM step_checkpoints
     WHERE run_id = $1
     ORDER BY id ASC`,
    [runId]
  );

  const { rows: approvals } = await pool.query<RunApproval>(
    `SELECT id, run_id, gate_key, status, prompt_data, payload, created_at, resolved_at
     FROM run_approvals
     WHERE run_id = $1
     ORDER BY id ASC`,
    [runId]
  );

  console.log('\n=== Run Summary ===');
  console.log(
    JSON.stringify(
      {
        id: run.id,
        tenant_id: run.tenant_id,
        workflow_name: run.workflow_name,
        idempotency_key: run.idempotency_key,
        status: run.status,
        attempt_count: run.attempt_count,
        max_attempts: run.max_attempts,
        current_lease_worker_id: run.current_lease_worker_id,
        lease_expires_at: formatIso(run.lease_expires_at),
        scheduled_at: formatIso(run.scheduled_at),
        created_at: formatIso(run.created_at),
        updated_at: formatIso(run.updated_at),
        input: run.input,
        output: run.output,
        error: run.error,
      },
      null,
      2
    )
  );

  console.log(`\n=== Step Checkpoints (${checkpoints.length}) ===`);
  if (checkpoints.length === 0) {
    console.log('(none)');
  } else {
    console.table(
      checkpoints.map((cp) => ({
        step_key: cp.step_key,
        step_type: cp.step_type,
        status: cp.status,
        metadata: formatJsonInline(cp.metadata),
        output: formatJsonInline(cp.output),
      }))
    );
  }

  console.log(`\n=== Run Approvals (${approvals.length}) ===`);
  if (approvals.length === 0) {
    console.log('(none)');
  } else {
    console.table(
      approvals.map((ap) => ({
        gate_key: ap.gate_key,
        status: ap.status,
        payload: formatJsonInline(ap.payload),
        resolved_at: formatIso(ap.resolved_at),
      }))
    );
  }

  return { run, checkpoints, approvals };
}

export async function inspectPendingApprovals(
  pool: Pool,
  baseUrl: string
): Promise<PendingApprovalRow[]> {
  const normalizedBaseUrl = baseUrl.replace(/\/+$/, '');
  const { rows } = await pool.query<PendingApprovalRow>(
    `SELECT
       r.id AS run_id,
       r.tenant_id,
       r.workflow_name,
       a.gate_key,
       a.prompt_data,
       a.created_at
     FROM runs r
     JOIN run_approvals a ON a.run_id = r.id
     WHERE r.status = 'WAITING_HUMAN'
       AND a.status = 'PENDING'
     ORDER BY a.created_at ASC`
  );

  if (rows.length === 0) {
    console.log('No runs currently awaiting human approval (WAITING_HUMAN).');
    return [];
  }

  console.log(`\n=== Pending Human Approvals (${rows.length}) ===`);
  for (const row of rows) {
    const approveBody = shellSingleQuoteEscape(
      JSON.stringify({
        gateKey: row.gate_key,
        payload: { approvedBy: 'cli-operator' },
      })
    );
    const rejectBody = shellSingleQuoteEscape(
      JSON.stringify({
        gateKey: row.gate_key,
        payload: { rejectedBy: 'cli-operator', reason: 'rejected via CLI' },
      })
    );

    console.log(`\nRun ID:       ${row.run_id}`);
    console.log(`Tenant:       ${row.tenant_id}`);
    console.log(`Workflow:     ${row.workflow_name}`);
    console.log(`Gate Key:     ${row.gate_key}`);
    console.log(`Created At:   ${formatIso(row.created_at)}`);
    console.log(`Prompt Data:  ${JSON.stringify(row.prompt_data)}`);
    console.log('Approve:');
    console.log(
      `  curl -X POST ${normalizedBaseUrl}/api/v1/runs/${row.run_id}/approve -H "Content-Type: application/json" -d '${approveBody}'`
    );
    console.log('Reject:');
    console.log(
      `  curl -X POST ${normalizedBaseUrl}/api/v1/runs/${row.run_id}/reject -H "Content-Type: application/json" -d '${rejectBody}'`
    );
  }

  return rows;
}

function printUsage(): void {
  console.log(`Usage:
  npm run inspect -- runs
  npm run inspect -- run <runId>
  npm run inspect -- pending-approvals`);
}

export async function runInspectCli(
  args: string[],
  pool: Pool,
  baseUrl: string
): Promise<void> {
  const command = args[0];

  switch (command) {
    case 'runs':
      await inspectRuns(pool);
      return;
    case 'run': {
      const runId = args[1];
      if (!runId) {
        printUsage();
        throw new Error('Missing <runId> argument for "run" command.');
      }
      await inspectRun(pool, runId);
      return;
    }
    case 'pending-approvals':
      await inspectPendingApprovals(pool, baseUrl);
      return;
    default:
      printUsage();
      throw new Error(
        command ? `Unknown inspect command "${command}".` : 'Missing inspect command.'
      );
  }
}

async function main(): Promise<void> {
  const config = loadEnv();
  const baseUrl = process.env.STEPFORGE_URL || `http://localhost:${config.PORT}`;
  const pool = createDbPool(config.DATABASE_URL);

  try {
    await runInspectCli(process.argv.slice(2), pool, baseUrl);
  } finally {
    await pool.end();
  }
}

const entryArg = process.argv[1];
const isMain =
  entryArg !== undefined &&
  fs.existsSync(path.resolve(entryArg)) &&
  fs.realpathSync(path.resolve(entryArg)) ===
    fs.realpathSync(fileURLToPath(import.meta.url));

if (isMain) {
  main().catch((err) => {
    console.error('[StepForge Inspect]', err instanceof Error ? err.message : err);
    process.exit(1);
  });
}
