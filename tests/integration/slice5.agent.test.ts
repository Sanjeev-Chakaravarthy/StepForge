import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import { Pool, createDbPool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import { createApp } from '../../src/server/app.js';
import { OutboxPoller } from '../../src/outbox/poller.js';
import { Worker } from '../../src/worker/worker.js';
import { WorkflowRegistry } from '../../src/workflow/registry.js';
import { RedisLeaseManager } from '../../src/redis/lease.js';
import type { Run, RunApproval, StepCheckpoint } from '../../src/types/domain.js';
import type { StepContext } from '../../src/step/context.js';

describe('StepForge - Slice 5 AI Agent Durable Runtime & HITL Tests', () => {
  let pgContainer: StartedPostgreSqlContainer;
  let redisContainer: StartedRedisContainer;
  let pool: Pool;
  let redis: Redis;
  let workerRedis: Redis;
  let app: ReturnType<typeof createApp>;

  beforeAll(async () => {
    pgContainer = await new PostgreSqlContainer('postgres:16-alpine').start();
    redisContainer = await new RedisContainer('redis:7-alpine').start();

    pool = createDbPool(pgContainer.getConnectionUri());
    redis = new Redis(redisContainer.getConnectionUrl());
    workerRedis = redis.duplicate();

    await runMigrations(pool);
    app = createApp({ pool, redis });
  });

  afterAll(async () => {
    if (pool) await pool.end();
    workerRedis?.disconnect();
    redis?.disconnect();
    if (pgContainer) await pgContainer.stop();
    if (redisContainer) await redisContainer.stop();
  });

  it('should apply 002_agent_runtime.sql idempotently with WAITING_HUMAN, run_approvals, and checkpoint metadata', async () => {
    // Re-running migrations must succeed idempotently
    await expect(runMigrations(pool)).resolves.toBeUndefined();

    const { rows: enumRows } = await pool.query<{ enumlabel: string }>(
      `SELECT e.enumlabel
       FROM pg_enum e
       JOIN pg_type t ON e.enumtypid = t.oid
       WHERE t.typname = 'run_status' AND e.enumlabel = 'WAITING_HUMAN'`
    );
    expect(enumRows.length).toBe(1);

    const { rows: colRows } = await pool.query<{ column_name: string }>(
      `SELECT column_name
       FROM information_schema.columns
       WHERE table_name = 'step_checkpoints'
         AND column_name IN ('step_type', 'metadata')`
    );
    expect(colRows.map((c) => c.column_name).sort()).toEqual(['metadata', 'step_type']);
  });

  it('should execute LLM -> Tool -> waitForApproval -> approve -> Sandboxed -> COMPLETED with zero lease holding and strict once-only prior steps', async () => {
    const streamKey = 'stepforge:stream:tasks:slice5-approve';
    const groupName = 'stepforge:workers:slice5-approve';
    const tenantId = 'agent-tenant-hitl';

    // Tenant has maxConcurrency = 1 to prove WAITING_HUMAN releases the Redis lease
    const tenantRes = await app.request('/api/v1/tenants', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: tenantId,
        name: 'Agent HITL Tenant',
        maxConcurrency: 1,
      }),
    });
    expect(tenantRes.status).toBe(201);

    let llmExecutionCount = 0;
    let toolExecutionCount = 0;

    const registry = new WorkflowRegistry();
    registry.register('ai-refund-agent', async (run: Run, step: StepContext) => {
      // Step 1: LLM planning step
      const plan = await step.llm('plan-refund', async ({ recordUsage }) => {
        llmExecutionCount++;
        recordUsage({
          promptTokens: 180,
          completionTokens: 45,
          totalTokens: 225,
          model: 'gemini-2.5-pro',
        });
        return {
          action: 'issue_refund',
          orderId: (run.input as { orderId: string }).orderId,
          amount: 75,
        };
      });

      // Step 2: Tool read step
      const orderRecord = await step.tool(
        'fetch-order-status',
        { orderId: plan.orderId },
        async (args) => {
          toolExecutionCount++;
          return {
            orderId: args.orderId,
            paidAmount: 75,
            status: 'delivered',
          };
        }
      );

      // Step 3: HITL approval gate before write/sandbox action
      const approval = await step.waitForApproval<{
        approvedBy: string;
        note: string;
      }>('confirm-refund-write', {
        proposedAction: plan.action,
        amount: plan.amount,
        orderId: orderRecord.orderId,
      });

      // Step 4: Sandboxed execution after approval
      const sandboxResult = await step.runSandboxed<{
        refunded: boolean;
        amount: number;
        approvedBy: string;
      }>('execute-refund-sandbox', {
        image: 'node:20-alpine',
        command: [
          'node',
          '-e',
          'const fs=require("fs");const i=JSON.parse(fs.readFileSync(0,"utf-8"));console.log(JSON.stringify({refunded:true,amount:i.amount,approvedBy:i.approvedBy}));',
        ],
        input: {
          amount: plan.amount,
          approvedBy: approval.approvedBy,
        },
      });

      return {
        plan,
        orderRecord,
        approval,
        sandboxResult,
      };
    });

    // Secondary fast workflow for same tenant (maxConcurrency: 1) to verify slot is freed
    registry.register('quick-ping', async (run: Run, step: StepContext) => {
      return step.run('ping', async () => ({ pong: true, runId: run.id }));
    });

    const poller = new OutboxPoller(pool, redis, {
      streamKey,
      pollIntervalMs: 30,
      batchSize: 20,
    });
    const worker = new Worker(pool, workerRedis, {
      workerId: 'agent-worker-1',
      streamKey,
      groupName,
      registry,
      blockTimeoutMs: 100,
      leaseTtlMs: 10000,
    });
    const leaseManager = new RedisLeaseManager(redis);

    poller.start();
    await worker.start();

    try {
      // 1. Submit the multi-step AI agent run
      const submitRes = await app.request('/api/v1/workflows/ai-refund-agent/runs', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': 'agent-hitl-run-1',
        },
        body: JSON.stringify({
          tenantId,
          input: { orderId: 'ORD-AGENT-55' },
        }),
      });
      expect(submitRes.status).toBe(201);
      const { run: createdRun } = (await submitRes.json()) as { run: Run };

      // 2. Wait until the run suspends in WAITING_HUMAN at Step 3
      let pausedPayload: {
        run: Run;
        checkpoints: StepCheckpoint[];
        approvals: RunApproval[];
      } | null = null;

      const waitHumanDeadline = Date.now() + 10000;
      while (Date.now() < waitHumanDeadline) {
        const res = await app.request(`/api/v1/runs/${createdRun.id}`);
        const body = (await res.json()) as {
          run: Run;
          checkpoints: StepCheckpoint[];
          approvals: RunApproval[];
        };
        if (body.run.status === 'WAITING_HUMAN') {
          pausedPayload = body;
          break;
        }
        await new Promise((r) => setTimeout(r, 40));
      }

      expect(pausedPayload).not.toBeNull();
      expect(pausedPayload!.run.status).toBe('WAITING_HUMAN');
      expect(pausedPayload!.run.current_lease_worker_id).toBeNull();
      expect(pausedPayload!.run.lease_expires_at).toBeNull();
      expect(llmExecutionCount).toBe(1);
      expect(toolExecutionCount).toBe(1);

      // CRITICAL: Active Redis lease count for this tenant must drop to 0 while waiting
      const activeLeasesWhileWaiting = await leaseManager.getActiveLeaseCount(tenantId);
      expect(activeLeasesWhileWaiting).toBe(0);

      // Verify pending gate metadata
      expect(pausedPayload!.approvals.length).toBe(1);
      expect(pausedPayload!.approvals[0]!.gate_key).toBe('confirm-refund-write');
      expect(pausedPayload!.approvals[0]!.status).toBe('PENDING');
      expect(pausedPayload!.approvals[0]!.prompt_data).toEqual({
        proposedAction: 'issue_refund',
        amount: 75,
        orderId: 'ORD-AGENT-55',
      });

      // Verify another run on the SAME tenant (maxConcurrency: 1) can execute while Run 1 is paused
      const pingSubmitRes = await app.request('/api/v1/workflows/quick-ping/runs', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': 'quick-ping-while-waiting',
        },
        body: JSON.stringify({ tenantId, input: {} }),
      });
      expect(pingSubmitRes.status).toBe(201);
      const { run: pingRun } = (await pingSubmitRes.json()) as { run: Run };

      let pingDone = false;
      const pingDeadline = Date.now() + 5000;
      while (Date.now() < pingDeadline) {
        const res = await app.request(`/api/v1/runs/${pingRun.id}`);
        const body = (await res.json()) as { run: Run };
        if (body.run.status === 'COMPLETED') {
          pingDone = true;
          break;
        }
        await new Promise((r) => setTimeout(r, 30));
      }
      expect(pingDone).toBe(true);

      // 3. Approve the pending HITL gate via HTTP API
      const approveRes = await app.request(`/api/v1/runs/${createdRun.id}/approve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          gateKey: 'confirm-refund-write',
          payload: {
            approvedBy: 'ops-lead-alice',
            note: 'Verified customer receipt',
          },
        }),
      });
      expect(approveRes.status).toBe(200);
      const approveBody = (await approveRes.json()) as {
        run: Run;
        approval: RunApproval;
      };
      expect(approveBody.approval.status).toBe('APPROVED');
      expect(approveBody.approval.resolved_at).not.toBeNull();

      // 4. Wait until the resumed run executes Step 4 (sandbox) and completes
      let completedPayload: {
        run: Run;
        checkpoints: StepCheckpoint[];
        approvals: RunApproval[];
      } | null = null;

      const completeDeadline = Date.now() + 15000;
      while (Date.now() < completeDeadline) {
        const res = await app.request(`/api/v1/runs/${createdRun.id}`);
        const body = (await res.json()) as {
          run: Run;
          checkpoints: StepCheckpoint[];
          approvals: RunApproval[];
        };
        if (body.run.status === 'COMPLETED') {
          completedPayload = body;
          break;
        }
        await new Promise((r) => setTimeout(r, 50));
      }

      expect(completedPayload).not.toBeNull();
      expect(completedPayload!.run.status).toBe('COMPLETED');
      expect(completedPayload!.run.output).toMatchObject({
        plan: { action: 'issue_refund', orderId: 'ORD-AGENT-55', amount: 75 },
        orderRecord: { orderId: 'ORD-AGENT-55', paidAmount: 75, status: 'delivered' },
        approval: { approvedBy: 'ops-lead-alice', note: 'Verified customer receipt' },
        sandboxResult: { refunded: true, amount: 75, approvedBy: 'ops-lead-alice' },
      });

      // CRITICAL: Steps 1 and 2 must have executed strictly once
      expect(llmExecutionCount).toBe(1);
      expect(toolExecutionCount).toBe(1);

      // Verify structured metadata persisted on LLM, TOOL, and SANDBOX checkpoints
      expect(completedPayload!.checkpoints.length).toBe(3);

      const llmCheckpoint = completedPayload!.checkpoints.find(
        (c) => c.step_key === 'plan-refund'
      );
      expect(llmCheckpoint).toBeDefined();
      expect(llmCheckpoint!.step_type).toBe('LLM');
      expect(llmCheckpoint!.metadata).toMatchObject({
        model: 'gemini-2.5-pro',
        tokenUsage: { promptTokens: 180, completionTokens: 45, totalTokens: 225 },
      });
      expect(typeof llmCheckpoint!.metadata?.latencyMs).toBe('number');

      const toolCheckpoint = completedPayload!.checkpoints.find((c) =>
        c.step_key.startsWith('tool:fetch-order-status:')
      );
      expect(toolCheckpoint).toBeDefined();
      expect(toolCheckpoint!.step_type).toBe('TOOL');
      expect(toolCheckpoint!.metadata).toMatchObject({
        toolName: 'fetch-order-status',
        args: { orderId: 'ORD-AGENT-55' },
      });
      expect(typeof toolCheckpoint!.metadata?.latencyMs).toBe('number');

      const sandboxCheckpoint = completedPayload!.checkpoints.find(
        (c) => c.step_key === 'execute-refund-sandbox'
      );
      expect(sandboxCheckpoint).toBeDefined();
      expect(sandboxCheckpoint!.step_type).toBe('SANDBOX');
      expect(sandboxCheckpoint!.metadata).toMatchObject({
        image: 'node:20-alpine',
        network: 'none',
      });

      // Final lease count is 0
      expect(await leaseManager.getActiveLeaseCount(tenantId)).toBe(0);
    } finally {
      await worker.stop();
      await poller.stop();
    }
  });

  it('should handle /reject by resuming and transitioning run to DEAD_LETTER without re-executing prior steps', async () => {
    const streamKey = 'stepforge:stream:tasks:slice5-reject';
    const groupName = 'stepforge:workers:slice5-reject';
    const tenantId = 'agent-tenant-reject';

    await app.request('/api/v1/tenants', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: tenantId,
        name: 'Agent Reject Tenant',
        maxConcurrency: 2,
      }),
    });

    let llmCalls = 0;
    let writeExecuted = false;

    const registry = new WorkflowRegistry();
    registry.register('dangerous-agent', async (_run: Run, step: StepContext) => {
      // Auto-extracts usage/model from returned LLM payload
      const plan = await step.llm('plan-danger', async () => {
        llmCalls++;
        return {
          command: 'drop-staging',
          model: 'claude-sonnet',
          usage: { promptTokens: 50, completionTokens: 15, totalTokens: 65 },
        };
      });

      await step.waitForApproval('confirm-danger', { command: plan.command });

      await step.run('execute-danger', async () => {
        writeExecuted = true;
        return { executed: true };
      });
    });

    const poller = new OutboxPoller(pool, redis, {
      streamKey,
      pollIntervalMs: 30,
    });
    const worker = new Worker(pool, workerRedis, {
      workerId: 'agent-worker-reject',
      streamKey,
      groupName,
      registry,
      blockTimeoutMs: 100,
    });

    poller.start();
    await worker.start();

    try {
      const submitRes = await app.request('/api/v1/workflows/dangerous-agent/runs', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': 'agent-reject-run-1',
        },
        body: JSON.stringify({ tenantId, input: {} }),
      });
      expect(submitRes.status).toBe(201);
      const { run: createdRun } = (await submitRes.json()) as { run: Run };

      // Wait for WAITING_HUMAN
      const waitDeadline = Date.now() + 8000;
      let isWaiting = false;
      while (Date.now() < waitDeadline) {
        const res = await app.request(`/api/v1/runs/${createdRun.id}`);
        const body = (await res.json()) as { run: Run };
        if (body.run.status === 'WAITING_HUMAN') {
          isWaiting = true;
          break;
        }
        await new Promise((r) => setTimeout(r, 30));
      }
      expect(isWaiting).toBe(true);
      expect(llmCalls).toBe(1);

      // Reject without specifying gateKey (resolves the single pending gate)
      const rejectRes = await app.request(`/api/v1/runs/${createdRun.id}/reject`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          payload: { rejectedBy: 'secops', reason: 'Denied in prod' },
        }),
      });
      expect(rejectRes.status).toBe(200);

      // Wait for DEAD_LETTER transition
      let finalPayload: {
        run: Run;
        checkpoints: StepCheckpoint[];
        approvals: RunApproval[];
      } | null = null;
      const dlqDeadline = Date.now() + 8000;
      while (Date.now() < dlqDeadline) {
        const res = await app.request(`/api/v1/runs/${createdRun.id}`);
        const body = (await res.json()) as {
          run: Run;
          checkpoints: StepCheckpoint[];
          approvals: RunApproval[];
        };
        if (body.run.status === 'DEAD_LETTER') {
          finalPayload = body;
          break;
        }
        await new Promise((r) => setTimeout(r, 30));
      }

      expect(finalPayload).not.toBeNull();
      expect(finalPayload!.run.status).toBe('DEAD_LETTER');
      expect(finalPayload!.run.error).toMatchObject({
        name: 'ApprovalRejectedError',
        gateKey: 'confirm-danger',
        payload: { rejectedBy: 'secops', reason: 'Denied in prod' },
      });
      expect(finalPayload!.approvals[0]!.status).toBe('REJECTED');
      expect(llmCalls).toBe(1);
      expect(writeExecuted).toBe(false);

      // Check auto-extracted LLM metadata
      expect(finalPayload!.checkpoints[0]!.step_type).toBe('LLM');
      expect(finalPayload!.checkpoints[0]!.metadata).toMatchObject({
        model: 'claude-sonnet',
        tokenUsage: { promptTokens: 50, completionTokens: 15, totalTokens: 65 },
      });
    } finally {
      await worker.stop();
      await poller.stop();
    }
  });
});
