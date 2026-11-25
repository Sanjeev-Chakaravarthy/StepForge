import crypto from 'node:crypto';
import { withTransaction, type Pool } from '../db/client.js';
import { DockerSandboxRunner, type SandboxOptions } from '../sandbox/docker.js';
import type { RunApproval, StepType } from '../types/domain.js';

export interface TokenUsage {
  promptTokens?: number;
  completionTokens?: number;
  totalTokens?: number;
  inputTokens?: number;
  outputTokens?: number;
}

export interface LlmStepHelpers {
  recordUsage: (usage: TokenUsage & { model?: string }) => void;
}

export interface ToolStepOptions {
  stepKey?: string;
}

export interface LlmStepOptions {
  model?: string;
}

export interface StepContextOptions {
  onSuspend?: () => Promise<void>;
}

export class RunSuspendedError extends Error {
  public readonly gateKey: string;
  public readonly promptData: Record<string, unknown>;

  constructor(gateKey: string, promptData: Record<string, unknown>) {
    super(`Run suspended awaiting human approval for gate "${gateKey}"`);
    this.name = 'RunSuspendedError';
    this.gateKey = gateKey;
    this.promptData = promptData;
  }
}

export class ApprovalRejectedError extends Error {
  public readonly gateKey: string;
  public readonly payload: unknown;

  constructor(gateKey: string, payload: unknown) {
    super(`Human approval rejected for gate "${gateKey}"`);
    this.name = 'ApprovalRejectedError';
    this.gateKey = gateKey;
    this.payload = payload;
  }
}

export interface StepContext {
  run: <T>(stepKey: string, fn: () => Promise<T>) => Promise<T>;
  runSandboxed: <T>(stepKey: string, options: SandboxOptions) => Promise<T>;
  waitForApproval: <T = Record<string, unknown>>(
    gateKey: string,
    promptData?: Record<string, unknown>
  ) => Promise<T>;
  tool: <TArgs, TResult>(
    toolName: string,
    args: TArgs,
    fn: (args: TArgs) => Promise<TResult>,
    options?: ToolStepOptions
  ) => Promise<TResult>;
  llm: <TResult>(
    stepKey: string,
    fn: (helpers: LlmStepHelpers) => Promise<TResult>,
    options?: LlmStepOptions
  ) => Promise<TResult>;
}

function canonicalSerialize(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value) ?? 'null';
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalSerialize(item)).join(',')}]`;
  }
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries
    .map(([k, v]) => `${JSON.stringify(k)}:${canonicalSerialize(v)}`)
    .join(',')}}`;
}

function buildToolStepKey(toolName: string, args: unknown): string {
  const hash = crypto
    .createHash('sha256')
    .update(canonicalSerialize(args ?? {}))
    .digest('hex')
    .slice(0, 12);
  return `tool:${toolName}:${hash}`;
}

function extractLlmMetadata(output: unknown): {
  tokenUsage?: TokenUsage;
  model?: string;
} {
  if (!output || typeof output !== 'object' || Array.isArray(output)) {
    return {};
  }
  const obj = output as Record<string, unknown>;
  const rawUsage = obj.usage ?? obj.tokenUsage ?? obj.tokens;
  const tokenUsage =
    rawUsage && typeof rawUsage === 'object' && !Array.isArray(rawUsage)
      ? (rawUsage as TokenUsage)
      : undefined;
  const model = typeof obj.model === 'string' ? obj.model : undefined;
  return {
    ...(tokenUsage ? { tokenUsage } : {}),
    ...(model ? { model } : {}),
  };
}

export class DurableStepContext implements StepContext {
  private pool: Pool;
  private runId: string;
  private onSuspend?: () => Promise<void>;

  constructor(pool: Pool, runId: string, options?: StepContextOptions) {
    this.pool = pool;
    this.runId = runId;
    if (options?.onSuspend) {
      this.onSuspend = options.onSuspend;
    }
  }

  /**
   * Shared checkpoint lookup, execution, and atomic persistence helper.
   */
  private async executeStep<T>(
    stepKey: string,
    stepType: StepType,
    executor: () => Promise<{ output: T; metadata?: Record<string, unknown> | null }>
  ): Promise<T> {
    if (!stepKey || typeof stepKey !== 'string') {
      throw new Error(`Invalid stepKey provided: "${stepKey}"`);
    }

    // 1. Check for existing checkpoint (Cache Hit)
    const { rows } = await this.pool.query<{ output: T; status: string }>(
      `SELECT output, status
       FROM step_checkpoints
       WHERE run_id = $1 AND step_key = $2`,
      [this.runId, stepKey]
    );

    const existing = rows[0];
    if (existing && existing.status === 'COMPLETED') {
      return existing.output;
    }

    // 2. Cache Miss: Execute user step function
    const { output, metadata = null } = await executor();

    // 3. Atomically persist completed checkpoint
    await this.pool.query(
      `INSERT INTO step_checkpoints (run_id, step_key, step_type, output, metadata, status)
       VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, 'COMPLETED')
       ON CONFLICT (run_id, step_key) DO UPDATE
       SET step_type = EXCLUDED.step_type,
           output = EXCLUDED.output,
           metadata = EXCLUDED.metadata,
           status = EXCLUDED.status`,
      [
        this.runId,
        stepKey,
        stepType,
        JSON.stringify(output === undefined ? null : output),
        metadata ? JSON.stringify(metadata) : null,
      ]
    );

    return output;
  }

  /**
   * Executes a durable workflow step.
   * Checks step_checkpoints(run_id, step_key) in PostgreSQL first.
   * If a completed checkpoint exists, returns the memoized JSON output immediately without executing fn().
   * If not, executes fn(), atomically persists the output, and returns the result.
   */
  public async run<T>(stepKey: string, fn: () => Promise<T>): Promise<T> {
    return this.executeStep(stepKey, 'STEP', async () => ({
      output: await fn(),
    }));
  }

  /**
   * Executes a durable step inside an ephemeral, resource-capped Docker container.
   * Inherits the same durable memoization guarantees as `step.run`:
   * if already completed, returns the cached output without spinning up a container.
   */
  public async runSandboxed<T>(stepKey: string, options: SandboxOptions): Promise<T> {
    return this.executeStep(stepKey, 'SANDBOX', async () => {
      const startMs = Date.now();
      const runner = new DockerSandboxRunner();
      const output = await runner.execute<T>(options);
      const latencyMs = Date.now() - startMs;

      return {
        output,
        metadata: {
          image: options.image ?? 'node:20-alpine',
          cpus: options.cpus ?? '1.0',
          memory: options.memory ?? '256m',
          network: options.network ?? 'none',
          latencyMs,
          executedAt: new Date().toISOString(),
        },
      };
    });
  }

  /**
   * Executes an idempotent agent tool call, memoizing the result and recording
   * tool name, arguments, and execution latency in step_checkpoints metadata.
   */
  public async tool<TArgs, TResult>(
    toolName: string,
    args: TArgs,
    fn: (args: TArgs) => Promise<TResult>,
    options?: ToolStepOptions
  ): Promise<TResult> {
    if (!toolName || typeof toolName !== 'string') {
      throw new Error(`Invalid toolName provided: "${toolName}"`);
    }

    const stepKey = options?.stepKey ?? buildToolStepKey(toolName, args);

    return this.executeStep(stepKey, 'TOOL', async () => {
      const startMs = Date.now();
      const output = await fn(args);
      const latencyMs = Date.now() - startMs;

      return {
        output,
        metadata: {
          toolName,
          args: args === undefined ? null : args,
          latencyMs,
          executedAt: new Date().toISOString(),
        },
      };
    });
  }

  /**
   * Executes an idempotent LLM inference step, memoizing the output and recording
   * latency, model, and token usage in step_checkpoints metadata.
   */
  public async llm<TResult>(
    stepKey: string,
    fn: (helpers: LlmStepHelpers) => Promise<TResult>,
    options?: LlmStepOptions
  ): Promise<TResult> {
    return this.executeStep(stepKey, 'LLM', async () => {
      let recordedUsage: TokenUsage | undefined;
      let recordedModel: string | undefined = options?.model;

      const helpers: LlmStepHelpers = {
        recordUsage: ({ model, ...usage }) => {
          recordedUsage = usage;
          if (model) {
            recordedModel = model;
          }
        },
      };

      const startMs = Date.now();
      const output = await fn(helpers);
      const latencyMs = Date.now() - startMs;

      const inferred = extractLlmMetadata(output);
      const tokenUsage = recordedUsage ?? inferred.tokenUsage;
      const model = recordedModel ?? inferred.model;

      return {
        output,
        metadata: {
          latencyMs,
          ...(model ? { model } : {}),
          ...(tokenUsage ? { tokenUsage } : {}),
          executedAt: new Date().toISOString(),
        },
      };
    });
  }

  /**
   * Human-in-the-Loop (HITL) suspend & resume gate.
   * - If the gate is already APPROVED in `run_approvals`, returns the approval payload immediately.
   * - If REJECTED, throws ApprovalRejectedError.
   * - Otherwise persists a PENDING gate, transitions the run to WAITING_HUMAN,
   *   invokes `onSuspend` to release the worker's Redis concurrency lease, and
   *   throws RunSuspendedError to halt workflow execution cleanly without failing.
   */
  public async waitForApproval<T = Record<string, unknown>>(
    gateKey: string,
    promptData: Record<string, unknown> = {}
  ): Promise<T> {
    if (!gateKey || typeof gateKey !== 'string') {
      throw new Error(`Invalid gateKey provided: "${gateKey}"`);
    }

    // Fast-path lookup when already resolved
    const { rows } = await this.pool.query<RunApproval>(
      `SELECT id, run_id, gate_key, status, prompt_data, payload, created_at, resolved_at
       FROM run_approvals
       WHERE run_id = $1 AND gate_key = $2`,
      [this.runId, gateKey]
    );

    const existing = rows[0];
    if (existing) {
      if (existing.status === 'APPROVED') {
        return (existing.payload ?? {}) as T;
      }
      if (existing.status === 'REJECTED') {
        throw new ApprovalRejectedError(gateKey, existing.payload);
      }
    }

    // Atomically create pending gate (if absent) and transition run to WAITING_HUMAN
    const resolved = await withTransaction(this.pool, async (client) => {
      await client.query(
        `INSERT INTO run_approvals (run_id, gate_key, status, prompt_data)
         VALUES ($1, $2, 'PENDING', $3::jsonb)
         ON CONFLICT (run_id, gate_key) DO NOTHING`,
        [this.runId, gateKey, JSON.stringify(promptData)]
      );

      const { rows: lockedRows } = await client.query<RunApproval>(
        `SELECT id, run_id, gate_key, status, prompt_data, payload, created_at, resolved_at
         FROM run_approvals
         WHERE run_id = $1 AND gate_key = $2
         FOR UPDATE`,
        [this.runId, gateKey]
      );

      const gate = lockedRows[0];
      if (!gate) {
        throw new Error(`Failed to persist approval gate "${gateKey}"`);
      }

      if (gate.status === 'APPROVED') {
        return { decision: 'APPROVED' as const, payload: gate.payload };
      }
      if (gate.status === 'REJECTED') {
        return { decision: 'REJECTED' as const, payload: gate.payload };
      }

      await client.query(
        `UPDATE run_approvals
         SET prompt_data = $2::jsonb
         WHERE id = $1 AND status = 'PENDING' AND prompt_data IS DISTINCT FROM $2::jsonb`,
        [gate.id, JSON.stringify(promptData)]
      );

      await client.query(
        `UPDATE runs
         SET status = 'WAITING_HUMAN'::run_status,
             current_lease_worker_id = NULL,
             lease_expires_at = NULL,
             attempt_count = GREATEST(0, attempt_count - 1),
             updated_at = NOW()
         WHERE id = $1 AND status = 'RUNNING'::run_status`,
        [this.runId]
      );

      return { decision: 'PENDING' as const, payload: null };
    });

    if (resolved.decision === 'APPROVED') {
      return (resolved.payload ?? {}) as T;
    }
    if (resolved.decision === 'REJECTED') {
      throw new ApprovalRejectedError(gateKey, resolved.payload);
    }

    if (this.onSuspend) {
      await this.onSuspend();
    }

    throw new RunSuspendedError(gateKey, promptData);
  }
}
