import type { Pool } from '../db/client.js';
import { DockerSandboxRunner, type SandboxOptions } from '../sandbox/docker.js';

export interface StepContext {
  run: <T>(stepKey: string, fn: () => Promise<T>) => Promise<T>;
  runSandboxed: <T>(stepKey: string, options: SandboxOptions) => Promise<T>;
}

export class DurableStepContext implements StepContext {
  private pool: Pool;
  private runId: string;

  constructor(pool: Pool, runId: string) {
    this.pool = pool;
    this.runId = runId;
  }

  /**
   * Executes a durable workflow step.
   * Checks step_checkpoints(run_id, step_key) in PostgreSQL first.
   * If a completed checkpoint exists, returns the memoized JSON output immediately without executing fn().
   * If not, executes fn(), atomically persists the output, and returns the result.
   */
  public async run<T>(stepKey: string, fn: () => Promise<T>): Promise<T> {
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
    const output = await fn();

    // 3. Atomically persist completed checkpoint
    await this.pool.query(
      `INSERT INTO step_checkpoints (run_id, step_key, output, status)
       VALUES ($1, $2, $3::jsonb, 'COMPLETED')
       ON CONFLICT (run_id, step_key) DO UPDATE
       SET output = EXCLUDED.output,
           status = EXCLUDED.status`,
      [this.runId, stepKey, JSON.stringify(output === undefined ? null : output)]
    );

    return output;
  }

  /**
   * Executes a durable step inside an ephemeral, resource-capped Docker container.
   * Inherits the same durable memoization guarantees as `step.run`:
   * if already completed, returns the cached output without spinning up a container.
   */
  public async runSandboxed<T>(stepKey: string, options: SandboxOptions): Promise<T> {
    return this.run(stepKey, async () => {
      const runner = new DockerSandboxRunner();
      return await runner.execute<T>(options);
    });
  }
}
