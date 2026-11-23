import type { Run } from '../types/domain.js';
import type { StepContext } from '../step/context.js';

export type WorkflowFunction = (
  run: Run,
  step: StepContext
) => Promise<Record<string, unknown> | void>;

export class WorkflowRegistry {
  private workflows: Map<string, WorkflowFunction> = new Map();

  public register(workflowName: string, fn: WorkflowFunction): void {
    this.workflows.set(workflowName, fn);
  }

  public get(workflowName: string): WorkflowFunction | undefined {
    return this.workflows.get(workflowName);
  }

  public has(workflowName: string): boolean {
    return this.workflows.has(workflowName);
  }
}

export const defaultWorkflowRegistry = new WorkflowRegistry();
