export type RunStatus =
  | 'PENDING'
  | 'QUEUED'
  | 'RUNNING'
  | 'WAITING_HUMAN'
  | 'RETRYING'
  | 'COMPLETED'
  | 'DEAD_LETTER';

export type OutboxStatus = 'PENDING' | 'PUBLISHED' | 'FAILED';

export type StepType = 'STEP' | 'SANDBOX' | 'TOOL' | 'LLM';

export type ApprovalStatus = 'PENDING' | 'APPROVED' | 'REJECTED';

export interface Tenant {
  id: string;
  name: string;
  max_concurrency: number;
  created_at: Date;
  updated_at: Date;
}

export interface Run {
  id: string;
  tenant_id: string;
  workflow_name: string;
  idempotency_key: string;
  status: RunStatus;
  input: Record<string, unknown>;
  output: Record<string, unknown> | null;
  error: Record<string, unknown> | null;
  attempt_count: number;
  max_attempts: number;
  current_lease_worker_id: string | null;
  lease_expires_at: Date | null;
  scheduled_at: Date;
  created_at: Date;
  updated_at: Date;
}

export interface StepCheckpoint {
  id: number;
  run_id: string;
  step_key: string;
  step_type: StepType;
  output: unknown;
  metadata: Record<string, unknown> | null;
  status: string;
  created_at: Date;
}

export interface RunApproval {
  id: number;
  run_id: string;
  gate_key: string;
  status: ApprovalStatus;
  prompt_data: Record<string, unknown>;
  payload: unknown;
  created_at: Date;
  resolved_at: Date | null;
}

export interface OutboxEvent {
  id: number;
  tenant_id: string;
  event_type: string;
  payload: Record<string, unknown>;
  status: OutboxStatus;
  available_at: Date;
  retry_count: number;
  last_error: string | null;
  created_at: Date;
  published_at: Date | null;
}
