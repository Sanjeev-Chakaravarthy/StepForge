import type { ApprovalStatus } from './domain.js';

export interface RunCreatedEventPayload {
  run_id: string;
  tenant_id: string;
  workflow_name: string;
  idempotency_key: string;
  input: Record<string, unknown>;
}

export interface RunResumedEventPayload {
  run_id: string;
  tenant_id: string;
  workflow_name: string;
  idempotency_key: string;
  gate_key: string;
  decision: Extract<ApprovalStatus, 'APPROVED' | 'REJECTED'>;
  input: Record<string, unknown>;
}

export type OutboxEventPayload =
  | RunCreatedEventPayload
  | RunResumedEventPayload
  | Record<string, unknown>;
