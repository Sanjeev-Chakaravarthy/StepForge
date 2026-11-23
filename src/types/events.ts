export interface RunCreatedEventPayload {
  run_id: string;
  tenant_id: string;
  workflow_name: string;
  idempotency_key: string;
  input: Record<string, unknown>;
}

export type OutboxEventPayload = RunCreatedEventPayload | Record<string, unknown>;
