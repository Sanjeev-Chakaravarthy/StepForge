# StepForge

**StepForge** is a headless, multi-tenant durable task orchestration, AI agent runtime, and sandboxed execution engine built in TypeScript (Node.js, Hono, PostgreSQL, Redis, Docker).

It guarantees reliable workflow completion, idempotent step-level retries across ungraceful worker crashes (`SIGKILL` / OOM), multi-tenant concurrency fairness, Human-in-the-Loop (`WAITING_HUMAN`) suspend/resume with zero lease blocking, structured LLM/tool checkpointing, and secure, isolated step execution inside resource-capped ephemeral Docker containers.

---

## Architecture Overview

```
                                +-------------------+
                                |    HTTP Clients   |
                                +---------+---------+
                                          |
                                          v
                               +---------------------+
                               |  Hono HTTP Engine   |
                               | (/runs, /approve,   |
                               |  /reject, /tenants) |
                               +----------+----------+
                                          |
                       (Atomic Write: Run + Outbox Event)
                                          |
                                          v
                         +--------------------------------+
                         |     PostgreSQL (State Store)   |
                         |  - runs                        |
                         |  - step_checkpoints            |
                         |  - run_approvals               |
                         |  - outbox_events               |
                         |  - tenants                     |
                         +---------------+----------------+
                                         |
                             (Poller / SKIP LOCKED)
                                         |
                                         v
                           +---------------------------+
                           |   Transactional Outbox    |
                           |         Publisher         |
                           +-------------+-------------+
                                         |
                                 (XADD Stream Events)
                                         v
                           +-----------------------------+
                           |    Redis Stream & State     |
                           |  - Stream: stepforge:tasks  |
                           |  - DelayQueue: delayed_tasks|
                           |  - Lua Concurrency Engine   |
                           |  - Tenant Lease Sets        |
                           +-------------+---------------+
                                         |
                             (XREADGROUP / Worker Loop)
                                         |
                                         v
                    +----------------------------------------+
                    |              Worker Pool               |
                    |  - Lua Lease Acquisition & Heartbeat   |
                    |  - Step Runner (run/llm/tool/sandbox)  |
                    |  - HITL Suspend/Resume (WAITING_HUMAN) |
                    |  - State Machine Transitions (Postgres)|
                    +--------------------+-------------------+
                                         |
                    +--------------------+-------------------+
                    |                                        |
                    v                                        v
       +--------------------------+             +--------------------------+
       | Internal JS/TS Executors |             | Ephemeral Docker Sandbox |
       | (LLM / Tool / HITL Gates)|             | (--cpus, --memory,       |
       |                          |             |  --network=none, tmpfs)  |
       +--------------------------+             +--------------------------+
                    ^
                    |
       +------------+---------------+
       |   Background Lease Reaper  |
       | (Crash / SIGKILL Recovery) |
       +----------------------------+
```

---

## Core Invariants & Guarantees

### 1. Transactional Outbox (Dual-Write Prevention)
- Run creation (`RUN_CREATED`), retry scheduling (`RUN_RETRY`), and HITL resumption (`RUN_RESUMED`) write state updates and outbox events inside the **exact same PostgreSQL transaction**.
- Avoids distributed transaction anomalies between PostgreSQL and Redis.
- An `OutboxPoller` continuously sweeps pending records using `SELECT ... FOR UPDATE SKIP LOCKED` where `status = 'PENDING' AND available_at <= NOW()`, publishes to Redis Streams via `XADD`, transitions ready runs to `QUEUED`, and marks the outbox event as `PUBLISHED`.

### 2. Multi-Tenant Fairness & Lua Concurrency Rate-Limiter
- Every tenant has an assigned `max_concurrency` quota.
- Workers atomically evaluate quota and acquire leases using an atomic Redis Lua script ([acquire_lease.lua](src/redis/scripts/acquire_lease.lua)):
  - Purges expired leases from the tenant's active sorted set.
  - Grants or renews a lease if active count is strictly below `max_concurrency`.
  - If quota is exceeded, returns `0`. The worker immediately `XACK`s the stream message and places the task into a delayed retry sorted set (`stepforge:delayed_tasks`) handled by `DelayQueue` with decorrelated jitter, preventing noisy-neighbor starvation.

### 3. Step Checkpoint Memoization (`step.run`)
- Workflows are structured as discrete, composable steps:
  ```typescript
  const inventory = await step.run('reserve-inventory', async () => {
    return await api.reserveItems(run.input.items);
  });
  ```
- Before executing any step, `step_checkpoints(run_id, step_key)` is checked.
- If a completed checkpoint exists, its output is returned **immediately without re-executing `fn()`**, providing guaranteed zero duplicate external side effects across retries.
- On cache miss, `fn()` executes and its output is atomically committed to PostgreSQL.

### 4. Lease Reaping & Crash Recovery (`SIGKILL` / OOM)
- While running, workers renew an active lease heartbeat every 3 seconds (updating Redis and PostgreSQL `runs.lease_expires_at`).
- If a worker is abruptly killed by the OS kernel (`SIGKILL`, node termination, out-of-memory error), its lease will expire.
- A background `LeaseReaper` identifies runs `WHERE status = 'RUNNING' AND lease_expires_at < NOW()`.
- Reclaims the orphaned run, increments `attempt_count`, applies exponential backoff with full jitter, and emits a retry event to `outbox_events`.
- If `attempt_count >= max_attempts`, the run transitions safely to `DEAD_LETTER`.

### 5. Ephemeral Resource-Capped Docker Sandbox (`step.runSandboxed`)
- For untrusted or sandboxed tasks, `step.runSandboxed(stepKey, options)` runs tasks in ephemeral containers:
  - `--cpus="1.0"`: CPU capping.
  - `--memory="256m" --memory-swap="256m"`: Hard memory limit with swap disabled.
  - `--pids-limit=64`: Fork-bomb prevention.
  - `--network=none`: Air-gapped container with zero inbound or outbound network access.
  - `--read-only`: Read-only root filesystem.
  - `--tmpfs /tmp:rw,noexec,nosuid,size=64m`: Sandboxed scratchpad.
  - Hard timeout timer: guaranteed container termination via `docker kill` if elapsed.
  - Inherits the exact same checkpoint memoization guarantees as native steps (`step_type = 'SANDBOX'`).

### 6. Human-in-the-Loop Suspend & Resume (`step.waitForApproval`)
- Long-running agents can pause safely before high-impact actions without occupying worker threads or tenant concurrency quotas:
  ```typescript
  const approval = await step.waitForApproval('confirm-refund-write', {
    amount: plan.amount,
    orderId: order.id,
  });
  ```
- If the gate in `run_approvals` is already `APPROVED`, `waitForApproval` returns the memoized approval payload immediately.
- If `PENDING` (or absent), it atomically persists the gate in `run_approvals`, transitions `runs.status` to `WAITING_HUMAN`, clears lease fields without consuming retry attempts, invokes `onSuspend()` to stop heartbeats and release the worker's Redis concurrency lease (`active_leases` drops to `0`), and halts execution cleanly via `RunSuspendedError`.
- Calling `POST /api/v1/runs/:runId/approve` or `/reject` records the human decision and atomically inserts a `RUN_RESUMED` outbox event so a worker picks up the run and resumes from saved checkpoints. Rejected gates throw `ApprovalRejectedError` (routing uncaught rejections straight to `DEAD_LETTER`).

### 7. Agent LLM & Tool Step Helpers (`step.llm` / `step.tool`)
- **`step.llm(stepKey, fn, options?)`**: Tracks execution `latencyMs`, `model`, and `tokenUsage` (either via `helpers.recordUsage(...)` or inferred from `usage`/`tokenUsage`/`tokens` on the output) in `step_checkpoints.metadata` with `step_type = 'LLM'`.
- **`step.tool(toolName, args, fn, options?)`**: Generates a deterministic canonical-JSON SHA-256 checkpoint key (`tool:<toolName>:<hash>`) unless overridden, recording `{ toolName, args, latencyMs, executedAt }` in `step_checkpoints.metadata` with `step_type = 'TOOL'`.
- Both wrappers share the exact-once PostgreSQL checkpoint memoization path.

---

## Quickstart

### 1. Prerequisites
- Docker & Docker Compose
- Node.js 20+

### 2. Start PostgreSQL & Redis
```bash
docker compose up -d postgres redis
```

### 3. Install Dependencies
```bash
npm install
```

### 4. Start the Full Runtime (API + OutboxPoller + DelayQueue + LeaseReaper + Worker)
```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/stepforge \
REDIS_URL=redis://localhost:6379 \
npm run dev
```
This runs migrations (`001_initial_schema.sql`, `002_agent_runtime.sql`), starts the Hono HTTP server, `OutboxPoller`, `DelayQueue`, `LeaseReaper`, and `Worker` (with a default echo workflow handler for unregistered workflow names), and supports graceful `SIGINT`/`SIGTERM` shutdown.

---

## API Reference

### Create Tenant
```http
POST /api/v1/tenants
Content-Type: application/json

{
  "id": "tenant-corp-a",
  "name": "Acme Corp",
  "maxConcurrency": 5
}
```

### Submit Workflow Run (Idempotent)
```http
POST /api/v1/workflows/order-fulfillment/runs
Content-Type: application/json
Idempotency-Key: order-req-12345

{
  "tenantId": "tenant-corp-a",
  "input": {
    "orderId": "ORD-999",
    "amount": 199.50
  }
}
```

### Approve or Reject a Paused (`WAITING_HUMAN`) Run
```http
POST /api/v1/runs/4f3b7d1e-2c8a-4d9f-9a1b-3e5c7d8f9a0b/approve
Content-Type: application/json

{
  "gateKey": "confirm-refund-write",
  "payload": {
    "approvedBy": "ops-lead-alice",
    "note": "Verified customer receipt"
  }
}
```
*(Or `POST /api/v1/runs/:runId/reject` with the same body shape; `gateKey` is optional when there is a single pending gate.)*

### Query Run, Step Checkpoints & Approvals
```http
GET /api/v1/runs/4f3b7d1e-2c8a-4d9f-9a1b-3e5c7d8f9a0b
```
Response:
```json
{
  "run": {
    "id": "4f3b7d1e-2c8a-4d9f-9a1b-3e5c7d8f9a0b",
    "tenant_id": "tenant-corp-a",
    "workflow_name": "ai-refund-agent",
    "status": "COMPLETED",
    "attempt_count": 1,
    "output": { "refunded": true }
  },
  "checkpoints": [
    {
      "step_key": "plan-refund",
      "step_type": "LLM",
      "status": "COMPLETED",
      "metadata": {
        "model": "gemini-2.5-pro",
        "tokenUsage": { "promptTokens": 180, "completionTokens": 45, "totalTokens": 225 },
        "latencyMs": 12
      },
      "output": { "action": "issue_refund", "amount": 75 }
    },
    {
      "step_key": "tool:fetch-order-status:a1b2c3d4e5f6",
      "step_type": "TOOL",
      "status": "COMPLETED",
      "metadata": {
        "toolName": "fetch-order-status",
        "args": { "orderId": "ORD-999" },
        "latencyMs": 4
      },
      "output": { "paidAmount": 75, "status": "delivered" }
    }
  ],
  "approvals": [
    {
      "gate_key": "confirm-refund-write",
      "status": "APPROVED",
      "prompt_data": { "proposedAction": "issue_refund", "amount": 75 },
      "payload": { "approvedBy": "ops-lead-alice", "note": "Verified customer receipt" },
      "resolved_at": "2026-11-25T08:12:40.000Z"
    }
  ]
}
```

---

## Test Verification Suite

All 5 slices and the full wired server runtime are validated by end-to-end integration tests using real PostgreSQL 16 and Redis 7 containers via Testcontainers:

```bash
npm run typecheck
npm test
```

### Test Suite Summary:
```
 ✓ tests/integration/slice1.test.ts (11 tests)
   - Schema creation, constraints, partial indexes
   - Idempotency key handling and deduplication
   - Outbox polling with FOR UPDATE SKIP LOCKED and available_at backoff delay

 ✓ tests/integration/slice2.test.ts (5 tests)
   - Atomic Redis Lua lease acquisition and automatic expiration purging
   - 500-job flood: Tenant A adheres strictly to max_concurrency = 2
   - Anti-starvation: Tenant B executes unhindered despite Tenant A flood
   - Lease reaper crash recovery and dead-letter queueing

 ✓ tests/integration/slice3.chaos.test.ts (1 test)
   - Process-level chaos test: OS child worker killed via SIGKILL mid-step
   - Lease reaper recovery by healthy worker
   - Proves Steps 1 & 2 execute strictly once (zero duplicate external side effects)

 ✓ tests/integration/slice4.sandbox.test.ts (4 tests)
   - Docker sandbox stdin/stdout JSON protocol
   - Hard timeout kill enforcement
   - --network=none air-gap isolation
   - Durable step memoization of sandboxed outputs

 ✓ tests/integration/slice5.agent.test.ts (3 tests)
   - Idempotent 002_agent_runtime.sql migration (WAITING_HUMAN, run_approvals, checkpoint metadata)
   - Full AI agent HITL flow: step.llm -> step.tool -> step.waitForApproval -> /approve -> step.runSandboxed -> COMPLETED
   - Verifies Redis active leases drop to 0 in WAITING_HUMAN (no blocking) and prior LLM/tool steps execute strictly once
   - /reject resumption and DEAD_LETTER handling

 ✓ tests/integration/runtime.e2e.test.ts (2 tests)
   - Wired startRuntime() executing queued runs to COMPLETED via echo handler
   - Spawning server/index.ts dev process, completing runs, and exiting 0 cleanly on SIGTERM

 Test Files  6 passed (6)
      Tests  26 passed (26)
```

---

## Load & Noisy-Neighbor Benchmarks

A reproducible `k6` noisy-neighbor benchmark is provided in [benchmarks/noisy_neighbor.js](benchmarks/noisy_neighbor.js).

### Running the Benchmark:
```bash
k6 run benchmarks/noisy_neighbor.js
```

### Performance Criteria:
- **Tenant A (Flooder)**: Flooding at 50 requests/sec with `max_concurrency = 2`.
- **Tenant B (Steady)**: 5 requests/sec with `max_concurrency = 5`.
- **Evaluated Invariants**:
  - $p95 < 250\text{ms}$ latency for Tenant B.
  - Zero dropped requests (`http_req_failed < 0.01`).
  - Tenant A actively throttles and defers to delayed queues without monopolizing worker capacity.

### Measured Benchmark Results

Results from a 30-second noisy-neighbor run against `localhost:3000` (`k6 v2.4.0`, PostgreSQL 16, Redis 7):

- **Total HTTP requests**: `1,654` (`~55.10 req/s`), `0.00%` failure rate, `1,652/1,652` checks passed (`100%`)
- **`tenant_a_flood` (50 iters/s)**: `avg 4.46ms`, `med 4.23ms`, `p90 6.12ms`, `p95 6.73ms`, `max 14.39ms`
- **`tenant_b_steady` (5 iters/s)**: `avg 3.81ms`, `med 3.70ms`, `p90 4.52ms`, `p95 4.70ms`, `p99 7.85ms`, `max 16.51ms`
- **Overall iteration duration**: `avg 4.90ms`, `p95 7.41ms`, `max 18.18ms`

| Scenario / Metric | Rate | avg | med | p90 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| `tenant_a_flood` (`http_req_duration`) | 50 iters/s | 4.46ms | 4.23ms | 6.12ms | 6.73ms | — | 14.39ms |
| `tenant_b_steady` (`http_req_duration`) | 5 iters/s | 3.81ms | 3.70ms | 4.52ms | 4.70ms | 7.85ms | 16.51ms |
| Overall `iteration_duration` | ~55.10 req/s | 4.90ms | — | — | 7.41ms | — | 18.18ms |
