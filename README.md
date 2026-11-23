# StepForge

**StepForge** is a headless, multi-tenant durable task orchestration and sandboxed execution engine built in TypeScript (Node.js, Hono, PostgreSQL, Redis, Docker). 

It guarantees reliable workflow completion, idempotent step-level retries across ungraceful worker crashes (`SIGKILL` / OOM), multi-tenant concurrency fairness, and secure, isolated step execution inside resource-capped ephemeral Docker containers.

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
                               +----------+----------+
                                          |
                       (Atomic Write: Run + Outbox Event)
                                          |
                                          v
                         +--------------------------------+
                         |     PostgreSQL (State Store)   |
                         |  - runs                        |
                         |  - step_checkpoints            |
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
                           +---------------------------+
                           |   Redis Stream & State    |
                           |  - Stream: stepforge:tasks|
                           |  - Lua Concurrency Engine |
                           |  - Tenant Lease Sets      |
                           +-------------+-------------+
                                         |
                             (XREADGROUP / Worker Loop)
                                         |
                                         v
                    +----------------------------------------+
                    |              Worker Pool               |
                    |  - Lua Lease Acquisition & Heartbeat   |
                    |  - Step Checkpoint Runner (step.run)   |
                    |  - State Machine Transitions (Postgres)|
                    +--------------------+-------------------+
                                         |
                    +--------------------+-------------------+
                    |                                        |
                    v                                        v
       +--------------------------+             +--------------------------+
       | Internal JS/TS Executors |             | Ephemeral Docker Sandbox |
       | (Native In-Memory)       |             | (--cpus, --memory,       |
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
- Run creation and outbox event logging occur inside the **exact same PostgreSQL transaction**.
- Avoids distributed transaction anomalies between PostgreSQL and Redis.
- An Outbox Poller continuously sweeps pending records using `SELECT ... FOR UPDATE SKIP LOCKED` where `status = 'PENDING' AND available_at <= NOW()`, publishes to Redis Streams via `XADD`, and marks the outbox event as `PUBLISHED`.

### 2. Multi-Tenant Fairness & Lua Concurrency Rate-Limiter
- Every tenant has an assigned `max_concurrency` quota.
- Workers atomically evaluate quota and acquire leases using an atomic Redis Lua script ([acquire_lease.lua](src/redis/scripts/acquire_lease.lua)):
  - Purges expired leases from the tenant's active sorted set.
  - Grants or renews a lease if active count is strictly below `max_concurrency`.
  - If quota is exceeded, returns `0`. The worker immediately `XACK`s the stream message and places the task into a delayed retry sorted set (`stepforge:delayed_tasks`) with decorrelated jitter, preventing noisy-neighbor starvation.

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
  - Inherits the exact same checkpoint memoization guarantees as native steps.

---

## Quickstart

### 1. Prerequisites
- Docker & Docker Compose
- Node.js 20+

### 2. Start PostgreSQL & Redis
```bash
docker compose up -d postgres redis
```

### 3. Install & Build
```bash
npm install
npm run build
```

### 4. Run Migrations & Start Server
```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/stepforge \
REDIS_URL=redis://localhost:6379 \
npm start
```

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

### Query Run & Step Checkpoints
```http
GET /api/v1/runs/4f3b7d1e-2c8a-4d9f-9a1b-3e5c7d8f9a0b
```
Response:
```json
{
  "run": {
    "id": "4f3b7d1e-2c8a-4d9f-9a1b-3e5c7d8f9a0b",
    "tenant_id": "tenant-corp-a",
    "workflow_name": "order-fulfillment",
    "status": "COMPLETED",
    "attempt_count": 1,
    "output": { "status": "shipped" }
  },
  "checkpoints": [
    {
      "step_key": "reserve-inventory",
      "status": "COMPLETED",
      "output": { "reserved": true }
    },
    {
      "step_key": "charge-card",
      "status": "COMPLETED",
      "output": { "paymentRef": "txn_8849" }
    }
  ]
}
```

---

## Test Verification Suite

All 4 slices are validated by end-to-end integration tests using real PostgreSQL and Redis containers via Testcontainers:

```bash
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

 Test Files  4 passed (4)
      Tests  21 passed (21)
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
