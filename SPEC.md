# StepForge Specification

**StepForge** is a headless, multi-tenant durable task orchestration and sandboxed execution engine written in TypeScript (Node.js, Hono, PostgreSQL, Redis, Docker). It guarantees reliable workflow completion, idempotent step-level retries across crashes (SIGKILL/OOM), multi-tenant fairness, and secure isolated step execution.

---

## 1. System Overview & Architecture

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
                             |  - Stream: stepforge:jobs |
                             |  - Lua Concurrency Engine |
                             |  - Tenant Lease Sets      |
                             +-------------+-------------+
                                           |
                               (XREADGROUP / Worker Loop)
                                           |
                                           v
                      +----------------------------------------+
                      |           Worker Pool                  |
                      |  - Lua Lease Acquisition / Heartbeat   |
                      |  - Step Checkpoint Runner (step.run)   |
                      |  - State Transitions (Postgres)        |
                      +--------------------+-------------------+
                                           |
                      +--------------------+-------------------+
                      |                                        |
                      v                                        v
         +--------------------------+             +--------------------------+
         | Internal JS/TS Executors |             | Ephemeral Docker Sandbox |
         | (Native In-Memory)       |             | (--cpus, --memory,       |
         |                          |             |  --network=none)         |
         +--------------------------+             +--------------------------+
                      ^
                      |
         +------------+---------------+
         |   Background Lease Reaper  |
         | (Crash / SIGKILL Recovery) |
         +----------------------------+
```

### Core Design Principles
1. **Durable State Machine**: Workflow and step progression is backed by transactional PostgreSQL state. No intermediate state exists only in memory.
2. **Dual-Write Elimination**: Redis is treated as an ephemeral scheduler/transport. All workflow enqueue actions write to PostgreSQL `outbox_events` in the primary transaction. A dedicated transactional outbox publisher pushes to Redis Streams.
3. **Multi-Tenant Fairness**: Concurrency quotas (`max_concurrency`) are enforced atomically in Redis using a Lua script before any task can transition to `RUNNING`.
4. **Idempotent Step Memoization (`step.run`)**: Every step check yields an atomic check in `step_checkpoints`. If a worker crashes or is killed via `SIGKILL` at step $N$, steps $1 \dots N-1$ are resolved from the database cache and never executed again.
5. **Crash Resilience & Lease Reaping**: Workers hold heartbeat-backed leases. If a worker dies ungracefully, a background lease reaper detects expired leases and reschedules runs with exponential backoff and jitter.
6. **Hardened Ephemeral Sandboxing**: Untrusted or user-supplied execution steps run in isolated, resource-constrained, non-networked Docker containers.

---

## 2. PostgreSQL Schema & State Machine

### 2.1 State Machine Lifecycle

```
    [Client POST /runs]
             |
             v
       +-----------+
       |  PENDING  | (Run created + Outbox event written)
       +-----+-----+
             |
             | (Outbox Publisher -> Redis Stream)
             v
       +-----------+
       |  QUEUED   | (Acknowledged in Redis queue)
       +-----+-----+
             |
             | (Worker pulls from stream & obtains Redis lease)
             v
       +-----------+
+----> |  RUNNING  | <------------------------------------+
|      +-----+-----+                                      |
|            |                                            |
|    +-------+-------+--------------------+               |
|    |               |                    |               |
| (All Steps OK) (Exception/Crash) (Lease Expired)        |
|    |               |                    |               |
|    v               v                    v               |
| +-----------+  +-------------------------------------+  |
| | COMPLETED |  | Check attempt_count < max_attempts? |  |
| +-----------+  +------------------+------------------+  |
|                                   |                     |
|                    +--------------+--------------+      |
|                    |                             |      |
|                 (Yes)                           (No)    |
|                    v                             v      |
|             +-------------+               +-------------+
|             |  RETRYING   |               | DEAD_LETTER |
|             +------+------+               +-------------+
|                    |                             
+--------------------+ (Backoff delay elapses & re-queued)
```

### 2.2 Schema Definition

```sql
-- Enums
CREATE TYPE run_status AS ENUM (
    'PENDING',
    'QUEUED',
    'RUNNING',
    'RETRYING',
    'COMPLETED',
    'DEAD_LETTER'
);

CREATE TYPE outbox_status AS ENUM (
    'PENDING',
    'PUBLISHED',
    'FAILED'
);

-- Tenants Table
CREATE TABLE tenants (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    max_concurrency INTEGER NOT NULL DEFAULT 5,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Workflow Runs Table
CREATE TABLE runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    workflow_name VARCHAR(128) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    status run_status NOT NULL DEFAULT 'PENDING',
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    output JSONB NULL,
    error JSONB NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    current_lease_worker_id VARCHAR(128) NULL,
    lease_expires_at TIMESTAMPTZ NULL,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tenant_idempotency UNIQUE(tenant_id, idempotency_key)
);

-- Step Checkpoints Table (Durable Step-Level Memoization)
CREATE TABLE step_checkpoints (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_key VARCHAR(255) NOT NULL,
    output JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(32) NOT NULL DEFAULT 'COMPLETED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_run_step UNIQUE(run_id, step_key)
);

-- Transactional Outbox Events Table
CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    status outbox_status NOT NULL DEFAULT 'PENDING',
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ NULL
);

-- Performance Indexes
CREATE INDEX idx_runs_tenant_status ON runs(tenant_id, status);
CREATE INDEX idx_runs_lease_reaper ON runs(status, lease_expires_at) 
    WHERE status = 'RUNNING';
CREATE INDEX idx_runs_scheduled_retrying ON runs(status, scheduled_at) 
    WHERE status = 'RETRYING';
CREATE INDEX idx_step_checkpoints_lookup ON step_checkpoints(run_id, step_key);
CREATE INDEX idx_outbox_pending ON outbox_events(available_at) 
    WHERE status = 'PENDING';
```

---

## 3. Transactional Outbox Pattern & Redis Streams

To avoid data loss or ghost jobs caused by non-atomic writes across PostgreSQL and Redis:

1. **Atomic Insertion**: When an API request or retry creates a job, `runs` and `outbox_events` are inserted inside the same database transaction:
   ```typescript
   await db.transaction(async (tx) => {
     const run = await tx.insert(runs).values({ ... }).returning();
     await tx.insert(outboxEvents).values({
       tenant_id: run.tenant_id,
       event_type: 'RUN_QUEUED',
       payload: { run_id: run.id, tenant_id: run.tenant_id, workflow: run.workflow_name }
     });
   });
   ```

2. **Poller Loop**:
   - Outbox Poller queries with concurrency control:
     ```sql
     SELECT id, tenant_id, event_type, payload
     FROM outbox_events
     WHERE status = 'PENDING' AND available_at <= NOW()
     ORDER BY id ASC
     LIMIT 50
     FOR UPDATE SKIP LOCKED;
     ```
   - Publishes each item to the global Redis stream `stepforge:stream:tasks` via `XADD`.
   - On Redis write confirmation:
     ```sql
     UPDATE outbox_events
     SET status = 'PUBLISHED', published_at = NOW()
     WHERE id = ANY($1::bigint[]);
     ```
   - Also transitions `runs.status` from `'PENDING'` to `'QUEUED'`.
   - Optional retention: Prune rows where `status = 'PUBLISHED'` older than 24 hours.

---

## 4. Multi-Tenant Concurrency & Lease Engine (Redis Lua)

Each tenant has a strict `max_concurrency` limit. Over-subscribing tenants must never starve other tenants or exhaust worker capacity.

### 4.1 Redis Data Structures
- **Stream**: `stepforge:stream:tasks` (Jobs published by outbox poller).
- **Tenant Active Lease Set**: `stepforge:tenant:{tenantId}:active_leases` (Sorted Set storing `run_id` with score = `lease_expiration_timestamp_ms`).
- **Tenant Concurrency Key**: `stepforge:tenant:{tenantId}:max_concurrency` (Cached integer).

### 4.2 Atomic Lua Lease Acquisition Script (`acquire_lease.lua`)

```lua
-- KEYS[1]: Tenant active leases ZSET (stepforge:tenant:{tenantId}:active_leases)
-- ARGV[1]: Run ID
-- ARGV[2]: Current Epoch MS
-- ARGV[3]: Lease TTL MS
-- ARGV[4]: Max Concurrency limit

local active_key = KEYS[1]
local run_id = ARGV[1]
local now = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local max_concurrency = tonumber(ARGV[4])

-- Step 1: Evict expired leases
redis.call('ZREMRANGEBYSCORE', active_key, '-inf', now)

-- Step 2: Check if this run already holds a valid lease (idempotent re-entry)
local existing_score = redis.call('ZSCORE', active_key, run_id)
if existing_score then
    -- Extend lease
    local new_expire = now + ttl
    redis.call('ZADD', active_key, new_expire, run_id)
    return {1, new_expire}
end

-- Step 3: Enforce concurrency limit
local current_count = redis.call('ZCARD', active_key)
if current_count >= max_concurrency then
    -- Quota exceeded: reject acquisition
    return {0, current_count}
end

-- Step 4: Grant new lease
local new_expire = now + ttl
redis.call('ZADD', active_key, new_expire, run_id)
return {1, new_expire}
```

### 4.3 Lease Release Script (`release_lease.lua`)

```lua
-- KEYS[1]: Tenant active leases ZSET
-- ARGV[1]: Run ID
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
```

### 4.4 Worker Heartbeat
While a task executes, the worker invokes a heartbeat every `ttl / 2` (e.g., every 5 seconds for a 10-second lease):
1. Runs the Lua lease extension.
2. Updates `runs.lease_expires_at = NOW() + INTERVAL '10 seconds'` in PostgreSQL.

### 4.5 Requeue & Delay Handling on Quota-Exceeded (Return Code 0)
When a worker receives a task from Redis Streams via `XREADGROUP` and executes `acquire_lease.lua`:
- If the Lua script returns `{0, current_count}`, the tenant's current concurrency has saturated its `max_concurrency` limit.
- **Handling Strategy**:
  1. **Immediate Acknowledgment & Deferred Requeue**: The worker invokes `XACK stepforge:stream:tasks <group> <messageId>` so the consumer group's Pending Entries List (PEL) is not clogged with blocked jobs.
  2. **Delayed Requeue via Redis Sorted Set (`stepforge:delayed_tasks`)**:
     - The job is placed in a delayed ZSET: `ZADD stepforge:delayed_tasks <now_ms + backoff_ms> <job_json>`.
     - `backoff_ms` is computed using a short tenant backoff window with jitter (e.g., 200ms–500ms).
  3. **Delay Dispatcher**:
     - A lightweight Redis poller/timer checks `ZRANGEBYSCORE stepforge:delayed_tasks 0 <now_ms> LIMIT 0 50`.
     - Matured jobs are atomically popped (`ZREM`) and re-emitted via `XADD` to `stepforge:stream:tasks`.
  4. **PostgreSQL Invariant**:
     - The run record in PostgreSQL remains safely in `QUEUED` status with `lease_expires_at = NULL`.
     - In the event of a total Redis flush/failure, any job remaining in `QUEUED` status for longer than a threshold (e.g., 60 seconds) is re-detected and pushed back into the outbox by the lease reaper.

---

## 5. Step-Level Checkpoint Runner (`step.run`)

StepForge exposes a durable workflow context to step definitions:

```typescript
export interface StepContext {
  run: <T>(stepKey: string, fn: () => Promise<T>) => Promise<T>;
  runSandboxed: <T>(stepKey: string, options: SandboxOptions) => Promise<T>;
}
```

### 5.1 Memoization Algorithm

When `step.run(stepKey, fn)` is called:
1. **Lookup Checkpoint**:
   ```sql
   SELECT output, status 
   FROM step_checkpoints 
   WHERE run_id = $1 AND step_key = $2;
   ```
2. **Cache Hit**: If a row exists with `status = 'COMPLETED'`:
   - Immediately deserialize and return `output`.
   - `fn` is **not** called. Any external side-effects inside `fn` are bypassed.
3. **Cache Miss**:
   - Execute `fn()`.
   - On successful resolution, save checkpoint:
     ```sql
     INSERT INTO step_checkpoints (run_id, step_key, output, status)
     VALUES ($1, $2, $3::jsonb, 'COMPLETED')
     ON CONFLICT (run_id, step_key) DO NOTHING;
     ```
   - Return the output to the workflow flow.
4. **Failure**:
   - If `fn()` throws or process exits before checkpoint commit, no checkpoint is saved.
   - When the workflow is re-run, execution starts at the beginning, but steps with checkpoints are skipped instantly until reaching the uncommitted step.

---

## 6. Crash Recovery & Background Lease Reaper

When a worker experiences a hard `SIGKILL`, system OOM, or hardware severance, it cannot execute graceful cleanup.

### 6.1 Lease Reaper Design
- Runs as a background timer loop (e.g., every 2–5 seconds).
- Identifies stale runs where the lease expired:
  ```sql
  SELECT id, tenant_id, attempt_count, max_attempts
  FROM runs
  WHERE status = 'RUNNING' 
    AND lease_expires_at < NOW()
  FOR UPDATE SKIP LOCKED;
  ```

### 6.2 Exponential Backoff with Decorrelated Jitter
For each stale run:
- If `attempt_count + 1 < max_attempts`:
  - Calculate delay:
    $$\text{backoff} = \min(\text{max\_backoff}, \text{base\_delay} \times 2^{\text{attempt\_count}})$$
    $$\text{jittered\_delay} = \text{random}(0, \text{backoff})$$
  - Update `runs`:
    ```sql
    UPDATE runs
    SET status = 'RETRYING',
        attempt_count = attempt_count + 1,
        scheduled_at = NOW() + ($1 || ' milliseconds')::interval,
        current_lease_worker_id = NULL,
        lease_expires_at = NULL,
        updated_at = NOW()
    WHERE id = $2;
    ```
  - Insert outbox event for re-queueing once `scheduled_at` is reached.
- Else (`attempt_count + 1 >= max_attempts`):
  - Mark as `DEAD_LETTER`:
    ```sql
    UPDATE runs
    SET status = 'DEAD_LETTER',
        error = jsonb_build_object('message', 'Lease expired and max attempts exceeded (Worker Crash/OOM)'),
        updated_at = NOW()
    WHERE id = $1;
    ```

---

## 7. Resource-Capped Ephemeral Docker Sandbox

For untrusted steps, the `DockerSandboxRunner` executes tasks inside short-lived, constrained containers via Docker Engine API (`/var/run/docker.sock` or `dockerode`).

### 7.1 Security & Resource Constraints
- **CPU Limit**: `--cpus="1.0"` (or configurable fraction, e.g., 0.5).
- **Memory Limit**: `--memory="256m"`, `--memory-swap="256m"` (disabling extra swap).
- **PID Limit**: `--pids-limit=64` (mitigates fork-bombs).
- **Network**: `--network=none` (fully isolated, no outbound/inbound traffic).
- **Filesystem**:
  - `--read-only` root filesystem.
  - Ephemeral tmpfs: `--tmpfs /tmp:rw,noexec,nosuid,size=64m`.
- **Hard Timeout**: Enforced via Node.js timer aborting the process; if timeout expires, sends `docker kill <container_id>`.

### 7.2 I/O Protocol
1. Input payload serialized to JSON and piped via container STDIN.
2. Container executes designated entrypoint (e.g., Node.js / Python runner).
3. STDOUT is reserved for valid JSON output; STDERR captures logs/exceptions.
4. Runner reads exit code:
   - `0`: Parse STDOUT as step checkpoint output.
   - Non-zero / Timeout: Bubble error up to step runner for retry or dead-lettering.

---

## 8. Chaos & Load Testing Strategy

### 8.1 Vitest + Testcontainers Chaos Test
- Spin up PostgreSQL and Redis containers dynamically using `@testcontainers/postgresql` and `@testcontainers/redis`.
- Define a 3-step workflow:
  - **Step 1**: Write a timestamp to an external side-effect mock tracker.
  - **Step 2**: If `attempt_count == 0`, invoke `process.kill(process.pid, 'SIGKILL')` or kill the child worker process mid-execution.
  - **Step 3**: Conclude workflow.
- **Assertion**:
  1. The lease reaper reclaims the job.
  2. The worker picks up retry (attempt 1).
  3. Step 1 checkpoint is loaded from DB. External side-effect count for Step 1 remains **exactly 1**.
  4. Step 2 completes on attempt 1.
  5. Workflow status transitions to `COMPLETED`.

### 8.2 k6 Noisy-Neighbor Load Test
- **Tenant A (Aggressive)**: Configured with `max_concurrency = 2`. Floods the API with 100 requests/sec.
- **Tenant B (Normal)**: Configured with `max_concurrency = 5`. Sends 5 requests/sec.
- **Metrics Evaluated**:
  - Redis Lua must throttle Tenant A so that at most 2 tasks run concurrently.
  - Tenant B requests must experience zero starvation, maintaining low queue latency ($p95 < 200\text{ms}$) regardless of Tenant A's backlog.

---

## 9. HTTP API Surface (Hono)

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/tenants` | Create tenant with `max_concurrency` config |
| `POST` | `/api/v1/workflows/:workflowName/runs` | Submit a new workflow run (writes Run + Outbox) |
| `GET` | `/api/v1/runs/:runId` | Retrieve workflow run status, outputs, and step checkpoints |
| `POST` | `/api/v1/runs/:runId/cancel` | Cancel an active or queued run |
| `GET` | `/health` | Healthcheck (Postgres ping, Redis ping) |
