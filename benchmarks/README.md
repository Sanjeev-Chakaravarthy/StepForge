# StepForge Noisy-Neighbor Benchmark

This benchmark tests and measures the multi-tenant fairness and anti-starvation invariants of StepForge under load using [k6](https://k6.io/).

## Benchmark Topology

- **Tenant A (Aggressive Flooder)**:
  - Concurrency quota: `max_concurrency = 2`
  - Ingestion rate: `50 requests/second` (constant arrival rate)
  - Workload: Large volume of background batch runs.

- **Tenant B (Steady Business Workload)**:
  - Concurrency quota: `max_concurrency = 5`
  - Ingestion rate: `5 requests/second` (constant arrival rate)
  - Workload: High-priority, latency-sensitive workflow runs.

## Performance Criteria Evaluated

1. **Anti-Starvation**: Tenant A flooding requests must never starve Tenant B's throughput or worker lease acquisition.
2. **Latency Isolation**: Tenant B's request latency (`p95 < 250ms`, `p99 < 400ms`) must remain steady regardless of Tenant A's backlog.
3. **Strict Concurrency Enforcement**: At no point in time does Tenant A consume more than 2 active concurrent worker leases.

## How to Run

1. Start PostgreSQL and Redis:
   ```bash
   docker compose up -d postgres redis
   ```

2. Start the StepForge server and worker pool:
   ```bash
   npm run build
   node dist/server/index.js
   ```

3. Execute the k6 load test:
   ```bash
   k6 run benchmarks/noisy_neighbor.js
   ```

   Or target a custom remote deployment:
   ```bash
   TARGET_URL=http://your-server-host:3000 k6 run benchmarks/noisy_neighbor.js
   ```
