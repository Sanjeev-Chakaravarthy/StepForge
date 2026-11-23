import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomString, uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  scenarios: {
    // Tenant A: Aggressive Flooder (Quota = 2)
    tenant_a_flood: {
      executor: 'constant-arrival-rate',
      rate: 50, // 50 workflow submissions / sec
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 20,
      maxVUs: 50,
      tags: { tenant: 'tenant_a_flood' },
      exec: 'tenantAFallFlood',
    },
    // Tenant B: Normal Steady Workload (Quota = 5)
    tenant_b_steady: {
      executor: 'constant-arrival-rate',
      rate: 5, // 5 workflow submissions / sec
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 5,
      maxVUs: 15,
      tags: { tenant: 'tenant_b_steady' },
      exec: 'tenantBSteady',
    },
  },
  thresholds: {
    // Zero overall submission failures
    http_req_failed: ['rate<0.01'],
    // Tenant B must not experience queue latency degradation from Tenant A's flood
    'http_req_duration{tenant:tenant_b_steady}': ['p(95)<250', 'p(99)<400'],
    // Tenant A submission latency remains bounded
    'http_req_duration{tenant:tenant_a_flood}': ['p(95)<300'],
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://localhost:3000';

export function setup() {
  console.log(`[k6] Initializing StepForge benchmark against ${BASE_URL}...`);

  // Provision Tenant A (max_concurrency = 2)
  http.post(
    `${BASE_URL}/api/v1/tenants`,
    JSON.stringify({
      id: 'tenant-a-flood',
      name: 'Tenant A Flood',
      maxConcurrency: 2,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  // Provision Tenant B (max_concurrency = 5)
  http.post(
    `${BASE_URL}/api/v1/tenants`,
    JSON.stringify({
      id: 'tenant-b-steady',
      name: 'Tenant B Steady',
      maxConcurrency: 5,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  console.log('[k6] Tenants provisioned. Starting benchmark execution.');
}

export function tenantAFallFlood() {
  const idempotencyKey = `flood-${uuidv4()}`;
  const payload = JSON.stringify({
    tenantId: 'tenant-a-flood',
    input: { batchId: randomString(8), timestamp: Date.now() },
  });

  const res = http.post(
    `${BASE_URL}/api/v1/workflows/heavy-batch-flow/runs`,
    payload,
    {
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': idempotencyKey,
      },
      tags: { tenant: 'tenant_a_flood' },
    }
  );

  check(res, {
    'Tenant A accepted (201)': (r) => r.status === 201,
  });
}

export function tenantBSteady() {
  const idempotencyKey = `steady-${uuidv4()}`;
  const payload = JSON.stringify({
    tenantId: 'tenant-b-steady',
    input: { customerId: `cust-${randomString(6)}`, priority: 'high' },
  });

  const res = http.post(
    `${BASE_URL}/api/v1/workflows/critical-payment-flow/runs`,
    payload,
    {
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': idempotencyKey,
      },
      tags: { tenant: 'tenant_b_steady' },
    }
  );

  check(res, {
    'Tenant B accepted (201)': (r) => r.status === 201,
  });
}
