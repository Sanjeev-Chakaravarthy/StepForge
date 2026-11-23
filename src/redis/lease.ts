import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Redis } from 'ioredis';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ACQUIRE_LEASE_LUA = fs.readFileSync(
  path.join(__dirname, 'scripts', 'acquire_lease.lua'),
  'utf-8'
);

const RELEASE_LEASE_LUA = fs.readFileSync(
  path.join(__dirname, 'scripts', 'release_lease.lua'),
  'utf-8'
);

export interface LeaseAcquisitionResult {
  acquired: boolean;
  expireAtMs: number | null;
  activeCount: number;
}

export class RedisLeaseManager {
  private redis: Redis;

  constructor(redis: Redis) {
    this.redis = redis;
  }

  public static getTenantLeaseKey(tenantId: string): string {
    return `stepforge:tenant:${tenantId}:active_leases`;
  }

  /**
   * Atomically evicts expired leases, verifies max_concurrency limit,
   * and grants or extends a worker lease.
   */
  public async acquireLease(
    tenantId: string,
    runId: string,
    ttlMs: number,
    maxConcurrency: number
  ): Promise<LeaseAcquisitionResult> {
    const key = RedisLeaseManager.getTenantLeaseKey(tenantId);
    const now = Date.now();

    const result = (await this.redis.eval(
      ACQUIRE_LEASE_LUA,
      1,
      key,
      runId,
      String(now),
      String(ttlMs),
      String(maxConcurrency)
    )) as [number, number];

    const [status, val] = result;
    if (status === 1) {
      return {
        acquired: true,
        expireAtMs: val,
        activeCount: -1,
      };
    } else {
      return {
        acquired: false,
        expireAtMs: null,
        activeCount: val,
      };
    }
  }

  /**
   * Explicitly releases a run's lease in Redis.
   */
  public async releaseLease(tenantId: string, runId: string): Promise<boolean> {
    const key = RedisLeaseManager.getTenantLeaseKey(tenantId);
    const result = await this.redis.eval(RELEASE_LEASE_LUA, 1, key, runId);
    return result === 1;
  }

  /**
   * Gets current count of active leases for a tenant (purging expired first).
   */
  public async getActiveLeaseCount(tenantId: string): Promise<number> {
    const key = RedisLeaseManager.getTenantLeaseKey(tenantId);
    const now = Date.now();
    await this.redis.zremrangebyscore(key, '-inf', now);
    return await this.redis.zcard(key);
  }

  /**
   * Manually purges expired leases for a tenant.
   */
  public async purgeExpiredLeases(tenantId: string, nowMs = Date.now()): Promise<number> {
    const key = RedisLeaseManager.getTenantLeaseKey(tenantId);
    return await this.redis.zremrangebyscore(key, '-inf', nowMs);
  }
}
