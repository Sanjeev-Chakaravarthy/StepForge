import { Redis } from 'ioredis';

export interface DelayedTask {
  eventId: string | number;
  tenantId: string;
  eventType: string;
  payload: Record<string, unknown>;
  scheduledAtMs: number;
}

const DISPATCH_DELAYED_LUA = `
local delayed_key = KEYS[1]
local stream_key = KEYS[2]
local now_ms = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

local items = redis.call('ZRANGEBYSCORE', delayed_key, 0, now_ms, 'LIMIT', 0, limit)
local count = 0

for _, raw in ipairs(items) do
    local task = cjson.decode(raw)
    local payloadStr = type(task.payload) == 'string' and task.payload or cjson.encode(task.payload)
    redis.call('XADD', stream_key, '*', 'eventId', tostring(task.eventId), 'tenantId', task.tenantId, 'eventType', task.eventType, 'payload', payloadStr)
    redis.call('ZREM', delayed_key, raw)
    count = count + 1
end

return count
`;

export interface DelayQueueOptions {
  delayedKey?: string;
  streamKey?: string;
  pollIntervalMs?: number;
  batchSize?: number;
}

export class DelayQueue {
  private redis: Redis;
  private delayedKey: string;
  private streamKey: string;
  private pollIntervalMs: number;
  private batchSize: number;
  private isRunning: boolean = false;
  private timer: NodeJS.Timeout | null = null;
  private isDispatching: boolean = false;

  constructor(redis: Redis, options?: DelayQueueOptions) {
    this.redis = redis;
    this.delayedKey = options?.delayedKey ?? 'stepforge:delayed_tasks';
    this.streamKey = options?.streamKey ?? 'stepforge:stream:tasks';
    this.pollIntervalMs = options?.pollIntervalMs ?? 100;
    this.batchSize = options?.batchSize ?? 50;
  }

  /**
   * Schedules a task to be re-emitted into the stream after delayMs.
   */
  public async schedule(task: Omit<DelayedTask, 'scheduledAtMs'>, delayMs: number): Promise<void> {
    const scheduledAtMs = Date.now() + delayMs;
    const taskRecord: DelayedTask = {
      ...task,
      scheduledAtMs,
    };

    await this.redis.zadd(this.delayedKey, scheduledAtMs, JSON.stringify(taskRecord));
  }

  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.scheduleNext();
  }

  public stop(): void {
    this.isRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  private scheduleNext(): void {
    if (!this.isRunning) return;
    this.timer = setTimeout(async () => {
      try {
        await this.dispatchOnce();
      } catch (err: any) {
        if (!this.isRunning || err?.message?.includes('Connection is closed')) return;
        console.error('Error in DelayQueue dispatcher:', err);
      } finally {
        this.scheduleNext();
      }
    }, this.pollIntervalMs);
  }

  /**
   * Atomically moves matured delayed tasks back into the active Redis Stream.
   */
  public async dispatchOnce(): Promise<number> {
    if (this.isDispatching) return 0;
    this.isDispatching = true;
    try {
      const nowMs = Date.now();
      const count = (await this.redis.eval(
        DISPATCH_DELAYED_LUA,
        2,
        this.delayedKey,
        this.streamKey,
        String(nowMs),
        String(this.batchSize)
      )) as number;

      return count;
    } finally {
      this.isDispatching = false;
    }
  }

  public async getPendingDelayedCount(): Promise<number> {
    return await this.redis.zcard(this.delayedKey);
  }
}
