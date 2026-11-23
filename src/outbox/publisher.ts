import { Redis } from 'ioredis';
import type { OutboxEvent } from '../types/domain.js';

export async function publishToRedisStream(
  redis: Redis,
  streamKey: string,
  event: OutboxEvent
): Promise<string> {
  const messageId = await redis.xadd(
    streamKey,
    '*',
    'eventId',
    String(event.id),
    'tenantId',
    event.tenant_id,
    'eventType',
    event.event_type,
    'payload',
    JSON.stringify(event.payload)
  );

  return messageId as string;
}
