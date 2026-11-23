import { z } from 'zod';
import dotenv from 'dotenv';

dotenv.config();

const envSchema = z.object({
  DATABASE_URL: z.string().default('postgresql://postgres:postgres@localhost:5432/stepforge'),
  REDIS_URL: z.string().default('redis://localhost:6379'),
  PORT: z.coerce.number().default(3000),
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  OUTBOX_POLL_INTERVAL_MS: z.coerce.number().default(100),
  OUTBOX_BATCH_SIZE: z.coerce.number().default(50),
  REDIS_STREAM_KEY: z.string().default('stepforge:stream:tasks'),
});

export type EnvConfig = z.infer<typeof envSchema>;

export function loadEnv(override?: Partial<EnvConfig>): EnvConfig {
  return envSchema.parse({
    DATABASE_URL: process.env.DATABASE_URL,
    REDIS_URL: process.env.REDIS_URL,
    PORT: process.env.PORT,
    NODE_ENV: process.env.NODE_ENV,
    OUTBOX_POLL_INTERVAL_MS: process.env.OUTBOX_POLL_INTERVAL_MS,
    OUTBOX_BATCH_SIZE: process.env.OUTBOX_BATCH_SIZE,
    REDIS_STREAM_KEY: process.env.REDIS_STREAM_KEY,
    ...override,
  });
}
