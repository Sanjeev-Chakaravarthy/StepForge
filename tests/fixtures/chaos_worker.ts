import { Redis } from 'ioredis';
import { createDbPool } from '../../src/db/client.js';
import { Worker } from '../../src/worker/worker.js';
import { WorkflowRegistry } from '../../src/workflow/registry.js';
import type { Run } from '../../src/types/domain.js';
import type { StepContext } from '../../src/step/context.js';

async function main() {
  const databaseUrl = process.env.DATABASE_URL;
  const redisUrl = process.env.REDIS_URL;
  const streamKey = process.env.STREAM_KEY || 'stepforge:stream:tasks:chaos';
  const groupName = process.env.GROUP_NAME || 'stepforge:workers:chaos';

  if (!databaseUrl || !redisUrl) {
    console.error('Missing DATABASE_URL or REDIS_URL in child worker');
    process.exit(1);
  }

  const pool = createDbPool(databaseUrl);
  const redis = new Redis(redisUrl);

  const registry = new WorkflowRegistry();

  registry.register('chaos-order-fulfillment', async (run: Run, step: StepContext) => {
    // Step 1: External side effect (e.g. reserving inventory)
    await step.run('step-1-inventory', async () => {
      await pool.query(
        'INSERT INTO test_side_effects (run_id, step_name) VALUES ($1, $2)',
        [run.id, 'step-1-inventory']
      );
      return { inventoryReserved: true, sku: 'ITEM-99' };
    });

    // Step 2: External side effect (e.g. charging payment)
    await step.run('step-2-payment', async () => {
      await pool.query(
        'INSERT INTO test_side_effects (run_id, step_name) VALUES ($1, $2)',
        [run.id, 'step-2-payment']
      );
      return { paymentCaptured: true, amount: 99.99 };
    });

    // Step 3: Mid-step simulated crash
    await step.run('step-3-shipment', async () => {
      await pool.query(
        'INSERT INTO test_side_effects (run_id, step_name) VALUES ($1, $2)',
        [run.id, 'step-3-started']
      );

      // Signal test harness that worker is actively running Step 3
      console.log('SIGNAL:STEP_3_STARTED');

      // Hang indefinitely until SIGKILL terminates the process
      await new Promise((resolve) => setTimeout(resolve, 60000));
      return { trackingNumber: 'SHIP-999' };
    });

    return { status: 'completed' };
  });

  const worker = new Worker(pool, redis, {
    workerId: 'chaos-worker-child',
    streamKey,
    groupName,
    registry,
    blockTimeoutMs: 500,
    leaseTtlMs: 10000,
  });

  await worker.initGroup();
  await worker.start();
  console.log('SIGNAL:WORKER_READY');
}

main().catch((err) => {
  console.error('Child chaos worker failed:', err);
  process.exit(1);
});
