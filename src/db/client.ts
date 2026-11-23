import pg from 'pg';
import type { PoolClient, QueryResult, QueryResultRow } from 'pg';

export type Pool = pg.Pool;
export const Pool = pg.Pool;
export type { PoolClient, QueryResult, QueryResultRow };

export function createDbPool(connectionString?: string): pg.Pool {
  return new pg.Pool({
    connectionString: connectionString ?? process.env.DATABASE_URL,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  });
}

export async function withTransaction<T>(
  pool: pg.Pool,
  callback: (client: PoolClient) => Promise<T>
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await callback(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
