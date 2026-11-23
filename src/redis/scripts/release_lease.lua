-- KEYS[1]: Tenant active leases ZSET
-- ARGV[1]: Run ID

redis.call('ZREM', KEYS[1], ARGV[1])
return 1
