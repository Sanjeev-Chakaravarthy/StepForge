-- KEYS[1]: Tenant active leases ZSET (stepforge:tenant:{tenantId}:active_leases)
-- ARGV[1]: Run ID
-- ARGV[2]: Current Epoch MS (now)
-- ARGV[3]: Lease TTL MS
-- ARGV[4]: Max Concurrency limit

local active_key = KEYS[1]
local run_id = ARGV[1]
local now = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local max_concurrency = tonumber(ARGV[4])

-- Step 1: Evict expired leases
redis.call('ZREMRANGEBYSCORE', active_key, '-inf', now)

-- Step 2: Check if this run already holds a valid lease (idempotent re-entry / heartbeat extension)
local existing_score = redis.call('ZSCORE', active_key, run_id)
if existing_score then
    local new_expire = now + ttl
    redis.call('ZADD', active_key, new_expire, run_id)
    return {1, new_expire}
end

-- Step 3: Enforce concurrency limit
local current_count = redis.call('ZCARD', active_key)
if current_count >= max_concurrency then
    -- Quota exceeded: return 0 with current active count
    return {0, current_count}
end

-- Step 4: Grant new lease
local new_expire = now + ttl
redis.call('ZADD', active_key, new_expire, run_id)
return {1, new_expire}
