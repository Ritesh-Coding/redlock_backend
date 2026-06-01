import { createClient } from 'redis';
import Redlock from 'redlock';
import dotenv from 'dotenv';

dotenv.config();

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

console.log(`[Redis] Connecting to ${REDIS_URL}...`);
const redisClient = createClient({ url: REDIS_URL });

redisClient.on('error', (err) => console.error('[Redis Error]:', err));
redisClient.on('connect', () => console.log('[Redis] Client connected successfully.'));

await redisClient.connect();

// Compatibility wrappers to adapt node-redis v4 to older Redlock method signatures
redisClient.evalsha = function(sha, numkeys, ...args) {
  const actualArgs = Array.isArray(args[0]) ? args[0] : args;
  const keys = actualArgs.slice(0, numkeys);
  const argv = actualArgs.slice(numkeys).map(arg => String(arg));
  return redisClient.evalSha(sha, {
    keys,
    arguments: argv
  });
};

const originalEval = redisClient.eval.bind(redisClient);
redisClient.eval = function(script, numkeys, ...args) {
  const actualArgs = Array.isArray(args[0]) ? args[0] : args;
  const keys = actualArgs.slice(0, numkeys);
  const argv = actualArgs.slice(numkeys).map(arg => String(arg));
  return originalEval(script, {
    keys,
    arguments: argv
  });
};

// Configure Redlock with the single Redis client (can be expanded to cluster/sentinel in production)
const redlock = new Redlock([redisClient], {
  driftFactor: 0.01, 
  retryCount: 0,     // Crucial: 0 retries means other instances immediately give up and skip the cron
  retryDelay: 200, 
  retryJitter: 200 
});

redlock.on('clientError', (err) => {
  console.error('[Redlock Client Error]:', err);
});

// Atomic Token Bucket Lua Script for Distributed Rate Limiting
const RATE_LIMIT_LUA = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2]) -- tokens per millisecond
local now = tonumber(ARGV[3]) -- current time in ms
local requested = tonumber(ARGV[4] or 1)

-- Retrieve bucket details
local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if not tokens then
    tokens = capacity
    last_refill = now
else
    -- Refill tokens based on time elapsed
    local elapsed = now - last_refill
    local refilled = elapsed * refill_rate
    tokens = math.min(capacity, tokens + refilled)
    last_refill = now
end

-- Deduct token if allowed
local allowed = false
if tokens >= requested then
    tokens = tokens - requested
    allowed = true
end

-- Save bucket state with 1-hour expiration
redis.call('HSET', key, 'tokens', tokens, 'last_refill', last_refill)
redis.call('EXPIRE', key, 3600)

return { allowed and 1 or 0, tokens }
`;

export { redisClient, redlock, RATE_LIMIT_LUA };

