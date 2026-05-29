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

export { redisClient, redlock };
