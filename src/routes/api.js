import { Router } from 'express';
import { redisClient, RATE_LIMIT_LUA } from '../config/redis.js';
import { simulateCompetition, registerSseClient, unregisterSseClient, logBuffer, addLog } from '../cron/job.js';
import { 
  enqueueTask, 
  getQueueStatus, 
  toggleWorker, 
  clearQueue, 
  spawnWorker, 
  terminateWorker, 
  getDlqTasks, 
  reenqueueDlqTask, 
  purgeDlq 
} from '../config/queue.js';

const router = Router();
const REDIS_KEY = 'my_ordered_items';

// ==========================================
// 1. REDLOCK CONCURRENCY CONTEXT
// ==========================================

// Fetch all sorted set items (Redlock)
router.get('/items', async (req, res) => {
  try {
    const items = await redisClient.zRangeWithScores(REDIS_KEY, 0, -1);
    const parsedItems = items.map(item => {
      try {
        return {
          score: item.score,
          data: JSON.parse(item.value)
        };
      } catch {
        return {
          score: item.score,
          data: { text: item.value }
        };
      }
    });
    res.json({ success: true, items: parsedItems });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Trigger concurrency race manually
router.post('/trigger', async (req, res) => {
  try {
    simulateCompetition();
    res.json({ success: true, message: 'Concurrency competition started!' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Clear Redlock database items
router.delete('/items', async (req, res) => {
  try {
    await redisClient.del(REDIS_KEY);
    logBuffer.length = 0;
    addLog('System', 'Redis sorted set and log history cleared.', 'info');
    res.json({ success: true, message: 'Sorted set cleared successfully.' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ==========================================
// 2. REAL-TIME OBSERVABILITY & TELEMETRY
// ==========================================

// Fetch parsed Redis telemetry stats (Memory, Client Load, Throughput, Hits)
router.get('/redis/telemetry', async (req, res) => {
  try {
    const rawInfo = await redisClient.info();
    const info = {};
    
    // Parse Redis raw INFO string into key-value pairs
    rawInfo.split('\r\n').forEach(line => {
      if (line && !line.startsWith('#')) {
        const [key, value] = line.split(':');
        if (key && value) {
          info[key.trim()] = value.trim();
        }
      }
    });

    const usedMemory = parseInt(info.used_memory || '0', 10);
    const usedMemoryPeak = parseInt(info.used_memory_peak || '0', 10);
    const fragmentationRatio = parseFloat(info.mem_fragmentation_ratio || '0');
    
    const connectedClients = parseInt(info.connected_clients || '0', 10);
    const blockedClients = parseInt(info.blocked_clients || '0', 10);
    
    const opsPerSec = parseInt(info.instantaneous_ops_per_sec || '0', 10);
    const commandsProcessed = parseInt(info.total_commands_processed || '0', 10);
    
    const hits = parseInt(info.keyspace_hits || '0', 10);
    const misses = parseInt(info.keyspace_misses || '0', 10);
    const cacheHitRatio = (hits + misses) > 0 
      ? parseFloat(((hits / (hits + misses)) * 100).toFixed(2)) 
      : 100.00;

    res.json({
      success: true,
      telemetry: {
        memory: {
          usedBytes: usedMemory,
          usedHuman: info.used_memory_human || '0B',
          peakBytes: usedMemoryPeak,
          peakHuman: info.used_memory_peak_human || '0B',
          fragmentation: fragmentationRatio
        },
        clients: {
          connected: connectedClients,
          blocked: blockedClients
        },
        throughput: {
          opsPerSec,
          commandsProcessed
        },
        cache: {
          hits,
          misses,
          hitRatio: cacheHitRatio
        },
        system: {
          uptimeSeconds: parseInt(info.uptime_in_seconds || '0', 10),
          role: info.role || 'master',
          version: info.redis_version || '7.x'
        }
      }
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ==========================================
// 3. ATOMIC LUA RATE LIMITING
// ==========================================

// Rate-limit test playground endpoint
router.post('/rate-limit-test', async (req, res) => {
  const clientId = req.body.clientId || req.ip || 'anonymous';
  const key = `rate_limit:${clientId}`;
  
  // Rate-limiting spec: 10 capacity, refilling 2 tokens per sec (0.002 per ms)
  const capacity = 10;
  const refillRate = 0.002;
  const now = Date.now();
  const requested = 1;

  try {
    // Run the Atomic Token Bucket Lua Script via Redis EVAL
    const result = await redisClient.eval(
      RATE_LIMIT_LUA,
      1,
      key,
      String(capacity),
      String(refillRate),
      String(now),
      String(requested)
    );

    const allowed = Number(result[0]) === 1;
    const remainingTokens = parseFloat(result[1]);

    if (!allowed) {
      addLog('Rate Limiter', `🚫 Blocked API request from "${clientId}". Token bucket exhausted!`, 'error');
      return res.status(429).json({
        success: false,
        allowed: false,
        remainingTokens,
        capacity,
        message: 'Rate limit exceeded. Refilling tokens in background...'
      });
    }

    addLog('Rate Limiter', `🔓 Approved request from "${clientId}". Tokens left: ${remainingTokens.toFixed(2)}/10.0`, 'success');
    res.json({
      success: true,
      allowed: true,
      remainingTokens,
      capacity
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ==========================================
// 4. REAL-TIME LOGS STREAM (SSE)
// ==========================================

router.get('/logs', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  registerSseClient(res);

  req.on('close', () => {
    unregisterSseClient(res);
  });
});

// ==========================================
// 5. REDIS TASK QUEUE & WORKERS
// ==========================================

// Fetch queue diagnostic status
router.get('/queue/status', async (req, res) => {
  try {
    const status = await getQueueStatus();
    res.json({ success: true, ...status });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Enqueue a task
router.post('/queue/enqueue', async (req, res) => {
  const { type, duration, forceFailure } = req.body;
  try {
    const task = await enqueueTask(type, duration, forceFailure);
    res.json({ success: true, task });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Toggle worker active state
router.post('/queue/workers', async (req, res) => {
  const { workerName, state } = req.body;
  try {
    const result = await toggleWorker(workerName, state);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Spawn a new dynamic worker
router.post('/queue/workers/spawn', async (req, res) => {
  const { workerName } = req.body;
  try {
    const result = await spawnWorker(workerName);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Terminate a dynamic worker
router.post('/queue/workers/terminate', async (req, res) => {
  const { workerName } = req.body;
  try {
    const result = await terminateWorker(workerName);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Flush entire queue keyspace
router.delete('/queue', async (req, res) => {
  try {
    await clearQueue();
    res.json({ success: true, message: 'Queue database cleared successfully.' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ==========================================
// 6. DEAD LETTER QUEUE (DLQ) ENDPOINTS
// ==========================================

// Get DLQ items
router.get('/queue/dlq', async (req, res) => {
  try {
    const tasks = await getDlqTasks();
    res.json({ success: true, tasks });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Re-enqueue a task from the DLQ back into the active queue
router.post('/queue/dlq/reenqueue', async (req, res) => {
  const { taskId } = req.body;
  try {
    const task = await reenqueueDlqTask(taskId);
    res.json({ success: true, task });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Purge the DLQ list completely
router.delete('/queue/dlq', async (req, res) => {
  try {
    const result = await purgeDlq();
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

export default router;
