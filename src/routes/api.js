import { Router } from 'express';
import { redisClient } from '../config/redis.js';
import { simulateCompetition, registerSseClient, unregisterSseClient, logBuffer, addLog } from '../cron/job.js';
import { enqueueTask, getQueueStatus, toggleWorker, clearQueue } from '../config/queue.js';

const router = Router();
const REDIS_KEY = 'my_ordered_items';

// 1. Fetch all items ordered by score (timestamp)
router.get('/items', async (req, res) => {
  try {
    // In node-redis v4, zRangeWithScores retrieves sorted set items with their scores
    const items = await redisClient.zRangeWithScores(REDIS_KEY, 0, -1);
    
    // Parse values if they are stored as JSON strings
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

// 2. Trigger the Redlock competition manually
router.post('/trigger', async (req, res) => {
  try {
    // Run simulation in background so API remains responsive
    simulateCompetition();
    res.json({ success: true, message: 'Concurrency competition started!' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 3. Clear the Redis Sorted Set and logs
router.delete('/items', async (req, res) => {
  try {
    await redisClient.del(REDIS_KEY);
    logBuffer.length = 0; // Clear backend log history
    addLog('System', 'Redis sorted set and log history cleared.', 'info');
    res.json({ success: true, message: 'Sorted set cleared successfully.' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 4. Server-Sent Events (SSE) route for real-time logs
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

// --- REDIS QUEUE API ROUTES ---

// Enqueue a new task
router.post('/queue/enqueue', async (req, res) => {
  const { type, duration, forceFailure } = req.body;
  try {
    const task = await enqueueTask(type, duration, forceFailure);
    res.json({ success: true, task });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Get queue status
router.get('/queue/status', async (req, res) => {
  try {
    const status = await getQueueStatus();
    res.json({ success: true, ...status });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Toggle queue worker state
router.post('/queue/workers', async (req, res) => {
  const { workerName, state } = req.body;
  try {
    const result = await toggleWorker(workerName, state);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Clear entire queue system
router.delete('/queue', async (req, res) => {
  try {
    await clearQueue();
    res.json({ success: true, message: 'Queue database cleared successfully.' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

export default router;
