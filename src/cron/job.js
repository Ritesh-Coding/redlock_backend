import cron from 'node-cron';
import { redisClient, redlock } from '../config/redis.js';

const REDIS_KEY = 'my_ordered_items';
const LOCK_KEY = 'locks:cron-job-1min';
const LOCK_TTL = 1000 * 60 * 4; // 4 minutes

// In-memory buffer for real-time logs shown in the UI
export const logBuffer = [];
const sseClients = new Set();

export const addLog = (worker, message, type = 'info') => {
  const logEntry = {
    timestamp: new Date().toLocaleTimeString(),
    worker,
    message,
    type, // 'info', 'success', 'warning', 'error'
    id: Date.now() + Math.random().toString(36).substr(2, 5)
  };
  logBuffer.push(logEntry);
  if (logBuffer.length > 100) logBuffer.shift(); // Cap logs
  
  console.log(`[${logEntry.timestamp}] [${worker}] ${message}`);
  
  // Broadcast to all connected SSE clients
  for (const client of sseClients) {
    client.write(`data: ${JSON.stringify(logEntry)}\n\n`);
  }
};

export const registerSseClient = (res) => {
  sseClients.add(res);
  // Send initial logs
  res.write(`data: ${JSON.stringify({ type: 'init', logs: logBuffer })}\n\n`);
};

export const unregisterSseClient = (res) => {
  sseClients.delete(res);
};

// Core job function representing a single worker instance
export const runWorker = async (workerName) => {
  addLog(workerName, `Attempting to acquire lock: "${LOCK_KEY}"...`, 'info');

  try {
    // Attempt to acquire the lock
    const lock = await redlock.acquire([LOCK_KEY], LOCK_TTL);
    addLog(workerName, `🟢 LOCK ACQUIRED! Executing cron job task...`, 'success');

    // --- PLACE YOUR ACTUAL CRON JOB LOGIC HERE ---
    // Example: Fetching/generating fresh data
    const timestamp = Date.now();
    const freshData = [
      { text: `Data from ${workerName} - Point A`, id: timestamp, worker: workerName },
      { text: `Data from ${workerName} - Point B`, id: timestamp + 1, worker: workerName }
    ];



    

    // Store the items atomically into your Sorted Set
    for (const item of freshData) {
      await redisClient.zAdd(REDIS_KEY, { score: item.id, value: JSON.stringify(item) });
    }
    
    addLog(workerName, `💾 Successfully added ${freshData.length} items to Sorted Set "${REDIS_KEY}" in Redis.`, 'success');
    // --------------------------------------------

    // Simulate a heavy operation (takes 70 seconds total) with periodic progress logging
    addLog(workerName, `⏳ Starting heavy operation (70 seconds total)...`, 'info');
    for (let i = 1; i <= 7; i++) {
      await new Promise((resolve) => setTimeout(resolve, 10000));
      addLog(workerName, `⏳ Heavy operation progress: ${i * 10}/70 seconds elapsed...`, 'info');
    }

    // Release the lock when completely finished
    await lock.release();
    addLog(workerName, `🔓 Lock released successfully.`, 'info');
  } catch (err) {
    // If Redlock throws an 'ExecutionError', it means another instance holds the lock
    if (err.name === 'ExecutionError') {
      addLog(workerName, `🔴 Redlock blocked: Lock already acquired by another worker. Skipping job safely!`, 'warning');
      return;
    }
    addLog(workerName, `❌ Cron Job Error: ${err.message}`, 'error');
  }
};

export const simulateCompetition = async () => {
  addLog('System', 'Triggering concurrency race between Worker A and Worker B...', 'info');
  
  // Trigger Worker A first
  runWorker('Worker A');
  
  // Simulate a minor 50ms clock drift/offset before triggering Worker B
  await new Promise(resolve => setTimeout(resolve, 50));
  
  // Trigger Worker B
  runWorker('Worker B');
};

// 4. Schedule the Cron to run every 1 minute (for testing overlapping runs)
cron.schedule('*/1 * * * *', () => {
  addLog('Cron Schedule', '1-minute cron job triggered. Running standard worker (Worker A)...', 'info');
  runWorker('Worker A');
});

console.log('[Cron] Standard cron scheduled for every 1 minute.');
