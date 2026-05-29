import { redisClient } from './redis.js';
import { addLog } from '../cron/job.js';

// Redis Keys
const QUEUE_LIST_KEY = 'queue:tasks';  //uses a redis list for FIFO Enquing
const QUEUE_HASH_KEY = 'queue:task_details'; // uses a redis hash for storing task details
const QUEUE_HISTORY_KEY = 'queue:history'; // uses a redis list for storing task history
const PROCESSED_COUNT_KEY = 'queue:processed_count'; // uses a redis string for storing processed count
const FAILED_COUNT_KEY = 'queue:failed_count'; // uses a redis string for storing failed count
const WORKER_STATES_KEY = 'queue:worker_states'; // uses a redis hash for storing worker states

// In-memory control for background processing loops
const workerLoops = {
  'Queue Worker Alpha': null,  // stores the timeout id for the background processing loop
  'Queue Worker Beta': null    // stores the timeout id for the background processing loop
};

// Initialize worker configurations in Redis (default both to 'active')
export const initQueueWorkers = async () => {
  try {
    const alphaState = await redisClient.hGet(WORKER_STATES_KEY, 'Queue Worker Alpha');
    const betaState = await redisClient.hGet(WORKER_STATES_KEY, 'Queue Worker Beta');

    if (!alphaState) await redisClient.hSet(WORKER_STATES_KEY, 'Queue Worker Alpha', 'active');
    if (!betaState) await redisClient.hSet(WORKER_STATES_KEY, 'Queue Worker Beta', 'active');

    console.log('[Queue] Workers initialized in Redis.');

    // Start background processing loops
    startWorkerLoop('Queue Worker Alpha');
    startWorkerLoop('Queue Worker Beta');
  } catch (err) {
    console.error('[Queue Initialization Error]:', err);
  }
};

// Start the background poll loop for a worker
const startWorkerLoop = (workerName) => {
  if (workerLoops[workerName]) return; // Already running

  const poll = async () => {
    try {
      // 1. Check if the worker is active in Redis
      const state = await redisClient.hGet(WORKER_STATES_KEY, workerName);
      if (state !== 'active') {
        // Worker is stopped. Poll again in 1 second.
        workerLoops[workerName] = setTimeout(poll, 1000);
        return;
      }

      // 2. Attempt to pop a task from the list
      const taskId = await redisClient.rPop(QUEUE_LIST_KEY);
      if (!taskId) {
        // No task in queue. Wait 1 second and poll again.
        workerLoops[workerName] = setTimeout(poll, 1000);
        return;
      }

      // 3. Process the popped task
      const taskDataStr = await redisClient.hGet(QUEUE_HASH_KEY, taskId);
      if (!taskDataStr) {
        // Task details missing. Continue immediately.
        poll();
        return;
      }

      const task = JSON.parse(taskDataStr);
      task.status = 'processing';
      task.worker = workerName;
      task.startedAt = Date.now();

      // Save updated state in Hash
      await redisClient.hSet(QUEUE_HASH_KEY, taskId, JSON.stringify(task));

      // Broadcast log
      addLog(
        workerName,
        `⚙️ Started processing task #${task.displayId}: "${task.type}" (Duration: ${task.duration}s)`,
        'info'
      );

      // Simulate heavy asynchronous background work
      await new Promise((resolve) => setTimeout(resolve, task.duration * 1000));

      // 4. Update task completion/failure status
      task.finishedAt = Date.now();

      if (task.forceFailure) {
        task.status = 'failed';
        task.error = 'Simulated Task Failure';
        await redisClient.hSet(QUEUE_HASH_KEY, taskId, JSON.stringify(task));
        await redisClient.incr(FAILED_COUNT_KEY);

        // Push to history (max 20 items)
        await redisClient.lPush(QUEUE_HISTORY_KEY, JSON.stringify(task));
        await redisClient.lTrim(QUEUE_HISTORY_KEY, 0, 19);

        // Delete from active details hash to save memory (optional, but let's keep details in history list and delete original taskId from details to keep backlog tidy)
        await redisClient.hDel(QUEUE_HASH_KEY, taskId);

        addLog(
          workerName,
          `🔴 Task #${task.displayId} FAILED! Error: Simulated Task Failure`,
          'error'
        );
      } else {
        task.status = 'completed';
        await redisClient.hSet(QUEUE_HASH_KEY, taskId, JSON.stringify(task));
        await redisClient.incr(PROCESSED_COUNT_KEY);

        // Push to history
        await redisClient.lPush(QUEUE_HISTORY_KEY, JSON.stringify(task));
        await redisClient.lTrim(QUEUE_HISTORY_KEY, 0, 19);

        await redisClient.hDel(QUEUE_HASH_KEY, taskId);

        addLog(
          workerName,
          `🟢 Task #${task.displayId} COMPLETED successfully!`,
          'success'
        );
      }

      // Check for next task immediately
      poll();
    } catch (err) {
      console.error(`[Worker error - ${workerName}]:`, err);
      addLog(workerName, `❌ Worker Exception: ${err.message}`, 'error');
      // Retry in 2 seconds on unexpected exceptions
      workerLoops[workerName] = setTimeout(poll, 2000);
    }
  };

  poll();
};

// Enqueue a new task
export const enqueueTask = async (type, duration, forceFailure = false) => {
  const timestamp = Date.now();
  const taskId = `task:${timestamp}:${Math.random().toString(36).substr(2, 5)}`;
  
  // Generate a friendly, sequential human-readable display ID
  // We can base this on a simple incrementing counter or timestamp
  const displayId = timestamp.toString().slice(-6);

  const task = {
    id: taskId,
    displayId,
    type,
    duration: parseInt(duration, 10) || 5,
    status: 'pending',
    forceFailure,
    worker: null,
    createdAt: timestamp,
    startedAt: null,
    finishedAt: null
  };

  // 1. Write task details in the Hash
  await redisClient.hSet(QUEUE_HASH_KEY, taskId, JSON.stringify(task));

  // 2. Push taskId into the FIFO List
  await redisClient.lPush(QUEUE_LIST_KEY, taskId);

  addLog(
    'System',
    `📥 Enqueued task #${displayId} ("${type}") with duration ${duration}s.`,
    'info'
  );

  return task;
};

// Fetch current status, counters, workers state, backlog, and history
export const getQueueStatus = async () => {
  // 1. Get backlog list size
  const backlogSize = await redisClient.lLen(QUEUE_LIST_KEY);

  // 2. Get all worker states
  const alphaState = (await redisClient.hGet(WORKER_STATES_KEY, 'Queue Worker Alpha')) || 'active';
  const betaState = (await redisClient.hGet(WORKER_STATES_KEY, 'Queue Worker Beta')) || 'active';

  // 3. Get total processed & failed counters
  const processedCount = parseInt(await redisClient.get(PROCESSED_COUNT_KEY) || '0', 10);
  const failedCount = parseInt(await redisClient.get(FAILED_COUNT_KEY) || '0', 10);

  // 4. Get active task details that are currently being processed
  // In our simple structure, tasks currently processing are still in the QUEUE_HASH_KEY, but NOT in the queue list, and they have status = 'processing'.
  const allHashEntries = await redisClient.hGetAll(QUEUE_HASH_KEY);
  const pendingTasks = [];
  const processingTasks = [];

  // Parse all hash entries
  Object.values(allHashEntries).forEach(entryStr => {
    try {
      const task = JSON.parse(entryStr);
      if (task.status === 'processing') {
        processingTasks.push(task);
      } else if (task.status === 'pending') {
        pendingTasks.push(task);
      }
    } catch (e) {
      // Ignore parse errors
    }
  });

  // Sort lists by creation timestamp so they display nicely
  pendingTasks.sort((a, b) => b.createdAt - a.createdAt);
  processingTasks.sort((a, b) => a.startedAt - b.startedAt);

  // 5. Get history items
  const historyList = await redisClient.lRange(QUEUE_HISTORY_KEY, 0, -1);
  const history = historyList.map(itemStr => {
    try {
      return JSON.parse(itemStr);
    } catch {
      return null;
    }
  }).filter(Boolean);

  return {
    backlogSize,
    processedCount,
    failedCount,
    workers: {
      'Queue Worker Alpha': alphaState,
      'Queue Worker Beta': betaState
    },
    pendingTasks,
    processingTasks,
    history
  };
};

// Toggle worker status
export const toggleWorker = async (workerName, state) => {
  if (workerName !== 'Queue Worker Alpha' && workerName !== 'Queue Worker Beta') {
    throw new Error('Invalid worker name');
  }

  const normalizedState = state === 'active' ? 'active' : 'stopped';
  await redisClient.hSet(WORKER_STATES_KEY, workerName, normalizedState);

  addLog(
    'System',
    `⚙️ ${workerName} status updated to: ${normalizedState.toUpperCase()}`,
    'info'
  );

  return { workerName, state: normalizedState };
};

// Flush/Clear all keys related to the Queue System
export const clearQueue = async () => {
  await redisClient.del(QUEUE_LIST_KEY);
  await redisClient.del(QUEUE_HASH_KEY);
  await redisClient.del(QUEUE_HISTORY_KEY);
  await redisClient.del(PROCESSED_COUNT_KEY);
  await redisClient.del(FAILED_COUNT_KEY);
  
  // Keep workers states but reset their logs
  addLog('System', '🧹 Redis Queue database and task history completely flushed.', 'info');
  return { success: true };
};
