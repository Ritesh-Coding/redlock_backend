import { redisClient } from './redis.js';
import { addLog } from '../cron/job.js';

// Redis Keys
const QUEUE_LIST_KEY = 'queue:tasks';          // Redis List for FIFO Enqueuing
const QUEUE_HASH_KEY = 'queue:task_details';     // Redis Hash for storing active task details
const QUEUE_HISTORY_KEY = 'queue:history';       // Redis List for storing task history (capped)
const PROCESSED_COUNT_KEY = 'queue:processed_count'; // Redis String for total processed count
const FAILED_COUNT_KEY = 'queue:failed_count';       // Redis String for total failed count
const WORKER_STATES_KEY = 'queue:worker_states';     // Redis Hash for active worker states (active/stopped)
const ACTIVE_WORKERS_SET = 'queue:active_workers';   // Redis Set for active worker names
const DELAYED_TASKS_KEY = 'queue:delayed';       // Redis Sorted Set for delayed retries
const DLQ_KEY = 'queue:dlq';                     // Redis List for Dead Letter Queue

// In-memory control to keep track of active NodeJS timeouts for polling loops
const workerTimeouts = {};

// Initialize queue workers from Redis state
export const initQueueWorkers = async () => {
  try {
    // 1. Get active worker list from Redis
    let workers = await redisClient.sMembers(ACTIVE_WORKERS_SET);
    
    // 2. Seed default workers if none exist
    if (workers.length === 0) {
      await redisClient.sAdd(ACTIVE_WORKERS_SET, 'Queue Worker Alpha');
      await redisClient.sAdd(ACTIVE_WORKERS_SET, 'Queue Worker Beta');
      workers = ['Queue Worker Alpha', 'Queue Worker Beta'];
    }

    // Initialize state mapping in Redis Hash
    for (const workerName of workers) {
      const state = await redisClient.hGet(WORKER_STATES_KEY, workerName);
      if (!state) {
        await redisClient.hSet(WORKER_STATES_KEY, workerName, 'active');
      }
    }

    console.log(`[Queue] Workers list loaded: ${workers.join(', ')}`);

    // 3. Start polling loop for all registered workers
    for (const workerName of workers) {
      startWorkerLoop(workerName);
    }

    // 4. Start the global scheduler that polls the delayed task Sorted Set
    startDelayedTaskScheduler();
  } catch (err) {
    console.error('[Queue Initialization Error]:', err);
  }
};

// Scheduler to pick up delayed tasks (exponential backoff retries)
const startDelayedTaskScheduler = () => {
  setInterval(async () => {
    try {
      const now = Date.now();
      // Find all tasks whose scheduled run time (score) has passed (<= now)
      const dueTaskIds = await redisClient.zRangeByScore(DELAYED_TASKS_KEY, 0, now);
      
      if (dueTaskIds.length > 0) {
        for (const taskId of dueTaskIds) {
          // Remove from delayed Sorted Set
          const removed = await redisClient.zRem(DELAYED_TASKS_KEY, taskId);
          if (removed > 0) {
            // Fetch task details
            const taskDataStr = await redisClient.hGet(QUEUE_HASH_KEY, taskId);
            if (taskDataStr) {
              const task = JSON.parse(taskDataStr);
              task.status = 'pending';
              
              // Update state in Hash
              await redisClient.hSet(QUEUE_HASH_KEY, taskId, JSON.stringify(task));
              
              // Push back into the FIFO List
              await redisClient.lPush(QUEUE_LIST_KEY, taskId);

              addLog(
                'System',
                `🔄 Re-enqueuing task #${task.displayId} (Retry ${task.retries}/${task.maxRetries} after backoff delay)`,
                'info'
              );
            }
          }
        }
      }
    } catch (err) {
      console.error('[Delayed Scheduler Error]:', err);
    }
  }, 1000); // Poll once per second
};

// Start background poll loop for a single worker
const startWorkerLoop = (workerName) => {
  if (workerTimeouts[workerName]) return; // Already running

  const poll = async () => {
    try {
      // 1. Verify if worker is still registered
      const isRegistered = await redisClient.sIsMember(ACTIVE_WORKERS_SET, workerName);
      if (!isRegistered) {
        // Worker was terminated. Stop this loop.
        delete workerTimeouts[workerName];
        return;
      }

      // 2. Check if the worker is active or paused
      const state = await redisClient.hGet(WORKER_STATES_KEY, workerName);
      if (state !== 'active') {
        // Polling paused, check again in 1s
        workerTimeouts[workerName] = setTimeout(poll, 1000);
        return;
      }

      // 3. Atomically pop taskId from queue
      const taskId = await redisClient.rPop(QUEUE_LIST_KEY);
      if (!taskId) {
        // Queue is empty, poll again in 1s
        workerTimeouts[workerName] = setTimeout(poll, 1000);
        return;
      }

      // 4. Retrieve task details
      const taskDataStr = await redisClient.hGet(QUEUE_HASH_KEY, taskId);
      if (!taskDataStr) {
        poll(); // Details lost, proceed to next
        return;
      }

      const task = JSON.parse(taskDataStr);
      task.status = 'processing';
      task.worker = workerName;
      task.startedAt = Date.now();

      // Update state in Hash
      await redisClient.hSet(QUEUE_HASH_KEY, taskId, JSON.stringify(task));

      addLog(
        workerName,
        `⚙️ Processing task #${task.displayId}: "${task.type}" (Attempt ${task.retries + 1}/${task.maxRetries + 1})`,
        'info'
      );

      // Simulate heavy processing job
      await new Promise((resolve) => setTimeout(resolve, task.duration * 1000));

      task.finishedAt = Date.now();

      // Handle Task Completion or Failure
      if (task.forceFailure) {
        // Trigger Failure / Retry Mechanism
        task.retries += 1;
        
        if (task.retries <= task.maxRetries) {
          // Calculate Backoff: exponential delay (e.g. 2s, 4s, 8s)
          const delayMs = Math.pow(2, task.retries) * 2000;
          task.status = 'retrying';
          task.error = 'Simulated Task Failure';
          
          await redisClient.hSet(QUEUE_HASH_KEY, taskId, JSON.stringify(task));
          
          // Schedule in Delayed Sorted Set
          await redisClient.zAdd(DELAYED_TASKS_KEY, { score: Date.now() + delayMs, value: taskId });

          addLog(
            workerName,
            `⚠️ Task #${task.displayId} failed. Scheduled for retry ${task.retries}/${task.maxRetries} in ${delayMs / 1000}s.`,
            'warning'
          );
        } else {
          // Perm-failed -> Move to Dead Letter Queue (DLQ)
          task.status = 'failed';
          task.error = 'Task exceeded maximum retries';
          
          // Push entire object onto DLQ List
          await redisClient.lPush(DLQ_KEY, JSON.stringify(task));
          await redisClient.incr(FAILED_COUNT_KEY);

          // Record in general history list (cap at 20 entries)
          await redisClient.lPush(QUEUE_HISTORY_KEY, JSON.stringify(task));
          await redisClient.lTrim(QUEUE_HISTORY_KEY, 0, 19);

          // Clean up active details
          await redisClient.hDel(QUEUE_HASH_KEY, taskId);

          addLog(
            workerName,
            `💀 Task #${task.displayId} failed permanently after ${task.retries} attempts! Routed to Dead Letter Queue (DLQ).`,
            'error'
          );
        }
      } else {
        // Task completed successfully
        task.status = 'completed';
        
        await redisClient.incr(PROCESSED_COUNT_KEY);

        // Record in history list
        await redisClient.lPush(QUEUE_HISTORY_KEY, JSON.stringify(task));
        await redisClient.lTrim(QUEUE_HISTORY_KEY, 0, 19);

        // Clean up active details
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
      console.error(`[Worker Exception - ${workerName}]:`, err);
      addLog(workerName, `❌ Worker Exception: ${err.message}`, 'error');
      workerTimeouts[workerName] = setTimeout(poll, 2000); // Retry polling in 2s
    }
  };

  poll();
};

// Spawn a new dynamic worker
export const spawnWorker = async (workerName) => {
  if (!workerName || typeof workerName !== 'string') {
    throw new Error('Invalid worker name');
  }
  
  // Save to active workers Set in Redis
  const added = await redisClient.sAdd(ACTIVE_WORKERS_SET, workerName);
  if (!added) {
    throw new Error('Worker with this name already exists');
  }

  // Set initial status to 'active'
  await redisClient.hSet(WORKER_STATES_KEY, workerName, 'active');

  // Spawn Javascript poll loop
  startWorkerLoop(workerName);

  addLog('System', `🚀 Spawned new processing worker: "${workerName}"`, 'success');
  return { workerName, state: 'active' };
};

// Terminate an active worker
export const terminateWorker = async (workerName) => {
  // Remove from active workers Set in Redis
  const removed = await redisClient.sRem(ACTIVE_WORKERS_SET, workerName);
  if (!removed) {
    throw new Error('Worker not found or already terminated');
  }

  // Clean up worker states hash in Redis
  await redisClient.hDel(WORKER_STATES_KEY, workerName);

  addLog('System', `🛑 Terminated worker: "${workerName}"`, 'info');
  return { workerName, terminated: true };
};

// Enqueue a new task
export const enqueueTask = async (type, duration, forceFailure = false) => {
  const timestamp = Date.now();
  const taskId = `task:${timestamp}:${Math.random().toString(36).substr(2, 5)}`;
  const displayId = timestamp.toString().slice(-6);

  const task = {
    id: taskId,
    displayId,
    type,
    duration: parseInt(duration, 10) || 5,
    status: 'pending',
    forceFailure: !!forceFailure,
    worker: null,
    retries: 0,
    maxRetries: 3, // 4 total attempts
    createdAt: timestamp,
    startedAt: null,
    finishedAt: null,
    error: null
  };

  // Store metadata
  await redisClient.hSet(QUEUE_HASH_KEY, taskId, JSON.stringify(task));

  // Push to FIFO List
  await redisClient.lPush(QUEUE_LIST_KEY, taskId);

  addLog(
    'System',
    `📥 Enqueued task #${displayId} ("${type}") with duration ${duration}s (Max Retries: 3).`,
    'info'
  );

  return task;
};

// Fetch complete queue diagnostics & states
export const getQueueStatus = async () => {
  // 1. Queue list size
  const backlogSize = await redisClient.lLen(QUEUE_LIST_KEY);

  // 2. Delayed Sorted Set size
  const delayedSize = await redisClient.zCard(DELAYED_TASKS_KEY);

  // 3. DLQ size
  const dlqSize = await redisClient.lLen(DLQ_KEY);

  // 4. Gather active worker states
  const activeWorkers = await redisClient.sMembers(ACTIVE_WORKERS_SET);
  const workers = {};
  for (const workerName of activeWorkers) {
    workers[workerName] = (await redisClient.hGet(WORKER_STATES_KEY, workerName)) || 'active';
  }

  // 5. Total processed & failed counters
  const processedCount = parseInt(await redisClient.get(PROCESSED_COUNT_KEY) || '0', 10);
  const failedCount = parseInt(await redisClient.get(FAILED_COUNT_KEY) || '0', 10);

  // 6. Gather pending, delayed & processing tasks
  const allHashEntries = await redisClient.hGetAll(QUEUE_HASH_KEY);
  const pendingTasks = [];
  const processingTasks = [];
  const delayedTasks = [];

  // Parse all hash entries
  for (const entryStr of Object.values(allHashEntries)) {
    try {
      const task = JSON.parse(entryStr);
      if (task.status === 'processing') {
        processingTasks.push(task);
      } else if (task.status === 'pending') {
        pendingTasks.push(task);
      } else if (task.status === 'retrying') {
        delayedTasks.push(task);
      }
    } catch (e) {
      // Ignore parse errors
    }
  }

  // Sort logically for UI
  pendingTasks.sort((a, b) => b.createdAt - a.createdAt);
  processingTasks.sort((a, b) => a.startedAt - b.startedAt);
  delayedTasks.sort((a, b) => a.createdAt - b.createdAt);

  // 7. Get history items
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
    delayedSize,
    dlqSize,
    processedCount,
    failedCount,
    workers,
    pendingTasks,
    processingTasks,
    delayedTasks,
    history
  };
};

// Fetch DLQ tasks
export const getDlqTasks = async () => {
  const list = await redisClient.lRange(DLQ_KEY, 0, -1);
  return list.map(itemStr => {
    try {
      return JSON.parse(itemStr);
    } catch {
      return null;
    }
  }).filter(Boolean);
};

// Re-enqueue a task from the DLQ back into the main queue
export const reenqueueDlqTask = async (taskId) => {
  const dlqTasksRaw = await redisClient.lRange(DLQ_KEY, 0, -1);
  let targetTask = null;
  let targetIndex = -1;

  for (let i = 0; i < dlqTasksRaw.length; i++) {
    try {
      const task = JSON.parse(dlqTasksRaw[i]);
      if (task.id === taskId) {
        targetTask = task;
        targetIndex = i;
        break;
      }
    } catch (e) {}
  }

  if (!targetTask) {
    throw new Error('Task not found in Dead Letter Queue');
  }

  // 1. Remove from DLQ list in Redis. Since lRem removes values, let's remove this exact element.
  await redisClient.lRem(DLQ_KEY, 1, dlqTasksRaw[targetIndex]);

  // 2. Reset task parameters
  targetTask.status = 'pending';
  targetTask.retries = 0;
  targetTask.error = null;
  targetTask.worker = null;
  targetTask.createdAt = Date.now();

  // 3. Write metadata to hash
  await redisClient.hSet(QUEUE_HASH_KEY, taskId, JSON.stringify(targetTask));

  // 4. Push back to active FIFO List
  await redisClient.lPush(QUEUE_LIST_KEY, taskId);

  addLog('System', `🔄 Rescued task #${targetTask.displayId} from DLQ and re-enqueued.`, 'success');
  return targetTask;
};

// Purge DLQ completely
export const purgeDlq = async () => {
  await redisClient.del(DLQ_KEY);
  addLog('System', '🧹 Dead Letter Queue (DLQ) completely purged.', 'info');
  return { success: true };
};

// Toggle worker active/stopped state
export const toggleWorker = async (workerName, state) => {
  const normalizedState = state === 'active' ? 'active' : 'stopped';
  await redisClient.hSet(WORKER_STATES_KEY, workerName, normalizedState);

  addLog(
    'System',
    `⚙️ Worker "${workerName}" state updated to: ${normalizedState.toUpperCase()}`,
    'info'
  );

  return { workerName, state: normalizedState };
};

// Flush all queue keys
export const clearQueue = async () => {
  await redisClient.del(QUEUE_LIST_KEY);
  await redisClient.del(QUEUE_HASH_KEY);
  await redisClient.del(QUEUE_HISTORY_KEY);
  await redisClient.del(PROCESSED_COUNT_KEY);
  await redisClient.del(FAILED_COUNT_KEY);
  await redisClient.del(DELAYED_TASKS_KEY);
  await redisClient.del(DLQ_KEY);

  // Retain active workers set but reset states to active
  const workers = await redisClient.sMembers(ACTIVE_WORKERS_SET);
  for (const workerName of workers) {
    await redisClient.hSet(WORKER_STATES_KEY, workerName, 'active');
  }

  addLog('System', '🧹 Redis Queue database and DLQ task history completely flushed.', 'info');
  return { success: true };
};
