import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import apiRouter from './src/routes/api.js';
import { addLog } from './src/cron/job.js';
import { initQueueWorkers } from './src/config/queue.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors({ origin: '*' })); // Allow React to query
app.use(express.json());

// API base endpoint
app.use('/api', apiRouter);

// Basic health check route
app.get('/health', (req, res) => {
  res.json({ status: 'OK', message: 'Redis Lock Backend is running.' });
});

// Start the server
app.listen(PORT, async () => {
  console.log(`⚡ [Server] Running on http://localhost:${PORT}`);
  addLog('System', `Server started on http://localhost:${PORT}. Waiting for Redis/Redlock triggers...`, 'info');
  
  // Initialize and spin up background Redis Queue workers
  await initQueueWorkers();
});
