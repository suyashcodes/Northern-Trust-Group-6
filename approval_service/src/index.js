/**
 * Approval Service — Human Task Microservice
 * Port: 3005
 *
 * Responsibilities:
 *  - Receive human_task dispatch from the orchestrator via BullMQ
 *  - Persist pending approval tasks in PostgreSQL (human_tasks table)
 *  - Expose REST endpoints for the dashboard to list/approve/reject tasks
 *  - Report results back to the orchestrator results queue
 *  - Broadcast real-time updates via Socket.IO
 */

require('dotenv').config();

const express = require('express');
const http = require('http');
const { Server: SocketIO } = require('socket.io'); // optional — see try/catch below
const cors = require('cors');
const { v4: uuidv4, validate: uuidValidate } = require('uuid');
const { Queue, Worker } = require('bullmq');
const { Pool } = require('pg');

// ─── Config ────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3005;
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379', 10);
const DB_URL = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/orchestrator';
const ORCHESTRATOR_URL = process.env.ORCHESTRATOR_URL || 'http://localhost:4000';
const API_KEY = process.env.API_KEY || null; // Set in production to enable auth
const NODE_ENV = process.env.NODE_ENV || 'development';
const RATE_LIMIT_WINDOW_MS = parseInt(process.env.RATE_LIMIT_WINDOW_MS || '60000', 10); // 1 minute
const RATE_LIMIT_MAX = parseInt(process.env.RATE_LIMIT_MAX || '100', 10);

const redisConnection = { host: REDIS_HOST, port: REDIS_PORT };

// ─── DB Pool ────────────────────────────────────────────────────────────────
const db = new Pool({ connectionString: DB_URL });
let dbReady = false;

// ─── BullMQ Queues ──────────────────────────────────────────────────────────
// Queue on which the orchestrator dispatches human_task jobs
const approvalQueue = new Queue('approval-service', { connection: redisConnection });

// Queue on which we post results back to the orchestrator
const resultsQueue = new Queue('orchestrator-results', { connection: redisConnection });

// ─── Express + Socket.IO ────────────────────────────────────────────────────
const app = express();
const server = http.createServer(app);

let io;
try {
  io = new SocketIO(server, { cors: { origin: '*' } });
  io.on('connection', (socket) => {
    console.log(`[WS] client connected: ${socket.id}`);
  });
} catch (_) {
  // socket.io optional; REST-only mode
  console.warn('[WS] socket.io not available — running in REST-only mode');
  io = { emit: () => {} };
}

app.use(cors());
app.use(express.json());

// ─── Middleware: API Key Authentication ─────────────────────────────────────
/**
 * When API_KEY env var is set, all mutating endpoints require
 * an `x-api-key` header matching the configured key.
 * Read-only endpoints (GET, /health) are exempt.
 */
function authMiddleware(req, res, next) {
  // Skip auth if no API_KEY configured (dev mode)
  if (!API_KEY) return next();

  // Skip auth for read-only and health endpoints
  if (req.method === 'GET') return next();

  const provided = req.headers['x-api-key'];
  if (!provided || provided !== API_KEY) {
    return res.status(401).json({ error: 'Unauthorized — invalid or missing x-api-key header' });
  }
  next();
}
app.use(authMiddleware);

// ─── Middleware: Simple In-Memory Rate Limiter ──────────────────────────────
const rateLimitMap = new Map();

function rateLimiter(req, res, next) {
  const key = req.ip || req.socket?.remoteAddress || 'unknown';
  const now = Date.now();

  if (!rateLimitMap.has(key)) {
    rateLimitMap.set(key, { count: 1, windowStart: now });
    return next();
  }

  const entry = rateLimitMap.get(key);
  if (now - entry.windowStart > RATE_LIMIT_WINDOW_MS) {
    // Reset window
    entry.count = 1;
    entry.windowStart = now;
    return next();
  }

  entry.count++;
  if (entry.count > RATE_LIMIT_MAX) {
    return res.status(429).json({ error: 'Too many requests — try again later' });
  }
  next();
}
app.use(rateLimiter);

// Periodically clean up stale rate-limit entries
setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of rateLimitMap) {
    if (now - entry.windowStart > RATE_LIMIT_WINDOW_MS * 2) {
      rateLimitMap.delete(key);
    }
  }
}, RATE_LIMIT_WINDOW_MS * 2);

// ─── Middleware: UUID param validation ──────────────────────────────────────
function validateUuidParam(req, res, next) {
  const { id } = req.params;
  if (id && !uuidValidate(id)) {
    return res.status(400).json({ error: 'Invalid task ID format — expected a UUID' });
  }
  next();
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/** Sanitize error messages — don't leak internals in production */
function safeErrorMessage(err) {
  if (NODE_ENV === 'production') {
    return 'Internal server error';
  }
  return err.message;
}

// ─── DB helpers ─────────────────────────────────────────────────────────────
async function ensureSchema() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS human_tasks (
      id                  UUID PRIMARY KEY,
      task_execution_id   UUID NOT NULL,
      workflow_execution_id UUID NOT NULL,
      task_id             VARCHAR(128) NOT NULL,
      assignee            VARCHAR(128) NOT NULL DEFAULT 'ops-team',
      order_id            VARCHAR(128),
      total_value         NUMERIC,
      order_payload       JSONB,
      status              VARCHAR(32)  NOT NULL DEFAULT 'PENDING',
      decided_by          VARCHAR(128),
      reject_reason       TEXT,
      decided_at          TIMESTAMPTZ,
      created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      CONSTRAINT uq_task_execution UNIQUE (task_execution_id)
    );
  `);

  // Ensure all columns exist for compatibility with existing database
  await db.query(`ALTER TABLE human_tasks ADD COLUMN IF NOT EXISTS workflow_execution_id UUID;`);
  await db.query(`ALTER TABLE human_tasks ADD COLUMN IF NOT EXISTS task_id VARCHAR(128);`);
  await db.query(`ALTER TABLE human_tasks ADD COLUMN IF NOT EXISTS assignee VARCHAR(128) DEFAULT 'ops-team';`);
  await db.query(`ALTER TABLE human_tasks ADD COLUMN IF NOT EXISTS order_id VARCHAR(128);`);
  await db.query(`ALTER TABLE human_tasks ADD COLUMN IF NOT EXISTS total_value NUMERIC;`);
  await db.query(`ALTER TABLE human_tasks ADD COLUMN IF NOT EXISTS order_payload JSONB;`);
  await db.query(`ALTER TABLE human_tasks ADD COLUMN IF NOT EXISTS decided_by VARCHAR(128);`);
  await db.query(`ALTER TABLE human_tasks ADD COLUMN IF NOT EXISTS reject_reason TEXT;`);
  await db.query(`ALTER TABLE human_tasks ADD COLUMN IF NOT EXISTS decided_at TIMESTAMPTZ;`);
  await db.query(`ALTER TABLE human_tasks ALTER COLUMN workflow_execution_id DROP NOT NULL;`);
  await db.query(`ALTER TABLE human_tasks ALTER COLUMN task_id DROP NOT NULL;`);
  await db.query(`ALTER TABLE human_tasks ALTER COLUMN assignee DROP NOT NULL;`);

  // Indexes for common query patterns
  await db.query(`CREATE INDEX IF NOT EXISTS idx_human_tasks_status ON human_tasks (status);`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_human_tasks_assignee ON human_tasks (assignee);`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_human_tasks_created_at ON human_tasks (created_at DESC);`);

  dbReady = true;
  console.log('[DB] human_tasks table and indexes ready');
}

// ─── Shared task-creation logic ─────────────────────────────────────────────
/**
 * Creates a human task row in PostgreSQL and emits a Socket.IO event.
 * Used by both the BullMQ worker and the REST POST endpoint.
 *
 * @param {object} data - Task creation payload
 * @param {string} data.task_execution_id
 * @param {string} data.workflow_execution_id
 * @param {string} data.task_id
 * @param {string} [data.assignee='ops-team']
 * @param {object} [data.input={}]
 * @returns {object} The created human_tasks row
 */
async function createHumanTask({ task_execution_id, workflow_execution_id, task_id, assignee, input = {} }) {
  const humanTaskId = uuidv4();

  // Use ?? (nullish coalescing) instead of || to preserve falsy-but-valid
  // values like total_value=0 or order_id=""
  await db.query(
    `INSERT INTO human_tasks
       (id, task_execution_id, workflow_execution_id, task_id, assignee,
        order_id, total_value, order_payload, status)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'PENDING')
     ON CONFLICT (task_execution_id) DO NOTHING`,
    [
      humanTaskId,
      task_execution_id,
      workflow_execution_id,
      task_id,
      assignee ?? 'ops-team',
      input.order_id ?? null,
      input.total_value ?? null,
      JSON.stringify(input),
    ]
  );

  // Fetch the row (may be existing if idempotent retry hit ON CONFLICT)
  const { rows } = await db.query(
    'SELECT * FROM human_tasks WHERE task_execution_id = $1',
    [task_execution_id]
  );
  const task = rows[0];

  // Only emit creation event if this was a new insert (status is PENDING and
  // the id matches what we generated)
  if (task && task.id === humanTaskId) {
    console.log(`[Task] Created human task ${humanTaskId} for workflow ${workflow_execution_id}`);
    io.emit('human_task:created', {
      id: task.id,
      task_execution_id: task.task_execution_id,
      workflow_execution_id: task.workflow_execution_id,
      task_id: task.task_id,
      assignee: task.assignee,
      order_id: task.order_id,
      total_value: task.total_value,
      status: task.status,
      created_at: task.created_at,
    });
  } else {
    console.log(`[Task] Idempotent retry — task for execution ${task_execution_id} already exists`);
  }

  return task;
}

// ─── BullMQ Worker ───────────────────────────────────────────────────────────
/**
 * The orchestrator pushes a job onto the "approval-service" queue whenever a
 * workflow hits a human_task step. The job payload follows the standard
 * task dispatch contract:
 *
 * {
 *   task_execution_id : UUID,
 *   workflow_execution_id : UUID,
 *   task_id           : string,
 *   assignee          : string,
 *   input             : { order_id, total_value, customer_id, ... }
 * }
 *
 * IMPORTANT: This worker completes the BullMQ job immediately after creating
 * the human_tasks row. The job result contains { status: "WAITING_HUMAN" }
 * so the orchestrator knows the workflow is now parked pending human input.
 *
 * The workflow resumes only when the approval/rejection result arrives on
 * the "orchestrator-results" queue (posted by resolveTask below).
 */
const worker = new Worker(
  'approval-service',
  async (job) => {
    const task = await createHumanTask(job.data);

    // The BullMQ job completes here. The orchestrator should inspect the
    // result's `status` field: "WAITING_HUMAN" means the workflow must park
    // until a result appears on the orchestrator-results queue.
    return { human_task_id: task.id, status: 'WAITING_HUMAN' };
  },
  { connection: redisConnection, concurrency: 10 }
);

worker.on('completed', (job, result) => {
  console.log(`[Worker] Job ${job.id} → human task ${result.human_task_id} (WAITING_HUMAN)`);
});

worker.on('failed', (job, err) => {
  console.error(`[Worker] Job ${job?.id} failed:`, err.message);
});

// ─── REST Endpoints ──────────────────────────────────────────────────────────

// Health — reflects actual DB connectivity
app.get('/health', async (_req, res) => {
  const health = { service: 'approval-service', status: 'ok', db: 'connected' };
  try {
    await db.query('SELECT 1');
  } catch {
    health.status = 'degraded';
    health.db = 'unreachable';
    return res.status(503).json(health);
  }
  res.json(health);
});

/**
 * GET /human-tasks
 * List all pending (or filtered) human tasks.
 * Query params: status (PENDING|APPROVED|REJECTED), assignee, limit, offset
 */
app.get('/human-tasks', async (req, res) => {
  try {
    const { status, assignee, limit = 50, offset = 0 } = req.query;
    const conditions = [];
    const params = [];

    if (status) {
      params.push(status);
      conditions.push(`status = $${params.length}`);
    }
    if (assignee) {
      params.push(assignee);
      conditions.push(`assignee = $${params.length}`);
    }

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    params.push(Number(limit), Number(offset));

    const { rows } = await db.query(
      `SELECT * FROM human_tasks ${where}
       ORDER BY created_at DESC
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params
    );

    const countRes = await db.query(
      `SELECT COUNT(*) FROM human_tasks ${where}`,
      params.slice(0, -2)
    );

    res.json({ tasks: rows, total: parseInt(countRes.rows[0].count, 10) });
  } catch (err) {
    console.error('[GET /human-tasks]', err);
    res.status(500).json({ error: safeErrorMessage(err) });
  }
});

/**
 * GET /human-tasks/:id
 * Get a single human task by id.
 */
app.get('/human-tasks/:id', validateUuidParam, async (req, res) => {
  try {
    const { rows } = await db.query('SELECT * FROM human_tasks WHERE id = $1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'Task not found' });
    res.json(rows[0]);
  } catch (err) {
    console.error('[GET /human-tasks/:id]', err);
    res.status(500).json({ error: safeErrorMessage(err) });
  }
});

/**
 * POST /human-tasks/:id/approve
 * Approve a pending human task.
 * Body: { decided_by: string }
 */
app.post('/human-tasks/:id/approve', validateUuidParam, async (req, res) => {
  try {
    const { decided_by = 'anonymous' } = req.body || {};
    const result = await resolveTask(req.params.id, 'APPROVED', decided_by, null);
    if (!result) return res.status(404).json({ error: 'Task not found or already decided' });
    res.json({ status: 'APPROVED', task: result });
  } catch (err) {
    console.error('[POST /approve]', err);
    res.status(500).json({ error: safeErrorMessage(err) });
  }
});

/**
 * POST /human-tasks/:id/reject
 * Reject a pending human task.
 * Body: { decided_by: string, reason: string }
 */
app.post('/human-tasks/:id/reject', validateUuidParam, async (req, res) => {
  try {
    const { decided_by = 'anonymous', reason = '' } = req.body || {};
    const result = await resolveTask(req.params.id, 'REJECTED', decided_by, reason);
    if (!result) return res.status(404).json({ error: 'Task not found or already decided' });
    res.json({ status: 'REJECTED', task: result });
  } catch (err) {
    console.error('[POST /reject]', err);
    res.status(500).json({ error: safeErrorMessage(err) });
  }
});

/**
 * POST /human-tasks  (direct creation — used when orchestrator calls REST instead of queue)
 * Allows the orchestrator to create a human task via HTTP rather than via BullMQ.
 */
app.post('/human-tasks', async (req, res) => {
  try {
    const { task_execution_id, workflow_execution_id, task_id, assignee, input = {} } = req.body || {};
    if (!task_execution_id || !workflow_execution_id || !task_id) {
      return res.status(400).json({ error: 'task_execution_id, workflow_execution_id and task_id are required' });
    }

    // Validate UUID format for IDs
    if (!uuidValidate(task_execution_id) || !uuidValidate(workflow_execution_id)) {
      return res.status(400).json({ error: 'task_execution_id and workflow_execution_id must be valid UUIDs' });
    }

    const task = await createHumanTask({ task_execution_id, workflow_execution_id, task_id, assignee, input });
    res.status(201).json(task);
  } catch (err) {
    console.error('[POST /human-tasks]', err);
    res.status(500).json({ error: safeErrorMessage(err) });
  }
});

// ─── Shared resolution logic ─────────────────────────────────────────────────
async function resolveTask(id, status, decidedBy, reason) {
  const { rows } = await db.query(
    `UPDATE human_tasks
     SET status = $1, decided_by = $2, reject_reason = $3, decided_at = NOW()
     WHERE id = $4 AND status = 'PENDING'
     RETURNING *`,
    [status, decidedBy, reason, id]
  );

  if (!rows.length) return null;

  const task = rows[0];

  // Report outcome to orchestrator via results queue
  await resultsQueue.add('task-result', {
    task_execution_id: task.task_execution_id,
    workflow_execution_id: task.workflow_execution_id,
    task_id: task.task_id,
    state: status === 'APPROVED' ? 'COMPLETED' : 'FAILED',
    result: {
      human_task_id: id,
      decision: status,
      decided_by: decidedBy,
      reason: reason || null,
    },
  });

  // Forward resolution to orchestrator via HTTP API
  try {
    const endpoint = status === 'APPROVED' ? 'approve' : 'reject';
    const body = status === 'APPROVED'
      ? { decided_by: decidedBy }
      : { reason: reason || 'Rejected by operator' };

    const res = await fetch(`${ORCHESTRATOR_URL}/api/v1/human-tasks/${id}/${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      console.error(`[Resolve] Failed to forward resolution to orchestrator: ${res.status} - ${await res.text()}`);
    } else {
      console.log(`[Resolve] Successfully forwarded resolution to orchestrator via HTTP`);
    }
  } catch (err) {
    console.error(`[Resolve] Error forwarding resolution to orchestrator:`, err.message);
  }

  console.log(`[Resolve] Task ${id} ${status} by ${decidedBy}`);

  // Broadcast to dashboard
  io.emit('human_task:resolved', {
    id,
    status,
    decided_by: decidedBy,
    reject_reason: reason,
    workflow_execution_id: task.workflow_execution_id,
  });

  return task;
}

// ─── Graceful Shutdown ──────────────────────────────────────────────────────
async function shutdown(signal) {
  console.log(`\n[Shutdown] Received ${signal} — draining connections...`);
  try {
    // Stop accepting new BullMQ jobs
    await worker.close();
    console.log('[Shutdown] BullMQ worker closed');
  } catch (err) {
    console.error('[Shutdown] Error closing worker:', err.message);
  }

  // Stop accepting new HTTP connections and wait for in-flight requests
  server.close(() => {
    console.log('[Shutdown] HTTP server closed');
  });

  try {
    // Close BullMQ queue connections
    await approvalQueue.close();
    await resultsQueue.close();
    console.log('[Shutdown] BullMQ queues closed');
  } catch (err) {
    console.error('[Shutdown] Error closing queues:', err.message);
  }

  try {
    // Drain the DB pool
    await db.end();
    console.log('[Shutdown] DB pool drained');
  } catch (err) {
    console.error('[Shutdown] Error closing DB:', err.message);
  }

  console.log('[Shutdown] Clean exit');
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// ─── Boot ────────────────────────────────────────────────────────────────────
(async () => {
  try {
    await ensureSchema();
  } catch (err) {
    console.error('[DB] FATAL — Could not connect to PostgreSQL:', err.message);
    console.error('[DB] The service cannot operate without a database. Exiting.');
    process.exit(1);
  }

  server.listen(PORT, () => {
    console.log(`\n✅  Approval Service running on port ${PORT}`);
    console.log(`   REST  : http://localhost:${PORT}/human-tasks`);
    console.log(`   Queue : approval-service (Redis ${REDIS_HOST}:${REDIS_PORT})`);
    console.log(`   Auth  : ${API_KEY ? 'API key required for mutations' : 'DISABLED (dev mode)'}`);
    console.log(`   Env   : ${NODE_ENV}\n`);
  });
})();
