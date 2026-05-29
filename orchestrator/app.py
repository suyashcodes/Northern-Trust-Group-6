import uuid
import yaml
import asyncio
import socketio
import os
import psycopg2.extras
from pathlib import Path
from typing import Dict, Any, Optional, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pathlib import Path
from dotenv import load_dotenv
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

from dag_runner import DAGRunner, TaskState, WorkflowStatus

# ─── Config ─────────────────────────────────────────────────────
WORKFLOWS_DIR = Path(__file__).resolve().parent.parent / "workflows"

# ─── Socket.IO ──────────────────────────────────────────────────
sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    logger=False,
    engineio_logger=False,
)

# Track subscriptions: socket_id -> set of workflow_ids
subscriptions: Dict[str, set] = {}

@sio.event
async def connect(sid, environ):
    subscriptions[sid] = set()

@sio.event
async def disconnect(sid):
    subscriptions.pop(sid, None)

@sio.event
async def subscribe(sid, workflow_id):
    if sid in subscriptions:
        subscriptions[sid].add(workflow_id)

@sio.event
async def unsubscribe(sid, workflow_id):
    if sid in subscriptions:
        subscriptions[sid].discard(workflow_id)


def broadcast_update(run_id: str, data: dict):
    """Send a workflow:update event to all subscribers of this run."""
    for sid, subs in subscriptions.items():
        if run_id in subs:
            asyncio.create_task(sio.emit("workflow:update", data, to=sid))


# ─── FastAPI App ────────────────────────────────────────────────
app = FastAPI(title="Workflow Orchestrator Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Wrap with Socket.IO ASGI app
socket_app = socketio.ASGIApp(sio, app)

# ─── In-Memory State ───────────────────────────────────────────
active_runs: Dict[str, DAGRunner] = {}

import db

# Helper for current timestamp
def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()

# ─── Pydantic Models ───────────────────────────────────────────

class StartWorkflowRequest(BaseModel):
    workflow: str = "order-fulfillment"
    input: Dict[str, Any] = {}

class HumanTaskApproveRequest(BaseModel):
    decided_by: str = "operator"

class HumanTaskRejectRequest(BaseModel):
    reason: str = "Rejected by operator"

class TaskCallbackRequest(BaseModel):
    task_id: str
    status: str
    result: Optional[Dict[str, Any]] = None
    logs: Optional[str] = None
    error: Optional[Any] = None

class TaskQueueResultRequest(BaseModel):
    taskExecutionId: str
    workflowExecutionId: str
    state: str
    resultPayload: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


# ─── Crash Recovery Logic ───────────────────────────────────────

async def recover_runner(workflow_id: str) -> Optional[DAGRunner]:
    """Recover a single runner state from the database and restart it if it was running."""
    conn = db.get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT we.id, wd.name as workflow_name, wd.definition_yaml, we.workflow_state, we.input_payload
                FROM workflow_executions we
                JOIN workflow_definitions wd ON we.workflow_definition_id = wd.id
                WHERE we.id = %s;
                """,
                (workflow_id,)
            )
            row = cur.fetchone()
            if not row:
                return None

            definition = yaml.safe_load(row["definition_yaml"])
            input_payload = row["input_payload"] or {}
            
            runner = DAGRunner(
                run_id=workflow_id,
                definition=definition,
                input_payload=input_payload,
                on_update=broadcast_update
            )
            runner.load_state_from_db()
            active_runs[workflow_id] = runner

            # Resume running loop if state is RUNNING
            if runner.status == WorkflowStatus.RUNNING:
                asyncio.create_task(runner.run())
            
            logger = db.logger
            logger.info(f"Successfully recovered workflow run {workflow_id} in state {runner.status}")
            return runner
    except Exception as e:
        db.logger.error(f"Failed to recover workflow run {workflow_id}: {e}")
        return None
    finally:
        conn.close()


@app.on_event("startup")
async def startup_event():
    """Recover running/paused workflows from the database on startup."""
    db.logger.info("Initializing background crash recovery...")
    conn = db.get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM workflow_executions WHERE workflow_state IN ('RUNNING', 'PAUSED');")
            rows = cur.fetchall()
            for row in rows:
                workflow_id = str(row["id"])
                db.logger.info(f"Found active workflow {workflow_id} in DB, attempting recovery...")
                await recover_runner(workflow_id)
    except Exception as e:
        db.logger.error(f"Error during startup crash recovery: {e}")
    finally:
        conn.close()


async def get_or_recover_runner(workflow_id: str) -> DAGRunner:
    runner = active_runs.get(workflow_id)
    if not runner:
        runner = await recover_runner(workflow_id)
    if not runner:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return runner


# ─── Health Check ───────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "service": "Workflow Orchestrator",
        "status": "online",
        "documentation": "/docs",
    }


# ─── Workflow Endpoints ─────────────────────────────────────────

@app.post("/api/v1/workflows/start")
async def start_workflow(req: StartWorkflowRequest):
    """Start a new workflow execution."""
    # Load workflow definition
    template_name = req.workflow.replace(" ", "-").lower()
    yaml_path = WORKFLOWS_DIR / f"{template_name}.yaml"

    # Try finding the YAML file, fall back to first available
    if not yaml_path.exists():
        yamls = list(WORKFLOWS_DIR.glob("*.yaml"))
        if not yamls:
            raise HTTPException(status_code=404, detail=f"No workflow templates found")
        yaml_path = yamls[0]

    with open(yaml_path, "r") as f:
        definition = yaml.safe_load(f)

    run_id = str(uuid.uuid4())
    runner = DAGRunner(
        run_id=run_id,
        definition=definition,
        input_payload=req.input,
        on_update=broadcast_update,
    )
    active_runs[run_id] = runner

    # Start in background
    asyncio.create_task(runner.run())

    return runner.to_summary()


@app.get("/api/v1/workflows")
async def list_workflows(
    state: Optional[str] = Query(None, description="Filter by workflow state"),
    limit: Optional[int] = Query(50, description="Max results to return"),
):
    """List all workflow executions from the database."""
    conn = db.get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            query = """
                SELECT we.id, wd.name as workflow_name, we.workflow_state as state, we.input_payload, we.started_at, we.completed_at
                FROM workflow_executions we
                JOIN workflow_definitions wd ON we.workflow_definition_id = wd.id
            """
            params = []
            if state:
                query += " WHERE we.workflow_state = %s"
                params.append(state.upper())
            
            query += " ORDER BY we.started_at DESC LIMIT %s;"
            params.append(limit or 50)
            
            cur.execute(query, params)
            rows = cur.fetchall()
            
            executions = []
            for r in rows:
                executions.append({
                    "id": str(r["id"]),
                    "workflow_name": r["workflow_name"],
                    "state": r["state"],
                    "input_payload": r["input_payload"] or {},
                    "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                    "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
                })
            
            return {
                "executions": executions,
                "total": len(executions)
            }
    except Exception as e:
        db.logger.error(f"Error listing workflows from DB: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/api/v1/workflows/{workflow_id}")
async def get_workflow(workflow_id: str):
    """Get full workflow detail including all task states, loading from DB if not active in memory."""
    runner = active_runs.get(workflow_id)
    if runner:
        return runner.to_detail()
    
    conn = db.get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT we.id, wd.name as workflow_name, we.workflow_state as state, we.input_payload, we.started_at, we.completed_at, wd.definition_yaml
                FROM workflow_executions we
                JOIN workflow_definitions wd ON we.workflow_definition_id = wd.id
                WHERE we.id = %s;
                """,
                (workflow_id,)
            )
            we = cur.fetchone()
            if not we:
                raise HTTPException(status_code=404, detail="Workflow not found")
                
            definition = yaml.safe_load(we["definition_yaml"])
            temp_runner = DAGRunner(run_id=workflow_id, definition=definition, input_payload=we["input_payload"] or {})
            temp_runner.load_state_from_db()
            return temp_runner.to_detail()
    except HTTPException as he:
        raise he
    except Exception as e:
        db.logger.error(f"Error fetching workflow detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/api/v1/workflows/{workflow_id}/logs")
async def get_workflow_logs(workflow_id: str):
    """Get the event log for a workflow."""
    runner = active_runs.get(workflow_id)
    if runner:
        return {"events": runner.events}
        
    conn = db.get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, event_type, event_payload, created_at FROM workflow_events WHERE workflow_execution_id = %s ORDER BY created_at ASC;", (workflow_id,))
            evs = cur.fetchall()
            events = []
            for ev in evs:
                events.append({
                    "id": str(ev["id"]),
                    "event_type": ev["event_type"],
                    "payload": ev["event_payload"] or {},
                    "created_at": ev["created_at"].isoformat() if ev["created_at"] else None
                })
            return {"events": events}
    except Exception as e:
        db.logger.error(f"Error fetching workflow logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/api/v1/workflows/{workflow_id}/pause")
async def pause_workflow(workflow_id: str):
    """Pause a running workflow."""
    runner = await get_or_recover_runner(workflow_id)
    runner.pause()
    return {"message": "Workflow paused"}


@app.post("/api/v1/workflows/{workflow_id}/resume")
async def resume_workflow(workflow_id: str):
    """Resume a paused workflow."""
    runner = await get_or_recover_runner(workflow_id)
    runner.resume()
    return {"message": "Workflow resumed"}


@app.post("/api/v1/workflows/{workflow_id}/terminate")
async def terminate_workflow(workflow_id: str):
    """Terminate a workflow and cancel pending tasks."""
    runner = await get_or_recover_runner(workflow_id)
    runner.terminate()
    return {"message": "Workflow terminated"}


# ─── Task Endpoints ─────────────────────────────────────────────

@app.post("/api/v1/tasks/{task_id}/retry")
async def retry_task(task_id: str):
    """Retry a failed task by its execution ID."""
    # Search in memory first
    for runner in active_runs.values():
        for te in runner.task_executions.values():
            if te.id == task_id:
                success = await runner.retry_task(task_id)
                if success:
                    return {"message": f"Task {task_id} retried"}
                else:
                    raise HTTPException(status_code=400, detail="Task cannot be retried")
                    
    # If not in memory, query database to find the workflow_execution_id
    conn = db.get_db_connection()
    workflow_id = None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT workflow_execution_id FROM task_executions WHERE id = %s;", (task_id,))
            row = cur.fetchone()
            if row:
                workflow_id = str(row["workflow_execution_id"])
    except Exception as e:
        db.logger.error(f"Error searching for retry task: {e}")
    finally:
        conn.close()
        
    if workflow_id:
        runner = await recover_runner(workflow_id)
        if runner:
            success = await runner.retry_task(task_id)
            if success:
                return {"message": f"Task {task_id} retried"}
            else:
                raise HTTPException(status_code=400, detail="Task cannot be retried")
                
    raise HTTPException(status_code=404, detail="Task not found")


@app.post("/api/v1/tasks/callback")
async def task_callback(req: TaskCallbackRequest):
    """Callback endpoint for asynchronous tasks (e.g. inventory reservation worker)."""
    db.logger.info(f"Received callback for task {req.task_id} with status {req.status}")
    
    # Search in active_runs first
    found_runner = None
    task_name = None
    for runner in active_runs.values():
        for t_name, te in runner.task_executions.items():
            if te.id == req.task_id:
                found_runner = runner
                task_name = t_name
                break
        if found_runner:
            break

    if not found_runner:
        # Check DB and load/resume if it's there
        conn = db.get_db_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT workflow_execution_id FROM task_executions WHERE id = %s;", (req.task_id,))
                row = cur.fetchone()
                if row:
                    workflow_id = str(row["workflow_execution_id"])
                    found_runner = await recover_runner(workflow_id)
                    if found_runner:
                        for t_name, te in found_runner.task_executions.items():
                            if te.id == req.task_id:
                                task_name = t_name
                                break
        except Exception as e:
            db.logger.error(f"Error searching database for callback task {req.task_id}: {e}")
        finally:
            conn.close()

    if not found_runner or not task_name:
        raise HTTPException(status_code=404, detail="Task not found or corresponding workflow run not active")

    te = found_runner.task_executions[task_name]
    
    if req.status == "SUCCESS":
        te.state = TaskState.COMPLETED
        te.result_payload = req.result
        te.completed_at = _now()
        found_runner._sync_task_to_db(te)
        if te.attempts:
            te.attempts[-1]["state"] = "COMPLETED"
            te.attempts[-1]["completed_at"] = _now()
        found_runner._emit_event("TASK_COMPLETED", {"task_id": task_name, **(req.result or {})})
    else:
        te.state = TaskState.FAILED
        te.error = req.error.get("message") if isinstance(req.error, dict) else str(req.error) if req.error else "Callback reported failure"
        te.result_payload = {"error": te.error}
        te.completed_at = _now()
        found_runner._sync_task_to_db(te)
        if te.attempts:
            te.attempts[-1]["state"] = "FAILED"
            te.attempts[-1]["completed_at"] = _now()
            te.attempts[-1]["error"] = te.error
        found_runner._emit_event("TASK_FAILED", {"task_id": task_name, "error": te.error})
        await found_runner._handle_failure_triggers(task_name)

    found_runner._notify_update()
    return {"status": "accepted"}


@app.post("/api/v1/internal/task-result")
async def task_queue_result_callback(req: TaskQueueResultRequest):
    """Callback endpoint for BullMQ task-queue workers."""
    db.logger.info(f"Received task-queue callback for task {req.taskExecutionId} with state {req.state}")
    
    # Map task-queue result to orchestrator callback request
    status = "SUCCESS" if req.state == "COMPLETED" else "FAILED"
    
    callback_req = TaskCallbackRequest(
        task_id=req.taskExecutionId,
        status=status,
        result=req.resultPayload,
        error={"message": req.error} if req.error else None
    )
    
    return await task_callback(callback_req)


# ─── Human Task Endpoints ───────────────────────────────────────

@app.get("/api/v1/human-tasks")
async def list_human_tasks():
    """List all pending human tasks from the database."""
    conn = db.get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ht.id, ht.task_execution_id, te.workflow_execution_id,
                       wd.name as workflow_name, te.task_name as task_id,
                       ht.status, ht.created_at, we.input_payload
                FROM human_tasks ht
                JOIN task_executions te ON ht.task_execution_id = te.id
                JOIN workflow_executions we ON te.workflow_execution_id = we.id
                JOIN workflow_definitions wd ON we.workflow_definition_id = wd.id
                WHERE ht.status = 'PENDING';
                """
            )
            rows = cur.fetchall()
            tasks = []
            for r in rows:
                ip = r["input_payload"] or {}
                tasks.append({
                    "id": str(r["id"]),
                    "task_execution_id": str(r["task_execution_id"]),
                    "workflow_execution_id": str(r["workflow_execution_id"]),
                    "workflow_name": r["workflow_name"],
                    "task_id": r["task_id"],
                    "assignee": "ops-team",
                    "status": r["status"],
                    "order_value": ip.get("total_value"),
                    "order_id": ip.get("order_id"),
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                })
            return tasks
    except Exception as e:
        db.logger.error(f"Error listing human tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/api/v1/human-tasks/{human_task_id}/approve")
async def approve_human_task(human_task_id: str, req: HumanTaskApproveRequest):
    """Approve a human-in-the-loop task."""
    for runner in active_runs.values():
        if human_task_id in runner.human_tasks:
            success = runner.resolve_human_task(human_task_id, approved=True, decided_by=req.decided_by)
            if success:
                return {"message": "Task approved"}
            else:
                raise HTTPException(status_code=400, detail="Task cannot be approved")
                
    conn = db.get_db_connection()
    workflow_id = None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT te.workflow_execution_id
                FROM human_tasks ht
                JOIN task_executions te ON ht.task_execution_id = te.id
                WHERE ht.id = %s;
                """,
                (human_task_id,)
            )
            row = cur.fetchone()
            if row:
                workflow_id = str(row["workflow_execution_id"])
    except Exception as e:
        db.logger.error(f"Error finding human task for approval: {e}")
    finally:
        conn.close()
        
    if workflow_id:
        runner = await recover_runner(workflow_id)
        if runner and human_task_id in runner.human_tasks:
            success = runner.resolve_human_task(human_task_id, approved=True, decided_by=req.decided_by)
            if success:
                return {"message": "Task approved"}
            else:
                raise HTTPException(status_code=400, detail="Task cannot be approved")
                
    raise HTTPException(status_code=404, detail="Human task not found")


@app.post("/api/v1/human-tasks/{human_task_id}/reject")
async def reject_human_task(human_task_id: str, req: HumanTaskRejectRequest):
    """Reject a human-in-the-loop task."""
    for runner in active_runs.values():
        if human_task_id in runner.human_tasks:
            success = runner.resolve_human_task(human_task_id, approved=False, reason=req.reason)
            if success:
                return {"message": "Task rejected"}
            else:
                raise HTTPException(status_code=400, detail="Task cannot be rejected")
                
    conn = db.get_db_connection()
    workflow_id = None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT te.workflow_execution_id
                FROM human_tasks ht
                JOIN task_executions te ON ht.task_execution_id = te.id
                WHERE ht.id = %s;
                """,
                (human_task_id,)
            )
            row = cur.fetchone()
            if row:
                workflow_id = str(row["workflow_execution_id"])
    except Exception as e:
        db.logger.error(f"Error finding human task for rejection: {e}")
    finally:
        conn.close()
        
    if workflow_id:
        runner = await recover_runner(workflow_id)
        if runner and human_task_id in runner.human_tasks:
            success = runner.resolve_human_task(human_task_id, approved=False, reason=req.reason)
            if success:
                return {"message": "Task rejected"}
            else:
                raise HTTPException(status_code=400, detail="Task cannot be rejected")
                
    raise HTTPException(status_code=404, detail="Human task not found")


# ─── Template Endpoints (bonus) ─────────────────────────────────

@app.get("/api/v1/templates")
async def list_templates():
    """List all available YAML workflow templates."""
    templates = [f.stem for f in WORKFLOWS_DIR.glob("*.yaml")]
    return {"templates": templates}