from db import get_db_connection
from datetime import datetime
import uuid


class StateManager:

    @staticmethod
    def create_workflow_run(run_id: str, workflow_name: str):
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO workflow_executions (id, workflow_state, started_at)
                    VALUES (%s, 'RUNNING', %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (run_id, datetime.utcnow())
                )
            conn.commit()
        except Exception as e:
            print("WORKFLOW INSERT ERROR:", str(e))

    @staticmethod
    def create_task(run_id: str, task_name: str):
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO task_executions (id, workflow_execution_id, task_id, task_name, task_state)
                    VALUES (%s, %s, %s, %s, 'PENDING')
                    ON CONFLICT DO NOTHING
                    """,
                    (str(uuid.uuid4()), run_id, task_name, task_name)
                )
            conn.commit()
        except Exception as e:
            print("TASK INSERT ERROR:", str(e))

    @staticmethod
    def update_task_status(run_id: str, task_name: str, status: str, error_message=None):
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE task_executions
                    SET task_state = %s, error_message = %s
                    WHERE workflow_execution_id = %s AND task_name = %s
                    """,
                    (status, error_message, run_id, task_name)
                )
            conn.commit()
        except Exception as e:
            print("TASK STATUS UPDATE ERROR:", str(e))

    @staticmethod
    def update_workflow_status(run_id: str, status: str, error_message=None):
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE workflow_executions
                    SET workflow_state = %s, error_message = %s
                    WHERE id = %s
                    """,
                    (status, error_message, run_id)
                )
            conn.commit()
        except Exception as e:
            print("WORKFLOW STATUS UPDATE ERROR:", str(e))

    @staticmethod
    def log_event(run_id: str, event_type: str, message: str):
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO workflow_events (workflow_execution_id, event_type, message)
                    VALUES (%s, %s, %s)
                    """,
                    (run_id, event_type, message)
                )
            conn.commit()
        except Exception as e:
            print("EVENT INSERT ERROR:", str(e))