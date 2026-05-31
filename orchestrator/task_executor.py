import httpx
import logging
import asyncio
import os
import json
import redis
from typing import Optional
from dotenv import load_dotenv

from datetime import datetime, timezone
def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

from pathlib import Path
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_QUEUE = os.getenv("REDIS_QUEUE", "queue:inventory:reserve")
TASK_QUEUE_URL = os.getenv("TASK_QUEUE_URL")


async def execute_task(service_name: str, task_name: str, payload: dict) -> dict:
    """
    Unified task executor.
    Dispatches tasks to corresponding real microservices via HTTP or Redis queue.
    """
    workflow_execution_id = payload.get("workflow_execution_id")
    task_execution_id = payload.get("task_execution_id")
    order_id = payload.get("order_id") or payload.get("orderId") or "ORD-UNKNOWN"
    items = payload.get("items", [])

    logger.info(f"Executing task '{task_name}' for service '{service_name}' (Run: {workflow_execution_id}, Task: {task_execution_id})")

    # If task-queue is configured, dispatch the task via BullMQ queue
    if TASK_QUEUE_URL and service_name in ["payment-service", "shipping-service", "notification-service"]:
        url = f"{TASK_QUEUE_URL}/api/v1/tasks/dispatch"
        endpoint = ""
        method = "POST"
        payload_to_send = {}

        if service_name == "payment-service":
            endpoint = "/api/v1/payment/order"
            payload_to_send = {
                "amount": payload.get("amount") or 10000.0,
                "workflow_execution_id": workflow_execution_id,
                "task_execution_id": task_execution_id
            }
        elif service_name == "shipping-service":
            endpoint = "/shipping/create-order"
            payload_to_send = {
                "workflow_execution_id": workflow_execution_id,
                "order_id": order_id,
                "items": items
            }
        elif service_name == "notification-service":
            endpoint = "/notify/email"
            to_email = payload.get("customer_email") or payload.get("email") or "customer@example.com"
            customer_name = payload.get("customer_name") or payload.get("customerName") or "Valued Customer"
            amount = payload.get("amount") or payload.get("total_value") or 10000.0
            payload_to_send = {
                "to": to_email,
                "template": "order_confirmed",
                "data": {
                    "orderId": order_id,
                    "customerName": customer_name,
                    "total": f"INR {amount}"
                },
                "workflow_execution_id": workflow_execution_id
            }

        async with httpx.AsyncClient() as client:
            try:
                body = {
                    "taskExecutionId": task_execution_id,
                    "workflowExecutionId": workflow_execution_id,
                    "taskId": task_name,
                    "service": service_name,
                    "endpoint": endpoint,
                    "method": method,
                    "payload": payload_to_send
                }
                logger.info(f"Dispatching task to task-queue: POST {url} with body={body}")
                resp = await client.post(url, json=body, timeout=10.0)
                if resp.status_code in [200, 202]:
                    return {
                        "status": "IN_PROGRESS",
                        "message": "Task queued in BullMQ via task-queue"
                    }
                else:
                    logger.error(f"task-queue rejected task dispatch: {resp.status_code} - {resp.text}")
            except Exception as e:
                logger.error(f"Failed to dispatch task to task-queue: {e}")

    # 1. Mock order-service tasks (validate-order, cancel-order)
    if service_name == "order-service":
        await asyncio.sleep(0.5)
        if task_name == "validate-order":
            return {
                "status": "SUCCESS",
                "result": {"valid": True, "message": "Order validated successfully"}
            }
        elif task_name == "cancel-order":
            return {
                "status": "SUCCESS",
                "result": {"cancelled": True}
            }
        return {
            "status": "SUCCESS",
            "result": {"completed": True}
        }

    # 2. Real Payment Service (port 3001)
    elif service_name == "payment-service":
        url = "http://localhost:3001/api/v1/payment/order"
        amount = payload.get("amount") or 10000.0
        async with httpx.AsyncClient() as client:
            try:
                body = {
                    "amount": amount,
                    "workflow_execution_id": workflow_execution_id,
                    "task_execution_id": task_execution_id
                }
                logger.info(f"POST {url} with body={body}")
                resp = await client.post(url, json=body, timeout=10.0)
                if resp.status_code == 201:
                    data = resp.json()
                    return {
                        "status": "IN_PROGRESS",
                        "result": data.get("data", data)
                    }
                else:
                    return {
                        "status": "FAILED",
                        "error": f"Payment Service returned status {resp.status_code}: {resp.text}"
                    }
            except Exception as e:
                logger.error(f"Failed to call Payment Service: {e}")
                return {
                    "status": "FAILED",
                    "error": f"Connection error: {str(e)}"
                }

    # 3. Real Shipping Service (port 3002)
    elif service_name == "shipping-service":
        url = "http://localhost:3002/shipping/create-order"
        async with httpx.AsyncClient() as client:
            try:
                body = {
                    "workflow_execution_id": workflow_execution_id,
                    "order_id": order_id,
                    "items": items
                }
                logger.info(f"POST {url} with body={body}")
                resp = await client.post(url, json=body, timeout=10.0)
                if resp.status_code == 200:
                    data = resp.json()
                    return {
                        "status": "SUCCESS",
                        "result": data.get("data", data)
                    }
                else:
                    return {
                        "status": "FAILED",
                        "error": f"Shipping Service returned status {resp.status_code}: {resp.text}"
                    }
            except Exception as e:
                logger.error(f"Failed to call Shipping Service: {e}")
                return {
                    "status": "FAILED",
                    "error": f"Connection error: {str(e)}"
                }

    # 4. Real Notification Service (port 3004)
    elif service_name == "notification-service":
        url = "http://localhost:3004/notify/email"
        to_email = payload.get("customer_email") or payload.get("email") or "customer@example.com"
        customer_name = payload.get("customer_name") or payload.get("customerName") or "Valued Customer"
        amount = payload.get("amount") or payload.get("total_value") or 10000.0
        
        async with httpx.AsyncClient() as client:
            try:
                body = {
                    "to": to_email,
                    "template": "order_confirmed",
                    "data": {
                        "orderId": order_id,
                        "customerName": customer_name,
                        "total": f"INR {amount}"
                    },
                    "workflow_execution_id": workflow_execution_id
                }
                logger.info(f"POST {url} with body={body}")
                resp = await client.post(url, json=body, timeout=10.0)
                if resp.status_code == 200:
                    data = resp.json()
                    return {
                        "status": "SUCCESS",
                        "result": data
                    }
                else:
                    return {
                        "status": "FAILED",
                        "error": f"Notification Service returned status {resp.status_code}: {resp.text}"
                    }
            except Exception as e:
                logger.error(f"Failed to call Notification Service: {e}")
                return {
                    "status": "FAILED",
                    "error": f"Connection error: {str(e)}"
                }

    # 5. Real Inventory Service (via Redis Queue and HTTP Callback)
    elif service_name == "inventory-service":
        use_fallback = False
        try:
            import socket
            # Fast TCP port availability check (timeout 0.1s)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.1)
                s.connect((REDIS_HOST, REDIS_PORT))
            
            logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT} to push task {task_name}")
            redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, socket_connect_timeout=1.0, socket_timeout=1.0)
            redis_client.ping()
            
            task_payload = {
                "task_id": task_execution_id,
                "payload": {
                    "workflow_execution_id": workflow_execution_id,
                    "order_id": order_id,
                    "items": items
                }
            }
            logger.info(f"LPUSH {REDIS_QUEUE} with payload={task_payload}")
            redis_client.lpush(REDIS_QUEUE, json.dumps(task_payload))
            
            return {
                "status": "IN_PROGRESS",
                "message": "Task dispatched to Redis queue, awaiting worker callback"
            }
        except Exception as redis_e:
            logger.warning(f"Failed to connect to Redis: {redis_e}. Falling back to direct database execution.")
            use_fallback = True
            
        if use_fallback:
            import db
            conn = None
            try:
                conn = db.get_db_connection()
                with conn.cursor() as cursor:
                    raw_items = items if items else [{"sku": "SKU-101", "quantity": 1}]
                    # Normalise: items might be strings (e.g. "Mechanical Keyboard") or dicts
                    def _normalise(item):
                        if isinstance(item, dict):
                            return item
                        # String item — map to default SKU
                        return {"sku": "SKU-101", "quantity": 1}
                    test_items = [_normalise(i) for i in raw_items]

                    # ── check-inventory: READ ONLY — just verify stock ──────────
                    if task_name == "check-inventory":
                        stock_ok = True
                        insufficient = []
                        for item in test_items:
                            sku = item.get("sku")
                            qty = item.get("qty") or item.get("quantity", 1)
                            cursor.execute(
                                "SELECT available_quantity FROM inventory_items WHERE sku = %s;",
                                (sku,)
                            )
                            row = cursor.fetchone()
                            avail = row[0] if row else 0
                            if not row or avail < qty:
                                stock_ok = False
                                insufficient.append({"sku": sku, "requested": qty, "available": avail})

                        if stock_ok:
                            return {
                                "status": "SUCCESS",
                                "result": {"available": True, "items_checked": len(test_items)}
                            }
                        else:
                            return {
                                "status": "FAILED",
                                "error": f"Insufficient stock: {insufficient}"
                            }

                    # ── reserve-stock: WRITE — reserve and log to DB ───────────
                    else:
                        stock_available = True
                        insufficient_items = []

                        for item in test_items:
                            sku = item.get("sku")
                            qty = item.get("qty") or item.get("quantity", 1)
                            cursor.execute(
                                "SELECT available_quantity FROM inventory_items WHERE sku = %s;",
                                (sku,)
                            )
                            row = cursor.fetchone()
                            avail = row[0] if row else 0
                            if not row or avail < qty:
                                stock_available = False
                                insufficient_items.append({"sku": sku, "requested": qty, "available": avail})

                        if stock_available:
                            for item in test_items:
                                sku = item.get("sku")
                                qty = item.get("qty") or item.get("quantity", 1)
                                cursor.execute(
                                    """
                                    UPDATE inventory_items
                                    SET available_quantity = available_quantity - %s,
                                        reserved_quantity = reserved_quantity + %s,
                                        updated_at = NOW()
                                    WHERE sku = %s;
                                    """,
                                    (qty, qty, sku)
                                )
                                cursor.execute(
                                    """
                                    INSERT INTO inventory_reservations
                                        (workflow_execution_id, order_id, sku, quantity, reservation_status, created_at)
                                    VALUES (%s, %s, %s, %s, 'RESERVED', NOW());
                                    """,
                                    (workflow_execution_id, order_id, sku, qty)
                                )
                            conn.commit()
                            return {
                                "status": "SUCCESS",
                                "result": {
                                    "tracking_id": "WH-DIRECT-FALLBACK",
                                    "reserved_at": _now(),
                                    "items_reserved": len(test_items)
                                }
                            }
                        else:
                            for item in test_items:
                                sku = item.get("sku")
                                qty = item.get("qty") or item.get("quantity", 1)
                                cursor.execute(
                                    """
                                    INSERT INTO inventory_reservations
                                        (workflow_execution_id, order_id, sku, quantity, reservation_status, created_at)
                                    VALUES (%s, %s, %s, %s, 'FAILED', NOW());
                                    """,
                                    (workflow_execution_id, order_id, sku, qty)
                                )
                            conn.commit()
                            return {
                                "status": "FAILED",
                                "error": f"Inventory reservation failed. Insufficient items: {insufficient_items}"
                            }
            except Exception as db_err:
                if conn:
                    conn.rollback()
                logger.error(f"Fallback database transaction failed: {db_err}")
                return {
                    "status": "FAILED",
                    "error": f"Database fallback error: {str(db_err)}"
                }
            finally:
                if conn:
                    conn.close()

    # Fallback / Unrecognized service
    else:
        logger.warning(f"Unrecognized service '{service_name}', falling back to SUCCESS")
        return {
            "status": "SUCCESS",
            "result": {"completed": True, "info": f"Mock completion for {service_name}"}
        }
