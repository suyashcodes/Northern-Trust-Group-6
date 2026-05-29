import os
import json
import uuid
import logging
import time
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import redis
import psycopg2
import httpx
from dotenv import load_dotenv

# Load environment variables FIRST before any connections
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("InventoryWorker")

# Load environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_QUEUE = os.getenv("REDIS_QUEUE", "queue:inventory:reserve")
ORCHESTRATOR_CALLBACK_URL = os.getenv("ORCHESTRATOR_CALLBACK_URL", "http://localhost:8000/api/v1/tasks/callback")

if not DATABASE_URL:
    logger.warning("WARNING: DATABASE_URL is not configured. Database connections will fail.")

def get_db_connection():
    """Establishes connection to PostgreSQL database."""
    return psycopg2.connect(DATABASE_URL)

def send_callback(payload: dict):
    """Sends the result back to the core Orchestrator via HTTP."""
    try:
        with httpx.Client() as client:
            response = client.post(ORCHESTRATOR_CALLBACK_URL, json=payload, timeout=5.0)
            if response.status_code == 200:
                logger.info(f"✅ Callback accepted by orchestrator for task: {payload['task_id']}")
            else:
                logger.error(f"⚠️ Orchestrator rejected callback: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"❌ Failed to reach orchestrator at {ORCHESTRATOR_CALLBACK_URL}: {e}")

def process_inventory_task(task_data: dict):
    """Handles PostgreSQL database transactions for inventory reservation."""
    task_id = task_data.get("task_id")
    payload = task_data.get("payload", {})
    order_id = payload.get("order_id")
    workflow_execution_id = payload.get("workflow_execution_id")
    items = payload.get("items", [])
    
    logger.info(f"📦 Processing task {task_id} for Order: {order_id}")
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        stock_available = True
        insufficient_items = []
        processed_skus = []
        
        # 1. Verify Stock Availability
        for item in items:
            sku = item["sku"]
            qty = item.get("qty") or item.get("quantity", 1)
            
            cursor.execute(
                "SELECT available_quantity FROM inventory_items WHERE sku = %s;",
                (sku,)
            )
            row = cursor.fetchone()
            
            available_qty = row[0] if row else 0
            if not row or available_qty < qty:
                stock_available = False
                insufficient_items.append({
                    "sku": sku,
                    "requested": qty,
                    "available": available_qty
                })

        # 2. Process Success Path
        if stock_available:
            for item in items:
                sku = item["sku"]
                qty = item.get("qty") or item.get("quantity", 1)
                
                # Update item stock inside transaction
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
                
                # Insert reservation record
                cursor.execute(
                    """
                    INSERT INTO inventory_reservations (workflow_execution_id, order_id, sku, quantity, reservation_status, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    (workflow_execution_id, order_id, sku, qty, 'RESERVED', datetime.utcnow())
                )
                processed_skus.append(sku)
                
            # Commit transaction atomically
            conn.commit()
            
            sku_list_str = ", ".join(processed_skus)
            callback_payload = {
                "task_id": task_id,
                "status": "SUCCESS",
                "result": {
                    "tracking_id": f"WH-{uuid.uuid4().hex[:4].upper()}",
                    "reserved_at": datetime.utcnow().isoformat() + "Z"
                },
                "logs": f"Inventory locked for SKU: {sku_list_str}",
                "error": None
            }
            
        # 3. Process Failure Path (Out of Stock)
        else:
            for item in items:
                item_qty = item.get("qty") or item.get("quantity", 1)
                cursor.execute(
                    """
                    INSERT INTO inventory_reservations (workflow_execution_id, order_id, sku, quantity, reservation_status, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    (workflow_execution_id, order_id, item["sku"], item_qty, 'FAILED', datetime.utcnow())
                )
                
            conn.commit()
            
            callback_payload = {
                "task_id": task_id,
                "status": "FAILED",
                "result": None,
                "logs": f"Inventory check failed. Insufficient items.",
                "error": {
                    "code": "OUT_OF_STOCK",
                    "message": "Requested quantities exceed current stock.",
                    "details": insufficient_items
                }
            }

        # Send response back to orchestrator
        send_callback(callback_payload)
        cursor.close()
        
    except Exception as e:
        logger.error(f"❌ Error executing PostgreSQL transaction: {e}")
        if conn:
            conn.rollback()
            
        # Report crash as a failure to orchestrator
        callback_payload = {
            "task_id": task_id,
            "status": "FAILED",
            "result": None,
            "logs": f"Database transaction failed: {str(e)}",
            "error": {
                "code": "DATABASE_ERROR",
                "message": str(e)
            }
        }
        send_callback(callback_payload)
    finally:
        if conn:
            conn.close()

def start_worker():
    """Continuously listens to the Redis queue for new tasks."""
    logger.info(f"🚀 Starting Inventory Redis Worker (PostgreSQL Version). Listening on queue: {REDIS_QUEUE}")
    
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redis_client.ping()
        logger.info("📡 Connected to Redis successfully.")
    except Exception as e:
        logger.error(f"❌ Could not connect to Redis: {e}")
        return

    while True:
        try:
            # Block until a task is pushed to the Redis list
            result = redis_client.blpop(REDIS_QUEUE, timeout=0)
            if result:
                queue_name, message_data = result
                task_json = json.loads(message_data)
                process_inventory_task(task_json)
        except json.JSONDecodeError:
            logger.error(f"⚠️ Received invalid JSON from Redis: {message_data}")
        except Exception as e:
            logger.error(f"⚠️ Worker loop encountered an error: {e}")
            time.sleep(2)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "healthy"}')

    def log_message(self, format, *args):
        # Suppress logging health check requests to keep stdout clean
        return

def run_health_server():
    port = int(os.getenv("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    logger.info(f"🩺 Health check server listening on port {port}")
    server.serve_forever()

if __name__ == "__main__":
    # Start Redis worker in a background daemon thread
    worker_thread = threading.Thread(target=start_worker, daemon=True)
    worker_thread.start()
    
    # Run HTTP health check server in main thread to satisfy Render's web service requirement
    run_health_server()