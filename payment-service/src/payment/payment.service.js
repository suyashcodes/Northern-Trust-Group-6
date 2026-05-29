import crypto from 'crypto';
import { razorpayInstance, razorpayConfig } from '../config/razorpay.config.js';
import { query } from '../config/database.js';

const ORCHESTRATOR_CALLBACK_URL = process.env.ORCHESTRATOR_URL
  ? `${process.env.ORCHESTRATOR_URL}/api/v1/tasks/callback`
  : 'http://localhost:4000/api/v1/tasks/callback';

/**
 * Fire-and-forget callback to orchestrator so it can advance the workflow.
 */
async function sendOrchestratorCallback(task_execution_id, status, result, error) {
  if (!task_execution_id) return;
  try {
    const body = { task_id: task_execution_id, status, result: result || null, error: error || null };
    const resp = await fetch(ORCHESTRATOR_CALLBACK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!resp.ok) {
      console.warn(`[Payment] Orchestrator callback returned ${resp.status}`);
    } else {
      console.log(`[Payment] Orchestrator callback sent → task=${task_execution_id} status=${status}`);
    }
  } catch (err) {
    console.error('[Payment] Failed to send orchestrator callback:', err.message);
  }
}

export class PaymentService {
  /**
   * Creates a new Razorpay Order, saves it to the database, and
   * calls the orchestrator callback so the workflow can advance.
   * @param {number} amount - Amount in INR
   * @param {string} workflow_execution_id - The workflow run ID
   * @param {string} task_execution_id - The task execution ID for callback
   */
  async createOrder(amount, workflow_execution_id = null, task_execution_id = null) {
    if (!amount || isNaN(amount) || amount <= 0) {
      throw new Error('Invalid amount provided. Amount must be a positive number.');
    }

    // Razorpay expects amount in paise (1 INR = 100 paise)
    const amountInPaise = Math.round(amount * 100);

    const options = {
      amount: amountInPaise,
      currency: 'INR',
      receipt: `receipt_order_${Date.now()}`,
    };

    try {
      const order = await razorpayInstance.orders.create(options);
      const orderAmountINR = order.amount / 100;

      // Persist initial payment record
      try {
        await query(
          `INSERT INTO payments (workflow_execution_id, order_id, amount, payment_status, payment_method)
           VALUES ($1, $2, $3, $4, $5)`,
          [workflow_execution_id, order.id, orderAmountINR, 'PENDING', 'razorpay']
        );
        console.log(`[Payment] Inserted pending record for order_id: ${order.id}`);
      } catch (dbError) {
        console.error('[Payment] Failed to save order record to database:', dbError.message);
      }

      const result = {
        id: order.id,
        amount: orderAmountINR,
        currency: order.currency,
        receipt: order.receipt,
        status: order.status,
        keyId: razorpayConfig.keyId,
      };

      // ── Notify orchestrator: payment task SUCCEEDED ─────────────────────
      // Creating a Razorpay order = payment initiated successfully.
      // The actual end-user payment collection happens via Razorpay checkout on the frontend.
      // (Do NOT send SUCCESS callback automatically; wait for frontend Checkout/Verification to notify).

      return result;
    } catch (error) {
      console.error('[Payment] Error creating Razorpay order:', error);
      const errMsg = error.message ||
                     error.description ||
                     (error.error && error.error.description) ||
                     JSON.stringify(error);

      // Notify orchestrator of failure so workflow can handle it
      await sendOrchestratorCallback(task_execution_id, 'FAILED', null, { message: errMsg });

      throw new Error(`Razorpay Order Creation Failed: ${errMsg}`);
    }
  }

  /**
   * Verifies the Razorpay payment signature and updates DB status.
   */
  async verifyPayment(orderId, paymentId, signature) {
    if (!orderId || !paymentId || !signature) {
      throw new Error('Missing signature verification parameters (orderId, paymentId, signature).');
    }

    const secret = razorpayConfig.keySecret;
    if (!secret || secret === 'placeholder_secret') {
      throw new Error('Razorpay Key Secret is not configured. Signature verification failed.');
    }

    const body = orderId + '|' + paymentId;
    const expectedSignature = crypto
      .createHmac('sha256', secret)
      .update(body.toString())
      .digest('hex');

    const isValid = expectedSignature === signature;

    try {
      if (isValid) {
        await query(
          `UPDATE payments
           SET payment_transaction_id = $1, payment_status = $2, payment_method = $3
           WHERE order_id = $4`,
          [paymentId, 'COMPLETED', 'razorpay', orderId]
        );
        console.log(`[Payment] Updated payment COMPLETED for order_id: ${orderId}`);
      } else {
        await query(
          `UPDATE payments
           SET payment_status = $1, failure_reason = $2
           WHERE order_id = $3`,
          ['FAILED', 'Signature verification failed', orderId]
        );
        console.log(`[Payment] Updated payment FAILED for order_id: ${orderId}`);
      }
    } catch (dbError) {
      console.error('[Payment] Failed to update payment status in database:', dbError.message);
    }

    return isValid;
  }
}

export const paymentService = new PaymentService();
