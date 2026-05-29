import { paymentService } from './payment.service.js';

export class PaymentController {
  /**
   * Handle order creation request.
   * POST /api/v1/payment/order
   */
  async createOrder(req, res) {
    try {
      const { amount, workflow_execution_id, task_execution_id } = req.body;
      if (!amount) {
        return res.status(400).json({
          success: false,
          error: 'Amount is required in request body.',
        });
      }

      const orderData = await paymentService.createOrder(amount, workflow_execution_id, task_execution_id);
      return res.status(201).json({
        success: true,
        data: orderData,
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: error.message,
      });
    }
  }

  /**
   * Handle payment verification request.
   * POST /api/v1/payment/verify
   */
  async verifyPayment(req, res) {
    try {
      const { razorpay_order_id, razorpay_payment_id, razorpay_signature } = req.body;

      if (!razorpay_order_id || !razorpay_payment_id || !razorpay_signature) {
        return res.status(400).json({
          success: false,
          error: 'razorpay_order_id, razorpay_payment_id, and razorpay_signature are required.',
        });
      }

      const isValid = await paymentService.verifyPayment(
        razorpay_order_id,
        razorpay_payment_id,
        razorpay_signature
      );

      if (isValid) {
        return res.status(200).json({
          success: true,
          message: 'Payment verified successfully.',
          data: {
            paymentId: razorpay_payment_id,
            orderId: razorpay_order_id,
          },
        });
      } else {
        return res.status(400).json({
          success: false,
          error: 'Signature verification failed. Invalid transaction.',
        });
      }
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: error.message,
      });
    }
  }
}

export const paymentController = new PaymentController();
