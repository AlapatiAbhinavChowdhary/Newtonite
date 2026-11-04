const express = require("express");
const { v4: uuidv4 } = require("uuid");
const {
  getAllOrders,
  getOrder,
  getProduct,
  reserveStock,
  addOrder,
  setIdempotencyKey,
  getIdempotencyKey,
  enqueue,
  getQueueLength,
} = require("../store/memoryStore");

const router = express.Router();

// ---------------------------------------------------------------------------
// GET /orders – list all orders
// ---------------------------------------------------------------------------
router.get("/", async (_req, res) => {
  const allOrders = getAllOrders();
  res.json({ success: true, count: allOrders.length, orders: allOrders });
});

// ---------------------------------------------------------------------------
// GET /orders/queue/status – check background queue depth
// (Registered BEFORE the /:orderId param route so "queue" isn't captured.)
// ---------------------------------------------------------------------------
router.get("/queue/status", async (_req, res) => {
  res.json({ success: true, queueLength: getQueueLength() });
});

// ---------------------------------------------------------------------------
// GET /orders/:orderId – get a single order by id
// ---------------------------------------------------------------------------
router.get("/:orderId", async (req, res) => {
  const { orderId } = req.params;
  const order = getOrder(orderId);

  if (!order) {
    return res.status(404).json({
      success: false,
      error: `Order "${orderId}" not found`,
    });
  }

  res.json({
    success: true,
    order: {
      orderId: order.orderId,
      userId: order.userId,
      productId: order.productId,
      quantity: order.quantity,
      status: order.status,
      createdAt: order.createdAt,
      updatedAt: order.updatedAt || null,
    },
  });
});

// ---------------------------------------------------------------------------
// POST /orders – place a new flash-sale order
// ---------------------------------------------------------------------------
router.post("/", async (req, res) => {
  const { userId, productId, quantity, idempotencyKey } = req.body;

  // ── 1. Input validation ──────────────────────────────────────────────────
  if (!userId || !productId || quantity === undefined || !idempotencyKey) {
    return res.status(400).json({
      success: false,
      error:
        "Missing required fields: userId, productId, quantity, idempotencyKey",
    });
  }

  // ── 2. Quantity must be a positive integer ────────────────────────────────
  if (!Number.isInteger(quantity) || quantity <= 0) {
    return res.status(400).json({
      success: false,
      error: "quantity must be a positive integer",
    });
  }

  // ── 3. Idempotency check — return the existing order for duplicate keys ──
  const existingOrderId = getIdempotencyKey(idempotencyKey);
  if (existingOrderId) {
    const existingOrder = getOrder(existingOrderId);
    return res.status(200).json({
      success: true,
      message: "Duplicate request — returning existing order",
      order: existingOrder,
    });
  }

  // ── 4. Check whether the product exists ───────────────────────────────────
  const product = getProduct(productId);
  if (!product) {
    return res.status(404).json({
      success: false,
      error: `Product "${productId}" not found`,
    });
  }

  // ── 5 & 6. Reserve stock (check + deduct in one synchronous step) ────────
  //
  // reserveStock performs the availability check AND the deduction in a
  // single synchronous call.  Because Node.js is single-threaded and does
  // not preempt synchronous code, no other request can read the stock value
  // between our check and our write — this eliminates the classic
  // check-then-act race condition without needing an external lock.
  //
  // See the detailed comment in memoryStore.js → reserveStock().
  const reservation = reserveStock(productId, quantity);

  if (!reservation.ok) {
    if (reservation.error === "INSUFFICIENT_STOCK") {
      return res.status(409).json({
        success: false,
        error: `Insufficient stock for "${productId}". Requested: ${quantity}, available: ${reservation.available}`,
      });
    }
    // Shouldn't reach here (product existence checked above) but guard anyway
    return res.status(404).json({ success: false, error: reservation.error });
  }

  // ── 7. Generate a unique orderId ──────────────────────────────────────────
  const orderId = uuidv4();

  // ── 8. Create the order with status PENDING ───────────────────────────────
  const order = {
    orderId,
    userId,
    productId,
    quantity,
    status: "PENDING",
    createdAt: new Date().toISOString(),
  };

  addOrder(order);

  // ── 9. Record the idempotency key → orderId mapping ───────────────────────
  setIdempotencyKey(idempotencyKey, orderId);

  // ── 10. Push onto the background processing queue ─────────────────────────
  // Note: `simulateFailure` is strictly a test/demo hook to exercise error recovery and the FAILED state transition.
  enqueue({
    orderId,
    productId,
    quantity,
    userId,
    simulateFailure: Boolean(req.body.simulateFailure),
  });

  // ── 11. Respond with 201 Created ──────────────────────────────────────────
  if (process.env.NODE_ENV !== "test") {
    console.log(
      `[FlashDrop] Order ${orderId} created – ${quantity}× ${productId} for user ${userId} (stock remaining: ${reservation.remaining})`
    );
  }

  return res.status(201).json({
    success: true,
    orderId,
    status: order.status,
  });
});

module.exports = router;
