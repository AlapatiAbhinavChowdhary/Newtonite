// ---------------------------------------------------------------------------
// Background order-processing worker for FlashDrop
//
// How it works:
// ─────────────
// 1.  A polling loop runs on a fixed interval (default 500 ms).  Every tick
//     it calls `dequeue()` which atomically pops the first job from the
//     in-memory queue (Array.shift).
//
// 2.  If the queue is empty, the tick does nothing and waits for the next
//     interval — there is no busy-spin.
//
// 3.  When a job is dequeued, we first check if the order still exists and
//     is still in PENDING status.  If the order was already CONFIRMED or
//     FAILED (e.g. a duplicate job somehow got enqueued), we skip it.
//     This is the duplicate-processing guard.
//
// 4.  Processing is simulated with a short async delay (configurable) to
//     stand in for real work such as payment capture, warehouse allocation,
//     or sending a confirmation email.
//
// 5.  On success the order status is updated to CONFIRMED.
//     On failure (any exception during processing) it is set to FAILED.
//
// 6.  Because `dequeue()` removes the job from the array in a single
//     synchronous call, and Node.js won't interleave that call with another
//     tick, no two poll cycles can grab the same job.
//
// In production this would be replaced by a proper consumer (e.g. a
// RabbitMQ consumer or Kafka consumer group) with acknowledgement semantics,
// but the same pattern applies: dequeue → guard → process → update status.
// ---------------------------------------------------------------------------

const { dequeue, getOrder, updateOrderStatus } = require("../store/memoryStore");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const POLL_INTERVAL_MS = 500;   // how often the worker checks the queue
const PROCESSING_DELAY_MS = 800; // simulated processing time per order

// ---------------------------------------------------------------------------
// Simulated order processing
// ---------------------------------------------------------------------------

/**
 * Simulate the actual work of processing an order (payment, fulfilment, etc.).
 * In a real system this would call external services and could fail.
 *
 * @param {object} job - { orderId, productId, quantity, userId }
 * @returns {Promise<void>}
 */
async function processOrder(job) {
  // Simulate async work (payment gateway, inventory system, email, etc.)
  await new Promise((resolve) => setTimeout(resolve, PROCESSING_DELAY_MS));

  // If simulateFailure is flagged (e.g. for testing error paths), throw an error
  if (job && job.simulateFailure) {
    throw new Error("Simulated payment or fulfillment processing failure");
  }
}

// ---------------------------------------------------------------------------
// Poll loop
// ---------------------------------------------------------------------------

let pollTimer = null;
let isProcessing = false; // Concurrency guard: prevents overlapping ticks when processing delay > poll interval

/**
 * One tick of the worker loop.
 * Dequeues a single job, guards against duplicates, processes it, and
 * updates the order status. Ensures single-job concurrency per worker instance.
 */
async function tick() {
  if (isProcessing) return; // Prevent overlapping ticks while an async job is already in flight

  const job = dequeue();

  // Nothing in the queue — wait for the next tick.
  if (!job) return;

  isProcessing = true;

  try {
    const { orderId } = job;

    // ── Duplicate / stale-job guard ──────────────────────────────────────────
    // Only process orders that are still PENDING.  If the order doesn't exist
    // or has already moved past PENDING (e.g. a retry of the same job), skip.
    const order = getOrder(orderId);

    if (!order) {
      console.warn(`[Worker] Order ${orderId} not found — skipping.`);
      return;
    }

    if (order.status !== "PENDING") {
      console.warn(
        `[Worker] Order ${orderId} is already "${order.status}" — skipping duplicate job.`
      );
      return;
    }

    // ── Process the order ────────────────────────────────────────────────────
    try {
      if (process.env.NODE_ENV !== "test") {
        console.log(`[Worker] Processing order ${orderId} …`);
      }
      await processOrder(job);

      updateOrderStatus(orderId, "CONFIRMED");
      if (process.env.NODE_ENV !== "test") {
        console.log(`[Worker] ✅ Order ${orderId} → CONFIRMED`);
      }
    } catch (err) {
      updateOrderStatus(orderId, "FAILED");
      if (process.env.NODE_ENV !== "test") {
        console.error(`[Worker] ❌ Order ${orderId} → FAILED — ${err.message}`);
      }
    }
  } finally {
    isProcessing = false;
  }
}

// ---------------------------------------------------------------------------
// Start / Stop
// ---------------------------------------------------------------------------

/**
 * Start the background worker.
 * Safe to call multiple times — it will not create duplicate timers.
 */
function start() {
  if (pollTimer) return; // already running

  console.log(
    `[Worker] Started — polling every ${POLL_INTERVAL_MS} ms, ` +
      `simulated processing delay ${PROCESSING_DELAY_MS} ms`
  );

  pollTimer = setInterval(tick, POLL_INTERVAL_MS);
}

/**
 * Gracefully stop the worker.
 */
function stop() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
    isProcessing = false;
    console.log("[Worker] Stopped.");
  }
}

module.exports = { start, stop };

