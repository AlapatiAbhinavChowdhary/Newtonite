// ---------------------------------------------------------------------------
// In-memory data stores for FlashDrop
// In production these would be backed by Redis / a database.
// ---------------------------------------------------------------------------

/**
 * Inventory – keyed by productId.
 * Each entry holds the current available stock.
 */
const inventory = {
  iphone_16: { productId: "iphone_16", name: "iPhone 16", stock: 100 },
};

/**
 * Orders – keyed by orderId.
 * Stores every order that has been accepted.
 */
const orders = {};

/**
 * Idempotency store – keyed by idempotency key (client-supplied).
 * Maps to the orderId so duplicate requests return the same result.
 */
const idempotencyStore = {};

/**
 * Simulated background order queue.
 * In production this would be a message broker (RabbitMQ, Kafka, etc.).
 * Items are pushed here for async processing.
 */
const orderQueue = [];

// ---------------------------------------------------------------------------
// Helper accessors
// ---------------------------------------------------------------------------

const getProduct = (productId) => inventory[productId] ?? null;

/**
 * Atomically check and deduct stock for a product.
 *
 * Concurrency note:
 * ─────────────────
 * Node.js runs JavaScript on a single thread via the event loop. A
 * synchronous block of code (like the read-compare-write below) will NEVER
 * be interleaved by another request — there are no preemptive context
 * switches within a single tick.  This means the classic race condition
 * (two requests both read stock=1, both think they can sell) cannot happen
 * as long as we keep the check-and-deduct in one synchronous step without
 * awaiting a promise between the check and the mutation.
 *
 * In a multi-process / clustered / distributed setup this guarantee
 * disappears and you would need Redis WATCH/MULTI, a database row-level
 * lock, or an optimistic-concurrency version field.
 *
 * @param {string} productId
 * @param {number} quantity
 * @returns {{ ok: boolean, remaining?: number, error?: string }}
 */
const reserveStock = (productId, quantity) => {
  const product = inventory[productId];

  if (!product) {
    return { ok: false, error: "PRODUCT_NOT_FOUND" };
  }

  if (product.stock < quantity) {
    return {
      ok: false,
      error: "INSUFFICIENT_STOCK",
      available: product.stock,
    };
  }

  // ── Synchronous deduction — safe within a single event-loop tick ──
  product.stock -= quantity;

  return { ok: true, remaining: product.stock };
};

const addOrder = (order) => {
  orders[order.orderId] = order;
};

/**
 * Update the status of an existing order.
 *
 * @param {string} orderId
 * @param {string} newStatus  - e.g. "CONFIRMED", "FAILED"
 * @returns {boolean} true if the order was found and updated
 */
const updateOrderStatus = (orderId, newStatus) => {
  const order = orders[orderId];
  if (!order) return false;

  order.status = newStatus;
  order.updatedAt = new Date().toISOString();
  return true;
};

const getOrder = (orderId) => orders[orderId] ?? null;

const getAllOrders = () => Object.values(orders);

const setIdempotencyKey = (key, orderId) => {
  idempotencyStore[key] = orderId;
};

const getIdempotencyKey = (key) => idempotencyStore[key] ?? null;

const enqueue = (job) => {
  orderQueue.push(job);
};

const dequeue = () => {
  return orderQueue.shift() ?? null;
};

const getQueueLength = () => orderQueue.length;

/**
 * Reset all stores to their initial state.
 * Used by tests to guarantee a clean starting point.
 *
 * @param {number} [initialStock=100] - stock to set for iphone_16
 */
const resetStores = (initialStock = 100) => {
  // Reset inventory
  inventory.iphone_16.stock = initialStock;

  // Clear orders
  for (const key of Object.keys(orders)) delete orders[key];

  // Clear idempotency store
  for (const key of Object.keys(idempotencyStore)) delete idempotencyStore[key];

  // Drain the queue
  orderQueue.length = 0;
};

module.exports = {
  inventory,
  orders,
  idempotencyStore,
  orderQueue,
  getProduct,
  reserveStock,
  addOrder,
  updateOrderStatus,
  getOrder,
  getAllOrders,
  setIdempotencyKey,
  getIdempotencyKey,
  enqueue,
  dequeue,
  getQueueLength,
  resetStores,
};

