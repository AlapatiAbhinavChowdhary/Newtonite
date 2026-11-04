// ---------------------------------------------------------------------------
// FlashDrop — Automated Test Suite
//
// Covers:
//   1. Successful order creation
//   2. Invalid request validation
//   3. Unknown product
//   4. Insufficient inventory
//   5. Duplicate idempotency key
//   6. Concurrent order requests (10,000 simultaneous)
//   7. Inventory never becomes negative
//   8. PENDING → CONFIRMED status transition
//   9. GET order details
//  10. PENDING → FAILED status transition (error simulation)
//  11. GET /inventory, /health, and /orders/queue/status
//
// Run:  npm test
// ---------------------------------------------------------------------------

process.env.NODE_ENV = "test";

const http = require("http");
const {
  inventory,
  orders,
  idempotencyStore,
  orderQueue,
  resetStores,
  getOrder,
  getAllOrders,
} = require("../src/store/memoryStore");
const orderWorker = require("../src/workers/orderWorker");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Allow many concurrent connections from the test client
const agent = new http.Agent({ keepAlive: true, maxSockets: 200 });

let server;
const PORT = 0; // let the OS pick a free port
let baseUrl;

function req(method, path, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(path, baseUrl);
    const opts = {
      hostname: url.hostname,
      port: url.port,
      path: url.pathname,
      method,
      agent,
      headers: { "Content-Type": "application/json" },
    };
    const r = http.request(opts, (res) => {
      let d = "";
      res.on("data", (c) => (d += c));
      res.on("end", () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(d) });
        } catch {
          resolve({ status: res.statusCode, body: d });
        }
      });
    });
    r.on("error", reject);
    if (body) r.write(JSON.stringify(body));
    r.end();
  });
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// Counters
let totalPassed = 0;
let totalFailed = 0;
const sectionResults = [];

function assert(label, condition) {
  if (condition) {
    totalPassed++;
  } else {
    totalFailed++;
    console.error(`    ✗ FAIL: ${label}`);
  }
}

function section(name) {
  console.log(`\n  ▸ ${name}`);
}

// ---------------------------------------------------------------------------
// Boot & teardown
// ---------------------------------------------------------------------------

async function startServer() {
  const originalLog = console.log;
  console.log = () => {};

  const app = require("../src/server");

  return new Promise((resolve) => {
    server = app.listen(PORT, () => {
      const addr = server.address();
      baseUrl = `http://127.0.0.1:${addr.port}`;
      console.log = originalLog;
      orderWorker.start();
      resolve();
    });
  });
}

function stopServer() {
  orderWorker.stop();
  return new Promise((resolve) => {
    if (server) server.close(resolve);
    else resolve();
  });
}

// ---------------------------------------------------------------------------
// Test suites
// ---------------------------------------------------------------------------

async function test1_successfulOrderCreation() {
  section("1. Successful order creation");
  resetStores(100);

  const res = await req("POST", "/orders", {
    userId: "user_1",
    productId: "iphone_16",
    quantity: 2,
    idempotencyKey: "t1-key-1",
  });

  assert("returns 201", res.status === 201);
  assert("success is true", res.body.success === true);
  assert("has orderId", typeof res.body.orderId === "string");
  assert("status is PENDING", res.body.status === "PENDING");
  assert("inventory deducted to 98", inventory.iphone_16.stock === 98);
  assert("order stored in memory", getOrder(res.body.orderId) !== null);
  assert("job enqueued", orderQueue.length === 1);
}

async function test2_invalidRequestValidation() {
  section("2. Invalid request validation");
  resetStores(100);

  // Missing fields
  const r1 = await req("POST", "/orders", { userId: "u1" });
  assert("missing fields → 400", r1.status === 400);
  assert("error mentions required fields", r1.body.error.includes("Missing"));

  // Empty body
  const r2 = await req("POST", "/orders", {});
  assert("empty body → 400", r2.status === 400);

  // Negative quantity
  const r3 = await req("POST", "/orders", {
    userId: "u1",
    productId: "iphone_16",
    quantity: -5,
    idempotencyKey: "t2-neg",
  });
  assert("negative quantity → 400", r3.status === 400);
  assert("error mentions positive integer", r3.body.error.includes("positive"));

  // Zero quantity
  const r4 = await req("POST", "/orders", {
    userId: "u1",
    productId: "iphone_16",
    quantity: 0,
    idempotencyKey: "t2-zero",
  });
  assert("zero quantity → 400", r4.status === 400);

  // Float quantity
  const r5 = await req("POST", "/orders", {
    userId: "u1",
    productId: "iphone_16",
    quantity: 2.5,
    idempotencyKey: "t2-float",
  });
  assert("float quantity → 400", r5.status === 400);

  // Inventory unchanged
  assert("inventory untouched (still 100)", inventory.iphone_16.stock === 100);
}

async function test3_unknownProduct() {
  section("3. Unknown product");
  resetStores(100);

  const res = await req("POST", "/orders", {
    userId: "u1",
    productId: "galaxy_s99",
    quantity: 1,
    idempotencyKey: "t3-key",
  });

  assert("returns 404", res.status === 404);
  assert("error mentions product", res.body.error.includes("galaxy_s99"));
  assert("inventory unchanged", inventory.iphone_16.stock === 100);
}

async function test4_insufficientInventory() {
  section("4. Insufficient inventory");
  resetStores(5);

  const res = await req("POST", "/orders", {
    userId: "u1",
    productId: "iphone_16",
    quantity: 10,
    idempotencyKey: "t4-key",
  });

  assert("returns 409", res.status === 409);
  assert("error mentions insufficient", res.body.error.includes("Insufficient"));
  assert("inventory unchanged (still 5)", inventory.iphone_16.stock === 5);
}

async function test5_duplicateIdempotencyKey() {
  section("5. Duplicate idempotency key");
  resetStores(100);

  // First request
  const r1 = await req("POST", "/orders", {
    userId: "u1",
    productId: "iphone_16",
    quantity: 1,
    idempotencyKey: "t5-dup",
  });
  assert("first request → 201", r1.status === 201);
  const orderId = r1.body.orderId;

  // Duplicate request (same idempotency key)
  const r2 = await req("POST", "/orders", {
    userId: "u1",
    productId: "iphone_16",
    quantity: 1,
    idempotencyKey: "t5-dup",
  });
  assert("duplicate → 200", r2.status === 200);
  assert("returns same orderId", r2.body.order.orderId === orderId);
  assert("mentions duplicate", r2.body.message.includes("Duplicate"));
  assert("inventory deducted only once (99)", inventory.iphone_16.stock === 99);
  assert("only 1 order in store", getAllOrders().length === 1);
}

async function test6_concurrentOrders() {
  section("6. Concurrent order requests (10,000 → 100 stock)");
  resetStores(100);

  const TOTAL_REQUESTS = 10000;
  const BATCH_SIZE = 500; // send in batches to avoid overwhelming the TCP backlog
  const allResults = [];

  for (let b = 0; b < TOTAL_REQUESTS; b += BATCH_SIZE) {
    const batch = [];
    const end = Math.min(b + BATCH_SIZE, TOTAL_REQUESTS);
    for (let i = b; i < end; i++) {
      batch.push(
        req("POST", "/orders", {
          userId: `user_${i}`,
          productId: "iphone_16",
          quantity: 1,
          idempotencyKey: `t6-concurrent-${i}`,
        })
      );
    }
    const batchResults = await Promise.all(batch);
    allResults.push(...batchResults);
  }

  const successes = allResults.filter((r) => r.status === 201);
  const conflicts = allResults.filter((r) => r.status === 409);

  console.log(
    `    → ${successes.length} succeeded (201), ${conflicts.length} rejected (409)`
  );

  assert("exactly 100 orders succeeded", successes.length === 100);
  assert("exactly 9900 rejected", conflicts.length === 9900);
  assert("inventory is exactly 0", inventory.iphone_16.stock === 0);
  assert("100 orders in store", getAllOrders().length === 100);
}

async function test7_inventoryNeverNegative() {
  section("7. Inventory never becomes negative");
  // Relies on state from test 6 — inventory at 0
  // But let's do a fresh explicit test
  resetStores(1);

  // Fire 50 simultaneous requests for 1 unit each
  const promises = [];
  for (let i = 0; i < 50; i++) {
    promises.push(
      req("POST", "/orders", {
        userId: `neg_user_${i}`,
        productId: "iphone_16",
        quantity: 1,
        idempotencyKey: `t7-neg-${i}`,
      })
    );
  }

  const results = await Promise.all(promises);
  const successes = results.filter((r) => r.status === 201);

  assert("exactly 1 order succeeded", successes.length === 1);
  assert("inventory is 0 (not negative)", inventory.iphone_16.stock === 0);
  assert("stock >= 0", inventory.iphone_16.stock >= 0);
}

async function test8_pendingToConfirmedTransition() {
  section("8. PENDING → CONFIRMED status transition");
  resetStores(100);

  // Place an order
  const res = await req("POST", "/orders", {
    userId: "worker_user",
    productId: "iphone_16",
    quantity: 1,
    idempotencyKey: "t8-worker",
  });
  const orderId = res.body.orderId;

  // Immediately check — should be PENDING
  const order1 = getOrder(orderId);
  assert("initially PENDING", order1.status === "PENDING");

  // Wait for the worker to process (poll 500ms + process 800ms ≈ 1.5s, give 3s)
  await sleep(3000);

  const order2 = getOrder(orderId);
  assert("transitions to CONFIRMED", order2.status === "CONFIRMED");
  assert("updatedAt is set", typeof order2.updatedAt === "string");
  assert("updatedAt > createdAt", order2.updatedAt > order2.createdAt);
}

async function test9_getOrderDetails() {
  section("9. GET order details");
  resetStores(100);

  // Create an order first
  const createRes = await req("POST", "/orders", {
    userId: "get_user",
    productId: "iphone_16",
    quantity: 3,
    idempotencyKey: "t9-get",
  });
  const orderId = createRes.body.orderId;

  // GET the order
  const r1 = await req("GET", `/orders/${orderId}`);
  assert("GET returns 200", r1.status === 200);
  assert("success is true", r1.body.success === true);

  const o = r1.body.order;
  assert("orderId matches", o.orderId === orderId);
  assert("userId is get_user", o.userId === "get_user");
  assert("productId is iphone_16", o.productId === "iphone_16");
  assert("quantity is 3", o.quantity === 3);
  assert("status is PENDING", o.status === "PENDING");
  assert("createdAt present", typeof o.createdAt === "string");
  assert("updatedAt is null (not yet processed)", o.updatedAt === null);

  // GET non-existent order → 404
  const r2 = await req("GET", "/orders/nonexistent-id");
  assert("non-existent order → 404", r2.status === 404);
  assert("error message present", r2.body.error.includes("not found"));

  // Wait for worker and check updated fields
  await sleep(3000);
  const r3 = await req("GET", `/orders/${orderId}`);
  assert("after worker: status is CONFIRMED", r3.body.order.status === "CONFIRMED");
  assert("after worker: updatedAt is set", r3.body.order.updatedAt !== null);
}

async function test10_pendingToFailedTransition() {
  section("10. PENDING → FAILED status transition on processing error");
  resetStores(100);

  const res = await req("POST", "/orders", {
    userId: "fail_user",
    productId: "iphone_16",
    quantity: 1,
    idempotencyKey: "t10-failed-worker",
    simulateFailure: true,
  });

  assert("returns 201 with orderId", res.status === 201 && !!res.body.orderId);
  const orderId = res.body.orderId;
  const order1 = getOrder(orderId);
  assert("initially PENDING", order1.status === "PENDING");

  // Wait for the worker to process and fail
  await sleep(2000);

  const order2 = getOrder(orderId);
  assert("transitions to FAILED", order2.status === "FAILED");
  assert("updatedAt is set on failure", typeof order2.updatedAt === "string");
}

async function test11_getInventoryAndHealth() {
  section("11. GET /inventory, /health, and /orders/queue/status");
  resetStores(75);

  const rHealth = await req("GET", "/health");
  assert("GET /health status 200", rHealth.status === 200);
  assert("health service is FlashDrop", rHealth.body.service === "FlashDrop");

  const rInv = await req("GET", "/inventory");
  assert("GET /inventory status 200", rInv.status === 200);
  assert("inventory stock matches", rInv.body.inventory.iphone_16.stock === 75);

  const rQueue = await req("GET", "/orders/queue/status");
  assert("GET /orders/queue/status status 200", rQueue.status === 200);
  assert("queueLength is number", typeof rQueue.body.queueLength === "number");
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

async function runAllTests() {
  console.log("\n╔══════════════════════════════════════════════╗");
  console.log("║     FlashDrop — Automated Test Suite        ║");
  console.log("╚══════════════════════════════════════════════╝");

  await startServer();
  console.log(`  Server started on ${baseUrl}`);

  try {
    await test1_successfulOrderCreation();
    await test2_invalidRequestValidation();
    await test3_unknownProduct();
    await test4_insufficientInventory();
    await test5_duplicateIdempotencyKey();
    await test6_concurrentOrders();
    await test7_inventoryNeverNegative();
    await test8_pendingToConfirmedTransition();
    await test9_getOrderDetails();
    await test10_pendingToFailedTransition();
    await test11_getInventoryAndHealth();
  } catch (err) {
    console.error("\n  UNEXPECTED ERROR:", err);
    totalFailed++;
  }

  console.log("\n  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log(`  ✅ Passed: ${totalPassed}`);
  console.log(`  ❌ Failed: ${totalFailed}`);
  console.log("  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

  await stopServer();
  process.exit(totalFailed > 0 ? 1 : 0);
}

runAllTests();
