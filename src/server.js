const express = require("express");
const orderRoutes = require("./routes/orderRoutes");
const errorHandler = require("./middleware/errorHandler");
const { inventory } = require("./store/memoryStore");
const orderWorker = require("./workers/orderWorker");

// ---------------------------------------------------------------------------
// App setup
// ---------------------------------------------------------------------------
const app = express();
const PORT = process.env.PORT || 3000;

// --- JSON body parsing ------------------------------------------------------
app.use(express.json());

// --- Request logger (lightweight) -------------------------------------------
if (process.env.NODE_ENV !== "test") {
  app.use((req, _res, next) => {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
    next();
  });
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// Health check
app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "FlashDrop" });
});

// Inventory snapshot
app.get("/inventory", (_req, res) => {
  res.json({ success: true, inventory });
});

// Order routes
app.use("/orders", orderRoutes);

// ---------------------------------------------------------------------------
// 404 catch-all
// ---------------------------------------------------------------------------
app.use((_req, res) => {
  res.status(404).json({ success: false, error: "Route not found" });
});

// ---------------------------------------------------------------------------
// Central error handler (must be registered last)
// ---------------------------------------------------------------------------
app.use(errorHandler);

// ---------------------------------------------------------------------------
// Start server (only when executed directly, not when imported by test suites)
// ---------------------------------------------------------------------------
if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`\n🚀 FlashDrop server running on http://localhost:${PORT}`);
    console.log(`   Inventory loaded: ${Object.keys(inventory).length} product(s)\n`);

    // Start the background order-processing worker
    orderWorker.start();
  });
}

module.exports = app;

