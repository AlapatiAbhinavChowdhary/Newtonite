// ---------------------------------------------------------------------------
// Central error-handling middleware for FlashDrop
// ---------------------------------------------------------------------------

/**
 * Catches any error thrown (or passed via next(err)) in route handlers
 * and returns a consistent JSON error response.
 */
const errorHandler = (err, _req, res, _next) => {
  const statusCode = err.statusCode || 500;
  const message = err.message || "Internal Server Error";

  console.error(`[FlashDrop Error] ${statusCode} – ${message}`);
  if (statusCode === 500) {
    console.error(err.stack);
  }

  res.status(statusCode).json({
    success: false,
    error: message,
  });
};

module.exports = errorHandler;

