export async function register() {
  if (process.env.NEXT_RUNTIME === "nodejs") {
    // Sanitize DD_API_KEY to prevent ERR_INVALID_CHAR during HTTP requests
    if (process.env.DD_API_KEY) {
      process.env.DD_API_KEY = process.env.DD_API_KEY.trim()
    }
    
    // Skip initialization during build if you want, or just let it initialize safely
    const tracer = await import("dd-trace")
    tracer.default.init({
      service: process.env.DD_SERVICE || "homecastr-next",
      env: process.env.DD_ENV || process.env.NODE_ENV || "development",
      version: process.env.VERCEL_GIT_COMMIT_SHA || process.env.DD_VERSION,
      logInjection: true,
      runtimeMetrics: true,
    })
    tracer.default.use("next-server")
  }
}

