export async function register() {
  if (process.env.NEXT_RUNTIME === "nodejs") {
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
