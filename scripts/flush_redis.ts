import { redis } from "../lib/redis"

async function flush() {
    if (!redis) {
        console.error("Redis client not initialized.")
        return
    }
    console.log("Connected to Redis...", process.env.REDIS_URL?.substring(0, 20) + "...")
    await redis.flushall()
    console.log("Redis flushall complete!")
    redis.disconnect()
}

flush().catch(console.error)
