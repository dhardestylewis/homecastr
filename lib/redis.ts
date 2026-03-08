import Redis, { Redis as RedisClient } from 'ioredis'

const globalForRedis = globalThis as unknown as {
    redis: RedisClient | undefined;
}

export const redis = globalForRedis.redis ?? (
    process.env.REDIS_URL
        ? new Redis(process.env.REDIS_URL)
        : null
)

// In dev, attach to global object to prevent socket exhaustion during HMR
if (process.env.NODE_ENV !== 'production' && redis) {
    globalForRedis.redis = redis
}

if (redis && !globalForRedis.redis) {
    redis.on('error', err => console.error('[Redis Client Error]', err))
}

/**
 * Helper to cache heavy asynchronous operations in Redis.
 * Useful for static rendering paths (e.g. /forecasts) that pull large aggregates from Supabase.
 * 
 * @param key The unique Redis key for this value
 * @param fetcher The async function to compute the value on cache miss
 * @param ttlSeconds Seconds to keep the value in cache (default 24h)
 * @returns The cached or freshly computed value
 */
export async function withRedisCache<T>(key: string, fetcher: () => Promise<T>, ttlSeconds = 86400): Promise<T> {
    if (!redis) {
        return fetcher()
    }

    let cached: string | null = null
    try {
        cached = await redis.get(key)
    } catch (e) {
        console.warn(`[Redis Cache] Error reading key ${key}:`, e)
    }

    if (cached !== null) {
        try {
            return JSON.parse(cached) as T
        } catch (e) {
            console.warn(`[Redis Cache] Error parsing JSON for key ${key}:`, e)
        }
    }

    // Fetch fresh data. We DO NOT catch errors here so they bubble up correctly.
    const fresh = await fetcher()

    try {
        await redis.set(key, JSON.stringify(fresh), 'EX', ttlSeconds)
    } catch (e) {
        console.warn(`[Redis Cache] Error setting key ${key}:`, e)
    }

    return fresh
}
