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
 * @param ttlSeconds Seconds to keep the value in cache (default 2h)
 * @returns The cached or freshly computed value
 */
export async function withRedisCache<T>(key: string, fetcher: () => Promise<T>, ttlSeconds = 7200): Promise<T> {
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
        // Use a short TTL for empty results so newly loaded data appears quickly
        const isEmpty = fresh == null
            || (Array.isArray(fresh) && fresh.length === 0)
            || (typeof fresh === 'object' && !Array.isArray(fresh) && Object.keys(fresh as any).length === 0)
        const effectiveTtl = isEmpty ? Math.min(ttlSeconds, 300) : ttlSeconds  // 5 min cap for empty
        await redis.set(key, JSON.stringify(fresh), 'EX', effectiveTtl)
    } catch (e) {
        console.warn(`[Redis Cache] Error setting key ${key}:`, e)
    }

    return fresh
}

/**
 * Binary-safe Redis cache for MVT tile buffers.
 * Unlike withRedisCache (which JSON-serializes), this stores raw Buffer data.
 * 
 * Returns null on cache miss so the caller can distinguish "cached empty tile"
 * (Buffer.byteLength === 0) from "not in cache" (null).
 */
const EMPTY_TILE_SENTINEL = Buffer.from('__EMPTY__')

export async function withRedisBinaryCache(
    key: string,
    fetcher: () => Promise<Buffer | null>,
    ttlSeconds = 14400,  // 4 hours default
): Promise<{ data: Buffer | null; fromCache: boolean }> {
    if (!redis) {
        return { data: await fetcher(), fromCache: false }
    }

    // Try cache first
    try {
        const cached = await redis.getBuffer(key)
        if (cached !== null) {
            // Check for empty-tile sentinel
            if (cached.equals(EMPTY_TILE_SENTINEL)) {
                return { data: null, fromCache: true }
            }
            return { data: cached, fromCache: true }
        }
    } catch (e) {
        console.warn(`[Redis Binary Cache] Error reading key ${key}:`, e)
    }

    // Cache miss — fetch from source
    const fresh = await fetcher()

    try {
        if (fresh === null || fresh.length === 0) {
            // Cache empty tiles with a short TTL (5 min) so new data appears quickly
            await redis.set(key, EMPTY_TILE_SENTINEL, 'EX', Math.min(ttlSeconds, 300))
        } else {
            await redis.set(key, fresh, 'EX', ttlSeconds)
        }
    } catch (e) {
        console.warn(`[Redis Binary Cache] Error setting key ${key}:`, e)
    }

    return { data: fresh, fromCache: false }
}
