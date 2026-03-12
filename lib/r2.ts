import {
    S3Client,
    HeadObjectCommand,
    PutObjectCommand,
    GetObjectCommand,
} from "@aws-sdk/client-s3"

let _client: S3Client | null = null

/**
 * Returns a singleton S3-compatible client for Cloudflare R2.
 * Reads credentials from R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID.
 */
function getR2Client(): S3Client {
    if (_client) return _client

    const accountId = process.env.R2_ACCOUNT_ID
    const accessKeyId = process.env.R2_ACCESS_KEY_ID
    const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY

    if (!accountId || !accessKeyId || !secretAccessKey) {
        throw new Error(
            "Missing R2 env vars: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY"
        )
    }

    _client = new S3Client({
        region: "auto",
        endpoint: `https://${accountId}.r2.cloudflarestorage.com`,
        credentials: { accessKeyId, secretAccessKey },
    })
    return _client
}

/**
 * Check if an object exists in the R2 bucket.
 */
export async function r2ObjectExists(
    bucket: string,
    key: string
): Promise<boolean> {
    try {
        await getR2Client().send(
            new HeadObjectCommand({ Bucket: bucket, Key: key })
        )
        return true
    } catch {
        return false
    }
}

/**
 * Upload bytes to R2. Sets public-read ACL equivalent via ContentType.
 */
export async function r2PutObject(
    bucket: string,
    key: string,
    body: Buffer | Uint8Array,
    contentType = "application/octet-stream"
): Promise<void> {
    await getR2Client().send(
        new PutObjectCommand({
            Bucket: bucket,
            Key: key,
            Body: body,
            ContentType: contentType,
        })
    )
}

/**
 * Build the public URL for an R2 object.
 * Requires a public custom domain or the dev URL to be enabled on the bucket.
 * Falls back to generating a pre-signed-style URL via the S3 endpoint.
 *
 * For now, we use the R2 dev URL pattern if R2_PUBLIC_DOMAIN is not set.
 */
export function r2PublicUrl(bucket: string, objectPath: string): string {
    const customDomain = process.env.R2_PUBLIC_DOMAIN
    if (customDomain) {
        return `https://${customDomain}/${objectPath}`
    }
    // Fallback: use the R2 public dev URL (must be enabled in dashboard)
    const accountId = process.env.R2_ACCOUNT_ID
    return `https://${bucket}.${accountId}.r2.dev/${objectPath}`
}

// ── Backwards-compatible GCS aliases ─────────────────────────────────────────
// These allow existing imports from "@/lib/gcs" to keep working by re-exporting
// the same interface names.

/**
 * @deprecated Use r2ObjectExists / r2PutObject / r2PublicUrl directly.
 * This shim exists only for backwards compatibility during migration.
 */
export function getR2Bucket(bucketName?: string) {
    const name = bucketName ?? process.env.R2_BUCKET
    if (!name) throw new Error("Missing env var: R2_BUCKET")
    return {
        name,
        file(key: string) {
            return {
                async exists(): Promise<[boolean]> {
                    const exists = await r2ObjectExists(name, key)
                    return [exists]
                },
                async save(
                    body: Buffer | Uint8Array,
                    opts?: { contentType?: string }
                ): Promise<void> {
                    await r2PutObject(
                        name,
                        key,
                        body,
                        opts?.contentType ?? "application/octet-stream"
                    )
                },
            }
        },
    }
}
