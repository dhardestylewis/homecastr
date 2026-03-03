import { Storage } from "@google-cloud/storage"

let _storage: Storage | null = null

/**
 * Returns a singleton @google-cloud/storage Storage instance.
 * Credentials are read from GCS_SERVICE_ACCOUNT_JSON or
 * GOOGLE_APPLICATION_CREDENTIALS_JSON (Python pipeline convention).
 */
function getStorage(): Storage {
    if (_storage) return _storage

    const raw = process.env.GCS_SERVICE_ACCOUNT_JSON
        ?? process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON
    if (!raw) throw new Error("Missing env var: GCS_SERVICE_ACCOUNT_JSON or GOOGLE_APPLICATION_CREDENTIALS_JSON")

    const credentials = JSON.parse(raw)
    _storage = new Storage({ credentials, projectId: credentials.project_id })
    return _storage
}

/**
 * Returns a GCS Bucket handle for the given bucket name.
 * Falls back to GCS_STREETVIEW_BUCKET env var if no name provided.
 */
export function getGcsBucket(bucketName?: string) {
    const name = bucketName ?? process.env.GCS_STREETVIEW_BUCKET
    if (!name) throw new Error("Missing env var: GCS_STREETVIEW_BUCKET")
    return getStorage().bucket(name)
}

/**
 * Build the public HTTPS URL for a GCS object.
 * Bucket must have uniform public access or the object must be publicRead.
 */
export function gcsPublicUrl(bucketName: string, objectPath: string): string {
    return `https://storage.googleapis.com/${bucketName}/${objectPath}`
}
