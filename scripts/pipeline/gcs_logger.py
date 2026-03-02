"""
GCS Log Streaming for Modal Functions
======================================
Tees stdout/stderr to both Modal's built-in log viewer AND a persistent
GCS blob. Every Modal function run creates a log file at:

    gs://properlytic-raw-data/logs/{app_name}/{func_name}/{timestamp}.log

Usage in any Modal function:
    from scripts.pipeline.gcs_logger import gcs_log_context

    @app.function(...)
    def my_function():
        with gcs_log_context("my-app", "my_function"):
            print("This goes to BOTH Modal viewer and GCS")
"""

import io, os, sys, time, json
from contextlib import contextmanager


class TeeWriter:
    """Writes to both the original stream and a buffer."""
    def __init__(self, original, buffer):
        self.original = original
        self.buffer = buffer

    def write(self, msg):
        self.original.write(msg)
        self.buffer.write(msg)

    def flush(self):
        self.original.flush()
        self.buffer.flush()

    # Forward any other attribute lookups to original
    def __getattr__(self, name):
        return getattr(self.original, name)


def _upload_to_gcs(bucket_name: str, blob_path: str, content: str):
    """Upload log content to GCS."""
    try:
        from google.cloud import storage
        creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
        if not creds_json:
            print(f"[GCS-LOG] No credentials, skipping upload to {blob_path}")
            return
        creds = json.loads(creds_json)
        client = storage.Client.from_service_account_info(creds)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(content, content_type="text/plain")
        print(f"[GCS-LOG] ✅ Saved {len(content)} bytes → gs://{bucket_name}/{blob_path}")
    except Exception as e:
        print(f"[GCS-LOG] ⚠️ Upload failed: {e}")


@contextmanager
def gcs_log_context(app_name: str, func_name: str,
                     bucket: str = "properlytic-raw-data"):
    """
    Context manager that tees stdout+stderr to a GCS log file.
    
    Usage:
        with gcs_log_context("inference-hcad", "run_inference"):
            print("all output captured")
    """
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    blob_path = f"logs/{app_name}/{func_name}/{timestamp}.log"
    
    buf = io.StringIO()
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    
    sys.stdout = TeeWriter(old_stdout, buf)
    sys.stderr = TeeWriter(old_stderr, buf)
    
    start = time.time()
    buf.write(f"=== {app_name}/{func_name} started at {timestamp} ===\n\n")
    
    try:
        yield blob_path
    except Exception as e:
        buf.write(f"\n\n=== EXCEPTION: {e} ===\n")
        import traceback
        traceback.print_exc(file=buf)
        raise
    finally:
        elapsed = time.time() - start
        buf.write(f"\n\n=== Completed in {elapsed:.1f}s ===\n")
        
        # Restore streams
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        
        # Upload to GCS
        _upload_to_gcs(bucket, blob_path, buf.getvalue())
        buf.close()
