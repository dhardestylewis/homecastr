import os, time, sys
import boto3
from botocore.config import Config

R2_ACCOUNT_ID      = os.environ.get("R2_ACCOUNT_ID", "7f58e07bff423d2120acf10aa6bf7a32")
R2_ACCESS_KEY_ID   = os.environ.get("R2_ACCESS_KEY_ID", "1eb4758155929638e94e9202c0643c60")
R2_SECRET_KEY      = os.environ.get("R2_SECRET_ACCESS_KEY", "d5f357875251b82ca2539a160fae6f79dc1f2d4d1951bb619ccc4c017475b887")
R2_ENDPOINT        = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
R2_BUCKET          = os.environ.get("R2_BUCKET", "properlytic-raw-data")

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name="auto",
    config=Config(retries={"max_attempts": 3, "mode": "adaptive"}, max_pool_connections=32)
)

def get_tx_size():
    try:
        # Check specifically the txgio prefix since that's the 43GB one
        res = s3.list_objects_v2(Bucket=R2_BUCKET, Prefix='txgio/')
        total = sum(o.get('Size', 0) for o in res.get('Contents', []))
        return total
    except Exception as e:
        print(f"Error checking size: {e}")
        return 0

print("Monitoring R2 upload progress for txgio/ (Target ~43.2 GB)...")
last_size = -1
start_time = time.time()

while True:
    current_size = get_tx_size()
    current_gb = current_size / (1024**3)
    
    if current_size != last_size:
        elapsed = time.time() - start_time
        print(f"[{elapsed:.0f}s] Current txgio/ size: {current_gb:.2f} GB / 43.24 GB")
        last_size = current_size
        
        if current_gb > 42.0:
            print("🚀 Upload complete or nearly complete! You can notify the user.")
            sys.exit(0)
            
    # Poll every 60 seconds
    time.sleep(60)
