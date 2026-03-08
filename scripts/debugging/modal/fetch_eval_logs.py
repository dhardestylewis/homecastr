import subprocess
import time

def fetch_logs(app_id):
    print(f"Fetching logs for {app_id}...")
    res = subprocess.run(["python", "-m", "modal", "app", "logs", app_id], capture_output=True, text=True)
    with open(f"logs/{app_id}_logs.txt", "w", encoding="utf-8") as f:
        f.write(res.stdout)
        f.write("\n\nSTDERR:\n")
        f.write(res.stderr)
    print(f"Done writing to logs/{app_id}_logs.txt")

if __name__ == "__main__":
    fetch_logs("ap-1BzUM079CSuUg8lmyxeg1c")
