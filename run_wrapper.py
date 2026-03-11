import sys, traceback
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')
sys.path.insert(0, ".")
from scripts.pipeline.training import train_local_eurostat_sb
try:
    train_local_eurostat_sb.main()
except Exception:
    with open("local_trace.txt", "w", encoding="utf-8") as f:
        traceback.print_exc(file=f)
    traceback.print_exc()
    sys.exit(1)
