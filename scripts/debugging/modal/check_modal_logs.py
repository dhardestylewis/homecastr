import json
import subprocess
import sys

def main():
    try:
        # Avoid PS encoding issues by capturing CLI straight into Python
        res = subprocess.run(["python", "-m", "modal", "app", "list", "--json"], capture_output=True, text=True)
        data = json.loads(res.stdout)
        
        import os
        os.makedirs("logs", exist_ok=True)
        # Print out all recent apps to a text file
        with open("logs/modal_report.txt", "w") as f:
            for app in data[:15]:
                desc = app.get("Description", "")
                app_id = app.get("App ID")
                state = app.get("State")
                created = app.get("Created at")
                f.write(f"{app_id} | {state} | {created} | {desc}\n")
        print("Done writing to logs/modal_report.txt")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
