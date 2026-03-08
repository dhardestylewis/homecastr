import subprocess

def main():
    with open("grand_panel_crash.txt", "w", encoding="utf-8") as f:
        print("Fetching logs for ap-T78BYjnWQaqGhpFwW6QYvU...")
        res = subprocess.run(["python", "-m", "modal", "app", "logs", "ap-T78BYjnWQaqGhpFwW6QYvU"], capture_output=True, text=True)
        f.write("STDOUT:\n")
        f.write(res.stdout)
        f.write("\n\nSTDERR:\n")
        f.write(res.stderr)
    print("Done writing to grand_panel_crash.txt")

if __name__ == "__main__":
    main()
