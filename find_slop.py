import subprocess

def find_slop():
    result = subprocess.run(["git", "diff", "main...HEAD", "-U0"], capture_output=True, text=True, cwd="e:/Projects/test")
    lines = result.stdout.splitlines()

    current_file = ""
    for line in lines:
        if line.startswith("+++ b/"):
            current_file = line[6:]
        elif line.startswith("+") and not line.startswith("+++"):
            code = line[1:].strip()
            lower_code = code.lower()
            if " as any" in lower_code or "any(" in lower_code:
                print(f"[ANY] {current_file}: {code}")
            if "try" in lower_code and "catch" in lower_code:
                print(f"[CATCH] {current_file}: {code}")
            if code.startswith("//") and len(code) > 20:
                print(f"[COMMENT] {current_file}: {code}")
            if "unwrap_or_else" in lower_code or "match " in lower_code:
                if "//" in code:
                    print(f"[MATCH_COMMENT] {current_file}: {code}")

if __name__ == "__main__":
    find_slop()
