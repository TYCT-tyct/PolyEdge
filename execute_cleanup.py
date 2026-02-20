import os

def clean_python_files():
    os.system("python remove_except_slop.py")
    os.system("python clean_python_slop.py")

def clean_rust_comments():
    bad_substrings = [
        'Use write lock for entire operation',
        'between read (matching) and write',
        'Remove filled orders within same write lock',
        'Calculate unrealized PnL',
        'If no prices provided, unrealized is treated as 0',
        'Unrealized = (current_price - avg_price) * position_size',
        'For yes position:',
        'For no position:',
        'Validate and clamp config values',
        'Check match inside lock, but do NOT do I/O',
        'Periodic Stats Log (Every 10s)',
        '1. Setup UDP Listener (Optimized)',
        '2. Setup WS Listener',
        'Spawn Writer Thread',
        'Use min to prevent negative position: only close up to existing position',
        'Only reduce by actual closed amount, not full fill size',
        'Validate price is finite and within valid range',
        'Validate price before creating tick',
        'Validate snapshot price levels',
        'Validate delta prices',
        'Lock poisoned - fail closed for safety',
        'Use SeqCst to ensure proper memory ordering',
        'This guarantees that writes to',
        'Expected edge vs. intended entry price',
        'Fast timestamp using SystemTime',
        'Normalize existing notional to handle potential negative values',
        'Use epsilon to prevent division by zero',
        'Hard drawdown stop - use abs to handle negative drawdown values',
        'Cooldown should end at +60s',
        'Remaining notional is',
        '价格收敛到公允价值 85%+，套利利润已吃满',
    ]

    with open("slop_report.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()

    files_to_check = set()
    for line in lines:
        if line.startswith("[COMMENT]") or line.startswith("[MATCH_COMMENT]"):
            parts = line.split("]: ", 1)
            if len(parts) == 2:
                file_path = parts[0].split(" ")[1].split(":")[0]
                files_to_check.add(file_path.strip())

    for fp in files_to_check:
        if not os.path.exists(fp): continue
        with open(fp, "r", encoding="utf-8") as f:
            lines = f.readlines()

        new_lines = []
        modified = False
        for line in lines:
            if line.strip().startswith("//"):
                if any(sub in line for sub in bad_substrings):
                    modified = True
                    continue
            new_lines.append(line)

        if modified:
            with open(fp, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
            print(f"Cleaned bad comments from {fp}")
            os.system(f"git add {fp}")

if __name__ == "__main__":
    clean_python_files()
    clean_rust_comments()
