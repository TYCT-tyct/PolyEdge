import glob
import re

def clean_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    out_lines = []
    i = 0
    modified = False

    while i < len(lines):
        line = lines[i]
        match = re.match(r'^([ \t]*)except Exception.*:', line)
        if match:
            indent = match.group(1)
            # Replace the except clause with one that re-raises
            out_lines.append(f"{indent}except Exception:\n")
            out_lines.append(f"{indent}    raise  # Linus: Fail loudly and explicitly\n")
            modified = True

            # Skip the body of the original except block
            i += 1
            while i < len(lines):
                next_line = lines[i]
                if next_line.strip() == "":
                    i += 1
                    continue
                # Find indent of next line
                next_indent_match = re.match(r'^([ \t]*)', next_line)
                next_indent = next_indent_match.group(1) if next_indent_match else ""

                if len(next_indent) > len(indent):
                    # It's inside the except block, skip it
                    i += 1
                else:
                    break
            continue # process the next line that broke the loop

        out_lines.append(line)
        i += 1

    if modified:
        with open(path, 'w', encoding='utf-8') as f:
            f.writelines(out_lines)
        print(f"Fixed exception slop in {path}")

def main():
    py_files = glob.glob('scripts/**/*.py', recursive=True) + glob.glob('ops/**/*.py', recursive=True)
    for p in py_files:
        clean_file(p)

if __name__ == "__main__":
    main()
