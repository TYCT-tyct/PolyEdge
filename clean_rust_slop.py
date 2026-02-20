import re

def process_rust_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Pattern to match:
    # "key" => {
    #     if let Ok(parsed) = val.parse::<type>() {
    #         cfg.field = parsed.method(args...);
    #     }
    # }

    # This regex is specifically looking for the multi-line block pattern
    # It catches both clamp/max calls and direct assignments
    pattern = re.compile(
        r'"([^"]+)"\s*=>\s*\{\s*if let Ok\(parsed\) = val\.parse::<([^>]+)>\(\)\s*\{\s*cfg\.([a-zA-Z0-9_]+)\s*=\s*parsed\.?([^;]*);\s*\}\s*\}',
        re.MULTILINE
    )

    def replacer(match):
        key = match.group(1)
        type_str = match.group(2)
        field = match.group(3)
        method = match.group(4)

        replacement = f'"{key}" => cfg.{field} = val.parse::<{type_str}>().unwrap()'
        if method:
            replacement += f'.{method}'
        replacement += ','
        return replacement

    new_content = pattern.sub(replacer, content)

    # Catch simple boolean parses: val.parse::<bool>()
    pattern_bool = re.compile(
        r'"([^"]+)"\s*=>\s*\{\s*if let Ok\(parsed\) = val\.parse::<bool>\(\)\s*\{\s*cfg\.([a-zA-Z0-9_]+)\s*=\s*parsed;\s*\}\s*\}',
        re.MULTILINE
    )

    def replacer_bool(match):
        key = match.group(1)
        field = match.group(2)
        return f'"{key}" => cfg.{field} = val.parse::<bool>().unwrap(),'

    new_content = pattern_bool.sub(replacer_bool, new_content)

    if content != new_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print("Cleaned up config_loader.rs")
    else:
        print("No changes needed in config_loader.rs")

if __name__ == "__main__":
    process_rust_file("crates/app_runner/src/config_loader.rs")
