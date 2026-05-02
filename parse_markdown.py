import json
import re
import os

def parse_fallacies(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    fallacies = []
    current_part = ""
    current_block = ""

    lines = content.split('\n')

    # Pattern for fallacy entries: **F001 · Name**
    fallacy_pattern = re.compile(r'^\*\*(F\d{3}) · (.*?)\*\*')

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if line.startswith('## '):
            if "APÉNDICE" in line:
                break
            current_part = line.replace('## ', '').strip()
            i += 1
            continue
        elif line.startswith('### '):
            current_block = line.replace('### ', '').strip()
            i += 1
            continue

        match = fallacy_pattern.match(line)
        if match:
            f_id = match.group(1)
            f_name = match.group(2)

            entry = {
                "id": f_id,
                "name": f_name,
                "part": current_part,
                "block": current_block,
                "severity": 0,
                "description": "",
                "example": "",
                "form": ""
            }

            i += 1
            while i < len(lines):
                sub_line = lines[i].strip()

                if fallacy_pattern.match(sub_line):
                    break
                if sub_line.startswith('## ') or sub_line.startswith('### '):
                    break

                if sub_line.startswith('Severidad:'):
                    entry['severity'] = sub_line.count('◆')
                elif sub_line.startswith('Forma:'):
                    entry['form'] = sub_line.replace('Forma:', '').strip()
                elif sub_line.startswith('Ejemplo:'):
                    entry['example'] = sub_line.replace('Ejemplo:', '').strip()
                elif sub_line and not sub_line.startswith('---') and not sub_line.startswith('>'):
                    if entry['description']:
                        entry['description'] += " " + sub_line
                    else:
                        entry['description'] = sub_line

                i += 1

            # Final cleaning
            entry['description'] = entry['description'].strip()
            fallacies.append(entry)
            continue

        i += 1

    return fallacies

if __name__ == "__main__":
    fallacies = parse_fallacies('inventario_falacias.md')
    with open('fallacies_inventory.json', 'w', encoding='utf-8') as f:
        json.dump(fallacies, f, indent=4, ensure_ascii=False)
    print(f"✅ Parsed {len(fallacies)} fallacies into fallacies_inventory.json")
