import sys
path = r'E:\1_Data\paper_engine.py'
with open(path, 'r', encoding='utf-8') as f:
    code = f.read()

target = """                "strategy_type": str(row.get("strategy_type", "UNKNOWN")),"""
replacement = """                "strategy_type": str(row.get("strategy_type")) if row.get("strategy_type") else ("SURGE" if is_surge else ("CARRYOVER" if is_carryover else "NORMAL")),"""

if target in code:
    code = code.replace(target, replacement)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(code)
    print('Patched strategy_type successfully!')
else:
    print('Target strategy_type not found!')

target_loop = """                    "strategy_type": str(row.get("strategy_type") or "UNKNOWN"),"""
replacement_loop = """                    "strategy_type": str(row.get("strategy_type") or ("SURGE" if row.get("is_surge") else ("CARRYOVER" if row.get("is_carryover") else "NORMAL"))),"""

if target_loop in code:
    code = code.replace(target_loop, replacement_loop)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(code)
    print('Patched loop strategy_type successfully!')
else:
    print('Target loop strategy_type not found!')

