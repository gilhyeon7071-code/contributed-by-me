import sys

path = r'E:\1_Data\paper_engine.py'
with open(path, 'r', encoding='utf-8') as f:
    code = f.read()

target1 = """                    "qty": row.get("qty", ""),
                    "order_id": str(row.get("order_id") or ""),
                }
            )"""

replacement1 = """                    "qty": row.get("qty", ""),
                    "order_id": str(row.get("order_id") or ""),
                    "strategy_type": str(row.get("strategy_type") or "UNKNOWN"),
                    "final_score": row.get("final_score", ""),
                    "alloc_weight": row.get("alloc_weight", ""),
                }
            )"""

target2 = """                "surge_gap_up_block_ref_date": row.get("_surge_gap_up_block_ref_date", ""),
            }
        )"""

replacement2 = """                "surge_gap_up_block_ref_date": row.get("_surge_gap_up_block_ref_date", ""),
                "strategy_type": str(row.get("strategy_type", "UNKNOWN")),
                "final_score": row.get("final_score", ""),
                "alloc_weight": row.get("alloc_weight", ""),
            }
        )"""

if target1 in code and target2 in code:
    code = code.replace(target1, replacement1)
    code = code.replace(target2, replacement2)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(code)
    print('Patched successfully!')
else:
    print('Targets not found!')
    print('target1 found:', target1 in code)
    print('target2 found:', target2 in code)
