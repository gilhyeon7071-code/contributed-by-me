import pathlib

p = pathlib.Path('E:/1_Data/paper_engine.py')
t = p.read_text(encoding='utf-8')

target = """            if event_ymd == now_ymd() and event_level in high_levels:
                action = str(event_policy.get("high_risk_action", "REDUCE") or "REDUCE").strip().upper()
                if action == "BLOCK":
                    if int(max_new) != 0:
                        status["actions"].append(f"event:market_risk_level={event_level} BLOCK")
                    max_new = 0
                else:
                    prev_max_new = int(max_new)
                    max_new = _cap_max_new(max_new, event_policy.get("high_risk_max_new_cap", 1))
                    if int(max_new) != prev_max_new:
                        status["actions"].append(f"event:market_risk_level={event_level} REDUCE {prev_max_new}->{int(max_new)}")
        except Exception as exc:"""

replacement = """            if event_ymd == now_ymd() and event_level in high_levels:
                action = str(event_policy.get("high_risk_action", "REDUCE") or "REDUCE").strip().upper()
                if action == "BLOCK":
                    if int(max_new) != 0:
                        status["actions"].append(f"event:market_risk_level={event_level} BLOCK")
                    max_new = 0
                else:
                    prev_max_new = int(max_new)
                    max_new = _cap_max_new(max_new, event_policy.get("high_risk_max_new_cap", 1))
                    if int(max_new) != prev_max_new:
                        status["actions"].append(f"event:market_risk_level={event_level} REDUCE {prev_max_new}->{int(max_new)}")
            elif event_ymd == now_ymd() and event_level == "BOOST":
                if len(candidate_df) > 0:
                    candidate_df["boost_applied"] = True
                    status["actions"].append("event:market_risk_level=BOOST APPLIED")
        except Exception as exc:"""

if target in t:
    p.write_text(t.replace(target, replacement), encoding='utf-8')
    print("Patched successfully")
else:
    print("Target not found")
