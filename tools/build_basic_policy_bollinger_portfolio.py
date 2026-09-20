import importlib.util
from pathlib import Path
R=Path(__file__).resolve().parents[1]
s=importlib.util.spec_from_file_location('p',R/'tools'/'build_basic_paper_policy_portfolio.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
