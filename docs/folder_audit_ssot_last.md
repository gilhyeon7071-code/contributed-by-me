# Folder Audit SSOT (last)

RootA: E:\1_Data
RootB: E:\vibe\buffett


> ref_abs = count of string matches for E:\1_Data\<folder> inside RootB .py/.ps1 (capped files).

> ref_name = heuristic word-boundary count for folder name (only if len>=4).


| folder | class | maturity | dirs | files | py | ps1 | README | tests_dir | entry(sample) | note | ref_abs | ref_name |

|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---:|---:|

| __pycache__ | UNKNOWN | L0(no-code) | 0 | 20 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| _bak | EVIDENCE | L0(no-code) | 0 | 48 | 0 | 0 | 0 | 0 |  | skip-deep-scan | 0 | 0 |

| _cache | EVIDENCE | L0(no-code) | 0 | 9 | 0 | 0 | 0 | 0 |  | skip-deep-scan | 0 | 0 |

| _dev | DEV_OR_DATA | L2(runnable-candidate) | 2 | 0 | 15 | 0 | 1 | 0 |  |  | 0 | 0 |

| _diag | EVIDENCE | L0(no-code) | 0 | 14 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| _krx_manual | SOURCE | L0(no-code) | 2 | 32 | 0 | 0 | 0 | 0 |  |  | 1 | 1 |

| _krx_seed_full | SOURCE | L0(no-code) | 0 | 1 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| _notes | DOCS | L2(runnable-candidate) | 6 | 11 | 45 | 0 | 1 | 0 | E:\1_Data\_notes\expectancy_winrate_system_project\expectancy_winrate_system\src\cli.py, E:\1_Data\_notes\slippage_defense_execution_arch_project\src\cli.py |  | 0 | 0 |

| .claude | UNKNOWN | L0(no-code) | 1 | 1 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| '') | UNKNOWN | L0(no-code) | 0 | 0 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| '').replace('-' | UNKNOWN | L0(no-code) | 0 | 0 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| (report['meta'].get('latest_date') | UNKNOWN | L0(no-code) | 0 | 0 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| 02_Audit | LEGACY | L0(no-code) | 0 | 2 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| 03_Distribution | LEGACY | L0(no-code) | 0 | 2 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| 10_Validation_Report | LEGACY | L0(no-code) | 0 | 1 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| 11_Autonomous_Evolution | LEGACY | L0(no-code) | 0 | 0 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| 12_Risk_Controlled | LEGACY | L0(no-code) | 0 | 38 | 0 | 0 | 0 | 0 |  |  | 0 | 3 |

| 13_Elite_Results | LEGACY | L0(no-code) | 0 | 0 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| 16_Audit_Report | LEGACY | L0(no-code) | 0 | 1 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| 2_Logs | EVIDENCE | L0(no-code) | 46 | 3056 | 0 | 0 | 0 | 0 |  | skip-deep-scan | 3 | 18 |

| 3_Evolution | LEGACY | L0(no-code) | 0 | 0 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| 4_GoldVein | LEGACY | L0(no-code) | 0 | 0 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| 5_StrategyMap | LEGACY | L0(no-code) | 0 | 1 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| 6_Final_Landscape | LEGACY | L0(no-code) | 0 | 1 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| 8_MetaEvolution | LEGACY | L0(no-code) | 0 | 6 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| 9_MasterAlpha | LEGACY | L0(no-code) | 0 | 1 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| 새 폴더 | UNKNOWN | L0(no-code) | 0 | 12 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| data | DERIVED | L0(no-code) | 1 | 0 | 0 | 0 | 0 | 0 |  |  | 0 | 297 |

| docs | DOCS | L0(no-code) | 1 | 4 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| Macro | UNKNOWN | L0(no-code) | 0 | 1 | 0 | 0 | 0 | 0 |  |  | 0 | 1 |

| mx | UNKNOWN | L0(no-code) | 0 | 0 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| news_trading | DEV_OR_DATA | L0(no-code) | 1 | 0 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| None | UNKNOWN | L0(no-code) | 0 | 0 | 0 | 0 | 0 | 0 |  |  | 0 | 778 |

| or | UNKNOWN | L0(no-code) | 0 | 0 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| paper | DERIVED | L0(no-code) | 1 | 72 | 0 | 0 | 0 | 0 |  |  | 2 | 58 |

| pq.ParquetFile(str(p)).metadata | UNKNOWN | L0(no-code) | 0 | 0 | 0 | 0 | 0 | 0 |  |  | 0 | 0 |

| Raw | SOURCE | L0(no-code) | 0 | 1 | 0 | 0 | 0 | 0 |  |  | 3 | 0 |

| tools | TOOLS | L2(runnable-candidate) | 3 | 106 | 49 | 5 | 1 | 0 |  |  | 4 | 54 |

| utils | TOOLS | L1(code-but-no-entry/docs/tests) | 1 | 2 | 2 | 0 | 0 | 0 |  |  | 0 | 2 |


## UNKNOWN Review List

- count: 11

- __pycache__

- .claude

- '')

- '').replace('-'

- (report['meta'].get('latest_date')

- 새 폴더

- Macro

- mx

- None

- or

- pq.ParquetFile(str(p)).metadata

