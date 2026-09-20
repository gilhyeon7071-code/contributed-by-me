---
id: question-2026-05-18-google-rss-probe-json-parse-issue
type: question
title: Google RSS Probe JSON Parse Issue
created: 2026-05-18
updated: 2026-05-18
status: raw
stage: 0

market:
ticker:
company:
theme:
  - news_pipeline
  - json_output

source:
  type: local_json_artifact
  name: google_news_rss_probe_latest.json
  url: E:\1_Data\2_Logs\google_news_rss_probe_latest.json
  published_at:
  collected_at: 2026-05-18

analysis:
  summary: PowerShell ConvertFrom-Json fails when google_news_rss_probe_latest.json is read with the default PowerShell encoding, but passes when the file is read as UTF-8.
  key_facts:
    - artifact=E:\1_Data\2_Logs\google_news_rss_probe_latest.json
    - artifact_size_bytes=35310
    - artifact_mtime=2026-05-18 12:26:30
    - parse_tool=PowerShell ConvertFrom-Json
    - default_encoding_parse_error_pos=4485
    - parse_error_line=88
    - utf8_encoding_parse=PASS
    - python_json_loads=PASS
  related_entities:
    - google_news_rss_probe_latest.json
    - news_pipeline
  possible_impact: JSON consumers can fail if they read the UTF-8 artifact with the wrong default encoding.
  uncertainty:
    - Generator code path that wrote the malformed JSON was not inspected in this pass.

verification:
  verified: false
  source_count: 1
  confidence: medium
  conflict_exists: false

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
  policy_link:

audit:
  human_reviewed: false
  ai_generated: false
  last_reviewed_at:
  change_reason: read-only parse diagnosis
---

# Google RSS Probe JSON Parse Issue

## Question
- Why does `E:\1_Data\2_Logs\google_news_rss_probe_latest.json` fail JSON parsing?

## Evidence
- File: `E:\1_Data\2_Logs\google_news_rss_probe_latest.json`
- Size: `35310` bytes
- Last write time: `2026-05-18 12:26:30`
- Parser: PowerShell `ConvertFrom-Json`
- Error position: `4515`
- Error line: `88`

## Observed Bad Region

```text
88: "title": "CJ?... ??"諛붿씠???ㅼ쟻 ????듦낵 以?" - ..."
89: "description": "CJ?... ??"諛붿씠???ㅼ쟻 ????듦낵 以?" ..."
```

## Facts
- The JSON artifact exists.
- PowerShell `Get-Content -Raw | ConvertFrom-Json` fails under the default read encoding.
- PowerShell `Get-Content -Raw -Encoding UTF8 | ConvertFrom-Json` passes.
- Python `json.loads(..., encoding='utf-8')` passes.
- The visible failure line contains escaped quote characters in the valid UTF-8 read.

## Interpretation
- This is an encoding-sensitive read problem, not confirmed as a writer JSON escaping defect.
- Consumers should read this artifact as UTF-8.
- It is separate from trading policy and should not change any Gate, score, order, fill, ledger, or stats behavior.

## Uncertainty
- Whether every downstream consumer explicitly reads UTF-8 was not checked.

## Next Check
- Locate any downstream consumers that parse `google_news_rss_probe_latest.json`.
- Ensure they use UTF-8 when reading the file.
- Do not change trading policy, Gate, score, order, fill, ledger, or stats behavior.
