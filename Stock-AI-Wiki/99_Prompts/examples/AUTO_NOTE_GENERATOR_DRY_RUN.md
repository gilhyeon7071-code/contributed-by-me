# Auto Note Generator Dry Run

## Command

```powershell
python Stock-AI-Wiki\tools\generate_coverage_notes.py --input E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv --date 2026-05-18 --limit 1
```

## Expected Behavior

```text
apply=false
existing files are reported as skip_exists
missing files are reported as would_write
no RootA trading files are modified
```

## Apply Behavior

```powershell
python Stock-AI-Wiki\tools\generate_coverage_notes.py --input E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv --date 2026-05-18 --limit 1 --apply
```

Apply writes only missing wiki files and never overwrites existing files.

