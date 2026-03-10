After-close summary (Paper)
==========================

CMD usage:
  cd /d E:\1_Data
  after_close_summary.cmd

Outputs:
  E:\1_Data\2_Logs\after_close_summary_last.json
  E:\1_Data\2_Logs\after_close_summary_last.csv

This tool does NOT modify your pipeline by default.

If you want it to run automatically after run_paper_daily.bat,
add this line at the very end of run_paper_daily.bat:

  call after_close_summary.cmd

(Recommended to run it after market close.)
