Option Explicit

Dim shell
Set shell = CreateObject("WScript.Shell")
shell.CurrentDirectory = "E:\1_Data"
shell.Run """E:\1_Data\run_intraday_watchdog.bat""", 0, True
