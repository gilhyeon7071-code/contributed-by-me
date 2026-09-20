Option Explicit

Dim shell, cmd, logPath, rc
Set shell = CreateObject("WScript.Shell")

shell.CurrentDirectory = "E:\1_Data"
logPath = "E:\1_Data\Stock-AI-Wiki\00_Inbox\logs\run_stock_ai_wiki_update_hidden_last.txt"
cmd = "%ComSpec% /c " & Chr(34) & Chr(34) & "E:\1_Data\run_stock_ai_wiki_update.bat" & Chr(34) & " > " & Chr(34) & logPath & Chr(34) & " 2>&1" & Chr(34)
rc = shell.Run(cmd, 0, True)
WScript.Quit rc
