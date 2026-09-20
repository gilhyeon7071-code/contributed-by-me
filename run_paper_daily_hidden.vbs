Option Explicit

Dim shell, fso, cmd, logPath, rc
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

shell.CurrentDirectory = "E:\1_Data"
If Not fso.FolderExists("E:\1_Data\2_Logs") Then
    fso.CreateFolder("E:\1_Data\2_Logs")
End If

logPath = "E:\1_Data\2_Logs\run_paper_daily_hidden_last.txt"
cmd = "%ComSpec% /c " & Chr(34) & Chr(34) & "E:\1_Data\run_paper_daily.bat" & Chr(34) & " > " & Chr(34) & logPath & Chr(34) & " 2>&1" & Chr(34)
rc = shell.Run(cmd, 0, True)
WScript.Quit rc
