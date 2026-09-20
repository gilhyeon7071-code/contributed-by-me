Option Explicit

Dim shell, fso, logPath, cmd, rc
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

shell.CurrentDirectory = "E:\1_Data"
If Not fso.FolderExists("E:\1_Data\2_Logs") Then
    fso.CreateFolder("E:\1_Data\2_Logs")
End If

shell.Environment("Process")("WS_HOGA_DURATION_SEC") = "23400"
shell.Environment("Process")("WS_HOGA_CHANNELS") = "hoga"
shell.Environment("Process")("WS_HOGA_MAX_CODES") = "36"
shell.Environment("Process")("WS_HOGA_MOCK") = "auto"
shell.Environment("Process")("WS_HOGA_NOTIFY_ON_ERROR") = "1"

logPath = "E:\1_Data\2_Logs\run_kis_ws_hoga_monitor_hidden_last.txt"
cmd = "%ComSpec% /c " & Chr(34) & Chr(34) & "E:\1_Data\run_kis_ws_hoga_monitor.bat" & Chr(34) & " > " & Chr(34) & logPath & Chr(34) & " 2>&1" & Chr(34)
rc = shell.Run(cmd, 0, True)
WScript.Quit rc
