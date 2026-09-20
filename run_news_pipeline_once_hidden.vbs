Option Explicit

Dim shell, fso, args, i, argText, cmd, logPath, rc, sessionName
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
Set args = WScript.Arguments

sessionName = ""
If args.Count > 0 Then
    sessionName = LCase(Trim(args.Item(0)))
End If

shell.CurrentDirectory = "E:\1_Data"
shell.Environment("PROCESS")("NEWS_OBSERVE_ONLY_SKIP") = "1"
shell.Environment("PROCESS")("NEWS_SOURCE_SIGNAL_SKIP") = "1"
If sessionName = "afterhours" Then
    shell.Environment("PROCESS")("NEWS_COLLECT_MAX_RUNTIME_SEC") = "600"
ElseIf sessionName = "intraday" Then
    shell.Environment("PROCESS")("NEWS_COLLECT_MAX_RUNTIME_SEC") = "240"
End If
If Not fso.FolderExists("E:\1_Data\2_Logs") Then
    fso.CreateFolder("E:\1_Data\2_Logs")
End If

argText = ""
For i = 0 To args.Count - 1
    argText = argText & " " & Chr(34) & Replace(args.Item(i), Chr(34), Chr(34) & Chr(34)) & Chr(34)
Next

logPath = "E:\1_Data\2_Logs\run_news_pipeline_once_hidden_last.txt"
cmd = "%ComSpec% /c " & Chr(34) & Chr(34) & "E:\1_Data\run_news_pipeline_once.bat" & Chr(34) & argText & " > " & Chr(34) & logPath & Chr(34) & " 2>&1" & Chr(34)
rc = shell.Run(cmd, 0, True)
WScript.Quit rc
