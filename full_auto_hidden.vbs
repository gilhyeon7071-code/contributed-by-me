Option Explicit

' STOC_FullAuto(매일 08:30) 를 창 없이 실행한다 (PLANS 2026-08-21 (33)).
' 기존에는 작업이 full_auto.bat 을 직접 실행해 콘솔 창이 16~75분간 떠 있었고,
' 2026-08-21 08:40:47 에 STATUS_CONTROL_C_EXIT 로 배치가 끊겼다.
' 저녁 배치(VIBE_Paper_Daily -> run_paper_daily_hidden.vbs)와 동일한 방식으로 맞춘다.
' 실패가 눈에 안 띄는 대가는 run_daily_auto_sync.ps1 의 알림과 소요시간 검사가 대신 잡는다.

Dim shell, fso, cmd, logPath, rc
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

shell.CurrentDirectory = "E:\1_Data"
If Not fso.FolderExists("E:\1_Data\2_Logs") Then
    fso.CreateFolder("E:\1_Data\2_Logs")
End If

logPath = "E:\1_Data\2_Logs\full_auto_hidden_last.txt"
cmd = "%ComSpec% /c " & Chr(34) & Chr(34) & "E:\1_Data\full_auto.bat" & Chr(34) & " > " & Chr(34) & logPath & Chr(34) & " 2>&1" & Chr(34)
rc = shell.Run(cmd, 0, True)
WScript.Quit rc
