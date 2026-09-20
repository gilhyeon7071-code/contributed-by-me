param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ForwardArgs
)

$py = "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if (-not (Test-Path -LiteralPath $py)) {
    Write-Error "python not found: $py"
    exit 9009
}

& $py @ForwardArgs
exit $LASTEXITCODE

