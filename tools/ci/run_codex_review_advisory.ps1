param(
    [string]$DiffRef = "",
    [int]$MaxDiffChars = 60000,
    [int]$MaxContextFileChars = 40000,
    [string]$ReviewOutputPath = "",
    [switch]$HardFailBlocks
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = Resolve-Path (Join-Path $ScriptDir "..\..")
$PromptPath = Join-Path $Root "tools\ci\codex_review_prompt.md"
$ContextBuilder = Join-Path $Root "tools\ci\build_codex_review_context.py"
$ResultParser = Join-Path $Root "tools\ci\parse_codex_review_result.py"

function Resolve-Python {
    $candidates = @(
        "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe",
        "C:\Users\jjtop\AppData\Local\Programs\Python\Python314\python.exe",
        "python"
    )

    foreach ($candidate in $candidates) {
        if ($candidate -eq "python") {
            $cmd = Get-Command python -ErrorAction SilentlyContinue
            if ($null -ne $cmd) {
                return $cmd.Source
            }
        } elseif (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }

    throw "Python executable not found."
}

$Python = Resolve-Python
Set-Location $Root

if ($ReviewOutputPath) {
    if (-not (Test-Path -LiteralPath $ReviewOutputPath)) {
        throw "Review output file not found: $ReviewOutputPath"
    }

    $parserArgs = @($ResultParser)
    if ($HardFailBlocks) {
        $parserArgs += "--hard-fail-blocks"
    }

    Get-Content -LiteralPath $ReviewOutputPath -Raw | & $Python @parserArgs
    exit $LASTEXITCODE
}

Write-Output "PROMPT_PATH=$PromptPath"
Write-Output "CONTEXT_BEGIN"

$builderArgs = @(
    $ContextBuilder,
    "--max-diff-chars",
    $MaxDiffChars,
    "--max-context-file-chars",
    $MaxContextFileChars
)

if ($DiffRef) {
    $builderArgs += "--diff-ref"
    $builderArgs += $DiffRef
}

& $Python @builderArgs
exit $LASTEXITCODE
