# SPDX-License-Identifier: GPL-3.0-only
param([string]$Output = (Join-Path $PSScriptRoot 'bin'))
$ErrorActionPreference = 'Stop'
$Output = [System.IO.Path]::GetFullPath($Output)
New-Item -ItemType Directory -Force -Path $Output | Out-Null
Push-Location $PSScriptRoot
try {
    $env:CGO_ENABLED = '0'
    go build -buildvcs=false '-ldflags=-H=windowsgui' -o (Join-Path $Output 'stqr-windows.exe') ./cmd/stqr-windows
    if ($LASTEXITCODE -ne 0) { throw 'Go backend build failed' }
    dotnet publish ui/STQR.UI.csproj -c Release -r win-x64 --self-contained true -o $Output
    if ($LASTEXITCODE -ne 0) { throw 'WinUI publish failed' }
    foreach ($name in @('stqr-windows.exe', 'stqr-windows.ui.exe')) {
        if (!(Test-Path (Join-Path $Output $name))) { throw "Missing bundle component: $name" }
    }
} finally { Pop-Location }
