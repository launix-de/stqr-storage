# SPDX-License-Identifier: GPL-3.0-only
param([string]$Output = (Join-Path $PSScriptRoot 'bin'))
$ErrorActionPreference = 'Stop'
$Output = [System.IO.Path]::GetFullPath($Output)
New-Item -ItemType Directory -Force -Path $Output | Out-Null
Push-Location $PSScriptRoot
try {
    $env:CGO_ENABLED = '0'
    $uiBuild = Join-Path $PSScriptRoot 'ui/bin/published'
    dotnet publish ui/STQR.UI.csproj -c Release -r win-x64 --self-contained true -o $uiBuild
    if ($LASTEXITCODE -ne 0) { throw 'WinUI publish failed' }
    $frontend = Join-Path $uiBuild 'stqr-windows.ui.exe'
    $digest = (Get-FileHash $frontend -Algorithm SHA256).Hash.ToLowerInvariant()
    $assets = Join-Path $PSScriptRoot 'cmd/stqr-windows/assets'
    New-Item -ItemType Directory -Force -Path $assets | Out-Null
    $inputFile = [System.IO.File]::OpenRead($frontend)
    $compressedFile = [System.IO.File]::Create((Join-Path $assets 'winui.exe.gz'))
    $gzip = [System.IO.Compression.GZipStream]::new($compressedFile, [System.IO.Compression.CompressionLevel]::Optimal)
    try { $inputFile.CopyTo($gzip) } finally { $gzip.Dispose(); $compressedFile.Dispose(); $inputFile.Dispose() }
    $source = @"
// SPDX-License-Identifier: GPL-3.0-only
//go:build windows && embedded_ui

package main

import _ "embed"

//go:embed assets/winui.exe.gz
var winUIPayload []byte

func bundledWinUI() ([]byte, string) { return winUIPayload, "$digest" }
"@
    [System.IO.File]::WriteAllText((Join-Path $PSScriptRoot 'cmd/stqr-windows/ui_payload_generated_windows.go'), $source, [System.Text.UTF8Encoding]::new($false))
    go build -tags embedded_ui -buildvcs=false '-ldflags=-H=windowsgui' -o (Join-Path $Output 'stqr-windows.exe') ./cmd/stqr-windows
    if ($LASTEXITCODE -ne 0) { throw 'Windows application build failed' }
    if (!(Test-Path (Join-Path $Output 'stqr-windows.exe'))) { throw 'Missing Windows application' }
} finally { Pop-Location }
