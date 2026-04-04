param(
    [string]$DataDir = (Join-Path (Split-Path $PSScriptRoot -Parent) ".postgres\data"),
    [string]$LogDir = (Join-Path (Split-Path $PSScriptRoot -Parent) ".postgres\logs"),
    [int]$Port = 5433
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}

pg_ctl -D $DataDir -l (Join-Path $LogDir "postgres.log") -o "-p $Port" start
