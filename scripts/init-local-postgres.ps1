param(
    [string]$DataDir = (Join-Path (Split-Path $PSScriptRoot -Parent) ".postgres\data"),
    [string]$LogDir = (Join-Path (Split-Path $PSScriptRoot -Parent) ".postgres\logs"),
    [int]$Port = 5433,
    [string]$Database = "terminal_ops",
    [string]$User = "postgres"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}

if (-not (Test-Path (Join-Path $DataDir "PG_VERSION"))) {
    New-Item -ItemType Directory -Force -Path (Split-Path $DataDir -Parent) | Out-Null
    initdb -D $DataDir -U $User -A trust -E UTF8
}

$ready = pg_isready -p $Port 2>$null
if ($LASTEXITCODE -ne 0) {
    pg_ctl -D $DataDir -l (Join-Path $LogDir "postgres.log") -o "-p $Port" start
    Start-Sleep -Seconds 2
}

$dbExists = psql -p $Port -U $User -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$Database';"
if ($dbExists.Trim() -ne "1") {
    createdb -p $Port -U $User $Database
}

Write-Host "Local PostgreSQL is ready on localhost:$Port with database '$Database'."
