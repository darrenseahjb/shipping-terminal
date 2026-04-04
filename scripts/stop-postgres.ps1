param(
    [string]$DataDir = (Join-Path (Split-Path $PSScriptRoot -Parent) ".postgres\data")
)

$ErrorActionPreference = "Stop"

pg_ctl -D $DataDir stop
