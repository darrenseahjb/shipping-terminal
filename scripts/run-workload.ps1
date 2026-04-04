param(
    [string]$Database = "terminal_ops",
    [int]$Port = 5433,
    [string]$User = "postgres"
)

$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent

psql -p $Port -U $User -d $Database -v ON_ERROR_STOP=1 -f (Join-Path $root "project_workload.sql")
