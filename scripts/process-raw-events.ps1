param(
    [int]$BatchSize = 5000,
    [switch]$RefreshAnalytics,
    [string]$Database = "terminal_ops",
    [int]$Port = 5433,
    [string]$User = "postgres"
)

$ErrorActionPreference = "Stop"
$script = Join-Path $PSScriptRoot "terminal_event_pipeline.py"

$argsList = @(
    $script,
    "--dbname", $Database,
    "--port", $Port,
    "--user", $User,
    "process-raw",
    "--batch-size", $BatchSize
)

if ($RefreshAnalytics) {
    $argsList += "--refresh-analytics"
}

python @argsList
