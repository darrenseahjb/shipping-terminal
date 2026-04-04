param(
    [string]$SuiteName = "option1_local_quality_gate",
    [string]$EnvironmentName = "local",
    [string]$TriggeredBy = "codex",
    [switch]$FailOnWarning,
    [int]$MaxPendingAgeMinutes = 30,
    [int]$MaxPendingRawEvents = 0,
    [string]$Database = "terminal_ops",
    [int]$Port = 5433,
    [string]$User = "postgres"
)

$ErrorActionPreference = "Stop"
$script = Join-Path $PSScriptRoot "run_data_validations.py"

$argsList = @(
    $script,
    "--dbname", $Database,
    "--port", $Port,
    "--user", $User,
    "--suite-name", $SuiteName,
    "--environment-name", $EnvironmentName,
    "--triggered-by", $TriggeredBy,
    "--max-pending-age-minutes", $MaxPendingAgeMinutes,
    "--max-pending-raw-events", $MaxPendingRawEvents
)

if ($FailOnWarning) {
    $argsList += "--fail-on-warning"
}

python @argsList
