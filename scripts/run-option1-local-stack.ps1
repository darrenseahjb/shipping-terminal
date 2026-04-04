param(
    [switch]$IncludeStream,
    [int]$BackfillBatchSize = 10000,
    [int]$StreamBatchSize = 1000
)

$ErrorActionPreference = "Stop"

& (Join-Path $PSScriptRoot "load-project.ps1")
& (Join-Path $PSScriptRoot "run-6-month-backfill.ps1")
& (Join-Path $PSScriptRoot "process-raw-events.ps1") -BatchSize $BackfillBatchSize -RefreshAnalytics
& (Join-Path $PSScriptRoot "run-data-validations.ps1") -TriggeredBy "option1_local_stack"

if ($IncludeStream) {
    & (Join-Path $PSScriptRoot "run-stream-simulator.ps1")
    & (Join-Path $PSScriptRoot "process-raw-events.ps1") -BatchSize $StreamBatchSize -RefreshAnalytics
    & (Join-Path $PSScriptRoot "run-data-validations.ps1") -TriggeredBy "option1_local_stack"
}
