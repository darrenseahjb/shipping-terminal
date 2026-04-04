param(
    [string]$Database = "terminal_ops",
    [int]$Port = 5433,
    [string]$User = "postgres"
)

$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent

$files = @(
    "project_clean.sql",
    "project_schema.sql",
    "project_triggers.sql",
    "project_data.sql",
    "project_queries.sql",
    "project_data_engineering.sql"
)

foreach ($file in $files) {
    $path = Join-Path $root $file
    Write-Host "Running $file"
    psql -p $Port -U $User -d $Database -v ON_ERROR_STOP=1 -f $path
    if ($LASTEXITCODE -ne 0) {
        throw "Failed while running $file"
    }
}

Write-Host "Project database loaded successfully."
