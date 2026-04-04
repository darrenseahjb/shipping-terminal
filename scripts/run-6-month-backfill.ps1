param(
    [int]$Months = 6,
    [int]$ShipsPerDayMin = 2,
    [int]$ShipsPerDayMax = 4,
    [int]$ContainersMin = 70,
    [int]$ContainersMax = 100,
    [double]$TransferRatio = 0.65,
    [int]$Berths = 10,
    [int]$Bays = 24,
    [int]$Rows = 8,
    [int]$Tiers = 5,
    [int]$FlushSize = 5000,
    [int]$Seed = 20260325,
    [string]$Database = "terminal_ops",
    [int]$Port = 5433,
    [string]$User = "postgres"
)

$ErrorActionPreference = "Stop"
$script = Join-Path $PSScriptRoot "terminal_event_pipeline.py"

python $script `
    --dbname $Database `
    --port $Port `
    --user $User `
    backfill-raw `
    --months $Months `
    --ships-per-day-min $ShipsPerDayMin `
    --ships-per-day-max $ShipsPerDayMax `
    --containers-min $ContainersMin `
    --containers-max $ContainersMax `
    --transfer-ratio $TransferRatio `
    --berths $Berths `
    --bays $Bays `
    --rows $Rows `
    --tiers $Tiers `
    --flush-size $FlushSize `
    --seed $Seed
