param(
    [int]$Hours = 24,
    [int]$ShipsPerDayMin = 1,
    [int]$ShipsPerDayMax = 2,
    [int]$ContainersMin = 30,
    [int]$ContainersMax = 60,
    [double]$TransferRatio = 0.55,
    [int]$Berths = 10,
    [int]$Bays = 24,
    [int]$Rows = 8,
    [int]$Tiers = 5,
    [int]$FlushSize = 250,
    [double]$SleepSeconds = 0.5,
    [int]$Seed = 20260326,
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
    stream-producer `
    --hours $Hours `
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
    --sleep-seconds $SleepSeconds `
    --seed $Seed
