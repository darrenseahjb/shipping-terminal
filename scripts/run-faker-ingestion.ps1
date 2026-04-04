param(
    [int]$Ships = 25,
    [int]$ContainersPerShip = 18,
    [int]$Berths = 8,
    [int]$Bays = 10,
    [int]$Rows = 4,
    [int]$Tiers = 4,
    [int]$Seed = 2002,
    [string]$Database = "terminal_ops",
    [int]$Port = 5433,
    [string]$User = "postgres"
)

$ErrorActionPreference = "Stop"
$script = Join-Path $PSScriptRoot "generate_faker_data.py"

python $script `
    --ships $Ships `
    --containers-per-ship $ContainersPerShip `
    --berths $Berths `
    --bays $Bays `
    --rows $Rows `
    --tiers $Tiers `
    --seed $Seed `
    --dbname $Database `
    --port $Port `
    --user $User
