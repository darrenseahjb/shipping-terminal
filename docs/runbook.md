# Local Runbook

## Full Option 1 Run

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-option1-local-stack.ps1 -IncludeStream
```

## Manual Flow

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\load-project.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\run-6-month-backfill.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\process-raw-events.ps1 -BatchSize 10000 -RefreshAnalytics
powershell -ExecutionPolicy Bypass -File .\scripts\run-data-validations.ps1
```


## What Success Looks Like

- `Data_Quality_Dashboard` returns zero failing rows.
- `Latest_Data_Validation_Run` returns `status = passed`.
- `Raw_Event_Queue_Stats` shows only `processed` rows after processing.
- analytics views like `Yard_Heatmap`, `Container_Dwell_Time`, and `mv_daily_terminal_kpis` return rows.

## Useful Validation and Analytics Queries

```sql
SELECT * FROM Latest_Data_Validation_Run;
SELECT * FROM Latest_Data_Validation_Results ORDER BY severity DESC, check_name;
SELECT * FROM Data_Quality_Dashboard ORDER BY check_name;
SELECT * FROM Pipeline_Freshness;
SELECT * FROM Yard_Heatmap ORDER BY occupancy_pct DESC, bay, row;
SELECT * FROM mv_daily_terminal_kpis ORDER BY activity_day;
```
