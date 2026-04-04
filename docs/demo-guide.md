# Demo Guide

## Walkthrough

1. Start with the relational and trigger-driven terminal model.
2. Show that the project can generate six months of logically valid raw activity.
3. Process raw events into curated operational tables.
4. Run the validation suite and show the stored results.
5. Show operational analytics and capacity signals directly from PostgreSQL views and materialised views.

## Useful Queries

```sql
SELECT * FROM Ingestion_Batches ORDER BY batch_id;
SELECT * FROM Data_Quality_Dashboard ORDER BY check_name;
SELECT * FROM Latest_Data_Validation_Run;
SELECT check_name, status, failing_rows FROM Latest_Data_Validation_Results ORDER BY severity DESC, check_name;
SELECT * FROM Container_Dwell_Time ORDER BY dwell_time DESC LIMIT 20;
SELECT * FROM Yard_Heatmap ORDER BY occupancy_pct DESC, bay, row LIMIT 20;
SELECT * FROM mv_daily_terminal_kpis ORDER BY activity_day;
SELECT * FROM mv_berth_utilization ORDER BY activity_day, berth_id;
```

## Screenshot List

- The local pipeline commands succeeding in order.
- `Latest_Data_Validation_Run` with a passing status.
- `Latest_Data_Validation_Results` showing all checks passed.
- `Yard_Heatmap` showing occupied versus total yard slots.
- `Container_Dwell_Time` showing container timing metrics.
- `mv_daily_terminal_kpis` showing daily activity counts.
- `mv_berth_utilization` showing berth usage over time.

## How To Explain It

- SQL and triggers handle the relational model and integrity rules.
- Python handles synthetic data generation, raw event simulation, and processing.
- Validation results are stored in PostgreSQL instead of being checked manually each time.
- Analytics views and materialised views sit on top of the operational model for reporting.
- The whole setup runs locally, so every part of it is easy to inspect.
