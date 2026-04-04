BEGIN;

DROP MATERIALIZED VIEW IF EXISTS mv_daily_terminal_kpis;
DROP MATERIALIZED VIEW IF EXISTS mv_berth_utilization;

DROP VIEW IF EXISTS Data_Quality_Dashboard;
DROP VIEW IF EXISTS Pipeline_Freshness;
DROP VIEW IF EXISTS Raw_Event_Queue_Stats;
DROP VIEW IF EXISTS Raw_Container_Lifecycle_Audit;
DROP VIEW IF EXISTS Latest_Data_Validation_Results;
DROP VIEW IF EXISTS Latest_Data_Validation_Run;
DROP VIEW IF EXISTS Yard_Heatmap;
DROP VIEW IF EXISTS Container_Dwell_Time;
DROP VIEW IF EXISTS Container_Event_Stream;
DROP VIEW IF EXISTS Occupied_Target_Violation_Check;
DROP VIEW IF EXISTS Unload_Window_Violation_Check;
DROP VIEW IF EXISTS Load_Window_Violation_Check;
DROP VIEW IF EXISTS Schedule_Conflict_Check;
DROP VIEW IF EXISTS Daily_Port_Movements;
DROP VIEW IF EXISTS Location_Status;
DROP VIEW IF EXISTS Berth_Status;
DROP VIEW IF EXISTS Ship_Status;
DROP VIEW IF EXISTS Container_Status;
DROP VIEW IF EXISTS Container_Movement_History;

DROP TRIGGER IF EXISTS check_load_trigger ON Load;
DROP TRIGGER IF EXISTS handle_transfer_trigger ON Transfer;
DROP TRIGGER IF EXISTS check_unload_trigger ON Unload;
DROP TRIGGER IF EXISTS check_schedule_overlap_trigger ON Schedule;

DROP FUNCTION IF EXISTS refresh_terminal_analytics();
DROP FUNCTION IF EXISTS ensure_raw_event_partitions(DATE, DATE);
DROP FUNCTION IF EXISTS ensure_raw_event_partition(DATE);
DROP FUNCTION IF EXISTS check_load();
DROP FUNCTION IF EXISTS handle_transfer();
DROP FUNCTION IF EXISTS check_unload();
DROP FUNCTION IF EXISTS check_schedule_overlap();
DROP FUNCTION IF EXISTS get_container_latest_event(CHAR(11));

DROP TABLE IF EXISTS Dead_Letter_Terminal_Events;
DROP TABLE IF EXISTS Raw_Terminal_Events;
DROP TABLE IF EXISTS Data_Validation_Results;
DROP TABLE IF EXISTS Data_Validation_Runs;
DROP TABLE IF EXISTS Ingestion_Batches;
DROP TABLE IF EXISTS Transfer;
DROP TABLE IF EXISTS Unload;
DROP TABLE IF EXISTS Load;
DROP TABLE IF EXISTS Schedule;
DROP TABLE IF EXISTS Location;
DROP TABLE IF EXISTS Containers;
DROP TABLE IF EXISTS Berths;
DROP TABLE IF EXISTS Ships;

COMMIT;
