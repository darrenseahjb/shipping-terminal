BEGIN;

CREATE TABLE Ingestion_Batches
(
    batch_id BIGSERIAL PRIMARY KEY,
    generator_name TEXT NOT NULL,
    generator_version TEXT NOT NULL,
    seed_value INT,
    requested_ship_count INT NOT NULL DEFAULT 0 CHECK (requested_ship_count >= 0),
    requested_container_count INT NOT NULL DEFAULT 0 CHECK (requested_container_count >= 0),
    inserted_ship_count INT NOT NULL DEFAULT 0 CHECK (inserted_ship_count >= 0),
    inserted_container_count INT NOT NULL DEFAULT 0 CHECK (inserted_container_count >= 0),
    inserted_schedule_count INT NOT NULL DEFAULT 0 CHECK (inserted_schedule_count >= 0),
    inserted_unload_count INT NOT NULL DEFAULT 0 CHECK (inserted_unload_count >= 0),
    inserted_transfer_count INT NOT NULL DEFAULT 0 CHECK (inserted_transfer_count >= 0),
    inserted_load_count INT NOT NULL DEFAULT 0 CHECK (inserted_load_count >= 0),
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'completed', 'failed')),
    notes TEXT
);


CREATE TABLE Raw_Terminal_Events
(
    raw_event_id BIGINT GENERATED ALWAYS AS IDENTITY,
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL
        CHECK (event_type IN ('SHIP_SCHEDULED', 'CONTAINER_UNLOADED', 'CONTAINER_TRANSFERRED', 'CONTAINER_LOADED')),
    event_time TIMESTAMP NOT NULL,
    event_start_time TIMESTAMP,
    event_end_time TIMESTAMP,
    ship_mmsi CHAR(9),
    berth_id INT,
    container_iso CHAR(11),
    location_bay INT,
    location_row INT,
    location_tier INT,
    start_bay INT,
    start_row INT,
    start_tier INT,
    end_bay INT,
    end_row INT,
    end_tier INT,
    source_mode TEXT NOT NULL
        CHECK (source_mode IN ('backfill', 'stream')),
    source_system TEXT NOT NULL,
    batch_id BIGINT REFERENCES Ingestion_Batches (batch_id),
    processing_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (processing_status IN ('pending', 'processed', 'failed')),
    processed_at TIMESTAMP,
    error_message TEXT,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    PRIMARY KEY (raw_event_id, event_time),
    UNIQUE (event_id, event_time)
) PARTITION BY RANGE (event_time);


CREATE TABLE Dead_Letter_Terminal_Events
(
    dead_letter_id BIGSERIAL PRIMARY KEY,
    raw_event_id BIGINT,
    event_id UUID,
    event_type TEXT NOT NULL,
    batch_id BIGINT,
    source_mode TEXT NOT NULL,
    source_system TEXT NOT NULL,
    failed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT NOT NULL,
    raw_payload JSONB NOT NULL
);


CREATE TABLE Data_Validation_Runs
(
    run_id BIGSERIAL PRIMARY KEY,
    suite_name TEXT NOT NULL,
    environment_name TEXT NOT NULL DEFAULT 'local',
    triggered_by TEXT NOT NULL DEFAULT CURRENT_USER,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'passed', 'failed')),
    total_checks INT NOT NULL DEFAULT 0 CHECK (total_checks >= 0),
    failed_checks INT NOT NULL DEFAULT 0 CHECK (failed_checks >= 0),
    notes TEXT
);


CREATE TABLE Data_Validation_Results
(
    run_id BIGINT NOT NULL REFERENCES Data_Validation_Runs (run_id) ON DELETE CASCADE,
    check_name TEXT NOT NULL,
    layer_name TEXT NOT NULL
        CHECK (layer_name IN ('raw', 'silver', 'gold', 'ops')),
    severity TEXT NOT NULL
        CHECK (severity IN ('error', 'warning')),
    status TEXT NOT NULL
        CHECK (status IN ('passed', 'failed')),
    failing_rows BIGINT NOT NULL DEFAULT 0 CHECK (failing_rows >= 0),
    threshold_value BIGINT NOT NULL DEFAULT 0 CHECK (threshold_value >= 0),
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    checked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, check_name)
);


CREATE OR REPLACE FUNCTION ensure_raw_event_partition(p_month_start DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    partition_name TEXT := format('raw_terminal_events_%s', to_char(p_month_start, 'YYYYMM'));
    next_month DATE := (p_month_start + INTERVAL '1 month')::DATE;
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF Raw_Terminal_Events FOR VALUES FROM (%L) TO (%L)',
        partition_name,
        p_month_start,
        next_month
    );
END;
$$;


CREATE OR REPLACE FUNCTION ensure_raw_event_partitions(p_start_date DATE, p_end_date DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    current_month DATE := date_trunc('month', p_start_date)::DATE;
    final_month DATE := date_trunc('month', p_end_date)::DATE;
BEGIN
    WHILE current_month <= final_month LOOP
        PERFORM ensure_raw_event_partition(current_month);
        current_month := (current_month + INTERVAL '1 month')::DATE;
    END LOOP;
END;
$$;


CREATE INDEX idx_raw_terminal_events_status_time
    ON Raw_Terminal_Events (processing_status, event_time);

CREATE INDEX idx_raw_terminal_events_batch
    ON Raw_Terminal_Events (batch_id, processing_status, event_time);


CREATE OR REPLACE VIEW Container_Event_Stream AS
SELECT
    u.container_ISO AS ISO,
    'UNLOAD'::TEXT AS event_type,
    u.start_time AS event_start_time,
    u.end_time AS event_end_time,
    CONCAT('BERTH-', u.berth_id) AS source_location,
    CONCAT(u.location_bay, '-', u.location_row, '-', u.location_tier) AS destination_location
FROM Unload u

UNION ALL

SELECT
    t.container_ISO AS ISO,
    'TRANSFER'::TEXT AS event_type,
    t.start_time AS event_start_time,
    t.end_time AS event_end_time,
    CONCAT(t.start_bay, '-', t.start_row, '-', t.start_tier) AS source_location,
    CONCAT(t.end_bay, '-', t.end_row, '-', t.end_tier) AS destination_location
FROM Transfer t

UNION ALL

SELECT
    l.container_ISO AS ISO,
    'LOAD'::TEXT AS event_type,
    l.start_time AS event_start_time,
    l.end_time AS event_end_time,
    'YARD' AS source_location,
    CONCAT('BERTH-', l.berth_id) AS destination_location
FROM Load l;


CREATE OR REPLACE VIEW Container_Dwell_Time AS
WITH unloads AS
(
    SELECT
        u.container_ISO,
        u.end_time AS unloaded_at
    FROM Unload u
),
loads AS
(
    SELECT
        l.container_ISO,
        l.start_time AS loaded_at
    FROM Load l
),
matched_dwell AS
(
    SELECT
        u.container_ISO,
        u.unloaded_at,
        MIN(l.loaded_at) AS loaded_at
    FROM unloads u
    LEFT JOIN loads l
      ON l.container_ISO = u.container_ISO
     AND l.loaded_at > u.unloaded_at
    GROUP BY u.container_ISO, u.unloaded_at
)
SELECT
    container_ISO,
    unloaded_at,
    loaded_at,
    loaded_at - unloaded_at AS dwell_time
FROM matched_dwell
WHERE loaded_at IS NOT NULL;


CREATE OR REPLACE VIEW Yard_Heatmap AS
SELECT
    bay,
    row,
    COUNT(*) FILTER (WHERE is_occupied) AS occupied_slots,
    COUNT(*) AS total_slots,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_occupied) / NULLIF(COUNT(*), 0),
        2
    ) AS occupancy_pct
FROM Location
GROUP BY bay, row
ORDER BY bay, row;


CREATE MATERIALIZED VIEW mv_daily_terminal_kpis AS
SELECT
    d.activity_day,
    COALESCE(SUM(CASE WHEN d.activity_type = 'Arrival' THEN d.total_activities END), 0) AS arrivals,
    COALESCE(SUM(CASE WHEN d.activity_type = 'Departure' THEN d.total_activities END), 0) AS departures,
    COALESCE(SUM(CASE WHEN d.activity_type = 'Loading' THEN d.total_activities END), 0) AS loads,
    COALESCE(SUM(CASE WHEN d.activity_type = 'Unloading' THEN d.total_activities END), 0) AS unloads,
    COALESCE(SUM(CASE WHEN d.activity_type = 'Transfer' THEN d.total_activities END), 0) AS transfers,
    COALESCE(SUM(d.total_activities), 0) AS total_tracked_movements
FROM Daily_Port_Movements d
GROUP BY d.activity_day
ORDER BY d.activity_day;


CREATE MATERIALIZED VIEW mv_berth_utilization AS
WITH daily_berth_windows AS
(
    SELECT
        s.berth_id,
        series.window_day::DATE AS activity_day,
        GREATEST(s.arrival_time, series.window_day) AS effective_start,
        LEAST(s.departure_time, series.window_day + INTERVAL '1 day') AS effective_end
    FROM Schedule s
    CROSS JOIN LATERAL generate_series(
        date_trunc('day', s.arrival_time),
        date_trunc('day', s.departure_time),
        INTERVAL '1 day'
    ) AS series(window_day)
)
SELECT
    berth_id,
    activity_day,
    ROUND(
        SUM(EXTRACT(EPOCH FROM (effective_end - effective_start)) / 60.0)::NUMERIC,
        2
    ) AS occupied_minutes,
    ROUND(
        (
            SUM(EXTRACT(EPOCH FROM (effective_end - effective_start)) / 60.0) / 1440.0 * 100.0
        )::NUMERIC,
        2
    ) AS berth_utilization_pct
FROM daily_berth_windows
WHERE effective_end > effective_start
GROUP BY berth_id, activity_day
ORDER BY activity_day, berth_id;


CREATE OR REPLACE VIEW Raw_Container_Lifecycle_Audit AS
WITH ordered_events AS
(
    SELECT
        r.raw_event_id,
        r.batch_id,
        r.processing_status,
        r.container_iso,
        r.event_type,
        r.event_time,
        LAG(r.event_type) OVER
        (
            PARTITION BY r.container_iso
            ORDER BY r.event_time, r.raw_event_id
        ) AS previous_event_type,
        LAG(r.event_time) OVER
        (
            PARTITION BY r.container_iso
            ORDER BY r.event_time, r.raw_event_id
        ) AS previous_event_time
    FROM Raw_Terminal_Events r
    WHERE r.container_iso IS NOT NULL
),
enriched_events AS
(
    SELECT
        o.raw_event_id,
        o.batch_id,
        o.processing_status,
        o.container_iso,
        o.event_type,
        o.event_time,
        o.previous_event_type,
        o.previous_event_time,
        COALESCE(
            o.previous_event_type,
            prior_event.previous_effective_event
        ) AS previous_effective_event
    FROM ordered_events o
    LEFT JOIN LATERAL
    (
        SELECT
            CASE h.action_type
                WHEN 'Unloaded' THEN 'CONTAINER_UNLOADED'
                WHEN 'Transferred' THEN 'CONTAINER_TRANSFERRED'
                WHEN 'Loaded' THEN 'CONTAINER_LOADED'
            END AS previous_effective_event
        FROM Container_Movement_History h
        WHERE h.ISO = o.container_iso
          AND h.action_time < o.event_time
        ORDER BY h.action_time DESC, h.action_type DESC
        LIMIT 1
    ) prior_event ON TRUE
)
SELECT
    raw_event_id,
    batch_id,
    processing_status,
    container_iso,
    event_type,
    event_time,
    previous_effective_event AS previous_event_type,
    previous_event_time,
    CASE
        WHEN event_type = 'CONTAINER_UNLOADED'
         AND previous_effective_event IN ('CONTAINER_UNLOADED', 'CONTAINER_TRANSFERRED')
            THEN 'container_unloaded_while_already_in_yard'
        WHEN event_type = 'CONTAINER_TRANSFERRED'
         AND COALESCE(previous_effective_event, '') NOT IN ('CONTAINER_UNLOADED', 'CONTAINER_TRANSFERRED')
            THEN 'container_transferred_without_yard_presence'
        WHEN event_type = 'CONTAINER_LOADED'
         AND COALESCE(previous_effective_event, '') NOT IN ('CONTAINER_UNLOADED', 'CONTAINER_TRANSFERRED')
            THEN 'container_loaded_without_yard_presence'
    END AS violation_reason
FROM enriched_events
WHERE CASE
    WHEN event_type = 'CONTAINER_UNLOADED'
     AND previous_effective_event IN ('CONTAINER_UNLOADED', 'CONTAINER_TRANSFERRED')
        THEN 'container_unloaded_while_already_in_yard'
    WHEN event_type = 'CONTAINER_TRANSFERRED'
     AND COALESCE(previous_effective_event, '') NOT IN ('CONTAINER_UNLOADED', 'CONTAINER_TRANSFERRED')
        THEN 'container_transferred_without_yard_presence'
    WHEN event_type = 'CONTAINER_LOADED'
     AND COALESCE(previous_effective_event, '') NOT IN ('CONTAINER_UNLOADED', 'CONTAINER_TRANSFERRED')
        THEN 'container_loaded_without_yard_presence'
END IS NOT NULL;


CREATE OR REPLACE VIEW Data_Quality_Dashboard AS
SELECT 'schedule_conflicts' AS check_name, COUNT(*)::BIGINT AS failing_rows
FROM Schedule_Conflict_Check

UNION ALL

SELECT 'load_window_violations' AS check_name, COUNT(*)::BIGINT AS failing_rows
FROM Load_Window_Violation_Check

UNION ALL

SELECT 'unload_window_violations' AS check_name, COUNT(*)::BIGINT AS failing_rows
FROM Unload_Window_Violation_Check

UNION ALL

SELECT 'occupied_target_violations' AS check_name, COUNT(*)::BIGINT AS failing_rows
FROM Occupied_Target_Violation_Check

UNION ALL

SELECT 'location_status_mismatches' AS check_name, COUNT(*)::BIGINT AS failing_rows
FROM
(
    SELECT
        l.bay,
        l.row,
        l.tier
    FROM Location l
    LEFT JOIN
    (
        SELECT
            cs.current_location
        FROM Container_Status cs
        WHERE cs.terminal_status = 'In terminal'
    ) active_locations
      ON active_locations.current_location = CONCAT(l.bay, '-', l.row, '-', l.tier)
    WHERE (l.is_occupied = TRUE AND active_locations.current_location IS NULL)
       OR (l.is_occupied = FALSE AND active_locations.current_location IS NOT NULL)
) mismatches

UNION ALL

SELECT 'raw_lifecycle_violations' AS check_name, COUNT(*)::BIGINT AS failing_rows
FROM Raw_Container_Lifecycle_Audit;


CREATE OR REPLACE VIEW Raw_Event_Queue_Stats AS
SELECT
    processing_status,
    COUNT(*) AS event_count,
    MIN(event_time) AS earliest_event_time,
    MAX(event_time) AS latest_event_time,
    MIN(ingested_at) AS earliest_ingested_at,
    MAX(ingested_at) AS latest_ingested_at
FROM Raw_Terminal_Events
GROUP BY processing_status
ORDER BY processing_status;


CREATE OR REPLACE VIEW Pipeline_Freshness AS
SELECT
    COUNT(*) FILTER (WHERE processing_status = 'pending') AS pending_events,
    COUNT(*) FILTER (WHERE processing_status = 'failed') AS failed_events,
    MAX(event_time) FILTER (WHERE processing_status = 'processed') AS latest_processed_event_time,
    MAX(ingested_at) FILTER (WHERE processing_status = 'processed') AS latest_processed_ingested_at,
    MAX(ingested_at) FILTER (WHERE processing_status = 'pending') AS latest_pending_ingested_at
FROM Raw_Terminal_Events;


CREATE OR REPLACE VIEW Latest_Data_Validation_Run AS
SELECT
    r.run_id,
    r.suite_name,
    r.environment_name,
    r.triggered_by,
    r.started_at,
    r.finished_at,
    r.status,
    r.total_checks,
    r.failed_checks,
    r.notes
FROM Data_Validation_Runs r
ORDER BY r.started_at DESC, r.run_id DESC
LIMIT 1;


CREATE OR REPLACE VIEW Latest_Data_Validation_Results AS
SELECT
    vr.run_id,
    vr.check_name,
    vr.layer_name,
    vr.severity,
    vr.status,
    vr.failing_rows,
    vr.threshold_value,
    vr.details,
    vr.checked_at
FROM Data_Validation_Results vr
JOIN Latest_Data_Validation_Run latest
  ON latest.run_id = vr.run_id
ORDER BY vr.severity DESC, vr.check_name;


CREATE OR REPLACE FUNCTION refresh_terminal_analytics()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_daily_terminal_kpis;
    REFRESH MATERIALIZED VIEW mv_berth_utilization;
END;
$$;

SELECT refresh_terminal_analytics();

COMMIT;
