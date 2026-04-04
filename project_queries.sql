BEGIN;

CREATE OR REPLACE VIEW Container_Movement_History AS
SELECT
    u.container_ISO AS ISO,
    u.end_time AS action_time,
    'Unloaded' AS action_type,
    CONCAT('BERTH-', u.berth_id) AS source_location,
    CONCAT(u.location_bay, '-', u.location_row, '-', u.location_tier) AS destination_location
FROM Unload u

UNION ALL

SELECT
    t.container_ISO AS ISO,
    t.end_time AS action_time,
    'Transferred' AS action_type,
    CONCAT(t.start_bay, '-', t.start_row, '-', t.start_tier) AS source_location,
    CONCAT(t.end_bay, '-', t.end_row, '-', t.end_tier) AS destination_location
FROM Transfer t

UNION ALL

SELECT
    l.container_ISO AS ISO,
    l.end_time AS action_time,
    'Loaded' AS action_type,
    'YARD' AS source_location,
    CONCAT('BERTH-', l.berth_id) AS destination_location
FROM Load l;


CREATE OR REPLACE VIEW Container_Status AS
WITH ranked_events AS
(
    SELECT
        h.ISO,
        h.action_time,
        h.action_type,
        h.destination_location,
        ROW_NUMBER() OVER (PARTITION BY h.ISO ORDER BY h.action_time DESC, h.action_type DESC) AS rn
    FROM Container_Movement_History h
)
SELECT
    c.ISO,
    COALESCE(
        CASE
            WHEN r.action_type = 'Loaded' THEN 'Not in terminal'
            WHEN r.action_type IN ('Unloaded', 'Transferred') THEN 'In terminal'
        END,
        'Awaiting first movement'
    ) AS terminal_status,
    r.action_type AS last_action,
    r.action_time AS last_action_time,
    r.destination_location AS current_location
FROM Containers c
LEFT JOIN ranked_events r
    ON c.ISO = r.ISO
   AND r.rn = 1;


CREATE OR REPLACE VIEW Ship_Status AS
WITH active_schedule AS
(
    SELECT
        s.MMSI,
        s.name,
        s.flag,
        sch.berth_id,
        sch.arrival_time,
        sch.departure_time
    FROM Ships s
    LEFT JOIN LATERAL
    (
        SELECT
            sc.berth_id,
            sc.arrival_time,
            sc.departure_time
        FROM Schedule sc
        WHERE sc.ship_MMSI = s.MMSI
          AND CURRENT_TIMESTAMP BETWEEN sc.arrival_time AND sc.departure_time
        ORDER BY sc.arrival_time DESC
        LIMIT 1
    ) sch ON TRUE
)
SELECT
    a.MMSI,
    a.name,
    a.flag,
    a.berth_id,
    a.arrival_time,
    a.departure_time,
    CASE
        WHEN a.berth_id IS NULL THEN 'at sea'
        WHEN EXISTS
        (
            SELECT 1
            FROM Load l
            WHERE l.berth_id = a.berth_id
              AND CURRENT_TIMESTAMP BETWEEN l.start_time AND l.end_time
        )
        AND EXISTS
        (
            SELECT 1
            FROM Unload u
            WHERE u.berth_id = a.berth_id
              AND CURRENT_TIMESTAMP BETWEEN u.start_time AND u.end_time
        ) THEN 'loading/unloading'
        WHEN EXISTS
        (
            SELECT 1
            FROM Load l
            WHERE l.berth_id = a.berth_id
              AND CURRENT_TIMESTAMP BETWEEN l.start_time AND l.end_time
        ) THEN 'loading'
        WHEN EXISTS
        (
            SELECT 1
            FROM Unload u
            WHERE u.berth_id = a.berth_id
              AND CURRENT_TIMESTAMP BETWEEN u.start_time AND u.end_time
        ) THEN 'unloading'
        ELSE 'at berth'
    END AS status
FROM active_schedule a;


CREATE OR REPLACE VIEW Berth_Status AS
SELECT
    b.berth_id,
    active_ship.ship_MMSI,
    active_ship.ship_name,
    active_ship.arrival_time,
    active_ship.departure_time,
    CASE
        WHEN active_ship.ship_MMSI IS NULL THEN 'available'
        ELSE 'occupied'
    END AS berth_status
FROM Berths b
LEFT JOIN LATERAL
(
    SELECT
        sch.ship_MMSI,
        s.name AS ship_name,
        sch.arrival_time,
        sch.departure_time
    FROM Schedule sch
    JOIN Ships s
      ON s.MMSI = sch.ship_MMSI
    WHERE sch.berth_id = b.berth_id
      AND CURRENT_TIMESTAMP BETWEEN sch.arrival_time AND sch.departure_time
    ORDER BY sch.arrival_time DESC
    LIMIT 1
) active_ship ON TRUE;


CREATE OR REPLACE VIEW Location_Status AS
WITH latest_container_positions AS
(
    SELECT
        latest.ISO,
        latest.destination_location,
        latest.action_time
    FROM
    (
        SELECT
            h.*,
            ROW_NUMBER() OVER (PARTITION BY h.ISO ORDER BY h.action_time DESC, h.action_type DESC) AS rn
        FROM Container_Movement_History h
    ) latest
    WHERE latest.rn = 1
      AND latest.action_type IN ('Unloaded', 'Transferred')
)
SELECT
    l.bay,
    l.row,
    l.tier,
    CASE
        WHEN l.is_occupied THEN 'Occupied'
        ELSE 'Vacant'
    END AS status,
    p.ISO AS occupying_container,
    p.action_time AS last_updated_at
FROM Location l
LEFT JOIN latest_container_positions p
  ON p.destination_location = CONCAT(l.bay, '-', l.row, '-', l.tier);


CREATE OR REPLACE VIEW Daily_Port_Movements AS
SELECT
    activity_day,
    activity_type,
    COUNT(*) AS total_activities
FROM
(
    SELECT date_trunc('day', arrival_time)::DATE AS activity_day, 'Arrival' AS activity_type
    FROM Schedule

    UNION ALL

    SELECT date_trunc('day', departure_time)::DATE AS activity_day, 'Departure' AS activity_type
    FROM Schedule

    UNION ALL

    SELECT date_trunc('day', start_time)::DATE AS activity_day, 'Loading' AS activity_type
    FROM Load

    UNION ALL

    SELECT date_trunc('day', start_time)::DATE AS activity_day, 'Unloading' AS activity_type
    FROM Unload

    UNION ALL

    SELECT date_trunc('day', start_time)::DATE AS activity_day, 'Transfer' AS activity_type
    FROM Transfer
) port_activities
GROUP BY activity_day, activity_type
ORDER BY activity_day, activity_type;


CREATE OR REPLACE VIEW Schedule_Conflict_Check AS
SELECT
    s1.berth_id,
    s1.ship_MMSI AS ship_mmsi_1,
    s2.ship_MMSI AS ship_mmsi_2,
    s1.arrival_time AS arrival_time_1,
    s1.departure_time AS departure_time_1,
    s2.arrival_time AS arrival_time_2,
    s2.departure_time AS departure_time_2
FROM Schedule s1
JOIN Schedule s2
  ON s1.berth_id = s2.berth_id
 AND s1.ship_MMSI < s2.ship_MMSI
 AND tsrange(s1.arrival_time, s1.departure_time, '[)') &&
     tsrange(s2.arrival_time, s2.departure_time, '[)');


CREATE OR REPLACE VIEW Load_Window_Violation_Check AS
SELECT
    l.berth_id,
    l.container_ISO,
    l.start_time,
    l.end_time
FROM Load l
WHERE NOT EXISTS
(
    SELECT 1
    FROM Schedule s
    WHERE s.berth_id = l.berth_id
      AND l.start_time >= s.arrival_time
      AND l.end_time <= s.departure_time
);


CREATE OR REPLACE VIEW Unload_Window_Violation_Check AS
SELECT
    u.berth_id,
    u.container_ISO,
    u.start_time,
    u.end_time
FROM Unload u
WHERE NOT EXISTS
(
    SELECT 1
    FROM Schedule s
    WHERE s.berth_id = u.berth_id
      AND u.start_time >= s.arrival_time
      AND u.end_time <= s.departure_time
);


CREATE OR REPLACE VIEW Occupied_Target_Violation_Check AS
WITH location_targets AS
(
    SELECT
        'Unload'::TEXT AS operation_type,
        u.container_ISO,
        u.location_bay AS bay,
        u.location_row AS row,
        u.location_tier AS tier,
        u.start_time,
        u.end_time
    FROM Unload u

    UNION ALL

    SELECT
        'Transfer'::TEXT AS operation_type,
        t.container_ISO,
        t.end_bay AS bay,
        t.end_row AS row,
        t.end_tier AS tier,
        t.start_time,
        t.end_time
    FROM Transfer t
)
SELECT
    a.operation_type AS operation_type_1,
    a.container_ISO AS container_iso_1,
    b.operation_type AS operation_type_2,
    b.container_ISO AS container_iso_2,
    a.bay,
    a.row,
    a.tier,
    a.start_time AS start_time_1,
    a.end_time AS end_time_1,
    b.start_time AS start_time_2,
    b.end_time AS end_time_2
FROM location_targets a
JOIN location_targets b
  ON a.container_ISO < b.container_ISO
 AND a.bay = b.bay
 AND a.row = b.row
 AND a.tier = b.tier
 AND tsrange(a.start_time, a.end_time, '[)') &&
     tsrange(b.start_time, b.end_time, '[)');

COMMIT;
