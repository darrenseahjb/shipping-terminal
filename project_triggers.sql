BEGIN;

CREATE OR REPLACE FUNCTION get_container_latest_event(p_container_iso CHAR(11))
RETURNS TABLE
(
    event_type TEXT,
    event_time TIMESTAMP,
    bay_no INT,
    row_no INT,
    tier_no INT,
    berth_id INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH events AS
    (
        SELECT
            'UNLOAD'::TEXT AS event_type,
            u.end_time AS event_time,
            u.location_bay AS bay,
            u.location_row AS row,
            u.location_tier AS tier,
            u.berth_id
        FROM Unload u
        WHERE u.container_ISO = p_container_iso

        UNION ALL

        SELECT
            'TRANSFER'::TEXT AS event_type,
            t.end_time AS event_time,
            t.end_bay AS bay,
            t.end_row AS row,
            t.end_tier AS tier,
            NULL::INT AS berth_id
        FROM Transfer t
        WHERE t.container_ISO = p_container_iso

        UNION ALL

        SELECT
            'LOAD'::TEXT AS event_type,
            l.end_time AS event_time,
            NULL::INT AS bay,
            NULL::INT AS row,
            NULL::INT AS tier,
            l.berth_id
        FROM Load l
        WHERE l.container_ISO = p_container_iso
    )
    SELECT
        e.event_type,
        e.event_time,
        e.bay,
        e.row,
        e.tier,
        e.berth_id
    FROM events e
    ORDER BY e.event_time DESC, e.event_type DESC
    LIMIT 1;
END;
$$;


CREATE OR REPLACE FUNCTION check_schedule_overlap()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.arrival_time >= NEW.departure_time THEN
        RAISE EXCEPTION 'Arrival time must be earlier than departure time.';
    END IF;

    IF EXISTS
    (
        SELECT 1
        FROM Schedule s
        WHERE s.berth_id = NEW.berth_id
          AND tsrange(s.arrival_time, s.departure_time, '[)') &&
              tsrange(NEW.arrival_time, NEW.departure_time, '[)')
          AND NOT
          (
              TG_OP = 'UPDATE'
              AND s.ship_MMSI = OLD.ship_MMSI
              AND s.berth_id = OLD.berth_id
              AND s.arrival_time = OLD.arrival_time
              AND s.departure_time = OLD.departure_time
          )
    ) THEN
        RAISE EXCEPTION 'Schedule overlap detected for berth %.', NEW.berth_id;
    END IF;

    IF EXISTS
    (
        SELECT 1
        FROM Schedule s
        WHERE s.ship_MMSI = NEW.ship_MMSI
          AND tsrange(s.arrival_time, s.departure_time, '[)') &&
              tsrange(NEW.arrival_time, NEW.departure_time, '[)')
          AND NOT
          (
              TG_OP = 'UPDATE'
              AND s.ship_MMSI = OLD.ship_MMSI
              AND s.berth_id = OLD.berth_id
              AND s.arrival_time = OLD.arrival_time
              AND s.departure_time = OLD.departure_time
          )
    ) THEN
        RAISE EXCEPTION 'Ship % already has an overlapping schedule.', NEW.ship_MMSI;
    END IF;

    RETURN NEW;
END;
$$;


CREATE OR REPLACE FUNCTION check_unload()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    last_event RECORD;
BEGIN
    IF NOT EXISTS
    (
        SELECT 1
        FROM Schedule s
        WHERE s.berth_id = NEW.berth_id
          AND NEW.start_time >= s.arrival_time
          AND NEW.end_time <= s.departure_time
    ) THEN
        RAISE EXCEPTION 'Unload for container % falls outside the berth schedule.', NEW.container_ISO;
    END IF;

    IF EXISTS
    (
        SELECT 1
        FROM Location l
        WHERE l.bay = NEW.location_bay
          AND l.row = NEW.location_row
          AND l.tier = NEW.location_tier
          AND l.is_occupied = TRUE
    ) THEN
        RAISE EXCEPTION 'Unload target location (%, %, %) is already occupied.',
            NEW.location_bay, NEW.location_row, NEW.location_tier;
    END IF;

    SELECT *
    INTO last_event
    FROM get_container_latest_event(NEW.container_ISO);

    IF FOUND AND last_event.event_type IN ('UNLOAD', 'TRANSFER') THEN
        RAISE EXCEPTION 'Container % is already recorded inside the yard.', NEW.container_ISO;
    END IF;

    UPDATE Location
    SET is_occupied = TRUE
    WHERE bay = NEW.location_bay
      AND row = NEW.location_row
      AND tier = NEW.location_tier;

    RETURN NEW;
END;
$$;


CREATE OR REPLACE FUNCTION handle_transfer()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    last_event RECORD;
BEGIN
    SELECT *
    INTO last_event
    FROM get_container_latest_event(NEW.container_ISO);

    IF NOT FOUND OR last_event.event_type NOT IN ('UNLOAD', 'TRANSFER') THEN
        RAISE EXCEPTION 'Container % is not currently in the yard and cannot be transferred.', NEW.container_ISO;
    END IF;

    IF (last_event.bay_no, last_event.row_no, last_event.tier_no) <>
       (NEW.start_bay, NEW.start_row, NEW.start_tier) THEN
        RAISE EXCEPTION 'Transfer start location does not match the latest known location for container %.',
            NEW.container_ISO;
    END IF;

    IF EXISTS
    (
        SELECT 1
        FROM Location l
        WHERE l.bay = NEW.end_bay
          AND l.row = NEW.end_row
          AND l.tier = NEW.end_tier
          AND l.is_occupied = TRUE
    ) THEN
        RAISE EXCEPTION 'Transfer target location (%, %, %) is already occupied.',
            NEW.end_bay, NEW.end_row, NEW.end_tier;
    END IF;

    UPDATE Location
    SET is_occupied = FALSE
    WHERE bay = NEW.start_bay
      AND row = NEW.start_row
      AND tier = NEW.start_tier;

    UPDATE Location
    SET is_occupied = TRUE
    WHERE bay = NEW.end_bay
      AND row = NEW.end_row
      AND tier = NEW.end_tier;

    RETURN NEW;
END;
$$;


CREATE OR REPLACE FUNCTION check_load()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    last_event RECORD;
BEGIN
    SELECT *
    INTO last_event
    FROM get_container_latest_event(NEW.container_ISO);

    IF NOT FOUND OR last_event.event_type NOT IN ('UNLOAD', 'TRANSFER') THEN
        RAISE EXCEPTION 'Container % is not currently in the yard and cannot be loaded.', NEW.container_ISO;
    END IF;

    IF NOT EXISTS
    (
        SELECT 1
        FROM Schedule s
        WHERE s.berth_id = NEW.berth_id
          AND NEW.start_time >= s.arrival_time
          AND NEW.end_time <= s.departure_time
    ) THEN
        RAISE EXCEPTION 'Load for container % falls outside the berth schedule.', NEW.container_ISO;
    END IF;

    UPDATE Location
    SET is_occupied = FALSE
    WHERE bay = last_event.bay_no
      AND row = last_event.row_no
      AND tier = last_event.tier_no;

    RETURN NEW;
END;
$$;


CREATE TRIGGER check_schedule_overlap_trigger
    BEFORE INSERT OR UPDATE ON Schedule
    FOR EACH ROW
    EXECUTE FUNCTION check_schedule_overlap();

CREATE TRIGGER check_unload_trigger
    BEFORE INSERT ON Unload
    FOR EACH ROW
    EXECUTE FUNCTION check_unload();

CREATE TRIGGER handle_transfer_trigger
    BEFORE INSERT ON Transfer
    FOR EACH ROW
    EXECUTE FUNCTION handle_transfer();

CREATE TRIGGER check_load_trigger
    BEFORE INSERT ON Load
    FOR EACH ROW
    EXECUTE FUNCTION check_load();

COMMIT;
