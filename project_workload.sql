BEGIN;

INSERT INTO Berths (berth_id)
VALUES (6);

INSERT INTO Ships (MMSI, name, flag, length, width)
VALUES ('741258963', 'DEMO TERMINAL RUN', 'Singapore', 278.50, 40.20);

INSERT INTO Containers (ISO)
VALUES ('DEMU1234567');

INSERT INTO Location (bay, row, tier)
VALUES (9, 1, 1);

INSERT INTO Schedule (ship_MMSI, berth_id, arrival_time, departure_time)
VALUES
(
    '741258963',
    6,
    CURRENT_TIMESTAMP - INTERVAL '3 hours',
    CURRENT_TIMESTAMP + INTERVAL '5 hours'
);

UPDATE Schedule
SET departure_time = departure_time + INTERVAL '30 minutes'
WHERE ship_MMSI = '741258963'
  AND berth_id = 6;

INSERT INTO Unload (berth_id, container_ISO, location_bay, location_row, location_tier, start_time, end_time)
VALUES
(
    6,
    'DEMU1234567',
    9,
    1,
    1,
    CURRENT_TIMESTAMP - INTERVAL '2 hours',
    CURRENT_TIMESTAMP - INTERVAL '1 hour 40 minutes'
);

INSERT INTO Transfer (container_ISO, start_bay, start_row, start_tier, end_bay, end_row, end_tier, start_time, end_time)
VALUES
(
    'DEMU1234567',
    9,
    1,
    1,
    4,
    3,
    3,
    CURRENT_TIMESTAMP - INTERVAL '90 minutes',
    CURRENT_TIMESTAMP - INTERVAL '75 minutes'
);

INSERT INTO Load (berth_id, container_ISO, start_time, end_time)
VALUES
(
    6,
    'DEMU1234567',
    CURRENT_TIMESTAMP - INTERVAL '30 minutes',
    CURRENT_TIMESTAMP - INTERVAL '5 minutes'
);

SELECT * FROM Ship_Status WHERE MMSI = '741258963';
SELECT * FROM Container_Status WHERE ISO = 'DEMU1234567';
SELECT * FROM Location_Status WHERE bay IN (4, 9) AND row IN (1, 3) AND tier IN (1, 3);
SELECT * FROM Daily_Port_Movements WHERE activity_day = CURRENT_DATE;
SELECT * FROM Data_Quality_Dashboard ORDER BY check_name;

DELETE FROM Load
WHERE berth_id = 6
  AND container_ISO = 'DEMU1234567';

DELETE FROM Transfer
WHERE container_ISO = 'DEMU1234567';

DELETE FROM Unload
WHERE berth_id = 6
  AND container_ISO = 'DEMU1234567';

DELETE FROM Schedule
WHERE ship_MMSI = '741258963'
  AND berth_id = 6;

DELETE FROM Containers
WHERE ISO = 'DEMU1234567';

DELETE FROM Ships
WHERE MMSI = '741258963';

DELETE FROM Berths
WHERE berth_id = 6;

DELETE FROM Location
WHERE bay = 9
  AND row = 1
  AND tier = 1;

ROLLBACK;
