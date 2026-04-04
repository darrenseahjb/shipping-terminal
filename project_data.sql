BEGIN;

INSERT INTO Ships (MMSI, name, flag, length, width) VALUES
    ('563159100', 'PACIFIC NINGBO', 'Singapore', 129.57, 20.83),
    ('356938000', 'MSC BEATRICE', 'Panama', 366.00, 51.20),
    ('477416700', 'CMA CGM ARGON', 'Hong Kong', 299.90, 48.20),
    ('258784000', 'MAERSK EINDHOVEN', 'Denmark', 353.00, 53.50),
    ('636021984', 'HAPAG-LLOYD DUBAI', 'Liberia', 320.00, 48.80),
    ('373119000', 'OOCL SAVANNAH', 'Panama', 323.00, 48.80)
ON CONFLICT (MMSI) DO NOTHING;

INSERT INTO Berths (berth_id)
SELECT berth_id
FROM generate_series(1, 5) AS berth_id
ON CONFLICT (berth_id) DO NOTHING;

INSERT INTO Containers (ISO) VALUES
    ('MSCU5285725'),
    ('CMAU1133557'),
    ('MAEU2468246'),
    ('OOLU1357913'),
    ('HLCU4455662'),
    ('TGHU9090901'),
    ('YMLU3344558'),
    ('KKFU2211334'),
    ('EISU2223334'),
    ('SUDU4567890'),
    ('TRHU1122334'),
    ('FSCU9988776')
ON CONFLICT (ISO) DO NOTHING;

INSERT INTO Location (bay, row, tier)
SELECT bay, row, tier
FROM generate_series(1, 4) AS bay
CROSS JOIN generate_series(1, 3) AS row
CROSS JOIN generate_series(1, 3) AS tier
ON CONFLICT (bay, row, tier) DO NOTHING;

INSERT INTO Schedule (ship_MMSI, berth_id, arrival_time, departure_time) VALUES
    ('563159100', 1, CURRENT_TIMESTAMP - INTERVAL '4 hours', CURRENT_TIMESTAMP + INTERVAL '8 hours'),
    ('356938000', 2, CURRENT_TIMESTAMP - INTERVAL '2 days', CURRENT_TIMESTAMP - INTERVAL '1 day 12 hours'),
    ('477416700', 3, CURRENT_TIMESTAMP + INTERVAL '1 day', CURRENT_TIMESTAMP + INTERVAL '1 day 10 hours'),
    ('258784000', 4, CURRENT_TIMESTAMP - INTERVAL '1 hour', CURRENT_TIMESTAMP + INTERVAL '7 hours'),
    ('636021984', 5, CURRENT_TIMESTAMP - INTERVAL '4 days', CURRENT_TIMESTAMP - INTERVAL '3 days 10 hours'),
    ('373119000', 2, CURRENT_TIMESTAMP + INTERVAL '2 days', CURRENT_TIMESTAMP + INTERVAL '2 days 12 hours')
ON CONFLICT DO NOTHING;

INSERT INTO Unload (berth_id, container_ISO, location_bay, location_row, location_tier, start_time, end_time) VALUES
    (1, 'MSCU5285725', 1, 1, 1, CURRENT_TIMESTAMP - INTERVAL '100 minutes', CURRENT_TIMESTAMP - INTERVAL '85 minutes'),
    (1, 'CMAU1133557', 1, 1, 2, CURRENT_TIMESTAMP - INTERVAL '35 minutes', CURRENT_TIMESTAMP - INTERVAL '20 minutes'),
    (1, 'TRHU1122334', 1, 3, 1, CURRENT_TIMESTAMP - INTERVAL '3 hours', CURRENT_TIMESTAMP - INTERVAL '2 hours 45 minutes'),
    (2, 'OOLU1357913', 2, 2, 1, CURRENT_TIMESTAMP - INTERVAL '1 day 22 hours', CURRENT_TIMESTAMP - INTERVAL '1 day 21 hours 45 minutes'),
    (5, 'FSCU9988776', 3, 3, 1, CURRENT_TIMESTAMP - INTERVAL '3 days 23 hours', CURRENT_TIMESTAMP - INTERVAL '3 days 22 hours 30 minutes'),
    (4, 'YMLU3344558', 2, 1, 1, CURRENT_TIMESTAMP - INTERVAL '10 minutes', CURRENT_TIMESTAMP + INTERVAL '20 minutes')
ON CONFLICT DO NOTHING;

INSERT INTO Transfer (container_ISO, start_bay, start_row, start_tier, end_bay, end_row, end_tier, start_time, end_time) VALUES
    ('MSCU5285725', 1, 1, 1, 1, 2, 1, CURRENT_TIMESTAMP - INTERVAL '70 minutes', CURRENT_TIMESTAMP - INTERVAL '55 minutes'),
    ('OOLU1357913', 2, 2, 1, 3, 1, 1, CURRENT_TIMESTAMP - INTERVAL '1 day 20 hours', CURRENT_TIMESTAMP - INTERVAL '1 day 19 hours 40 minutes'),
    ('FSCU9988776', 3, 3, 1, 4, 1, 1, CURRENT_TIMESTAMP - INTERVAL '3 days 21 hours', CURRENT_TIMESTAMP - INTERVAL '3 days 20 hours 30 minutes')
ON CONFLICT DO NOTHING;

INSERT INTO Load (berth_id, container_ISO, start_time, end_time) VALUES
    (1, 'TRHU1122334', CURRENT_TIMESTAMP - INTERVAL '5 minutes', CURRENT_TIMESTAMP + INTERVAL '25 minutes'),
    (2, 'OOLU1357913', CURRENT_TIMESTAMP - INTERVAL '1 day 18 hours', CURRENT_TIMESTAMP - INTERVAL '1 day 17 hours 30 minutes')
ON CONFLICT DO NOTHING;

COMMIT;
