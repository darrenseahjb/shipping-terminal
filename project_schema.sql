BEGIN;

CREATE TABLE Ships
(
    MMSI CHAR(9) PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    flag VARCHAR(128) NOT NULL,
    length NUMERIC(8, 2) NOT NULL CHECK (length > 0),
    width NUMERIC(8, 2) NOT NULL CHECK (width > 0),
    CHECK (MMSI ~ '^[0-9]{9}$')
);


CREATE TABLE Berths
(
    berth_id INT PRIMARY KEY,
    CHECK (berth_id > 0)
);


CREATE TABLE Containers
(
    ISO CHAR(11) PRIMARY KEY,
    CHECK (ISO ~ '^[A-Z]{4}[0-9]{7}$')
);


CREATE TABLE Location
(
    bay INT NOT NULL,
    row INT NOT NULL,
    tier INT NOT NULL,
    is_occupied BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (bay, row, tier),
    CHECK (bay > 0),
    CHECK (row > 0),
    CHECK (tier > 0)
);


CREATE TABLE Schedule
(
    ship_MMSI CHAR(9) NOT NULL,
    berth_id INT NOT NULL,
    arrival_time TIMESTAMP NOT NULL,
    departure_time TIMESTAMP NOT NULL,
    source_event_id UUID UNIQUE,
    PRIMARY KEY (ship_MMSI, berth_id, arrival_time, departure_time),
    FOREIGN KEY (ship_MMSI) REFERENCES Ships (MMSI),
    FOREIGN KEY (berth_id) REFERENCES Berths (berth_id),
    CHECK (arrival_time < departure_time)
);


CREATE TABLE Load
(
    berth_id INT NOT NULL,
    container_ISO CHAR(11) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    source_event_id UUID UNIQUE,
    PRIMARY KEY (berth_id, container_ISO, start_time, end_time),
    FOREIGN KEY (berth_id) REFERENCES Berths (berth_id),
    FOREIGN KEY (container_ISO) REFERENCES Containers (ISO),
    CHECK (start_time < end_time)
);


CREATE TABLE Unload
(
    berth_id INT NOT NULL,
    container_ISO CHAR(11) NOT NULL,
    location_bay INT NOT NULL,
    location_row INT NOT NULL,
    location_tier INT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    source_event_id UUID UNIQUE,
    PRIMARY KEY (berth_id, container_ISO, location_bay, location_row, location_tier, start_time, end_time),
    FOREIGN KEY (berth_id) REFERENCES Berths (berth_id),
    FOREIGN KEY (container_ISO) REFERENCES Containers (ISO),
    FOREIGN KEY (location_bay, location_row, location_tier) REFERENCES Location (bay, row, tier),
    CHECK (start_time < end_time)
);


CREATE TABLE Transfer
(
    container_ISO CHAR(11) NOT NULL,
    start_bay INT NOT NULL,
    start_row INT NOT NULL,
    start_tier INT NOT NULL,
    end_bay INT NOT NULL,
    end_row INT NOT NULL,
    end_tier INT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    source_event_id UUID UNIQUE,
    PRIMARY KEY (container_ISO, start_bay, start_row, start_tier, end_bay, end_row, end_tier, start_time, end_time),
    FOREIGN KEY (container_ISO) REFERENCES Containers (ISO),
    FOREIGN KEY (start_bay, start_row, start_tier) REFERENCES Location (bay, row, tier),
    FOREIGN KEY (end_bay, end_row, end_tier) REFERENCES Location (bay, row, tier),
    CHECK (start_time < end_time),
    CHECK ((start_bay, start_row, start_tier) <> (end_bay, end_row, end_tier))
);


CREATE INDEX idx_schedule_berth_window
    ON Schedule (berth_id, arrival_time, departure_time);

CREATE INDEX idx_schedule_ship_window
    ON Schedule (ship_MMSI, arrival_time, departure_time);

CREATE INDEX idx_load_container_end_time
    ON Load (container_ISO, end_time DESC);

CREATE INDEX idx_load_berth_window
    ON Load (berth_id, start_time, end_time);

CREATE INDEX idx_unload_container_end_time
    ON Unload (container_ISO, end_time DESC);

CREATE INDEX idx_unload_berth_window
    ON Unload (berth_id, start_time, end_time);

CREATE INDEX idx_unload_location
    ON Unload (location_bay, location_row, location_tier, end_time DESC);

CREATE INDEX idx_transfer_container_end_time
    ON Transfer (container_ISO, end_time DESC);

CREATE INDEX idx_transfer_start_location
    ON Transfer (start_bay, start_row, start_tier, end_time DESC);

CREATE INDEX idx_transfer_end_location
    ON Transfer (end_bay, end_row, end_tier, end_time DESC);

COMMIT;
