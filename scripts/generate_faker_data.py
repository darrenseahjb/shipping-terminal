from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from datetime import datetime, timedelta

import psycopg2
from faker import Faker
from psycopg2.extras import execute_values


CARRIERS = [
    "MSC",
    "MAERSK",
    "CMA CGM",
    "HAPAG-LLOYD",
    "OOCL",
    "ONE",
    "EVERGREEN",
    "YANG MING",
    "ZIM",
    "COSCO",
]

FLAGS = [
    "Singapore",
    "Panama",
    "Liberia",
    "Hong Kong",
    "Denmark",
    "Marshall Islands",
    "Malta",
]

OWNER_PREFIXES = [
    "MSCU",
    "MAEU",
    "OOLU",
    "CMAU",
    "HLCU",
    "YMLU",
    "EISU",
    "FSCU",
    "TGHU",
    "TRHU",
    "KKFU",
    "SUDU",
]


@dataclass
class Config:
    host: str
    port: int
    dbname: str
    user: str
    password: str | None
    ships: int
    containers_per_ship: int
    berths: int
    bays: int
    rows: int
    tiers: int
    seed: int


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Generate scalable synthetic container terminal data.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5433)
    parser.add_argument("--dbname", default="terminal_ops")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default=None)
    parser.add_argument("--ships", type=int, default=25)
    parser.add_argument("--containers-per-ship", type=int, default=18)
    parser.add_argument("--berths", type=int, default=8)
    parser.add_argument("--bays", type=int, default=10)
    parser.add_argument("--rows", type=int, default=4)
    parser.add_argument("--tiers", type=int, default=4)
    parser.add_argument("--seed", type=int, default=2002)
    args = parser.parse_args()
    return Config(**vars(args))


def connect(config: Config):
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    )


def fetch_existing_ids(cur):
    cur.execute("SELECT MMSI FROM Ships")
    ships = {row[0].strip() for row in cur.fetchall()}

    cur.execute("SELECT ISO FROM Containers")
    containers = {row[0].strip() for row in cur.fetchall()}
    return ships, containers


def ensure_capacity(cur, config: Config):
    cur.execute(
        """
        INSERT INTO Berths (berth_id)
        SELECT berth_id
        FROM generate_series(1, %s) AS berth_id
        ON CONFLICT (berth_id) DO NOTHING
        """,
        (config.berths,),
    )

    cur.execute(
        """
        INSERT INTO Location (bay, row, tier)
        SELECT bay, row, tier
        FROM generate_series(1, %s) AS bay
        CROSS JOIN generate_series(1, %s) AS row
        CROSS JOIN generate_series(1, %s) AS tier
        ON CONFLICT (bay, row, tier) DO NOTHING
        """,
        (config.bays, config.rows, config.tiers),
    )


def next_mmsi(rng: random.Random, existing: set[str]) -> str:
    while True:
        value = "".join(str(rng.randint(0, 9)) for _ in range(9))
        if value not in existing:
            existing.add(value)
            return value


def next_container_iso(rng: random.Random, existing: set[str]) -> str:
    while True:
        value = f"{rng.choice(OWNER_PREFIXES)}{rng.randint(0, 9_999_999):07d}"
        if value not in existing:
            existing.add(value)
            return value


def ship_name(fake: Faker, rng: random.Random) -> str:
    return f"{rng.choice(CARRIERS)} {fake.city().upper()}"


def insert_batch_start(cur, config: Config) -> int:
    cur.execute(
        """
        INSERT INTO Ingestion_Batches
        (
            generator_name,
            generator_version,
            seed_value,
            requested_ship_count,
            requested_container_count
        )
        VALUES (%s, %s, %s, %s, %s)
        RETURNING batch_id
        """,
        (
            "scripts/generate_faker_data.py",
            "1.0.0",
            config.seed,
            config.ships,
            config.ships * config.containers_per_ship,
        ),
    )
    return cur.fetchone()[0]


def fetch_berths(cur) -> list[int]:
    cur.execute("SELECT berth_id FROM Berths ORDER BY berth_id")
    return [row[0] for row in cur.fetchall()]


def fetch_all_locations(cur) -> list[tuple[int, int, int]]:
    cur.execute(
        """
        SELECT bay, row, tier
        FROM Location
        WHERE is_occupied = FALSE
        ORDER BY bay, row, tier
        """
    )
    return [tuple(row) for row in cur.fetchall()]


def fetch_historical_anchor(cur) -> datetime:
    cur.execute("SELECT COALESCE(MIN(arrival_time), LOCALTIMESTAMP) FROM Schedule")
    anchor = cur.fetchone()[0]
    if anchor.tzinfo is not None:
        anchor = anchor.replace(tzinfo=None)
    return anchor - timedelta(days=180)


def choose_available_location(
    rng: random.Random,
    location_available_at: dict[tuple[int, int, int], datetime],
    event_time: datetime,
    exclude: tuple[int, int, int] | None = None,
) -> tuple[int, int, int]:
    candidates = [
        location
        for location, available_at in location_available_at.items()
        if available_at <= event_time and location != exclude
    ]
    if not candidates:
        raise RuntimeError("No yard location is available for the requested historical event time.")
    return rng.choice(candidates)


def main():
    config = parse_args()
    rng = random.Random(config.seed)
    fake = Faker()
    Faker.seed(config.seed)

    conn = connect(config)
    conn.autocommit = False
    cur = conn.cursor()

    batch_id = insert_batch_start(cur, config)
    conn.commit()

    inserted_ship_count = 0
    inserted_container_count = 0
    inserted_schedule_count = 0
    inserted_unload_count = 0
    inserted_transfer_count = 0
    inserted_load_count = 0

    try:
        ensure_capacity(cur, config)

        existing_ships, existing_containers = fetch_existing_ids(cur)

        ships = []
        containers_by_ship: dict[str, list[str]] = {}
        all_containers = []

        for _ in range(config.ships):
            mmsi = next_mmsi(rng, existing_ships)
            ships.append(
                (
                    mmsi,
                    ship_name(fake, rng),
                    rng.choice(FLAGS),
                    round(rng.uniform(180.0, 400.0), 2),
                    round(rng.uniform(28.0, 60.0), 2),
                )
            )
            container_ids = [
                next_container_iso(rng, existing_containers)
                for _ in range(config.containers_per_ship)
            ]
            containers_by_ship[mmsi] = container_ids
            all_containers.extend((container_id,) for container_id in container_ids)

        execute_values(
            cur,
            """
            INSERT INTO Ships (MMSI, name, flag, length, width)
            VALUES %s
            """,
            ships,
        )
        inserted_ship_count = len(ships)

        execute_values(
            cur,
            """
            INSERT INTO Containers (ISO)
            VALUES %s
            """,
            all_containers,
        )
        inserted_container_count = len(all_containers)

        berth_ids = fetch_berths(cur)
        historical_anchor = fetch_historical_anchor(cur)
        all_locations = fetch_all_locations(cur)
        location_available_at = {
            location: historical_anchor - timedelta(days=1)
            for location in all_locations
        }
        next_available = {
            berth_id: historical_anchor + timedelta(hours=rng.randint(0, 12))
            for berth_id in berth_ids
        }

        for ship_mmsi, _, _, _, _ in ships:
            berth_id = min(next_available, key=next_available.get)
            arrival_time = next_available[berth_id] + timedelta(hours=rng.randint(2, 8))
            stay_hours = max(18, min(96, int(config.containers_per_ship * 1.75) + rng.randint(6, 12)))
            departure_time = arrival_time + timedelta(hours=stay_hours)
            next_available[berth_id] = departure_time + timedelta(hours=rng.randint(3, 8))

            cur.execute(
                """
                INSERT INTO Schedule (ship_MMSI, berth_id, arrival_time, departure_time)
                VALUES (%s, %s, %s, %s)
                """,
                (ship_mmsi, berth_id, arrival_time, departure_time),
            )
            inserted_schedule_count += 1

            event_cursor = arrival_time + timedelta(minutes=30)

            for container_iso in containers_by_ship[ship_mmsi]:
                if event_cursor + timedelta(minutes=75) >= departure_time:
                    break

                unload_start = event_cursor + timedelta(minutes=rng.randint(0, 15))
                unload_end = unload_start + timedelta(minutes=rng.randint(10, 20))
                current_location = choose_available_location(
                    rng,
                    location_available_at,
                    unload_start,
                )
                location_available_at[current_location] = datetime.max

                cur.execute(
                    """
                    INSERT INTO Unload
                    (
                        berth_id,
                        container_ISO,
                        location_bay,
                        location_row,
                        location_tier,
                        start_time,
                        end_time
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        berth_id,
                        container_iso,
                        current_location[0],
                        current_location[1],
                        current_location[2],
                        unload_start,
                        unload_end,
                    ),
                )
                inserted_unload_count += 1
                event_cursor = unload_end

                if rng.random() < 0.6:
                    transfer_start = event_cursor + timedelta(minutes=rng.randint(5, 20))
                    transfer_end = transfer_start + timedelta(minutes=rng.randint(8, 18))
                    next_location = choose_available_location(
                        rng,
                        location_available_at,
                        transfer_start,
                        exclude=current_location,
                    )
                    location_available_at[next_location] = datetime.max

                    cur.execute(
                        """
                        INSERT INTO Transfer
                        (
                            container_ISO,
                            start_bay,
                            start_row,
                            start_tier,
                            end_bay,
                            end_row,
                            end_tier,
                            start_time,
                            end_time
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            container_iso,
                            current_location[0],
                            current_location[1],
                            current_location[2],
                            next_location[0],
                            next_location[1],
                            next_location[2],
                            transfer_start,
                            transfer_end,
                        ),
                    )
                    inserted_transfer_count += 1
                    location_available_at[current_location] = transfer_start
                    current_location = next_location
                    event_cursor = transfer_end

                load_start = event_cursor + timedelta(minutes=rng.randint(10, 25))
                load_end = load_start + timedelta(minutes=rng.randint(10, 25))

                if load_end >= departure_time:
                    load_end = departure_time - timedelta(minutes=5)
                    load_start = load_end - timedelta(minutes=10)

                cur.execute(
                    """
                    INSERT INTO Load (berth_id, container_ISO, start_time, end_time)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (berth_id, container_iso, load_start, load_end),
                )
                inserted_load_count += 1
                location_available_at[current_location] = load_start
                event_cursor = load_end + timedelta(minutes=rng.randint(1, 10))

        cur.execute("SELECT refresh_terminal_analytics()")
        cur.execute(
            """
            UPDATE Ingestion_Batches
            SET inserted_ship_count = %s,
                inserted_container_count = %s,
                inserted_schedule_count = %s,
                inserted_unload_count = %s,
                inserted_transfer_count = %s,
                inserted_load_count = %s,
                finished_at = CURRENT_TIMESTAMP,
                status = 'completed',
                notes = %s
            WHERE batch_id = %s
            """,
            (
                inserted_ship_count,
                inserted_container_count,
                inserted_schedule_count,
                inserted_unload_count,
                inserted_transfer_count,
                inserted_load_count,
                "Synthetic historical data generated with Faker and integrity-preserving triggers.",
                batch_id,
            ),
        )
        conn.commit()

        print(
            f"Batch {batch_id} completed: "
            f"{inserted_ship_count} ships, "
            f"{inserted_container_count} containers, "
            f"{inserted_schedule_count} schedules, "
            f"{inserted_unload_count} unloads, "
            f"{inserted_transfer_count} transfers, "
            f"{inserted_load_count} loads."
        )
    except Exception as exc:
        conn.rollback()
        cur.execute(
            """
            UPDATE Ingestion_Batches
            SET finished_at = CURRENT_TIMESTAMP,
                status = 'failed',
                notes = %s
            WHERE batch_id = %s
            """,
            (str(exc)[:1000], batch_id),
        )
        conn.commit()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
