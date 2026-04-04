from __future__ import annotations

import argparse
import math
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from typing import Any

import psycopg2
from faker import Faker
from psycopg2.extras import Json, RealDictCursor, execute_values


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

SOURCE_SYSTEM = "faker_terminal_event_pipeline"


@dataclass
class DatabaseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str | None


@dataclass
class YardContainer:
    container_iso: str
    available_at: datetime
    location: tuple[int, int, int]


@dataclass
class RawEvent:
    event_id: str
    event_type: str
    event_time: datetime
    event_start_time: datetime | None
    event_end_time: datetime | None
    ship_mmsi: str | None = None
    berth_id: int | None = None
    container_iso: str | None = None
    location_bay: int | None = None
    location_row: int | None = None
    location_tier: int | None = None
    start_bay: int | None = None
    start_row: int | None = None
    start_tier: int | None = None
    end_bay: int | None = None
    end_row: int | None = None
    end_tier: int | None = None
    source_mode: str = "backfill"
    source_system: str = SOURCE_SYSTEM
    batch_id: int | None = None
    raw_payload: dict[str, Any] | None = None


class RawEventWriter:
    def __init__(
        self,
        conn,
        cur,
        batch_id: int,
        source_mode: str,
        flush_size: int,
        sleep_seconds: float = 0.0,
    ) -> None:
        self.conn = conn
        self.cur = cur
        self.batch_id = batch_id
        self.source_mode = source_mode
        self.flush_size = flush_size
        self.sleep_seconds = sleep_seconds
        self.buffer: list[tuple[Any, ...]] = []
        self.counts = {
            "SHIP_SCHEDULED": 0,
            "CONTAINER_UNLOADED": 0,
            "CONTAINER_TRANSFERRED": 0,
            "CONTAINER_LOADED": 0,
        }

    def queue(self, event: RawEvent) -> None:
        self.buffer.append(
            (
                event.event_id,
                event.event_type,
                event.event_time,
                event.event_start_time,
                event.event_end_time,
                event.ship_mmsi,
                event.berth_id,
                event.container_iso,
                event.location_bay,
                event.location_row,
                event.location_tier,
                event.start_bay,
                event.start_row,
                event.start_tier,
                event.end_bay,
                event.end_row,
                event.end_tier,
                event.source_mode,
                event.source_system,
                event.batch_id,
                Json(event.raw_payload or {}),
            )
        )
        self.counts[event.event_type] += 1
        if len(self.buffer) >= self.flush_size:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        execute_values(
            self.cur,
            """
            INSERT INTO Raw_Terminal_Events
            (
                event_id,
                event_type,
                event_time,
                event_start_time,
                event_end_time,
                ship_mmsi,
                berth_id,
                container_iso,
                location_bay,
                location_row,
                location_tier,
                start_bay,
                start_row,
                start_tier,
                end_bay,
                end_row,
                end_tier,
                source_mode,
                source_system,
                batch_id,
                raw_payload
            )
            VALUES %s
            """,
            self.buffer,
            page_size=self.flush_size,
        )
        self.conn.commit()
        self.buffer.clear()
        if self.sleep_seconds > 0:
            time.sleep(self.sleep_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Raw-event batch and stream pipeline for the terminal project.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5433)
    parser.add_argument("--dbname", default="terminal_ops")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default=None)

    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill = subparsers.add_parser("backfill-raw", help="Generate six months of raw event backfill.")
    backfill.add_argument("--months", type=int, default=6)
    backfill.add_argument("--ships-per-day-min", type=int, default=2)
    backfill.add_argument("--ships-per-day-max", type=int, default=4)
    backfill.add_argument("--containers-min", type=int, default=70)
    backfill.add_argument("--containers-max", type=int, default=100)
    backfill.add_argument("--transfer-ratio", type=float, default=0.65)
    backfill.add_argument("--berths", type=int, default=10)
    backfill.add_argument("--bays", type=int, default=24)
    backfill.add_argument("--rows", type=int, default=8)
    backfill.add_argument("--tiers", type=int, default=5)
    backfill.add_argument("--flush-size", type=int, default=5000)
    backfill.add_argument("--seed", type=int, default=20260325)

    process = subparsers.add_parser("process-raw", help="Consume raw events into curated tables.")
    process.add_argument("--batch-size", type=int, default=5000)
    process.add_argument("--refresh-analytics", action="store_true")

    stream = subparsers.add_parser("stream-producer", help="Emit raw events in micro-batches.")
    stream.add_argument("--hours", type=int, default=24)
    stream.add_argument("--ships-per-day-min", type=int, default=1)
    stream.add_argument("--ships-per-day-max", type=int, default=2)
    stream.add_argument("--containers-min", type=int, default=30)
    stream.add_argument("--containers-max", type=int, default=60)
    stream.add_argument("--transfer-ratio", type=float, default=0.55)
    stream.add_argument("--berths", type=int, default=10)
    stream.add_argument("--bays", type=int, default=24)
    stream.add_argument("--rows", type=int, default=8)
    stream.add_argument("--tiers", type=int, default=5)
    stream.add_argument("--flush-size", type=int, default=250)
    stream.add_argument("--sleep-seconds", type=float, default=0.5)
    stream.add_argument("--seed", type=int, default=20260326)

    return parser.parse_args()


def database_config(args: argparse.Namespace) -> DatabaseConfig:
    return DatabaseConfig(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )


def connect(config: DatabaseConfig):
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    )


def normalise_timestamp(value: datetime) -> datetime:
    return value.replace(tzinfo=None) if value.tzinfo is not None else value


def ensure_capacity(cur, berths: int, bays: int, rows: int, tiers: int) -> None:
    cur.execute(
        """
        INSERT INTO Berths (berth_id)
        SELECT berth_id
        FROM generate_series(1, %s) AS berth_id
        ON CONFLICT (berth_id) DO NOTHING
        """,
        (berths,),
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
        (bays, rows, tiers),
    )


def fetch_existing_ids(cur) -> tuple[set[str], set[str]]:
    cur.execute("SELECT MMSI FROM Ships")
    ship_ids = {row[0].strip() for row in cur.fetchall()}
    cur.execute("SELECT ISO FROM Containers")
    container_ids = {row[0].strip() for row in cur.fetchall()}
    return ship_ids, container_ids


def fetch_berths(cur) -> list[int]:
    cur.execute("SELECT berth_id FROM Berths ORDER BY berth_id")
    return [row[0] for row in cur.fetchall()]


def fetch_locations(cur) -> list[tuple[int, int, int]]:
    cur.execute(
        """
        SELECT bay, row, tier
        FROM Location
        ORDER BY bay, row, tier
        """
    )
    return [tuple(row) for row in cur.fetchall()]


def fetch_free_locations(cur) -> list[tuple[int, int, int]]:
    cur.execute(
        """
        SELECT bay, row, tier
        FROM Location
        WHERE is_occupied = FALSE
        ORDER BY bay, row, tier
        """
    )
    return [tuple(row) for row in cur.fetchall()]


def fetch_active_yard_state(cur) -> dict[str, YardContainer]:
    cur.execute(
        """
        SELECT
            cs.ISO,
            cs.last_action_time,
            split_part(cs.current_location, '-', 1)::INT AS bay,
            split_part(cs.current_location, '-', 2)::INT AS row_no,
            split_part(cs.current_location, '-', 3)::INT AS tier_no
        FROM Container_Status cs
        WHERE cs.terminal_status = 'In terminal'
          AND cs.current_location ~ '^[0-9]+-[0-9]+-[0-9]+$'
        """
    )
    return {
        row[0].strip(): YardContainer(
            container_iso=row[0].strip(),
            available_at=normalise_timestamp(row[1]),
            location=(row[2], row[3], row[4]),
        )
        for row in cur.fetchall()
    }


def insert_batch(cur, generator_name: str, seed: int, notes: str) -> int:
    cur.execute(
        """
        INSERT INTO Ingestion_Batches
        (
            generator_name,
            generator_version,
            seed_value,
            notes
        )
        VALUES (%s, %s, %s, %s)
        RETURNING batch_id
        """,
        (generator_name, "2.1.0", seed, notes),
    )
    return cur.fetchone()[0]


def update_batch(cur, batch_id: int, writer: RawEventWriter, status: str, notes: str) -> None:
    cur.execute(
        """
        UPDATE Ingestion_Batches
        SET inserted_ship_count = %s,
            inserted_schedule_count = %s,
            inserted_container_count = %s,
            inserted_unload_count = %s,
            inserted_transfer_count = %s,
            inserted_load_count = %s,
            finished_at = CURRENT_TIMESTAMP,
            status = %s,
            notes = %s
        WHERE batch_id = %s
        """,
        (
            writer.counts["SHIP_SCHEDULED"],
            writer.counts["SHIP_SCHEDULED"],
            writer.counts["CONTAINER_UNLOADED"],
            writer.counts["CONTAINER_UNLOADED"],
            writer.counts["CONTAINER_TRANSFERRED"],
            writer.counts["CONTAINER_LOADED"],
            status,
            notes,
            batch_id,
        ),
    )


def next_mmsi(rng: Any, existing: set[str]) -> str:
    while True:
        value = "".join(str(rng.randint(0, 9)) for _ in range(9))
        if value not in existing:
            existing.add(value)
            return value


def next_container_iso(rng: Any, existing: set[str]) -> str:
    while True:
        value = f"{rng.choice(OWNER_PREFIXES)}{rng.randint(0, 9_999_999):07d}"
        if value not in existing:
            existing.add(value)
            return value


def schedule_duration_hours(inbound_count: int, outbound_count: int, lane_count: int, rng: Any) -> int:
    work_units = inbound_count + outbound_count
    return max(8, min(36, math.ceil((work_units * 0.8) / max(lane_count, 1)) + rng.randint(3, 7)))


def choose_location(
    rng: Any,
    location_available_at: dict[tuple[int, int, int], datetime],
    at_time: datetime,
    exclude: tuple[int, int, int] | None = None,
) -> tuple[int, int, int]:
    candidates = [
        location
        for location, available_at in location_available_at.items()
        if available_at <= at_time and location != exclude
    ]
    if not candidates:
        raise RuntimeError("No free location is available for the requested event time.")
    return rng.choice(candidates)


def count_available_locations(
    location_available_at: dict[tuple[int, int, int], datetime],
    at_time: datetime,
    exclude: tuple[int, int, int] | None = None,
) -> int:
    return sum(
        1
        for location, available_at in location_available_at.items()
        if available_at <= at_time and location != exclude
    )


def next_location_release(
    location_available_at: dict[tuple[int, int, int], datetime],
    at_time: datetime,
    exclude: tuple[int, int, int] | None = None,
) -> datetime | None:
    future_times = [
        available_at
        for location, available_at in location_available_at.items()
        if available_at > at_time and location != exclude
    ]
    return min(future_times) if future_times else None


def select_yard_container(
    yard_pool: dict[str, YardContainer],
    at_time: datetime,
    min_dwell: timedelta,
) -> YardContainer | None:
    eligible = [
        container
        for container in yard_pool.values()
        if container.available_at <= at_time - min_dwell
    ]
    if not eligible:
        return None
    return min(eligible, key=lambda container: container.available_at)


def next_yard_container_ready(yard_pool: dict[str, YardContainer], min_dwell: timedelta) -> datetime | None:
    if not yard_pool:
        return None
    return min(container.available_at + min_dwell for container in yard_pool.values())


def queue_schedule_event(writer: RawEventWriter, ship_mmsi: str, berth_id: int, arrival_time: datetime, departure_time: datetime, payload: dict[str, Any]) -> None:
    writer.queue(
        RawEvent(
            event_id=str(uuid.uuid4()),
            event_type="SHIP_SCHEDULED",
            event_time=arrival_time,
            event_start_time=arrival_time,
            event_end_time=departure_time,
            ship_mmsi=ship_mmsi,
            berth_id=berth_id,
            batch_id=writer.batch_id,
            source_mode=writer.source_mode,
            raw_payload=payload,
        )
    )


def queue_unload_event(writer: RawEventWriter, container_iso: str, berth_id: int, location: tuple[int, int, int], start_time: datetime, end_time: datetime, payload: dict[str, Any]) -> None:
    writer.queue(
        RawEvent(
            event_id=str(uuid.uuid4()),
            event_type="CONTAINER_UNLOADED",
            event_time=start_time,
            event_start_time=start_time,
            event_end_time=end_time,
            berth_id=berth_id,
            container_iso=container_iso,
            location_bay=location[0],
            location_row=location[1],
            location_tier=location[2],
            batch_id=writer.batch_id,
            source_mode=writer.source_mode,
            raw_payload=payload,
        )
    )


def queue_transfer_event(writer: RawEventWriter, container_iso: str, start_location: tuple[int, int, int], end_location: tuple[int, int, int], start_time: datetime, end_time: datetime, payload: dict[str, Any]) -> None:
    writer.queue(
        RawEvent(
            event_id=str(uuid.uuid4()),
            event_type="CONTAINER_TRANSFERRED",
            event_time=start_time,
            event_start_time=start_time,
            event_end_time=end_time,
            container_iso=container_iso,
            start_bay=start_location[0],
            start_row=start_location[1],
            start_tier=start_location[2],
            end_bay=end_location[0],
            end_row=end_location[1],
            end_tier=end_location[2],
            batch_id=writer.batch_id,
            source_mode=writer.source_mode,
            raw_payload=payload,
        )
    )


def queue_load_event(writer: RawEventWriter, container_iso: str, berth_id: int, start_time: datetime, end_time: datetime, payload: dict[str, Any]) -> None:
    writer.queue(
        RawEvent(
            event_id=str(uuid.uuid4()),
            event_type="CONTAINER_LOADED",
            event_time=start_time,
            event_start_time=start_time,
            event_end_time=end_time,
            berth_id=berth_id,
            container_iso=container_iso,
            batch_id=writer.batch_id,
            source_mode=writer.source_mode,
            raw_payload=payload,
        )
    )


def simulate_window(cur, writer: RawEventWriter, args: argparse.Namespace, start_window: datetime, end_window: datetime) -> None:
    rng = random.Random(args.seed)
    fake = Faker()
    Faker.seed(args.seed)

    ensure_capacity(cur, args.berths, args.bays, args.rows, args.tiers)
    cur.execute("SELECT ensure_raw_event_partitions(%s, %s)", (start_window.date(), end_window.date()))
    writer.conn.commit()

    existing_ships, existing_containers = fetch_existing_ids(cur)
    berth_ids = fetch_berths(cur)
    locations = fetch_locations(cur) if writer.source_mode == "stream" else fetch_free_locations(cur)

    location_available_at = {location: start_window - timedelta(days=1) for location in locations}
    yard_pool = fetch_active_yard_state(cur) if writer.source_mode == "stream" else {}
    for container in yard_pool.values():
        location_available_at[container.location] = datetime.max
    next_available = {
        berth_id: start_window + timedelta(hours=rng.randint(0, 6))
        for berth_id in berth_ids
    }

    total_seconds = max((end_window - start_window).total_seconds(), 1.0)
    current_day = start_window.date()

    while current_day <= end_window.date():
        day_start = datetime.combine(current_day, dt_time(0, 0))
        day_end = day_start + timedelta(days=1)
        active_day_start = max(day_start, start_window)
        active_day_end = min(day_end, end_window)
        if active_day_start >= active_day_end:
            current_day += timedelta(days=1)
            continue
        active_hours = max((active_day_end - active_day_start).total_seconds() / 3600.0, 1.0)
        activity_scale = min(active_hours / 24.0, 1.0)
        ship_calls_today = max(1, math.ceil(rng.randint(args.ships_per_day_min, args.ships_per_day_max) * activity_scale))
        arrival_buffer = timedelta(hours=8)

        for _ in range(ship_calls_today):
            berth_id = min(next_available, key=next_available.get)
            latest_arrival = active_day_end - arrival_buffer
            arrival_floor = max(next_available[berth_id], active_day_start + timedelta(minutes=30))
            if latest_arrival <= arrival_floor:
                continue
            arrival_minutes = int((latest_arrival - arrival_floor).total_seconds() // 60)
            if writer.source_mode == "stream":
                arrival_minutes = max(0, int(arrival_minutes * 0.35))
            arrival_time = normalise_timestamp(arrival_floor + timedelta(minutes=rng.randint(0, max(arrival_minutes, 0))))

            inbound_target = rng.randint(args.containers_min, args.containers_max)
            progress = (arrival_time - start_window).total_seconds() / total_seconds
            yard_utilisation = len(yard_pool) / max(len(locations), 1)
            eligible_now = [
                container
                for container in yard_pool.values()
                if container.available_at <= arrival_time
            ]
            free_slots_at_arrival = count_available_locations(location_available_at, arrival_time)
            outbound_base = int(inbound_target * rng.uniform(0.35, 0.75))
            if progress > 0.65:
                outbound_base = int(inbound_target * rng.uniform(0.75, 1.05))
            if progress > 0.85 or yard_utilisation > 0.45:
                outbound_base = int(inbound_target * rng.uniform(0.95, 1.35))
            required_outbound = max(0, inbound_target - free_slots_at_arrival)
            outbound_target = min(len(eligible_now), max(required_outbound, outbound_base))
            inbound_target = min(inbound_target, free_slots_at_arrival + outbound_target)
            if inbound_target == 0 and outbound_target == 0:
                next_available[berth_id] = arrival_time + timedelta(hours=2)
                continue

            lane_count = rng.randint(3, 5)
            departure_time = arrival_time + timedelta(hours=schedule_duration_hours(inbound_target, outbound_target, lane_count, rng))
            departure_time = normalise_timestamp(departure_time)
            if departure_time >= end_window:
                continue

            ship_mmsi = next_mmsi(rng, existing_ships)
            ship_payload = {
                "ship_name": f"{rng.choice(CARRIERS)} {fake.city().upper()}",
                "flag": rng.choice(FLAGS),
                "length": round(rng.uniform(180.0, 400.0), 2),
                "width": round(rng.uniform(28.0, 60.0), 2),
                "lane_count": lane_count,
            }
            queue_schedule_event(writer, ship_mmsi, berth_id, arrival_time, departure_time, ship_payload)
            next_available[berth_id] = departure_time + timedelta(hours=rng.randint(2, 6))

            lane_cursors = [arrival_time + timedelta(minutes=rng.randint(20, 35)) for _ in range(lane_count)]
            remaining_inbound = inbound_target
            remaining_outbound = outbound_target

            while True:
                lane_index = min(range(len(lane_cursors)), key=lambda idx: lane_cursors[idx])
                lane_cursor = lane_cursors[lane_index]
                if lane_cursor >= departure_time - timedelta(minutes=20):
                    break

                free_slots_now = count_available_locations(location_available_at, lane_cursor)
                next_task = "UNLOAD"
                candidate_container = None
                if remaining_outbound > 0:
                    candidate_container = select_yard_container(yard_pool, lane_cursor, timedelta(minutes=45))
                    if candidate_container and (
                        free_slots_now == 0
                        or remaining_inbound == 0
                        or rng.random() < ((45 + int(progress * 35)) / 100.0)
                    ):
                        next_task = "LOAD"

                if next_task == "LOAD" and candidate_container is not None:
                    load_start = lane_cursor + timedelta(minutes=rng.randint(0, 8))
                    load_end = load_start + timedelta(minutes=rng.randint(8, 18))
                    if load_end >= departure_time - timedelta(minutes=5):
                        lane_cursors[lane_index] = departure_time
                        continue
                    queue_load_event(
                        writer,
                        candidate_container.container_iso,
                        berth_id,
                        load_start,
                        load_end,
                        {"lane": lane_index + 1, "operation_profile": "outbound"},
                    )
                    location_available_at[candidate_container.location] = load_start
                    yard_pool.pop(candidate_container.container_iso, None)
                    remaining_outbound -= 1
                    lane_cursors[lane_index] = load_end + timedelta(minutes=rng.randint(2, 8))
                    continue

                if remaining_inbound <= 0:
                    break

                if free_slots_now == 0:
                    next_ready_time = next_yard_container_ready(yard_pool, timedelta(minutes=45))
                    next_release_time = next_location_release(location_available_at, lane_cursor)
                    wait_candidates = [
                        value
                        for value in (next_ready_time, next_release_time)
                        if value is not None and value < departure_time - timedelta(minutes=20)
                    ]
                    if not wait_candidates:
                        lane_cursors[lane_index] = departure_time
                        continue
                    lane_cursors[lane_index] = min(wait_candidates) + timedelta(minutes=1)
                    continue

                unload_start = lane_cursor + timedelta(minutes=rng.randint(0, 8))
                unload_end = unload_start + timedelta(minutes=rng.randint(8, 18))
                if unload_end >= departure_time - timedelta(minutes=5):
                    lane_cursors[lane_index] = departure_time
                    continue

                try:
                    location = choose_location(rng, location_available_at, unload_start)
                except RuntimeError:
                    next_release_time = next_location_release(location_available_at, unload_start)
                    if next_release_time is None or next_release_time >= departure_time - timedelta(minutes=20):
                        lane_cursors[lane_index] = departure_time
                    else:
                        lane_cursors[lane_index] = next_release_time + timedelta(minutes=1)
                    continue
                container_iso = next_container_iso(rng, existing_containers)
                queue_unload_event(
                    writer,
                    container_iso,
                    berth_id,
                    location,
                    unload_start,
                    unload_end,
                    {"lane": lane_index + 1, "operation_profile": "inbound"},
                )
                location_available_at[location] = datetime.max
                container_state = YardContainer(container_iso=container_iso, available_at=unload_end, location=location)

                if rng.random() < args.transfer_ratio:
                    transfer_start = unload_end + timedelta(minutes=rng.randint(30, 12 * 60))
                    if transfer_start < end_window - timedelta(minutes=30):
                        try:
                            new_location = choose_location(rng, location_available_at, transfer_start, exclude=location)
                            transfer_end = transfer_start + timedelta(minutes=rng.randint(12, 35))
                            queue_transfer_event(
                                writer,
                                container_iso,
                                location,
                                new_location,
                                transfer_start,
                                transfer_end,
                                {"transfer_reason": rng.choice(["stack_rebalance", "outbound_staging", "yard_optimisation"])},
                            )
                            location_available_at[location] = transfer_start
                            location_available_at[new_location] = datetime.max
                            container_state.location = new_location
                            container_state.available_at = transfer_end
                        except RuntimeError:
                            pass

                yard_pool[container_iso] = container_state
                remaining_inbound -= 1
                lane_cursors[lane_index] = unload_end + timedelta(minutes=rng.randint(2, 8))

        current_day += timedelta(days=1)

    while writer.source_mode == "backfill" and yard_pool:
        berth_id = min(next_available, key=next_available.get)
        soonest_container_ready = min(container.available_at for container in yard_pool.values())
        arrival_time = max(next_available[berth_id] + timedelta(hours=1), soonest_container_ready + timedelta(minutes=30))
        if arrival_time >= end_window - timedelta(minutes=45):
            break
        eligible = [container for container in yard_pool.values() if container.available_at <= arrival_time]
        if not eligible:
            next_available[berth_id] = arrival_time + timedelta(minutes=30)
            continue
        lane_count = 6
        available_minutes = max(0, int((end_window - arrival_time - timedelta(minutes=5)).total_seconds() // 60))
        if available_minutes < 25:
            break
        max_cycles_per_lane = max(1, math.floor(max(available_minutes - 7, 0) / 16) + 1)
        outbound_target = min(len(eligible), lane_count * max_cycles_per_lane)
        cleanup_duration_minutes = max(45, min(6 * 60, 25 + math.ceil(outbound_target / lane_count) * 16))
        departure_time = arrival_time + timedelta(minutes=cleanup_duration_minutes)
        if departure_time >= end_window:
            departure_time = end_window - timedelta(minutes=5)
        if departure_time <= arrival_time + timedelta(minutes=25):
            break
        ship_mmsi = next_mmsi(rng, existing_ships)
        queue_schedule_event(
            writer,
            ship_mmsi,
            berth_id,
            arrival_time,
            departure_time,
            {
                "ship_name": f"{rng.choice(CARRIERS)} {fake.city().upper()}",
                "flag": rng.choice(FLAGS),
                "length": round(rng.uniform(180.0, 400.0), 2),
                "width": round(rng.uniform(28.0, 60.0), 2),
                "lane_count": lane_count,
                "cleanup_schedule": True,
            },
        )
        next_available[berth_id] = departure_time + timedelta(hours=2)
        lane_cursors = [arrival_time + timedelta(minutes=20) for _ in range(lane_count)]
        while yard_pool:
            lane_index = min(range(len(lane_cursors)), key=lambda idx: lane_cursors[idx])
            lane_cursor = lane_cursors[lane_index]
            if lane_cursor >= departure_time - timedelta(minutes=20):
                break
            candidate_container = select_yard_container(yard_pool, lane_cursor, timedelta(minutes=0))
            if candidate_container is None:
                break
            load_start = lane_cursor + timedelta(minutes=2)
            load_end = load_start + timedelta(minutes=10)
            if load_end >= departure_time - timedelta(minutes=5):
                break
            queue_load_event(
                writer,
                candidate_container.container_iso,
                berth_id,
                load_start,
                load_end,
                {"lane": lane_index + 1, "operation_profile": "cleanup_outbound"},
            )
            location_available_at[candidate_container.location] = load_start
            yard_pool.pop(candidate_container.container_iso, None)
            lane_cursors[lane_index] = load_end + timedelta(minutes=2)

    writer.flush()

    if writer.source_mode == "backfill" and yard_pool:
        raise RuntimeError("Backfill ended with containers still in yard. Increase outbound capacity or extend the window.")


def build_windows(cur, command: str, months: int | None = None, hours: int | None = None) -> tuple[datetime, datetime]:
    cur.execute("SELECT COALESCE(MIN(arrival_time), LOCALTIMESTAMP) FROM Schedule")
    earliest_schedule = normalise_timestamp(cur.fetchone()[0])
    cur.execute("SELECT COALESCE(MAX(departure_time), LOCALTIMESTAMP) FROM Schedule")
    latest_schedule = normalise_timestamp(cur.fetchone()[0])

    if command == "backfill-raw":
        end_window = earliest_schedule - timedelta(days=1)
        start_window = end_window - timedelta(days=(months or 6) * 30)
        return start_window, end_window

    start_window = max(datetime.now(), latest_schedule) + timedelta(hours=1)
    end_window = start_window + timedelta(hours=hours or 24)
    return start_window, end_window


def apply_raw_event(cur, event: dict[str, Any]) -> None:
    payload = event["raw_payload"] or {}
    event_id = str(event["event_id"])

    if event["event_type"] == "SHIP_SCHEDULED":
        cur.execute("INSERT INTO Berths (berth_id) VALUES (%s) ON CONFLICT DO NOTHING", (event["berth_id"],))
        cur.execute(
            """
            INSERT INTO Ships (MMSI, name, flag, length, width)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (MMSI) DO NOTHING
            """,
            (
                event["ship_mmsi"],
                payload["ship_name"],
                payload["flag"],
                payload["length"],
                payload["width"],
            ),
        )
        cur.execute(
            """
            INSERT INTO Schedule (ship_MMSI, berth_id, arrival_time, departure_time, source_event_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source_event_id) DO NOTHING
            """,
            (event["ship_mmsi"], event["berth_id"], event["event_start_time"], event["event_end_time"], event_id),
        )
        return

    if event["container_iso"] is not None:
        cur.execute("INSERT INTO Containers (ISO) VALUES (%s) ON CONFLICT DO NOTHING", (event["container_iso"],))

    if event["berth_id"] is not None:
        cur.execute("INSERT INTO Berths (berth_id) VALUES (%s) ON CONFLICT DO NOTHING", (event["berth_id"],))

    if event["location_bay"] is not None:
        cur.execute(
            """
            INSERT INTO Location (bay, row, tier)
            VALUES (%s, %s, %s)
            ON CONFLICT (bay, row, tier) DO NOTHING
            """,
            (event["location_bay"], event["location_row"], event["location_tier"]),
        )

    if event["start_bay"] is not None:
        cur.execute(
            """
            INSERT INTO Location (bay, row, tier)
            VALUES (%s, %s, %s)
            ON CONFLICT (bay, row, tier) DO NOTHING
            """,
            (event["start_bay"], event["start_row"], event["start_tier"]),
        )

    if event["end_bay"] is not None:
        cur.execute(
            """
            INSERT INTO Location (bay, row, tier)
            VALUES (%s, %s, %s)
            ON CONFLICT (bay, row, tier) DO NOTHING
            """,
            (event["end_bay"], event["end_row"], event["end_tier"]),
        )

    if event["event_type"] == "CONTAINER_UNLOADED":
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
                end_time,
                source_event_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_event_id) DO NOTHING
            """,
            (
                event["berth_id"],
                event["container_iso"],
                event["location_bay"],
                event["location_row"],
                event["location_tier"],
                event["event_start_time"],
                event["event_end_time"],
                event_id,
            ),
        )
    elif event["event_type"] == "CONTAINER_TRANSFERRED":
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
                end_time,
                source_event_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_event_id) DO NOTHING
            """,
            (
                event["container_iso"],
                event["start_bay"],
                event["start_row"],
                event["start_tier"],
                event["end_bay"],
                event["end_row"],
                event["end_tier"],
                event["event_start_time"],
                event["event_end_time"],
                event_id,
            ),
        )
    elif event["event_type"] == "CONTAINER_LOADED":
        cur.execute(
            """
            INSERT INTO Load (berth_id, container_ISO, start_time, end_time, source_event_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source_event_id) DO NOTHING
            """,
            (
                event["berth_id"],
                event["container_iso"],
                event["event_start_time"],
                event["event_end_time"],
                event_id,
            ),
        )


def run_backfill_or_stream(args: argparse.Namespace) -> None:
    config = database_config(args)
    conn = connect(config)
    conn.autocommit = False
    cur = conn.cursor()

    notes = (
        f"Historical raw backfill pipeline ({getattr(args, 'months', 6)} months)"
        if args.command == "backfill-raw"
        else f"Micro-batch stream simulator ({getattr(args, 'hours', 24)} hours)"
    )
    batch_id = insert_batch(cur, "scripts/terminal_event_pipeline.py", args.seed, notes)
    conn.commit()

    writer = RawEventWriter(
        conn=conn,
        cur=cur,
        batch_id=batch_id,
        source_mode="backfill" if args.command == "backfill-raw" else "stream",
        flush_size=args.flush_size,
        sleep_seconds=getattr(args, "sleep_seconds", 0.0),
    )

    try:
        start_window, end_window = build_windows(cur, args.command, getattr(args, "months", None), getattr(args, "hours", None))
        simulate_window(cur, writer, args, start_window, end_window)
        update_batch(cur, batch_id, writer, "completed", f"{notes} generated successfully.")
        conn.commit()
        print(
            f"Batch {batch_id} completed with "
            f"{sum(writer.counts.values())} raw events "
            f"({writer.counts['SHIP_SCHEDULED']} schedules, "
            f"{writer.counts['CONTAINER_UNLOADED']} unloads, "
            f"{writer.counts['CONTAINER_TRANSFERRED']} transfers, "
            f"{writer.counts['CONTAINER_LOADED']} loads)."
        )
    except Exception as exc:
        conn.rollback()
        update_batch(cur, batch_id, writer, "failed", str(exc)[:1000])
        conn.commit()
        raise
    finally:
        cur.close()
        conn.close()


def run_raw_processor(args: argparse.Namespace) -> None:
    config = database_config(args)
    conn = connect(config)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)

    processed = 0
    failed = 0

    while True:
        cur.execute(
            """
            SELECT *
            FROM Raw_Terminal_Events
            WHERE processing_status = 'pending'
            ORDER BY event_time, raw_event_id
            LIMIT %s
            """,
            (args.batch_size,),
        )
        events = cur.fetchall()
        if not events:
            break

        for event in events:
            cur.execute("SAVEPOINT raw_event_sp")
            try:
                apply_raw_event(cur, event)
                cur.execute(
                    """
                    UPDATE Raw_Terminal_Events
                    SET processing_status = 'processed',
                        processed_at = CURRENT_TIMESTAMP,
                        error_message = NULL
                    WHERE raw_event_id = %s
                      AND event_time = %s
                    """,
                    (event["raw_event_id"], event["event_time"]),
                )
                cur.execute("RELEASE SAVEPOINT raw_event_sp")
                processed += 1
            except Exception as exc:
                cur.execute("ROLLBACK TO SAVEPOINT raw_event_sp")
                cur.execute(
                    """
                    INSERT INTO Dead_Letter_Terminal_Events
                    (
                        raw_event_id,
                        event_id,
                        event_type,
                        batch_id,
                        source_mode,
                        source_system,
                        error_message,
                        raw_payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        event["raw_event_id"],
                        event["event_id"],
                        event["event_type"],
                        event["batch_id"],
                        event["source_mode"],
                        event["source_system"],
                        str(exc)[:1000],
                        Json(event["raw_payload"] or {}),
                    ),
                )
                cur.execute(
                    """
                    UPDATE Raw_Terminal_Events
                    SET processing_status = 'failed',
                        processed_at = CURRENT_TIMESTAMP,
                        error_message = %s
                    WHERE raw_event_id = %s
                      AND event_time = %s
                    """,
                    (str(exc)[:1000], event["raw_event_id"], event["event_time"]),
                )
                cur.execute("RELEASE SAVEPOINT raw_event_sp")
                failed += 1

        conn.commit()

    if args.refresh_analytics:
        cur.execute("SELECT refresh_terminal_analytics()")
        conn.commit()

    print(f"Processed {processed} raw events; {failed} moved to the dead-letter table.")
    cur.close()
    conn.close()


def main() -> None:
    args = parse_args()
    if args.command in {"backfill-raw", "stream-producer"}:
        run_backfill_or_stream(args)
    elif args.command == "process-raw":
        run_raw_processor(args)


if __name__ == "__main__":
    main()
