from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

import psycopg2
from psycopg2.extras import Json


@dataclass(frozen=True)
class CheckDefinition:
    name: str
    layer: str
    severity: str
    sql: str
    threshold_arg: str
    description: str


CHECKS = [
    CheckDefinition(
        name="schedule_conflicts",
        layer="silver",
        severity="error",
        sql="SELECT COUNT(*) AS failing_rows FROM Schedule_Conflict_Check",
        threshold_arg="max_schedule_conflicts",
        description="No berth or ship schedule overlaps should exist.",
    ),
    CheckDefinition(
        name="load_window_violations",
        layer="silver",
        severity="error",
        sql="SELECT COUNT(*) AS failing_rows FROM Load_Window_Violation_Check",
        threshold_arg="max_load_window_violations",
        description="Load events must stay inside a valid berth schedule.",
    ),
    CheckDefinition(
        name="unload_window_violations",
        layer="silver",
        severity="error",
        sql="SELECT COUNT(*) AS failing_rows FROM Unload_Window_Violation_Check",
        threshold_arg="max_unload_window_violations",
        description="Unload events must stay inside a valid berth schedule.",
    ),
    CheckDefinition(
        name="occupied_target_violations",
        layer="silver",
        severity="error",
        sql="SELECT COUNT(*) AS failing_rows FROM Occupied_Target_Violation_Check",
        threshold_arg="max_occupied_target_violations",
        description="Containers should not be assigned into occupied yard slots.",
    ),
    CheckDefinition(
        name="location_status_mismatches",
        layer="gold",
        severity="error",
        sql="""
            SELECT failing_rows
            FROM Data_Quality_Dashboard
            WHERE check_name = 'location_status_mismatches'
        """,
        threshold_arg="max_location_status_mismatches",
        description="Location occupancy flags must match the latest container status view.",
    ),
    CheckDefinition(
        name="raw_lifecycle_violations",
        layer="raw",
        severity="error",
        sql="SELECT COUNT(*) AS failing_rows FROM Raw_Container_Lifecycle_Audit",
        threshold_arg="max_raw_lifecycle_violations",
        description="Raw container event order must remain operationally valid.",
    ),
    CheckDefinition(
        name="dead_letter_events",
        layer="ops",
        severity="error",
        sql="SELECT COUNT(*) AS failing_rows FROM Dead_Letter_Terminal_Events",
        threshold_arg="max_dead_letter_events",
        description="No events should land in the dead-letter table.",
    ),
    CheckDefinition(
        name="failed_raw_events",
        layer="ops",
        severity="error",
        sql="SELECT COUNT(*) AS failing_rows FROM Raw_Terminal_Events WHERE processing_status = 'failed'",
        threshold_arg="max_failed_raw_events",
        description="No raw events should remain marked as failed.",
    ),
    CheckDefinition(
        name="pending_raw_events",
        layer="ops",
        severity="warning",
        sql="SELECT COUNT(*) AS failing_rows FROM Raw_Terminal_Events WHERE processing_status = 'pending'",
        threshold_arg="max_pending_raw_events",
        description="Pending raw events should stay within the allowed backlog.",
    ),
    CheckDefinition(
        name="failed_ingestion_batches",
        layer="ops",
        severity="error",
        sql="SELECT COUNT(*) AS failing_rows FROM Ingestion_Batches WHERE status = 'failed'",
        threshold_arg="max_failed_ingestion_batches",
        description="Historical batch runs should not be recorded as failed.",
    ),
    CheckDefinition(
        name="pending_raw_event_age_minutes",
        layer="ops",
        severity="warning",
        sql="""
            SELECT COALESCE(
                MAX(CEILING(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - ingested_at)) / 60.0)),
                0
            )::BIGINT AS failing_rows
            FROM Raw_Terminal_Events
            WHERE processing_status = 'pending'
        """,
        threshold_arg="max_pending_age_minutes",
        description="Pending raw events should not sit unprocessed for too long.",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local data validations against the terminal project.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5433)
    parser.add_argument("--dbname", default="terminal_ops")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default=None)
    parser.add_argument("--suite-name", default="option1_local_quality_gate")
    parser.add_argument("--environment-name", default="local")
    parser.add_argument("--triggered-by", default="codex")
    parser.add_argument("--fail-on-warning", action="store_true")

    parser.add_argument("--max-schedule-conflicts", type=int, default=0)
    parser.add_argument("--max-load-window-violations", type=int, default=0)
    parser.add_argument("--max-unload-window-violations", type=int, default=0)
    parser.add_argument("--max-occupied-target-violations", type=int, default=0)
    parser.add_argument("--max-location-status-mismatches", type=int, default=0)
    parser.add_argument("--max-raw-lifecycle-violations", type=int, default=0)
    parser.add_argument("--max-dead-letter-events", type=int, default=0)
    parser.add_argument("--max-failed-raw-events", type=int, default=0)
    parser.add_argument("--max-pending-raw-events", type=int, default=0)
    parser.add_argument("--max-failed-ingestion-batches", type=int, default=0)
    parser.add_argument("--max-pending-age-minutes", type=int, default=30)
    return parser.parse_args()


def connect(args: argparse.Namespace):
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )


def threshold_for(check: CheckDefinition, args: argparse.Namespace) -> int:
    return getattr(args, check.threshold_arg)


def insert_validation_run(cur, args: argparse.Namespace) -> int:
    cur.execute(
        """
        INSERT INTO Data_Validation_Runs
        (
            suite_name,
            environment_name,
            triggered_by,
            notes
        )
        VALUES (%s, %s, %s, %s)
        RETURNING run_id
        """,
        (
            args.suite_name,
            args.environment_name,
            args.triggered_by,
            "Local validation suite executed by scripts/run_data_validations.py",
        ),
    )
    return cur.fetchone()[0]


def update_validation_run(cur, run_id: int, status: str, total_checks: int, failed_checks: int, notes: str) -> None:
    cur.execute(
        """
        UPDATE Data_Validation_Runs
        SET finished_at = CURRENT_TIMESTAMP,
            status = %s,
            total_checks = %s,
            failed_checks = %s,
            notes = %s
        WHERE run_id = %s
        """,
        (status, total_checks, failed_checks, notes, run_id),
    )


def insert_validation_result(cur, run_id: int, check: CheckDefinition, observed: int, threshold: int, status: str) -> None:
    cur.execute(
        """
        INSERT INTO Data_Validation_Results
        (
            run_id,
            check_name,
            layer_name,
            severity,
            status,
            failing_rows,
            threshold_value,
            details
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            check.name,
            check.layer,
            check.severity,
            status,
            observed,
            threshold,
            Json(
                {
                    "description": check.description,
                    "observed_value": observed,
                    "threshold_value": threshold,
                }
            ),
        ),
    )


def print_summary(results: list[dict[str, object]], run_id: int, overall_status: str) -> None:
    print(f"Validation run {run_id} finished with status: {overall_status}")
    print("")
    print(f"{'check_name':32} {'layer':8} {'severity':8} {'status':8} {'observed':8} {'threshold':9}")
    print("-" * 82)
    for result in results:
        print(
            f"{str(result['check_name']):32} "
            f"{str(result['layer_name']):8} "
            f"{str(result['severity']):8} "
            f"{str(result['status']):8} "
            f"{str(result['failing_rows']):8} "
            f"{str(result['threshold_value']):9}"
        )


def main() -> int:
    args = parse_args()
    conn = connect(args)
    conn.autocommit = False

    run_id: int | None = None
    error_failures = 0
    warning_failures = 0
    results: list[dict[str, object]] = []

    try:
        with conn.cursor() as cur:
            run_id = insert_validation_run(cur, args)
            conn.commit()

            for check in CHECKS:
                cur.execute(check.sql)
                row = cur.fetchone()
                observed = int(row[0] if row and row[0] is not None else 0)
                threshold = threshold_for(check, args)
                status = "passed" if observed <= threshold else "failed"

                if status == "failed" and check.severity == "error":
                    error_failures += 1
                elif status == "failed":
                    warning_failures += 1

                insert_validation_result(cur, run_id, check, observed, threshold, status)
                results.append(
                    {
                        "check_name": check.name,
                        "layer_name": check.layer,
                        "severity": check.severity,
                        "status": status,
                        "failing_rows": observed,
                        "threshold_value": threshold,
                    }
                )

            overall_status = "failed" if error_failures > 0 or (args.fail_on_warning and warning_failures > 0) else "passed"
            failed_checks = error_failures + warning_failures
            notes = f"errors={error_failures}, warnings={warning_failures}, fail_on_warning={args.fail_on_warning}"
            update_validation_run(cur, run_id, overall_status, len(CHECKS), failed_checks, notes)
            conn.commit()

        print_summary(results, run_id, overall_status)
        return 1 if overall_status == "failed" else 0
    except Exception as exc:
        conn.rollback()
        if run_id is not None:
            with conn.cursor() as cur:
                update_validation_run(cur, run_id, "failed", len(CHECKS), error_failures + warning_failures, str(exc)[:1000])
                conn.commit()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
