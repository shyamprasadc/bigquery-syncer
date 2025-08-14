import argparse
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from google.cloud import bigquery

from .config_loader import load_config
from .logger import get_logger, with_context
from .state import StateConfig, StateStore
from .schema import get_bq_table_schema, schema_difference, add_missing_columns
from .extract import MySQLExtractor, build_mysql_params
from .transform import cast_nulls, normalize_timestamps, align_columns
from .load import BigQueryLoader
from .report import RunReport, write_daily_report
from .scheduler import schedule_daily

logger = get_logger("etl.main")


def _build_state_store(cfg: Dict[str, Any]) -> StateStore:
    state_cfg = StateConfig(
        store=cfg.get("state_store", "local"),
        local_path=cfg.get("state_local_path", "state/state.json"),
        gcs_bucket=cfg.get("state_gcs_bucket"),
        gcs_prefix=cfg.get("state_gcs_prefix"),
    )
    return StateStore(state_cfg)


def _run_mapping(
    mapping: Dict[str, Any], global_cfg: Dict[str, Any], state: StateStore
) -> RunReport:
    start_time = time.time()
    name = mapping["name"]
    mode = mapping.get("mode", "append")
    report = RunReport(mapping_name=name, mode=mode)
    context_logger = with_context(logger, {"mapping": name, "mode": mode})

    mysql_params = build_mysql_params()
    extractor = MySQLExtractor(mysql_params)
    bq_loader = BigQueryLoader(
        project=global_cfg["project"],
        dataset=global_cfg["dataset"],
        location=global_cfg.get("location", "US"),
    )

    # Schema discovery
    mysql_columns = extractor.fetch_columns(mapping["mysql_table"])  # [(name, type)]
    bq_loader.ensure_table(mapping["bigquery_table"])
    bq_schema = get_bq_table_schema(
        bq_loader.client, global_cfg["dataset"], mapping["bigquery_table"]
    )
    diff = schema_difference(mysql_columns, bq_schema)
    if diff["missing"] and global_cfg.get("allow_schema_additions", False):
        add_missing_columns(
            bq_loader.client,
            global_cfg["dataset"],
            mapping["bigquery_table"],
            diff["missing"],
        )
    elif diff["missing"]:
        context_logger.warning("Missing columns in BigQuery: %s", diff["missing"])

    # Incremental window
    incremental_column = mapping["incremental_column"]
    last_sync = state.get_last_sync(name) or mapping.get("backfill_start")
    if not last_sync:
        last_sync = "1970-01-01T00:00:00Z"

    # Fetch batches with pagination
    columns = [c for c, _ in mysql_columns]
    all_records: List[Dict[str, Any]] = []
    next_start = last_sync
    while True:
        batch = extractor.fetch_incremental(
            mapping["mysql_table"],
            columns,
            incremental_column,
            start_value=next_start,
            end_value=None,
            batch_size=10000,
            operator=">=" if not all_records else ">",
        )
        if not batch:
            break
        all_records.extend(batch)
        next_start = batch[-1][incremental_column]

    # Transform
    records = cast_nulls(all_records)
    records = normalize_timestamps(
        records, tz=global_cfg.get("default_timezone", "UTC")
    )
    records = align_columns(records, columns)

    # Load
    if mode == "append":
        loaded = bq_loader.load_append(mapping["bigquery_table"], records)
    else:
        loaded = bq_loader.load_upsert(
            mapping["bigquery_table"], records, primary_keys=mapping["primary_keys"]
        )

    report.records_processed = loaded
    # advance watermark
    if all_records:
        max_val = all_records[-1][incremental_column]
        state.set_last_sync(name, str(max_val))
    report.duration_seconds = time.time() - start_time
    return report


def run_once(config_path: str) -> List[RunReport]:
    load_dotenv()
    cfg = load_config(config_path)
    state = _build_state_store(cfg)
    reports: List[RunReport] = []
    for mapping in cfg.get("mappings", []):
        try:
            reports.append(_run_mapping(mapping, cfg, state))
        except Exception as exc:
            logger.exception("Failed mapping %s: %s", mapping.get("name"), exc)
    try:
        path = write_daily_report(reports)
        logger.info("Wrote run report to %s", path)
    except Exception as exc:
        logger.warning("Failed to write run report: %s", exc)
    return reports


def backfill(
    config_path: str, mapping_name: str, start: str, end: Optional[str]
) -> RunReport:
    load_dotenv()
    cfg = load_config(config_path)
    state = _build_state_store(cfg)
    mapping = next(
        (m for m in cfg.get("mappings", []) if m["name"] == mapping_name), None
    )
    if not mapping:
        raise ValueError(f"Mapping not found: {mapping_name}")

    mysql_params = build_mysql_params()
    extractor = MySQLExtractor(mysql_params)
    bq_loader = BigQueryLoader(
        project=cfg["project"],
        dataset=cfg["dataset"],
        location=cfg.get("location", "US"),
    )

    mysql_columns = extractor.fetch_columns(mapping["mysql_table"])
    bq_loader.ensure_table(mapping["bigquery_table"])
    columns = [c for c, _ in mysql_columns]

    records = extractor.fetch_incremental(
        mapping["mysql_table"],
        columns,
        mapping["incremental_column"],
        start_value=start,
        end_value=end,
        batch_size=100000,
    )
    records = cast_nulls(records)
    records = normalize_timestamps(records, tz=cfg.get("default_timezone", "UTC"))
    records = align_columns(records, columns)

    mode = mapping.get("mode", "append")
    if mode == "append":
        loaded = bq_loader.load_append(mapping["bigquery_table"], records)
    else:
        loaded = bq_loader.load_upsert(
            mapping["bigquery_table"], records, primary_keys=mapping["primary_keys"]
        )

    rep = RunReport(mapping_name=mapping_name, mode=mode, records_processed=loaded)
    return rep


def main():
    parser = argparse.ArgumentParser(description="MySQL → BigQuery Syncer")
    parser.add_argument("command", choices=["run-once", "schedule", "backfill"])
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--mapping", help="Mapping name for backfill")
    parser.add_argument("--start", help="Backfill start (ISO)")
    parser.add_argument("--end", help="Backfill end (ISO)")
    args = parser.parse_args()

    if args.command == "run-once":
        reports = run_once(args.config)
        for r in reports:
            logger.info("Run report: %s", r.to_dict())
    elif args.command == "schedule":
        cfg = load_config(args.config)
        cron = cfg.get("schedule", {}).get("cron", "0 2 * * *")

        def job():
            reports = run_once(args.config)
            for r in reports:
                logger.info("Run report: %s", r.to_dict())

        schedule_daily(cron, job)
    elif args.command == "backfill":
        if not args.mapping or not args.start:
            raise SystemExit("--mapping and --start are required for backfill")
        rep = backfill(args.config, args.mapping, args.start, args.end)
        logger.info("Backfill report: %s", rep.to_dict())


if __name__ == "__main__":
    main()
