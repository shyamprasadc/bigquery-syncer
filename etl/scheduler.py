from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from .logger import get_logger

logger = get_logger("etl.scheduler")


def schedule_daily(cron: str, func, *args, **kwargs):
    sched = BlockingScheduler()
    sched.add_job(
        func, trigger="cron", **_cron_args_from_string(cron), args=args, kwargs=kwargs
    )
    logger.info("Starting scheduler with cron=%s", cron)
    sched.start()


def _cron_args_from_string(cron: str):
    # very simple parser: "m h dom mon dow"
    parts = cron.split()
    if len(parts) != 5:
        raise ValueError("Cron must be 'm h dom mon dow'")
    return {
        "minute": parts[0],
        "hour": parts[1],
        "day": parts[2],
        "month": parts[3],
        "day_of_week": parts[4],
    }
