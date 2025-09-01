from apscheduler.schedulers.asyncio import AsyncIOScheduler
from typing import Callable

def setup_scheduler(job_coro_factory: Callable, interval_minutes: int) -> AsyncIOScheduler:
    """
    job_coro_factory: функция без аргументов, возвращающая корутину (или непосредственно корутину).
    """
    scheduler = AsyncIOScheduler()
    scheduler.add_job(job_coro_factory, "interval", minutes=interval_minutes,
                      id="fetch_job", coalesce=True, max_instances=1)
    scheduler.start()
    return scheduler