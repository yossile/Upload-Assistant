import time

from src.console import logger
from src.meta import Meta


def log_ptp_timing(meta: Meta, stage: str, started_at: float) -> None:
    if meta.benchmark:
        logger.info(f"[PTP timing] {stage}: {time.perf_counter() - started_at:.2f}s")