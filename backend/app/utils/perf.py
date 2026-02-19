from __future__ import annotations

from time import perf_counter


class PerfTimer:
    def __init__(self) -> None:
        self._start = 0.0
        self.elapsed_seconds = 0.0

    def __enter__(self) -> "PerfTimer":
        self._start = perf_counter()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.elapsed_seconds = perf_counter() - self._start
