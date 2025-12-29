# src/market_streaming/infrastructure/json_file_logger.py

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Mapping, Any, Optional

from market_streaming.application.ports import LoggerPort

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "pipeline.log"


class JsonFileLogger(LoggerPort):
    def __init__(self) -> None:
        self._logger = logging.getLogger("pipeline")
        self._logger.setLevel(logging.INFO)

        if not self._logger.handlers:
            handler = logging.FileHandler(LOG_FILE)
            handler.setLevel(logging.INFO)
            handler.setFormatter(logging.Formatter("%(message)s"))
            self._logger.addHandler(handler)

    def _emit(
        self, level: str, event: str, fields: Optional[Mapping[str, Any]]
    ) -> None:
        payload = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "level": level,
            "event": event,
        }
        if fields:
            payload.update(fields)
        line = json.dumps(payload)
        if level == "ERROR":
            self._logger.error(line)
        else:
            self._logger.info(line)

    def info(self, event: str, fields: Optional[Mapping[str, Any]] = None) -> None:
        self._emit("INFO", event, fields)

    def error(self, event: str, fields: Optional[Mapping[str, Any]] = None) -> None:
        self._emit("ERROR", event, fields)
