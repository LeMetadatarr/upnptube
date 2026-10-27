"""Values that must survive a restart, such as screen ids, in one JSON file."""
import json
import logging
import os
from pathlib import Path
from typing import Any

LOG = logging.getLogger(__name__)


class Store:
    def __init__(self, path: Path | None):
        self.path = path
        self.data: dict[str, Any] = {}
        if path and path.exists():
            try:
                self.data = json.loads(path.read_text())
            except (OSError, ValueError) as error:
                LOG.warning("Cannot read %s, starting empty: %s", path, error)

    def get(self, key: str) -> Any:
        return self.data.get(key)

    def set(self, key: str, value: Any):
        if self.data.get(key) == value:
            return
        self.data[key] = value
        if not self.path:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.data, indent=1, sort_keys=True))
        os.replace(temporary, self.path)
