from dataclasses import dataclass, field
from typing import Dict, List
import json
import os


@dataclass
class RunReport:
    mapping_name: str
    mode: str
    records_processed: int = 0
    errors: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0

    def to_dict(self) -> Dict:
        return {
            "mapping_name": self.mapping_name,
            "mode": self.mode,
            "records_processed": self.records_processed,
            "errors": self.errors,
            "duration_seconds": self.duration_seconds,
        }


def write_daily_report(reports: List[RunReport], directory: str = "etl/reports") -> str:
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, "report.json")
    data = [r.to_dict() for r in reports]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    return path
