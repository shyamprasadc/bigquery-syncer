from datetime import datetime
from typing import Any, Dict, Iterable, List

import pandas as pd
import pytz


def normalize_timestamps(
    records: Iterable[Dict[str, Any]], tz: str
) -> List[Dict[str, Any]]:
    if not records:
        return []
    tzinfo = pytz.timezone(tz)
    out: List[Dict[str, Any]] = []
    for r in records:
        converted = {}
        for k, v in r.items():
            if isinstance(v, (pd.Timestamp, datetime)):
                if v.tzinfo is None:
                    v = tzinfo.localize(v)
                v = v.astimezone(pytz.UTC)
                converted[k] = v.isoformat()
            else:
                converted[k] = v
        out.append(converted)
    return out


def cast_nulls(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{k: (None if v == "" else v) for k, v in r.items()} for r in records]


def align_columns(
    records: List[Dict[str, Any]], columns: List[str]
) -> List[Dict[str, Any]]:
    if not records:
        return []
    aligned = []
    for r in records:
        aligned.append({c: r.get(c) for c in columns})
    return aligned
