import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from google.cloud import storage


@dataclass
class StateConfig:
    store: str  # local | gcs
    local_path: str
    gcs_bucket: Optional[str]
    gcs_prefix: Optional[str]


class StateStore:
    def __init__(self, cfg: StateConfig):
        self.cfg = cfg

    def _load_local(self) -> Dict[str, Any]:
        if not os.path.exists(self.cfg.local_path):
            return {}
        with open(self.cfg.local_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_local(self, data: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(self.cfg.local_path), exist_ok=True)
        with open(self.cfg.local_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)

    def _gcs_blob(self):
        if not self.cfg.gcs_bucket or not self.cfg.gcs_prefix:
            raise ValueError("GCS state bucket/prefix not configured")
        client = storage.Client()
        bucket = client.bucket(self.cfg.gcs_bucket)
        path = f"{self.cfg.gcs_prefix.rstrip('/')}/state.json"
        return bucket.blob(path)

    def _load_gcs(self) -> Dict[str, Any]:
        blob = self._gcs_blob()
        if not blob.exists():
            return {}
        content = blob.download_as_text(encoding="utf-8")
        return json.loads(content)

    def _save_gcs(self, data: Dict[str, Any]) -> None:
        blob = self._gcs_blob()
        blob.upload_from_string(
            json.dumps(data, indent=2, sort_keys=True), content_type="application/json"
        )

    def load(self) -> Dict[str, Any]:
        if self.cfg.store == "local":
            return self._load_local()
        elif self.cfg.store == "gcs":
            return self._load_gcs()
        raise ValueError(f"Unknown state store: {self.cfg.store}")

    def save(self, data: Dict[str, Any]) -> None:
        if self.cfg.store == "local":
            self._save_local(data)
        elif self.cfg.store == "gcs":
            self._save_gcs(data)
        else:
            raise ValueError(f"Unknown state store: {self.cfg.store}")

    def get_last_sync(self, mapping_name: str) -> Optional[str]:
        data = self.load()
        return data.get("last_sync", {}).get(mapping_name)

    def set_last_sync(self, mapping_name: str, iso_timestamp: str) -> None:
        data = self.load()
        if "last_sync" not in data:
            data["last_sync"] = {}
        data["last_sync"][mapping_name] = iso_timestamp
        self.save(data)
