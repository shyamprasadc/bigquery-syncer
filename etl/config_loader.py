import os
import re
from typing import Any, Dict

import yaml

_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _interpolate_env_vars(raw: str) -> str:
    def replacer(match: re.Match[str]) -> str:
        var = match.group(1)
        return os.getenv(var, "")

    return _ENV_VAR_PATTERN.sub(replacer, raw)


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    text = _interpolate_env_vars(raw)
    data = yaml.safe_load(text) or {}
    return data
