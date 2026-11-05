import json
import os
from pathlib import Path
import re
from .errors import ConfigurationError


def resolve_env(value, strict=True):
    if isinstance(value, dict):
        return {key: resolve_env(item, strict) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_env(item, strict) for item in value]
    if not isinstance(value, str):
        return value
    def replace(match):
        name = match.group(1)
        if name not in os.environ:
            if strict:
                raise ConfigurationError(f"Set environment variable {name}")
            return ""
        return os.environ[name]
    return re.sub(r"\$\{([A-Z_][A-Z0-9_]*)\}", replace, value)


def load_config(path, broker, execute=False):
    try:
        data = json.loads(Path(path).expanduser().read_text())
    except (OSError, ValueError):
        raise ConfigurationError(f"Cannot read JSON configuration: {path}") from None
    if not isinstance(data, dict) or not isinstance(data.get("brokers"), dict):
        raise ConfigurationError("Config must have a brokers object")
    if broker not in data["brokers"]:
        raise ConfigurationError(f"Missing brokers.{broker} configuration")
    # Resolve only the selected broker; unused accounts do not require credentials.
    selected = resolve_env(data["brokers"][broker], strict=execute)
    if not isinstance(selected, dict):
        raise ConfigurationError("Broker configuration must be an object")
    if broker == "bitget" and execute:
        # Live CLI/service entry points share this default, including older
        # private configs that predate the independent monitor.
        monitor = selected.setdefault("monitor", {})
        if not isinstance(monitor, dict):
            raise ConfigurationError("Bitget monitor must be an object")
        monitor.setdefault("enabled", True)
        monitor.setdefault("poll_seconds", 1.2)
    elif broker == "bitget" and isinstance(selected.get("monitor"), dict):
        # Offline previews do not start workers or need an account quota file.
        selected["monitor"]["enabled"] = False
    return data, selected
