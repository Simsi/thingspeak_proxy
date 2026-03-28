from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

DEFAULT_CONFIG_PATH = Path(os.getenv("APP_CONFIG_PATH", "/app/config.json"))
DEFAULT_MEMORY_PATH = Path(os.getenv("APP_MEMORY_PATH", "/app/memory.json"))


class ConfigError(Exception):
    pass


class JsonFileError(Exception):
    pass


def _read_json_file(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError as exc:
        raise JsonFileError(f"Файл не найден: {path}") from exc
    except json.JSONDecodeError as exc:
        raise JsonFileError(f"Невалидный JSON в файле {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise JsonFileError(f"Ожидался JSON-объект в {path}")
    return data



def load_app_config(path: Path | None = None) -> dict[str, Any]:
    path = path or DEFAULT_CONFIG_PATH
    data = _read_json_file(path)

    users = data.get("users", [])
    if not isinstance(users, list) or not users:
        raise ConfigError("В config.json должен быть непустой список users")

    normalized_users: list[dict[str, str]] = []
    for item in users:
        if not isinstance(item, dict):
            raise ConfigError("Каждый пользователь в config.json должен быть объектом")
        username = str(item.get("username", "")).strip()
        password = item.get("password")
        password_hash = item.get("password_hash")
        if not username:
            raise ConfigError("У каждого пользователя должен быть username")
        if not password and not password_hash:
            raise ConfigError(
                f"У пользователя {username!r} должен быть password или password_hash"
            )
        normalized_users.append(
            {
                "username": username,
                "password": str(password or ""),
                "password_hash": str(password_hash or ""),
            }
        )

    poll_interval = int(data.get("poll_interval_seconds", 15))
    if poll_interval < 5:
        raise ConfigError("poll_interval_seconds должен быть не меньше 5 секунд")

    results_per_request = int(data.get("thingspeak_results_per_request", 100))
    if results_per_request < 1 or results_per_request > 8000:
        raise ConfigError("thingspeak_results_per_request должен быть в диапазоне 1..8000")

    secret_key = str(data.get("secret_key", "change-me-immediately"))
    if len(secret_key) < 16:
        raise ConfigError("secret_key должен быть длиной не менее 16 символов")

    return {
        "users": normalized_users,
        "secret_key": secret_key,
        "poll_interval_seconds": poll_interval,
        "thingspeak_results_per_request": results_per_request,
        "session_cookie_secure": bool(data.get("session_cookie_secure", False)),
        "session_cookie_httponly": bool(data.get("session_cookie_httponly", True)),
        "session_cookie_samesite": str(data.get("session_cookie_samesite", "Lax")),
        "log_level": str(data.get("log_level", "INFO")).upper(),
    }



def load_memory(path: Path | None = None) -> dict[str, Any]:
    path = path or DEFAULT_MEMORY_PATH
    data = _read_json_file(path)

    devices = data.get("devices", [])
    destinations = data.get("destinations", [])
    if not isinstance(devices, list):
        raise JsonFileError("Поле devices в memory.json должно быть списком")
    if not isinstance(destinations, list):
        raise JsonFileError("Поле destinations в memory.json должно быть списком")

    return {
        "devices": devices,
        "destinations": destinations,
    }
