from __future__ import annotations

import json
import os
import re
import threading
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .config_loader import DEFAULT_MEMORY_PATH, load_memory


class MemoryValidationError(Exception):
    pass


_CHANNEL_RE = re.compile(r"/channels/(?P<channel_id>\d+)(?:/|$)")


class MemoryStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or DEFAULT_MEMORY_PATH
        self.lock = threading.RLock()
        self.data = self._normalize(load_memory(self.path))

    def get_snapshot(self) -> dict[str, Any]:
        with self.lock:
            return deepcopy(self.data)

    def replace_devices(self, devices: list[dict[str, Any]]) -> dict[str, Any]:
        with self.lock:
            normalized = self._normalize_devices(devices)
            self.data["devices"] = normalized
            self._write()
            return self.get_snapshot()

    def replace_destinations(self, destinations: list[dict[str, Any]]) -> dict[str, Any]:
        with self.lock:
            normalized = self._normalize_destinations(destinations)
            self.data["destinations"] = normalized
            self._write()
            return self.get_snapshot()

    def _normalize(self, data: dict[str, Any]) -> dict[str, Any]:
        return {
            "devices": self._normalize_devices(data.get("devices", [])),
            "destinations": self._normalize_destinations(data.get("destinations", [])),
        }

    def _normalize_devices(self, devices: list[dict[str, Any]]) -> list[dict[str, str]]:
        if not isinstance(devices, list):
            raise MemoryValidationError("devices должен быть списком")

        normalized: list[dict[str, str]] = []
        seen_names: set[str] = set()
        seen_hashes: set[str] = set()

        for index, item in enumerate(devices, start=1):
            if not isinstance(item, dict):
                raise MemoryValidationError(f"Строка устройства #{index} должна быть объектом")

            name = str(item.get("name", "")).strip()
            thingspeak_url = str(item.get("thingspeak_url", "")).strip()
            if not name:
                raise MemoryValidationError(f"У устройства #{index} не заполнено имя")
            if name in seen_names:
                raise MemoryValidationError(f"Имя устройства {name!r} должно быть уникальным")
            if not thingspeak_url:
                raise MemoryValidationError(
                    f"У устройства {name!r} не заполнена ссылка ThingSpeak"
                )

            device_hash = self.extract_channel_id(thingspeak_url)
            if device_hash in seen_hashes:
                raise MemoryValidationError(
                    f"Канал ThingSpeak {device_hash!r} уже назначен другому устройству"
                )

            normalized.append(
                {
                    "name": name,
                    "thingspeak_url": thingspeak_url,
                    "device_hash": device_hash,
                }
            )
            seen_names.add(name)
            seen_hashes.add(device_hash)

        return normalized

    def _normalize_destinations(self, destinations: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not isinstance(destinations, list):
            raise MemoryValidationError("destinations должен быть списком")

        normalized: list[dict[str, Any]] = []
        seen_names: set[str] = set()
        for index, item in enumerate(destinations, start=1):
            if not isinstance(item, dict):
                raise MemoryValidationError(f"Строка конечного сервера #{index} должна быть объектом")

            name = str(item.get("name", "")).strip()
            host = str(item.get("host", "")).strip()
            path = str(item.get("path", "")).strip()
            port_raw = item.get("port", "")

            if not name:
                raise MemoryValidationError(f"У конечного сервера #{index} не заполнено имя")
            if name in seen_names:
                raise MemoryValidationError(f"Имя конечного сервера {name!r} должно быть уникальным")
            if not host:
                raise MemoryValidationError(f"У конечного сервера {name!r} не заполнен host")
            if not path:
                raise MemoryValidationError(f"У конечного сервера {name!r} не заполнен path")
            if not path.startswith("/"):
                raise MemoryValidationError(
                    f"Путь {path!r} у конечного сервера {name!r} должен начинаться с '/'"
                )
            try:
                port = int(port_raw)
            except (TypeError, ValueError) as exc:
                raise MemoryValidationError(
                    f"Порт у конечного сервера {name!r} должен быть числом"
                ) from exc
            if port < 1 or port > 65535:
                raise MemoryValidationError(
                    f"Порт у конечного сервера {name!r} должен быть в диапазоне 1..65535"
                )

            normalized.append(
                {
                    "name": name,
                    "host": host,
                    "port": port,
                    "path": path,
                }
            )
            seen_names.add(name)

        return normalized

    def _write(self) -> None:
        target_dir = self.path.parent
        os.makedirs(target_dir, exist_ok=True)

        serialized = json.dumps(self.data, ensure_ascii=False, indent=2) + "\n"
        try:
            with self.path.open("w", encoding="utf-8") as fh:
                fh.write(serialized)
                fh.flush()
                os.fsync(fh.fileno())
        except OSError as exc:
            raise OSError(f"ошибка записи файла {self.path}: {exc}") from exc

    @staticmethod
    def extract_channel_id(thingspeak_url: str) -> str:
        parsed = urlparse(thingspeak_url)
        if parsed.scheme not in {"http", "https"}:
            raise MemoryValidationError(
                f"Ссылка ThingSpeak должна начинаться с http:// или https://: {thingspeak_url!r}"
            )
        match = _CHANNEL_RE.search(parsed.path)
        if not match:
            raise MemoryValidationError(
                "Не удалось извлечь номер канала из ссылки ThingSpeak. "
                "Ожидается формат вроде https://thingspeak.mathworks.com/channels/2918084"
            )
        return match.group("channel_id")
