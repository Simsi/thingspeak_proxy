from __future__ import annotations

import logging
import math
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from .database import Database
from .memory_store import MemoryStore

logger = logging.getLogger(__name__)


@dataclass
class RuntimeStatus:
    last_poll_at: str | None = None
    last_success_at: str | None = None
    last_error: str | None = None


class ThingSpeakClient:
    def __init__(self, results_per_request: int = 100) -> None:
        self.results_per_request = results_per_request
        self.session = requests.Session()

    def fetch_recent(self, channel_id: str) -> dict[str, Any]:
        url = f"https://thingspeak.mathworks.com/channels/{channel_id}/feeds.json"
        response = self.session.get(
            url,
            params={"results": self.results_per_request},
            timeout=(5, 15),
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError("ThingSpeak вернул неожиданный формат данных")
        return data


class DestinationDispatcher:
    def __init__(self) -> None:
        self.executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="destination-sender")

    def dispatch(self, destinations: list[dict[str, Any]], payload: dict[str, Any]) -> None:
        safe_payload = sanitize_payload(payload)
        for destination in destinations:
            self.executor.submit(self._send, destination, safe_payload.copy())

    @staticmethod
    def _send(destination: dict[str, Any], payload: dict[str, Any]) -> None:
        target_url = f"http://{destination['host']}:{destination['port']}{destination['path']}"
        try:
            requests.post(target_url, json=payload, timeout=(1.5, 1.5))
            logger.info(
                "Событие %s отправлено на конечный сервер %s (%s)",
                payload.get("event_id"),
                destination.get("name"),
                target_url,
            )
        except requests.RequestException as exc:
            logger.warning(
                "Не удалось отправить событие %s на конечный сервер %s (%s): %s",
                payload.get("event_id"),
                destination.get("name"),
                target_url,
                exc,
            )


class SensorPoller:
    def __init__(
        self,
        database: Database,
        memory_store: MemoryStore,
        thingspeak_client: ThingSpeakClient,
        dispatcher: DestinationDispatcher,
        interval_seconds: int,
        runtime_status: RuntimeStatus,
    ) -> None:
        self.database = database
        self.memory_store = memory_store
        self.thingspeak_client = thingspeak_client
        self.dispatcher = dispatcher
        self.interval_seconds = interval_seconds
        self.runtime_status = runtime_status
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._run_forever, name="sensor-poller", daemon=True)
        self.thread.start()
        logger.info("Фоновый опросчик ThingSpeak запущен")

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=3)

    def _run_forever(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.run_cycle()
            except Exception as exc:
                self.runtime_status.last_error = str(exc)
                logger.exception("Ошибка фонового опросчика: %s", exc)
            self.stop_event.wait(self.interval_seconds)

    def run_cycle(self) -> None:
        snapshot = self.memory_store.get_snapshot()
        devices = snapshot.get("devices", [])
        destinations = snapshot.get("destinations", [])
        self.runtime_status.last_poll_at = datetime.utcnow().isoformat() + "Z"

        if not devices:
            logger.info("Список устройств пуст, опрашивать нечего")
            self.runtime_status.last_success_at = self.runtime_status.last_poll_at
            self.runtime_status.last_error = None
            return

        for device in devices:
            self._poll_device(device, destinations)

        self.runtime_status.last_success_at = datetime.utcnow().isoformat() + "Z"
        self.runtime_status.last_error = None

    def _poll_device(self, device: dict[str, Any], destinations: list[dict[str, Any]]) -> None:
        device_name = device["name"]
        device_hash = device["device_hash"]
        logger.info("Опрос устройства %s (канал %s)", device_name, device_hash)

        last_event_id = self.database.get_last_event_id(device_hash)
        payload = self.thingspeak_client.fetch_recent(device_hash)
        feeds = payload.get("feeds", [])
        if not isinstance(feeds, list):
            raise ValueError(f"ThingSpeak вернул некорректное поле feeds для {device_name}")

        inserted_count = 0
        for feed in sorted(feeds, key=lambda item: int(item.get("entry_id") or 0)):
            event_id = int(feed.get("entry_id") or 0)
            if event_id <= last_event_id:
                continue
            normalized = normalize_measurement(device_name, device_hash, feed)
            inserted = self.database.insert_measurement(normalized)
            if inserted:
                inserted_count += 1
                self.dispatcher.dispatch(destinations, {
                    "device_name": normalized["device_name"],
                    "device_hash": normalized["device_hash"],
                    "event_id": normalized["event_id"],
                    "air_temp": normalized["air_temp"],
                    "air_hum": normalized["air_hum"],
                    "warm_stream": normalized["warm_stream"],
                    "surface_temp": normalized["surface_temp"],
                })

        logger.info("Устройство %s: добавлено новых записей %s", device_name, inserted_count)



def parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def sanitize_number(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: sanitize_number(value) for key, value in payload.items()}



def parse_created_at(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None



def normalize_measurement(device_name: str, device_hash: str, feed: dict[str, Any]) -> dict[str, Any]:
    event_id = int(feed.get("entry_id") or 0)
    if event_id <= 0:
        raise ValueError(f"Некорректный entry_id для устройства {device_name}")

    return {
        "device_name": device_name,
        "device_hash": device_hash,
        "event_id": event_id,
        "warm_stream": parse_float(feed.get("field1")),
        "surface_temp": parse_float(feed.get("field2")),
        "air_temp": parse_float(feed.get("field3")),
        "air_hum": parse_float(feed.get("field4")),
        "source_created_at": parse_created_at(feed.get("created_at")),
    }
