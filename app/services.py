from __future__ import annotations

import logging
import math
import threading
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
    API_BASE_URL = "https://api.thingspeak.com"

    def __init__(self, results_per_request: int = 100) -> None:
        self.results_per_request = max(1, min(int(results_per_request), 8000))
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "thingspeak-monitor/1.0"})

    def fetch_recent(self, channel_id: str) -> dict[str, Any]:
        return self._fetch(channel_id, results=self.results_per_request)

    def fetch_page(self, channel_id: str, *, results: int = 8000, end: datetime | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"results": max(1, min(int(results), 8000))}
        if end is not None:
            params["end"] = end.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        return self._fetch(channel_id, **params)

    def _fetch(self, channel_id: str, **params: Any) -> dict[str, Any]:
        url = f"{self.API_BASE_URL}/channels/{channel_id}/feeds.json"
        response = self.session.get(url, params=params, timeout=(5, 20))
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError("ThingSpeak вернул неожиданный формат данных")
        feeds = data.get("feeds")
        if feeds is None or not isinstance(feeds, list):
            raise ValueError("ThingSpeak не вернул список feeds")
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

    def backfill_existing_data(self) -> None:
        snapshot = self.memory_store.get_snapshot()
        devices = snapshot.get("devices", [])
        if not devices:
            logger.info("Историческая загрузка пропущена: список устройств пуст")
            return
        for device in devices:
            self._backfill_device(device)

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

    def _backfill_device(self, device: dict[str, Any]) -> None:
        device_name = device["name"]
        device_hash = device["device_hash"]
        logger.info("Стартовая загрузка истории для %s (канал %s)", device_name, device_hash)

        end: datetime | None = None
        total_seen = 0
        total_changed = 0
        page_no = 0
        previous_oldest_entry_id: int | None = None

        while True:
            page_no += 1
            payload = self.thingspeak_client.fetch_page(device_hash, results=8000, end=end)
            channel_info = payload.get("channel") if isinstance(payload.get("channel"), dict) else {}
            mapping = detect_field_mapping(channel_info)
            feeds = payload.get("feeds", [])
            if not feeds:
                break

            page_changed = 0
            page_seen = 0
            ordered_feeds = sorted(feeds, key=lambda item: int(item.get("entry_id") or 0))
            for feed in ordered_feeds:
                normalized = normalize_measurement(device_name, device_hash, feed, mapping)
                if self.database.upsert_measurement(normalized):
                    page_changed += 1
                page_seen += 1

            total_seen += page_seen
            total_changed += page_changed

            oldest_feed = ordered_feeds[0]
            oldest_entry_id = int(oldest_feed.get("entry_id") or 0)
            oldest_created_at = parse_created_at(oldest_feed.get("created_at"))
            logger.info(
                "История %s: страница %s, получено %s, вставлено/обновлено %s, oldest_entry_id=%s",
                device_name,
                page_no,
                page_seen,
                page_changed,
                oldest_entry_id,
            )

            if page_seen < 8000:
                break
            if previous_oldest_entry_id is not None and oldest_entry_id >= previous_oldest_entry_id:
                break
            if oldest_created_at is None:
                break

            previous_oldest_entry_id = oldest_entry_id
            end = oldest_created_at - timedelta(seconds=1)

        logger.info(
            "Историческая загрузка %s завершена: обработано %s записей, вставлено/обновлено %s",
            device_name,
            total_seen,
            total_changed,
        )

    def _poll_device(self, device: dict[str, Any], destinations: list[dict[str, Any]]) -> None:
        device_name = device["name"]
        device_hash = device["device_hash"]
        logger.info("Опрос устройства %s (канал %s)", device_name, device_hash)

        last_event_id = self.database.get_last_event_id(device_hash)
        payload = self.thingspeak_client.fetch_recent(device_hash)
        channel_info = payload.get("channel") if isinstance(payload.get("channel"), dict) else {}
        mapping = detect_field_mapping(channel_info)
        feeds = payload.get("feeds", [])

        inserted_count = 0
        for feed in sorted(feeds, key=lambda item: int(item.get("entry_id") or 0)):
            event_id = int(feed.get("entry_id") or 0)
            if event_id <= last_event_id:
                continue
            normalized = normalize_measurement(device_name, device_hash, feed, mapping)
            if self.database.upsert_measurement(normalized):
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

        logger.info("Устройство %s: вставлено/обновлено записей %s", device_name, inserted_count)


DEFAULT_FIELD_MAPPING = {
    "surface_temp": "field1",
    "warm_stream": "field2",
    "air_hum": "field3",
    "air_temp": "field4",
}

FIELD_KEYWORDS: dict[str, tuple[str, ...]] = {
    "surface_temp": ("surface", "поверх", "стекл", "skin temp"),
    "warm_stream": ("warm stream", "heat flux", "теплов", "поток", "flux", "wt/m", "вт/м"),
    "air_temp": ("air temp", "air temperature", "температура воздуха", "воздух"),
    "air_hum": ("humidity", "влажн", "hum"),
}


def _normalize_label(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).strip().lower()
    return " ".join(normalized.split())


def detect_field_mapping(channel_info: dict[str, Any] | None) -> dict[str, str]:
    mapping = dict(DEFAULT_FIELD_MAPPING)
    if not channel_info:
        return mapping

    available: dict[str, str] = {}
    for index in range(1, 9):
        field_key = f"field{index}"
        field_label = channel_info.get(field_key)
        if isinstance(field_label, str) and field_label.strip():
            available[field_key] = _normalize_label(field_label)

    used_fields: set[str] = set()
    for semantic_name, keywords in FIELD_KEYWORDS.items():
        for field_key, field_label in available.items():
            if field_key in used_fields:
                continue
            if any(keyword in field_label for keyword in keywords):
                mapping[semantic_name] = field_key
                used_fields.add(field_key)
                break

    return mapping


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
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass
    try:
        parsed = parsedate_to_datetime(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError, IndexError):
        return None


def normalize_measurement(device_name: str, device_hash: str, feed: dict[str, Any], field_mapping: dict[str, str] | None = None) -> dict[str, Any]:
    event_id = int(feed.get("entry_id") or 0)
    if event_id <= 0:
        raise ValueError(f"Некорректный entry_id для устройства {device_name}")
    mapping = field_mapping or DEFAULT_FIELD_MAPPING
    return {
        "device_name": device_name,
        "device_hash": device_hash,
        "event_id": event_id,
        "warm_stream": parse_float(feed.get(mapping["warm_stream"])),
        "surface_temp": parse_float(feed.get(mapping["surface_temp"])),
        "air_temp": parse_float(feed.get(mapping["air_temp"])),
        "air_hum": parse_float(feed.get(mapping["air_hum"])),
        "source_created_at": parse_created_at(feed.get("created_at")),
    }
