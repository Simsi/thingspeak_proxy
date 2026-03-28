from __future__ import annotations

import logging
import math
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from json import JSONDecodeError
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
    API_BASE_URLS = (
        "https://api.thingspeak.com",
        "https://thingspeak.mathworks.com",
    )
    MAX_RESULTS_PER_REQUEST = 8000

    def __init__(self, results_per_request: int = 100) -> None:
        self.results_per_request = results_per_request
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "thingspeak-monitor/1.0",
            }
        )

    def fetch_recent(self, channel_id: str) -> dict[str, Any]:
        return self._fetch(
            channel_id,
            params={"results": min(self.results_per_request, self.MAX_RESULTS_PER_REQUEST)},
        )

    def fetch_history(self, channel_id: str) -> dict[str, Any]:
        all_feeds: list[dict[str, Any]] = []
        seen_entry_ids: set[int] = set()
        end_cursor: datetime | None = None
        page_number = 0
        channel_info: dict[str, Any] = {}

        while True:
            page_number += 1
            params: dict[str, Any] = {"results": self.MAX_RESULTS_PER_REQUEST}
            if end_cursor is not None:
                params["end"] = format_thingspeak_datetime(end_cursor)

            payload = self._fetch(channel_id, params=params)
            channel = payload.get("channel")
            if isinstance(channel, dict) and channel:
                channel_info = channel

            feeds = payload.get("feeds", [])
            if not isinstance(feeds, list) or not feeds:
                break

            page_feeds: list[dict[str, Any]] = []
            oldest_created_at: datetime | None = None

            for feed in feeds:
                try:
                    entry_id = int(feed.get("entry_id") or 0)
                except (TypeError, ValueError):
                    continue
                if entry_id <= 0 or entry_id in seen_entry_ids:
                    continue

                seen_entry_ids.add(entry_id)
                page_feeds.append(feed)

                created_at = parse_created_at(feed.get("created_at"))
                if created_at is not None and (oldest_created_at is None or created_at < oldest_created_at):
                    oldest_created_at = created_at

            if page_feeds:
                all_feeds.extend(page_feeds)

            logger.info(
                "ThingSpeak history backfill page %s for channel %s: received=%s unique=%s",
                page_number,
                channel_id,
                len(feeds),
                len(page_feeds),
            )

            if len(feeds) < self.MAX_RESULTS_PER_REQUEST:
                break
            if oldest_created_at is None:
                logger.warning(
                    "Cannot continue historical backfill for channel %s: oldest item has no created_at",
                    channel_id,
                )
                break

            next_end_cursor = oldest_created_at - timedelta(seconds=1)
            if end_cursor is not None and next_end_cursor >= end_cursor:
                logger.warning(
                    "Stopping historical backfill for channel %s to avoid pagination loop",
                    channel_id,
                )
                break
            end_cursor = next_end_cursor

        all_feeds.sort(key=lambda item: int(item.get("entry_id") or 0))
        return {"channel": channel_info, "feeds": all_feeds}

    def _fetch(self, channel_id: str, params: dict[str, Any]) -> dict[str, Any]:
        errors: list[str] = []

        for base_url in self.API_BASE_URLS:
            url = f"{base_url}/channels/{channel_id}/feeds.json"
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=(5, 20),
                )
                response.raise_for_status()

                raw_text = response.text.strip()
                if raw_text == "-1":
                    raise ValueError(
                        f"ThingSpeak отклонил доступ к каналу {channel_id}. "
                        "Для приватного канала нужен read api_key."
                    )

                try:
                    data = response.json()
                except JSONDecodeError as exc:
                    preview = raw_text[:200].replace("\n", " ").replace("\r", " ")
                    raise ValueError(
                        f"ThingSpeak вернул не JSON для канала {channel_id} по адресу {url}: {preview!r}"
                    ) from exc

                if not isinstance(data, dict):
                    raise ValueError(
                        f"ThingSpeak вернул неожиданный формат данных для канала {channel_id}"
                    )
                feeds = data.get("feeds")
                if not isinstance(feeds, list):
                    raise ValueError(
                        f"ThingSpeak вернул некорректное поле feeds для канала {channel_id}"
                    )

                logger.debug(
                    "ThingSpeak: канал %s успешно прочитан через %s, записей=%s, params=%s",
                    channel_id,
                    url,
                    len(feeds),
                    params,
                )
                return data
            except (requests.RequestException, ValueError) as exc:
                logger.warning(
                    "Не удалось прочитать канал %s через %s: %s",
                    channel_id,
                    url,
                    exc,
                )
                errors.append(f"{url}: {exc}")

        raise RuntimeError(
            f"Не удалось получить данные ThingSpeak для канала {channel_id}. "
            f"Проверены адреса: {'; '.join(errors)}"
        )


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

    def _backfill_device(self, device: dict[str, Any]) -> None:
        device_name = device["name"]
        device_hash = device["device_hash"]
        logger.info("Старт исторической загрузки для устройства %s (канал %s)", device_name, device_hash)

        payload = self.thingspeak_client.fetch_history(device_hash)
        feeds = payload.get("feeds", [])
        if not isinstance(feeds, list):
            raise ValueError(f"ThingSpeak вернул некорректное поле feeds для {device_name}")

        inserted_count = 0
        duplicate_count = 0
        invalid_count = 0
        for feed in feeds:
            try:
                normalized = normalize_measurement(device_name, device_hash, feed)
            except ValueError:
                invalid_count += 1
                continue
            inserted = self.database.insert_measurement(normalized)
            if inserted:
                inserted_count += 1
            else:
                duplicate_count += 1

        logger.info(
            "Историческая загрузка устройства %s завершена: получено=%s, добавлено=%s, уже было=%s, пропущено некорректных=%s",
            device_name,
            len(feeds),
            inserted_count,
            duplicate_count,
            invalid_count,
        )

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
                self.dispatcher.dispatch(
                    destinations,
                    {
                        "device_name": normalized["device_name"],
                        "device_hash": normalized["device_hash"],
                        "event_id": normalized["event_id"],
                        "air_temp": normalized["air_temp"],
                        "air_hum": normalized["air_hum"],
                        "warm_stream": normalized["warm_stream"],
                        "surface_temp": normalized["surface_temp"],
                    },
                )

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



def format_thingspeak_datetime(value: datetime) -> str:
    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S")



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
