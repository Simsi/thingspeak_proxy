#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


LOGGER = logging.getLogger("measurement_receiver")


@dataclass
class MeasurementEvent:
    received_at: str
    request_path: str
    remote_addr: str
    device_name: str
    device_hash: str
    event_id: int
    air_temp: float | int | None
    air_hum: float | int | None
    warm_stream: float | int | None
    surface_temp: float | int | None
    raw_payload: dict[str, Any]


class EventStore:
    def __init__(self, output_file: Path | None = None, max_events: int = 5000) -> None:
        self.output_file = output_file
        self.max_events = max(1, int(max_events))
        self._events: list[MeasurementEvent] = []
        self._lock = threading.Lock()
        self._seen_keys: set[tuple[str, int]] = set()

        if self.output_file:
            self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def add(self, event: MeasurementEvent) -> tuple[bool, int]:
        key = (event.device_hash, event.event_id)
        with self._lock:
            is_new = key not in self._seen_keys
            if is_new:
                self._seen_keys.add(key)
            self._events.append(event)
            if len(self._events) > self.max_events:
                self._events = self._events[-self.max_events :]
            count = len(self._events)
            if self.output_file:
                with self.output_file.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(asdict(event), ensure_ascii=False) + "\n")
            return is_new, count

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return [asdict(item) for item in self._events]

    def last(self) -> dict[str, Any] | None:
        with self._lock:
            if not self._events:
                return None
            return asdict(self._events[-1])

    def stats(self) -> dict[str, Any]:
        with self._lock:
            total = len(self._events)
            unique = len(self._seen_keys)
            by_device: dict[str, int] = {}
            for item in self._events:
                by_device[item.device_hash] = by_device.get(item.device_hash, 0) + 1
            return {
                "stored_events": total,
                "unique_device_event_pairs": unique,
                "devices_in_buffer": by_device,
            }


def sanitize_number(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return None
        try:
            number = float(text)
        except ValueError:
            return None
        return int(number) if number.is_integer() else number
    return None


class MeasurementHandler(BaseHTTPRequestHandler):
    server_version = "MeasurementReceiver/1.0"

    def _json_response(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0") or 0)
        if content_length <= 0:
            raise ValueError("Пустое тело запроса")
        raw = self.rfile.read(content_length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"Тело запроса не является валидным JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("Ожидался JSON-объект")
        return payload

    def _validate_measurement(self, payload: dict[str, Any]) -> MeasurementEvent:
        device_name = str(payload.get("device_name", "")).strip()
        device_hash = str(payload.get("device_hash", "")).strip()
        event_raw = payload.get("event_id")

        if not device_name:
            raise ValueError("Поле 'device_name' обязательно")
        if not device_hash:
            raise ValueError("Поле 'device_hash' обязательно")
        try:
            event_id = int(event_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("Поле 'event_id' должно быть целым числом") from exc
        if event_id <= 0:
            raise ValueError("Поле 'event_id' должно быть > 0")

        return MeasurementEvent(
            received_at=datetime.now(timezone.utc).isoformat(),
            request_path=urlparse(self.path).path,
            remote_addr=self.client_address[0] if self.client_address else "unknown",
            device_name=device_name,
            device_hash=device_hash,
            event_id=event_id,
            air_temp=sanitize_number(payload.get("air_temp")),
            air_hum=sanitize_number(payload.get("air_hum")),
            warm_stream=sanitize_number(payload.get("warm_stream")),
            surface_temp=sanitize_number(payload.get("surface_temp")),
            raw_payload=payload,
        )

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        store: EventStore = self.server.event_store  # type: ignore[attr-defined]

        if path == "/health":
            self._json_response(HTTPStatus.OK, {
                "ok": True,
                "service": "test_measurements_receiver",
                "port": self.server.server_port,  # type: ignore[attr-defined]
                "time": datetime.now(timezone.utc).isoformat(),
            })
            return

        if path == "/stats":
            self._json_response(HTTPStatus.OK, {"ok": True, **store.stats()})
            return

        if path == "/last":
            self._json_response(HTTPStatus.OK, {"ok": True, "item": store.last()})
            return

        if path == "/events":
            self._json_response(HTTPStatus.OK, {"ok": True, "items": store.snapshot()})
            return

        self._json_response(HTTPStatus.NOT_FOUND, {
            "ok": False,
            "error": "Маршрут не найден",
            "available_routes": ["POST /ingest", "POST /sensors/data", "POST /sensors/receive", "GET /health", "GET /stats", "GET /last", "GET /events"],
        })

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path not in {"/ingest", "/sensors/data", "/sensors/receive"}:
            self._json_response(HTTPStatus.NOT_FOUND, {
                "ok": False,
                "error": f"Маршрут {path} не поддерживается для приёма замеров",
                "supported_routes": ["/ingest", "/sensors/data", "/sensors/receive"],
            })
            return

        try:
            payload = self._read_json()
            event = self._validate_measurement(payload)
            store: EventStore = self.server.event_store  # type: ignore[attr-defined]
            is_new, buffer_size = store.add(event)
            LOGGER.info(
                "Принято событие device=%s hash=%s event_id=%s path=%s unique=%s",
                event.device_name,
                event.device_hash,
                event.event_id,
                event.request_path,
                is_new,
            )
            self._json_response(HTTPStatus.ACCEPTED, {
                "ok": True,
                "accepted": True,
                "duplicate": not is_new,
                "buffer_size": buffer_size,
                "received": {
                    "device_name": event.device_name,
                    "device_hash": event.device_hash,
                    "event_id": event.event_id,
                },
            })
        except ValueError as exc:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Внутренняя ошибка при обработке запроса")
            self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": f"Внутренняя ошибка сервера: {exc}"})

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        LOGGER.info("%s - %s", self.address_string(), format % args)


class ReceiverServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], handler_cls: type[BaseHTTPRequestHandler], event_store: EventStore) -> None:
        super().__init__(server_address, handler_cls)
        self.event_store = event_store


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Тестовый HTTP-сервер для приёма замеров от ThingSpeak Monitor")
    parser.add_argument("--host", default="0.0.0.0", help="Адрес для прослушивания. По умолчанию 0.0.0.0")
    parser.add_argument("--port", type=int, default=4016, help="Порт сервера. По умолчанию 4016")
    parser.add_argument(
        "--output-file",
        default="received_measurements.jsonl",
        help="Файл для сохранения принятых событий в формате JSON Lines. По умолчанию received_measurements.jsonl",
    )
    parser.add_argument("--max-events", type=int, default=5000, help="Максимум событий в памяти. По умолчанию 5000")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Уровень логирования")
    return parser.parse_args()


def configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

    output_file = Path(args.output_file).resolve() if args.output_file else None
    store = EventStore(output_file=output_file, max_events=args.max_events)
    server = ReceiverServer((args.host, args.port), MeasurementHandler, store)

    shutdown_event = threading.Event()

    def _shutdown(signum: int, _frame: Any) -> None:
        LOGGER.info("Получен сигнал %s, останавливаю сервер", signum)
        shutdown_event.set()
        server.shutdown()

    for signame in ("SIGINT", "SIGTERM"):
        if hasattr(signal, signame):
            signal.signal(getattr(signal, signame), _shutdown)

    routes_text = ", ".join(["POST /ingest", "POST /sensors/data", "POST /sensors/receive", "GET /health", "GET /stats", "GET /last", "GET /events"])
    LOGGER.info("Тестовый сервер запущен на http://%s:%s", args.host, args.port)
    LOGGER.info("Поддерживаемые маршруты: %s", routes_text)
    if output_file:
        LOGGER.info("События будут сохраняться в %s", output_file)

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        LOGGER.info("Остановка по Ctrl+C")
    finally:
        server.server_close()
        LOGGER.info("Сервер остановлен")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
