from __future__ import annotations

import atexit
import logging
import os

from flask import Flask

from .config_loader import load_app_config
from .database import Database
from .memory_store import MemoryStore
from .routes import bp
from .services import DestinationDispatcher, RuntimeStatus, SensorPoller, ThingSpeakClient



def create_app() -> Flask:
    config = load_app_config()

    logging.basicConfig(
        level=getattr(logging, config["log_level"], logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = config["secret_key"]
    app.config.update(
        APP_USERS=config["users"],
        SESSION_COOKIE_HTTPONLY=config["session_cookie_httponly"],
        SESSION_COOKIE_SECURE=config["session_cookie_secure"],
        SESSION_COOKIE_SAMESITE=config["session_cookie_samesite"],
    )

    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://monitor:monitor@db:5432/monitoring",
    )
    database = Database(database_url)
    database.create_tables()

    memory_store = MemoryStore()
    runtime_status = RuntimeStatus()
    thingspeak_client = ThingSpeakClient(config["thingspeak_results_per_request"])
    dispatcher = DestinationDispatcher()
    poller = SensorPoller(
        database=database,
        memory_store=memory_store,
        thingspeak_client=thingspeak_client,
        dispatcher=dispatcher,
        interval_seconds=config["poll_interval_seconds"],
        runtime_status=runtime_status,
    )

    app.extensions["database"] = database
    app.extensions["memory_store"] = memory_store
    app.extensions["runtime_status"] = runtime_status
    app.extensions["poller"] = poller

    app.register_blueprint(bp)

    if os.getenv("ENABLE_POLLER", "1") == "1":
        poller.start()
        atexit.register(poller.stop)

    return app
