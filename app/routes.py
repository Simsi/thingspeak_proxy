from __future__ import annotations

import hmac
import secrets
from functools import wraps
from typing import Any

from flask import (
    Blueprint,
    Response,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash

from .memory_store import MemoryValidationError

bp = Blueprint("web", __name__)


def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("web.login"))
        return view_func(*args, **kwargs)

    return wrapper



def api_login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("authenticated"):
            return jsonify({"ok": False, "error": "Требуется авторизация"}), 401
        return view_func(*args, **kwargs)

    return wrapper



def verify_csrf() -> None:
    token = request.headers.get("X-CSRF-Token", "")
    expected = session.get("csrf_token", "")
    if not token or not expected or not hmac.compare_digest(token, expected):
        raise PermissionError("Недействительный CSRF-токен")



def _check_credentials(username: str, password: str) -> bool:
    users = current_app.config["APP_USERS"]
    for user in users:
        if user["username"] != username:
            continue
        if user.get("password_hash"):
            return check_password_hash(user["password_hash"], password)
        return hmac.compare_digest(user.get("password", ""), password)
    return False


@bp.route("/")
def index() -> Response:
    if session.get("authenticated"):
        return redirect(url_for("web.monitoring"))
    return redirect(url_for("web.login"))


@bp.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if _check_credentials(username, password):
            session.clear()
            session["authenticated"] = True
            session["username"] = username
            session["csrf_token"] = secrets.token_urlsafe(32)
            return redirect(url_for("web.monitoring"))
        error = "Неверный логин или пароль"
    return render_template("login.html", error=error)


@bp.post("/logout")
@login_required
def logout() -> Response:
    form_token = request.form.get("csrf_token", "")
    if not hmac.compare_digest(form_token, session.get("csrf_token", "")):
        return redirect(url_for("web.monitoring"))
    session.clear()
    return redirect(url_for("web.login"))


@bp.route("/monitoring")
@login_required
def monitoring() -> str:
    memory_snapshot = current_app.extensions["memory_store"].get_snapshot()
    runtime_status = current_app.extensions["runtime_status"]
    return render_template(
        "monitoring.html",
        username=session.get("username"),
        csrf_token=session.get("csrf_token"),
        devices=memory_snapshot["devices"],
        destinations=memory_snapshot["destinations"],
        runtime_status=runtime_status,
    )


@bp.get("/health")
def health() -> Response:
    db = current_app.extensions["database"]
    try:
        db.ping()
        db_status = "ok"
    except Exception as exc:
        db_status = f"error: {exc}"
    return jsonify({"ok": True, "database": db_status})


@bp.get("/api/bootstrap")
@api_login_required
def bootstrap() -> Response:
    memory_snapshot = current_app.extensions["memory_store"].get_snapshot()
    runtime_status = current_app.extensions["runtime_status"]
    return jsonify(
        {
            "ok": True,
            "devices": memory_snapshot["devices"],
            "destinations": memory_snapshot["destinations"],
            "selected_device": memory_snapshot["devices"][0]["name"] if memory_snapshot["devices"] else None,
            "runtime_status": {
                "last_poll_at": runtime_status.last_poll_at,
                "last_success_at": runtime_status.last_success_at,
                "last_error": runtime_status.last_error,
            },
        }
    )


@bp.get("/api/measurements")
@api_login_required
def measurements() -> Response:
    device_name = request.args.get("device_name", "").strip()
    if not device_name:
        return jsonify({"ok": False, "error": "Не передан device_name"}), 400

    db = current_app.extensions["database"]
    rows = db.get_measurements(device_name=device_name, limit=150)
    return jsonify(
        {
            "ok": True,
            "items": [
                {
                    "event_id": row.event_id,
                    "device_name": row.device_name,
                    "device_hash": row.device_hash,
                    "air_temp": row.air_temp,
                    "air_hum": row.air_hum,
                    "warm_stream": row.warm_stream,
                    "surface_temp": row.surface_temp,
                    "source_created_at": row.source_created_at.isoformat() if row.source_created_at else None,
                    "inserted_at": row.inserted_at.isoformat() if row.inserted_at else None,
                }
                for row in rows
            ],
        }
    )


@bp.post("/api/devices/replace")
@api_login_required
def replace_devices() -> Response:
    try:
        verify_csrf()
        payload: dict[str, Any] = request.get_json(force=True)
        devices = payload.get("devices", [])
        snapshot = current_app.extensions["memory_store"].replace_devices(devices)
        return jsonify({"ok": True, "devices": snapshot["devices"]})
    except PermissionError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 403
    except MemoryValidationError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@bp.post("/api/destinations/replace")
@api_login_required
def replace_destinations() -> Response:
    try:
        verify_csrf()
        payload: dict[str, Any] = request.get_json(force=True)
        destinations = payload.get("destinations", [])
        snapshot = current_app.extensions["memory_store"].replace_destinations(destinations)
        return jsonify({"ok": True, "destinations": snapshot["destinations"]})
    except PermissionError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 403
    except MemoryValidationError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
