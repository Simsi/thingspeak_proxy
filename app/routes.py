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
from .services import sanitize_number

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
    return render_template(
        "monitoring.html",
        username=session.get("username"),
        csrf_token=session.get("csrf_token"),
        devices=memory_snapshot["devices"],
        destinations=memory_snapshot["destinations"],
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
    runtime_status = current_app.extensions["runtime_status"].snapshot()
    return jsonify(
        {
            "ok": True,
            "devices": memory_snapshot["devices"],
            "destinations": memory_snapshot["destinations"],
            "selected_device": memory_snapshot["devices"][0]["name"] if memory_snapshot["devices"] else None,
            "runtime_status": runtime_status,
        }
    )


@bp.get("/api/measurements")
@api_login_required
def measurements() -> Response:
    device_hash = request.args.get("device_hash", "").strip()
    device_name = request.args.get("device_name", "").strip()
    if not device_hash and not device_name:
        return jsonify({"ok": False, "error": "Не передан device_hash или device_name"}), 400

    limit_arg = request.args.get("limit", "").strip()
    limit: int | None = None
    if limit_arg:
        try:
            limit = max(1, min(int(limit_arg), 50000))
        except ValueError:
            return jsonify({"ok": False, "error": "Параметр limit должен быть числом"}), 400

    db = current_app.extensions["database"]
    rows = db.get_measurements(device_hash=device_hash or None, device_name=device_name or None, limit=limit)
    return jsonify(
        {
            "ok": True,
            "items": [
                {
                    "event_id": row.event_id,
                    "device_name": row.device_name,
                    "device_hash": row.device_hash,
                    "air_temp": sanitize_number(row.air_temp),
                    "air_hum": sanitize_number(row.air_hum),
                    "warm_stream": sanitize_number(row.warm_stream),
                    "surface_temp": sanitize_number(row.surface_temp),
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
    except OSError as exc:
        current_app.logger.exception("Ошибка сохранения memory.json")
        return jsonify({"ok": False, "error": f"Не удалось сохранить memory.json: {exc}"}), 500
    except Exception as exc:
        current_app.logger.exception("Неожиданная ошибка при сохранении списка устройств")
        return jsonify({"ok": False, "error": f"Не удалось сохранить список устройств: {exc}"}), 500


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
    except OSError as exc:
        current_app.logger.exception("Ошибка сохранения memory.json")
        return jsonify({"ok": False, "error": f"Не удалось сохранить memory.json: {exc}"}), 500
    except Exception as exc:
        current_app.logger.exception("Неожиданная ошибка при сохранении списка конечных серверов")
        return jsonify({"ok": False, "error": f"Не удалось сохранить список конечных серверов: {exc}"}), 500
