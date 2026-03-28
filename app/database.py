from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Iterator

from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from .models import Base, Measurement


class Database:
    def __init__(self, database_url: str) -> None:
        self.engine = create_engine(database_url, future=True, pool_pre_ping=True)
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False, future=True)

    def create_tables(self) -> None:
        Base.metadata.create_all(self.engine)

    @contextmanager
    def session(self) -> Iterator[Session]:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def ping(self) -> None:
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    def get_last_event_id(self, device_hash: str) -> int:
        with self.session() as session:
            result = session.execute(
                select(Measurement.event_id)
                .where(Measurement.device_hash == device_hash)
                .order_by(Measurement.event_id.desc())
                .limit(1)
            ).scalar_one_or_none()
            return int(result or 0)

    def insert_measurement(self, payload: dict) -> bool:
        measurement = Measurement(
            device_name=payload["device_name"],
            device_hash=payload["device_hash"],
            event_id=payload["event_id"],
            air_temp=payload.get("air_temp"),
            air_hum=payload.get("air_hum"),
            warm_stream=payload.get("warm_stream"),
            surface_temp=payload.get("surface_temp"),
            source_created_at=payload.get("source_created_at"),
        )
        try:
            with self.session() as session:
                session.add(measurement)
            return True
        except IntegrityError:
            return False

    def get_measurements(self, device_name: str, limit: int = 120) -> list[Measurement]:
        with self.session() as session:
            rows = list(
                session.execute(
                    select(Measurement)
                    .where(Measurement.device_name == device_name)
                    .order_by(Measurement.event_id.desc())
                    .limit(limit)
                ).scalars()
            )
        rows.reverse()
        return rows
