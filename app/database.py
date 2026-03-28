from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
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

    def upsert_measurement(self, payload: dict) -> bool:
        values = {
            "device_name": payload["device_name"],
            "device_hash": payload["device_hash"],
            "event_id": payload["event_id"],
            "air_temp": payload.get("air_temp"),
            "air_hum": payload.get("air_hum"),
            "warm_stream": payload.get("warm_stream"),
            "surface_temp": payload.get("surface_temp"),
            "source_created_at": payload.get("source_created_at"),
        }

        stmt = pg_insert(Measurement).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["device_hash", "event_id"],
            set_={
                "device_name": stmt.excluded.device_name,
                "air_temp": stmt.excluded.air_temp,
                "air_hum": stmt.excluded.air_hum,
                "warm_stream": stmt.excluded.warm_stream,
                "surface_temp": stmt.excluded.surface_temp,
                "source_created_at": stmt.excluded.source_created_at,
            },
        ).returning(Measurement.id)

        with self.session() as session:
            changed_id = session.execute(stmt).scalar_one_or_none()
            return changed_id is not None

    def get_measurements(self, *, device_hash: str | None = None, device_name: str | None = None, limit: int | None = None) -> list[Measurement]:
        if not device_hash and not device_name:
            return []
        with self.session() as session:
            stmt = select(Measurement)
            if device_hash:
                stmt = stmt.where(Measurement.device_hash == device_hash)
            elif device_name:
                stmt = stmt.where(Measurement.device_name == device_name)
            stmt = stmt.order_by(Measurement.source_created_at.asc().nulls_last(), Measurement.event_id.asc())
            if limit is not None:
                stmt = stmt.limit(limit)
            return list(session.execute(stmt).scalars())
