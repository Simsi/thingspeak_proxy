from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Measurement(Base):
    __tablename__ = "measurements"
    __table_args__ = (
        UniqueConstraint("device_hash", "event_id", name="uq_measurements_device_event"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    device_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    air_temp: Mapped[float | None] = mapped_column(Float, nullable=True)
    air_hum: Mapped[float | None] = mapped_column(Float, nullable=True)
    warm_stream: Mapped[float | None] = mapped_column(Float, nullable=True)
    surface_temp: Mapped[float | None] = mapped_column(Float, nullable=True)
    source_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    inserted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
