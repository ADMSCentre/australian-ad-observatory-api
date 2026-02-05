"""Observation ORM model for storing indexed ad records."""

from sqlalchemy import BigInteger, String
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class ObservationORM(Base):
    """ORM model for indexed observations (ads).
    
    Each row represents an ad that has been processed by the RDO pipeline
    and indexed for querying.
    """
    __tablename__ = 'observations'
    
    observation_id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        index=True,
        nullable=False
    )
    observer_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        index=True
    )
    timestamp: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False
    )

    def __repr__(self):
        return f"<Observation(observation_id={self.observation_id}, observer_id={self.observer_id}, timestamp={self.timestamp})>"
