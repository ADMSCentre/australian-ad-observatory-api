"""Clip classification ORM model for storing ad classification data."""

from uuid import uuid4
from dataclasses import dataclass
from sqlalchemy import BigInteger, Float, String
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class ClipClassificationORM(Base):
    """ORM model for storing ad clip classification data.
    
    Each row represents a single classification label for an observation,
    as an observation can have multiple classification labels with different scores.
    """
    __tablename__ = 'ad_classifications'
    
    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid4())
    )
    observation_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        index=True
    )
    label: Mapped[str] = mapped_column(
        String(100),
        nullable=False
    )
    score: Mapped[float] = mapped_column(
        Float,
        nullable=False
    )
    created_at: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False
    )
    updated_at: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False
    )

    def __repr__(self):
        return f"<ClipClassification(id={self.id}, observation_id={self.observation_id}, label={self.label}, score={self.score})>"


@dataclass
class CompositeClassification:
    """Data class for parsing S3 JSON composite classification data."""
    ranking: int
    label: str
    score_normalized: float
