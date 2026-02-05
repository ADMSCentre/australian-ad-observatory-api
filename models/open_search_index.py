"""OpenSearch index ORM model for tracking index versions."""

from uuid import uuid4
from sqlalchemy import BigInteger, String
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class OpenSearchIndexORM(Base):
    """ORM model for tracking OpenSearch index versions.
    
    Each row represents an OpenSearch index with its lifecycle status.
    """
    __tablename__ = 'open_search_indices'
    
    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        index=True,
        default=lambda: str(uuid4())
    )
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False
    )
    created_at: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False
    )

    def __repr__(self):
        return f"<OpenSearchIndex(name={self.name}, status={self.status})>"
