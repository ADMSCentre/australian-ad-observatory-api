"""Database connection and session management.

This module provides a simple SQLAlchemy session factory for database operations.
"""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from config import config
from models.base import Base


# Create the engine using the database URL from config
_engine = None


def get_engine():
    """Get or create the SQLAlchemy engine.
    
    Uses lazy initialization to avoid connection on module import.
    """
    global _engine
    if _engine is None:
        _engine = create_engine(
            config.postgres.url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10
        )
    return _engine


# Session factory - created lazily
_session_factory = None


def _get_session_factory():
    """Get or create the session factory."""
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(
            bind=get_engine(),
            autocommit=False,
            autoflush=False
        )
    return _session_factory


# Alias for direct use
SessionLocal = property(lambda self: _get_session_factory())


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Get a database session with automatic cleanup.
    
    Usage:
        with get_session() as session:
            session.query(Model).all()
            session.add(instance)
            session.commit()
    
    The session is automatically closed when exiting the context,
    and rolled back if an exception occurs.
    """
    session = _get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_tables():
    """Create all tables defined by ORM models.
    
    This is primarily for testing or initial setup.
    Production should use migrations.
    """
    Base.metadata.create_all(bind=get_engine())


def drop_tables():
    """Drop all tables defined by ORM models.
    
    WARNING: This will delete all data. Use with caution.
    """
    Base.metadata.drop_all(bind=get_engine())
