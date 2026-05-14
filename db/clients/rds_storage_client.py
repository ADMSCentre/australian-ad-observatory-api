from botocore.exceptions import ClientError
from db.clients.base_storage_client import BaseStorageClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import config
from models.base import Base as BaseORM

DB_HOST = config.postgres.host
DB_PORT = config.postgres.port
DB_DATABASE = config.postgres.database
DB_USERNAME = config.postgres.username
DB_PASSWORD = config.postgres.password

db_url = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}'

def get_db_session(db_url: str):
    """Establishes a connection to the PostgreSQL database and returns a session and engine."""
    try:
        engine = create_engine(db_url)
        SessionLocal = sessionmaker(bind=engine)
        return SessionLocal, engine
    except Exception as e:
        print(f"Could not connect to the database: {e}")
        return None, None

class RdsSession:
    """Context manager for RDS database sessions."""
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.session_maker, self.engine = get_db_session(db_url)
        if not self.session_maker or not self.engine:
            raise ConnectionError("Failed to connect to the RDS database.")

    def __enter__(self):
        self.session = self.session_maker()
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        self.engine.dispose()

class RdsStorageClient(BaseStorageClient):
    """A client for Amazon RDS storage."""
    def __init__(self, **config: dict):
        """Initialize the RDS storage client with configuration parameters.

        Args:
            base_orm (any): The base ORM class for the database models.
        """
        super().__init__(**config)
        self.db_url = db_url
        if not self.db_url:
            raise ValueError("Database URL must be provided in the configuration.")
        self.base_orm: __class__ = config.get('base_orm')
        if not self.base_orm:
            raise ValueError("Base ORM class must be provided in the configuration.")
        self.session_maker = None
        self.engine = None
        self.connected = False

    def connect(self):
        """Connect to the RDS service."""
        self.session_maker, self.engine = get_db_session(self.db_url)
        if self.session_maker and self.engine:
            self.connected = True
        else:
            raise ConnectionError("Failed to connect to the RDS database.")

    def disconnect(self):
        """Disconnect from the RDS service."""
        if self.engine:
            self.engine.dispose()
        self.connected = False

    def get(self, keys, **kwargs):
        """Retrieve an object from the RDS table."""
        if not self.connected:
            raise ConnectionError("Not connected to the RDS database.")
        try:
            with self.session_maker() as session:
                orm = session.query(self.base_orm).filter_by(**keys)
                # If only one object is expected, return the first one,
                # otherwise return all matching objects
                builder = kwargs.get('builder', None)
                if builder:
                    orm = builder(orm)
                if orm:
                    return [obj.__dict__ for obj in orm]
                return []
        except Exception as e:
            raise e

    def build_query(self, builder, **kwargs):
        """Query the RDS table with a custom query."""
        if not self.connected:
            raise ConnectionError("Not connected to the RDS database.")
        try:
            with self.session_maker() as session:
                query = session.query(self.base_orm)
                if builder:
                    query = builder(query, **kwargs)
                return [obj.__dict__ for obj in query.all()]
        except Exception as e:
            raise e

    def put(self, value: dict):
        """Store an object in the RDS table."""
        if not self.connected:
            raise ConnectionError("Not connected to the RDS database.")
        try:
            with self.session_maker() as session:
                # Check if the value is already in the database
                keys = self.base_orm.__table__.primary_key.columns.keys()
                value_keys = {key: value[key] for key in keys if key in value}
                existing_orm = session.query(self.base_orm).filter_by(**value_keys).first()
                # If it exists, update the existing object
                if existing_orm:
                    for key, val in value.items():
                        setattr(existing_orm, key, val)
                    session.commit()
                    return
                # Otherwise, create a new object
                orm = self.base_orm(**value)
                session.add(orm)
                session.commit()
        except Exception as e:
            raise e

    def delete(self, keys):
        """Delete an object from the RDS table."""
        if not self.connected:
            raise ConnectionError("Not connected to the RDS database.")
        # Protect against empty keys to avoid accidental deletion of all objects
        if not keys:
            raise ValueError("Keys must not be empty. Provide at least one key to delete an object.")
        try:
            with self.session_maker() as session:
                query = session.query(self.base_orm).filter_by(**keys)
                results = query.all()
                if not results:
                    raise ValueError(f"No objects found with keys: {keys}")
                for result in results:
                    session.delete(result)
                session.commit()
        except Exception as e:
            raise e

    def list_ids(self):
        """List all object IDs in the RDS table."""
        if not self.connected:
            raise ConnectionError("Not connected to the RDS database.")
        try:
            with self.session_maker() as session:
                primary_keys = self.base_orm.__table__.primary_key.columns.keys()
                return [
                    {key: getattr(orm, key) for key in primary_keys}
                    for orm in session.query(self.base_orm).all()
                ]
        except Exception as e:
            raise e

    def list(self):
        """List all objects in the RDS table."""
        if not self.connected:
            raise ConnectionError("Not connected to the RDS database.")
        try:
            with self.session_maker() as session:
                return [
                    {
                        "keys": {key: getattr(orm, key) for key in self.base_orm.__table__.primary_key.columns.keys()},
                        "value": orm.__dict__
                    } 
                    for orm in session.query(self.base_orm).all()
                ]
        except Exception as e:
            raise e