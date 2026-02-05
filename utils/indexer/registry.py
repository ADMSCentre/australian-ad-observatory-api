"""Index registry for tracking OpenSearch index versions."""

from datetime import datetime
from typing import Optional

from db.database import get_session
from models.open_search_index import OpenSearchIndexORM


class IndexRegistry:
    """Manages OpenSearch index lifecycle and status tracking."""
    
    def __init__(self):
        self.name: Optional[str] = None
    
    def prepare(self, prefix: str = 'index_') -> 'IndexRegistry':
        """Create a new index registry entry with 'created' status.
        
        Args:
            prefix: Prefix for the index name (default: 'index_')
            
        Returns:
            Self for method chaining
        """
        now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        self.name = f"{prefix}{now}"
        print(f"Starting index registry with name: {self.name}")
        
        with get_session() as session:
            index = OpenSearchIndexORM(
                name=self.name,
                created_at=int(datetime.now().timestamp()),
                status='created'
            )
            session.add(index)
        
        return self
        
    def from_latest(self, status: str = 'ready') -> 'IndexRegistry':
        """Load the latest index with the given status.
        
        Args:
            status: Filter by status (default: 'ready')
            
        Returns:
            Self for method chaining
            
        Raises:
            ValueError: If no index with the given status is found
        """
        latest_index = self.get_latest(status=status)
        if not latest_index:
            raise ValueError(f"No index registry found with status '{status}'.")
        self.name = latest_index.name
        print(f"Using latest index registry: {self.name}")
        return self
    
    def get_latest(self, status: str = 'ready') -> Optional[OpenSearchIndexORM]:
        """Get the latest index registry with the given status.
        
        Args:
            status: Filter by status (default: 'ready')
            
        Returns:
            The latest OpenSearchIndexORM or None if not found
        """
        with get_session() as session:
            index = session.query(OpenSearchIndexORM).filter_by(
                status=status
            ).order_by(
                OpenSearchIndexORM.created_at.desc()
            ).first()
            
            if index:
                # Detach from session before returning
                session.expunge(index)
            return index
    
    def _update_status(self, new_status: str):
        """Update the status of the current index.
        
        Args:
            new_status: The new status to set
            
        Raises:
            ValueError: If no index name is set
        """
        if not self.name:
            raise ValueError("Index registry has not been prepared. Call prepare() first.")
        
        with get_session() as session:
            index = session.query(OpenSearchIndexORM).filter_by(
                name=self.name
            ).first()
            
            if not index:
                raise ValueError(f"Index registry '{self.name}' not found.")
            
            index.status = new_status
    
    def start(self):
        """Mark the index as 'in_progress'."""
        print(f"Index registry {self.name} started.")
        self._update_status('in_progress')
    
    def fail(self):
        """Mark the index as 'failed'."""
        print(f"Index registry {self.name} failed.")
        self._update_status('failed')
    
    def complete(self):
        """Mark the index as 'ready'."""
        print(f"Index registry {self.name} completed.")
        self._update_status('ready')
