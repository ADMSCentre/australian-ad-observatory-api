"""Indexer module for handling ad indexing to RDS and OpenSearch."""

from typing_extensions import Literal

from db.database import get_session
from models.observation import ObservationORM
from utils.opensearch.rdo_open_search import AdWithRDO, RdoOpenSearch


class Indexer:
    """Handles indexing of ads to both RDS and OpenSearch."""
    
    def __init__(self, 
                 stage: Literal['prod', 'test', 'staging'] = 'prod',
                 index_name: str | None = None, 
                 skip_on_error: bool = True):
        self.stage = stage
        self.skip_on_error = skip_on_error
        self.index_name = index_name
        
    def put_index_rds(self, observer_id: str, timestamp: str, ad_id: str):
        """Add an observation to the RDS observations table."""
        try:
            with get_session() as session:
                observation = ObservationORM(
                    observer_id=observer_id,
                    observation_id=ad_id,
                    timestamp=int(timestamp)
                )
                session.merge(observation)  # merge handles upsert
        except Exception as e:
            print(f"Error indexing ad {ad_id}: {str(e)}")
            if not self.skip_on_error:
                raise e
        
    def put_index_open_search(self, observer_id: str, timestamp: str, ad_id: str):
        """Put an ad into OpenSearch index."""
        if not self.index_name:
            raise ValueError("No index name provided. Please set index_name before indexing.")
        
        try:
            open_search = RdoOpenSearch(index=self.index_name)
            open_search.put(ad_with_rdo=AdWithRDO(
                observer_id=observer_id,
                timestamp=timestamp,
                ad_id=ad_id
            ))
        except Exception as e:
            print(f"Error indexing ad {ad_id}: {str(e)}")
            if not self.skip_on_error:
                raise e
            
    def delete_index_rds(self, observer_id: str, timestamp: str, ad_id: str):
        """Delete an observation from the RDS table."""
        try:
            with get_session() as session:
                session.query(ObservationORM).filter_by(
                    observer_id=observer_id,
                    observation_id=ad_id
                ).delete()
        except Exception as e:
            print(f"Error deleting ad {ad_id} from RDS: {str(e)}")
            if not self.skip_on_error:
                raise e
            
    def delete_index_open_search(self, observer_id: str, timestamp: str, ad_id: str):
        """Delete an ad from OpenSearch index."""
        if not self.index_name:
            raise ValueError("No index name provided. Please set index_name before deleting.")
        
        try:
            open_search = RdoOpenSearch(index=self.index_name)
            open_search.delete(AdWithRDO(
                observer_id=observer_id,
                timestamp=timestamp,
                ad_id=ad_id
            ).open_search_id)
        except Exception as e:
            print(f"Error deleting ad {ad_id} from OpenSearch: {str(e)}")
            if not self.skip_on_error:
                raise e
    
    def delete(self, observer_id: str, timestamp: str, ad_id: str):
        """Delete an ad from both RDS and OpenSearch."""
        self.delete_index_rds(observer_id, timestamp, ad_id)
        self.delete_index_open_search(observer_id, timestamp, ad_id)
    
    def put(self, observer_id: str, timestamp: str, ad_id: str):
        """Put an ad into both RDS and OpenSearch."""
        self.put_index_rds(observer_id, timestamp, ad_id)
        self.put_index_open_search(observer_id, timestamp, ad_id)
