#!/usr/bin/env python3
"""Backfill script for RDO entries to RDS and OpenSearch.
 
This script handles:
- Scanning S3 for all RDO output files
- Indexing observations to RDS (observations table)
- Indexing reduced RDOs to OpenSearch
- Managing OpenSearch index lifecycle
- Cleaning up/deleting OpenSearch indices and their registry entries

Usage:
    python -m scripts.index_rdo [--new] [--workers N] [--rds-only] [--opensearch-only] [--dry-run] [--observer-id OBSERVER] [--observation-id AD_ID]
    python -m scripts.index_rdo --cleanup

Options:
    --new              Create a new OpenSearch index instead of updating the latest
    --workers N        Number of worker processes for parallel indexing (default: 50)
    --prefix PREFIX    OpenSearch index name prefix (default: reduced-rdo-index_)
    --rds-only         Only index to RDS, skip OpenSearch
    --opensearch-only  Only index to OpenSearch, skip RDS
    --observer-id OBSERVER    Limit backfill to a single observer (UUID)
    --observation-id AD_ID    Select a single observation/ad by ID (requires --observer-id)
    --cleanup          Delete an OpenSearch index (with confirmation)
"""

import multiprocessing
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

from tqdm import tqdm

from db.database import get_session
from models.observation import ObservationORM
from models.open_search_index import OpenSearchIndexORM
from utils import observations_sub_bucket
from utils.indexer.registry import IndexRegistry
from utils.indexer.indexer import Indexer
from utils.opensearch.rdo_open_search import RdoOpenSearch
from utils.logging import get_backfill_logger
from utils.retry import retry_with_exponential_backoff, RetryConfig

logger = get_backfill_logger("rdo")

# Retry configuration for database and OpenSearch operations
DB_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    initial_delay=1.0,
    max_delay=10.0,
    retryable_exceptions=(Exception,)  # Catch transient failures
)

OPENSEARCH_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    initial_delay=2.0,
    max_delay=10.0,
    retryable_exceptions=(Exception,)  # Catch transient failures
)


DEFAULT_WORKERS = 50


def list_all_observers() -> List[str]:
    """List all observer IDs from S3.
    
    Returns:
        List of observer UUIDs
    """
    observers = []
    root_dirs = observations_sub_bucket.list_dir("", list_all=True)
    for d in root_dirs:
        if d.endswith('/'):
            observer_id = d.rstrip('/').split('/')[0]
            # Basic UUID validation
            if len(observer_id) == 36 and observer_id.count('-') == 4:
                observers.append(observer_id)
    return observers


def list_rdo_entries_for_observer(observer_id: str) -> List[Dict[str, str]]:
    """List all RDO entries for an observer.
    
    Args:
        observer_id: The observer UUID
        
    Returns:
        List of dictionaries with observer_id, timestamp, and ad_id
    """
    entries = []
    rdo_path = f"{observer_id}/rdo/"
    
    try:
        rdo_dirs = observations_sub_bucket.list_dir(rdo_path, list_all=True)
        for rdo_dir in rdo_dirs:
            if rdo_dir.endswith('/'):
                # Extract timestamp.ad_id from path
                dir_name = rdo_dir.rstrip('/').split('/')[-1]
                if '.' in dir_name:
                    parts = dir_name.split('.')
                    if len(parts) == 2:
                        timestamp, ad_id = parts
                        if timestamp.isdigit() and len(ad_id) == 36:
                            entries.append({
                                'observer_id': observer_id,
                                'timestamp': timestamp,
                                'ad_id': ad_id
                            })
    except Exception as e:
        logger.error(f"Error scanning observer {observer_id}: {e}")
    
    return entries


def list_all_rdo_entries() -> List[Dict[str, str]]:
    """List all RDO entries from S3.
    
    Scans S3 for all RDO output files and returns their parsed info.
    
    Returns:
        List of dictionaries with observer_id, timestamp, and ad_id
    """
    logger.info("Scanning S3 for RDO entries...")
    
    observers = list_all_observers()
    logger.info(f"Found {len(observers)} observers")
    
    all_entries = []
    for observer_id in tqdm(observers, desc="Scanning observers"):
        entries = list_rdo_entries_for_observer(observer_id)
        all_entries.extend(entries)
    
    logger.info(f"Found {len(all_entries)} RDO entries")
    return all_entries


def _index_opensearch_worker(args) -> bool:
    """Worker function for multiprocessing to index a single ad to OpenSearch."""
    observer_id, timestamp, ad_id, index_name = args
    try:
        indexer = Indexer(skip_on_error=True, index_name=index_name)
        # Wrap indexing with retry logic
        @retry_with_exponential_backoff(max_attempts=3)
        def index_with_retry():
            indexer.put_index_open_search(observer_id, timestamp, ad_id)
        index_with_retry()
        return True
    except Exception:
        return False


def index_to_opensearch(entries: List[Dict[str, str]], index_name: str, 
                        max_workers: int = DEFAULT_WORKERS):
    """Index RDO entries to OpenSearch using parallel processing.
    
    Args:
        entries: List of RDO entry dictionaries
        index_name: The OpenSearch index name
        max_workers: Number of parallel workers
    """
    if max_workers == 1:
        # Sequential processing
        for entry in tqdm(entries, desc="Indexing to OpenSearch", unit="entry"):
            indexer = Indexer(skip_on_error=True, index_name=index_name)
            indexer.put_index_open_search(
                entry["observer_id"], 
                entry["timestamp"], 
                entry["ad_id"]
            )
        return

    # Parallel processing
    logger.info(f"Using {max_workers} workers to index to OpenSearch...")
    with multiprocessing.Pool(processes=max_workers) as pool:
        args = [(e["observer_id"], e["timestamp"], e["ad_id"], index_name) 
                for e in entries]
        
        results = list(tqdm(
            pool.imap_unordered(_index_opensearch_worker, args), 
            total=len(args), 
            desc="Indexing to OpenSearch", 
            unit="entry"
        ))
    
    success_count = sum(1 for r in results if r)
    logger.info(f"Successfully indexed {success_count}/{len(entries)} entries to OpenSearch")


def index_to_rds(entries: List[Dict[str, str]], clear_existing: bool = True):
    """Index RDO entries to RDS observations table.
    
    Args:
        entries: List of RDO entry dictionaries
        clear_existing: If True, clear the table before inserting
    """
    logger.info("Indexing to RDS...")
    
    @retry_with_exponential_backoff(max_attempts=3, initial_delay=1.0, max_delay=10.0)
    def _do_index():
        with get_session() as session:
            if clear_existing:
                deleted = session.query(ObservationORM).delete()
                logger.info(f"Cleared {deleted} existing observations")
            
            # Insert all entries
            observations = [
                ObservationORM(
                    observation_id=entry["ad_id"],
                    observer_id=entry["observer_id"],
                    timestamp=int(entry["timestamp"])
                ) for entry in entries
            ]
            
            session.add_all(observations)
    
    _do_index()
    logger.info(f"Indexed {len(entries)} observations to RDS")


def backfill_rdo(
    new_index: bool = False,
    index_prefix: str = "reduced-rdo-index_",
    max_workers: int = DEFAULT_WORKERS,
    rds_only: bool = False,
    opensearch_only: bool = False,
    dry_run: bool = False,
    observer_id: Optional[str] = None,
    observation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the full RDO backfill process.
    
    Args:
        new_index: If True, create a new OpenSearch index
        index_prefix: Prefix for new OpenSearch index names
        max_workers: Number of parallel workers
        rds_only: If True, only index to RDS
        opensearch_only: If True, only index to OpenSearch
        dry_run: If True, preview changes without making them
        observer_id: If set, only process entries for this observer
        observation_id: If set, only process a single observation/ad (requires observer_id)
        
    Returns:
        Statistics dictionary
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logger.info(f"Starting RDO backfill at {timestamp}")
    
    if dry_run:
        logger.warning("*** DRY-RUN MODE: No changes will be made ***")
    
    # Validate selection arguments
    if observation_id and not observer_id:
        raise ValueError("When using --observation-id, you must provide --observer-id")

    # Collect all RDO entries
    if observer_id:
        logger.info(f"Filtering RDO entries for observer: {observer_id}")
        entries = list_rdo_entries_for_observer(observer_id)
        if observation_id:
            entries = [e for e in entries if e["ad_id"] == observation_id]
    else:
        entries = list_all_rdo_entries()

    if not entries:
        logger.warning("No RDO entries found")
        return {'entries_found': 0}
    
    stats: Dict[str, Any] = {'entries_found': len(entries)}
    
    if dry_run:
        # Show sample of entries
        logger.info(f"Preview: Would index {len(entries)} entries")
        logger.info("Sample entries (first 10):")
        for i, entry in enumerate(entries[:10], 1):
            logger.info(f"  {i}. Observer: {entry['observer_id']}, Timestamp: {entry['timestamp']}, AD ID: {entry['ad_id']}")
        if len(entries) > 10:
            logger.info(f"  ... and {len(entries) - 10} more entries")
        return stats
    
    # Index to RDS
    if not opensearch_only:
        index_to_rds(entries, clear_existing=True)
        stats['rds_indexed'] = len(entries)
    
    # Index to OpenSearch
    if not rds_only:
        registry = IndexRegistry()
        
        if new_index:
            registry.prepare(prefix=index_prefix)
        else:
            registry.from_latest(status='ready')
        
        if not registry.name:
            raise ValueError("No OpenSearch index available")
        
        registry.start()
        
        try:
            index_to_opensearch(entries, registry.name, max_workers=max_workers)
            registry.complete()
            stats['opensearch_index'] = registry.name
            
            latest = registry.get_latest()
            if latest:
                logger.info(f"OpenSearch index ready: {latest.name}")
                
        except Exception as e:
            logger.error(f"Error indexing to OpenSearch: {e}")
            registry.fail()
            raise
    
    logger.info("=== RDO Backfill Complete ===")
    logger.info(f"Entries found: {stats['entries_found']}")
    if 'rds_indexed' in stats:
        logger.info(f"RDS indexed: {stats['rds_indexed']}")
    if 'opensearch_index' in stats:
        logger.info(f"OpenSearch index: {stats['opensearch_index']}")
    
    return stats


def list_all_indices() -> List[OpenSearchIndexORM]:
    """List all OpenSearch indices from the registry.
    
    Returns:
        List of OpenSearchIndexORM objects
    """
    with get_session() as session:
        indices = session.query(OpenSearchIndexORM).order_by(
            OpenSearchIndexORM.created_at.desc()
        ).all()
        
        # Detach from session
        session.expunge_all()
        return indices


def show_index_status(index: OpenSearchIndexORM):
    """Display index information.
    
    Args:
        index: The OpenSearchIndexORM object
    """
    created_dt = datetime.fromtimestamp(index.created_at)
    print(f"  Name:    {index.name}")
    print(f"  Status:  {index.status}")
    print(f"  Created: {created_dt.strftime('%Y-%m-%d %H:%M:%S')}")


def cleanup_index(index_name: str) -> bool:
    """Delete an OpenSearch index and its registry entry.
    
    Deletes the index from both OpenSearch and the RDS registry.
    
    Args:
        index_name: Name of the index to delete
        
    Returns:
        True if successful, False otherwise
    """
    # Get index from registry
    with get_session() as session:
        index = session.query(OpenSearchIndexORM).filter_by(
            name=index_name
        ).first()
        
        if not index:
            logger.warning(f"Index '{index_name}' not found in registry")
            return False
        
        # Show status before deletion
        logger.info("Index Status:")
        created_dt = datetime.fromtimestamp(index.created_at)
        logger.info(f"  Name:    {index.name}")
        logger.info(f"  Status:  {index.status}")
        logger.info(f"  Created: {created_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Ask for confirmation
        logger.warning(f"About to delete index '{index_name}' from:")
        logger.warning("  1. OpenSearch cluster")
        logger.warning("  2. RDS registry")
        
        response = input(f"\nType 'yes' to confirm deletion: ").strip().lower()
        
        if response != 'yes':
            logger.info("Deletion cancelled")
            return False
        
        # Delete from OpenSearch
        logger.info(f"Deleting index '{index_name}' from OpenSearch...")
        try:
            opensearch = RdoOpenSearch(index=index_name)
            opensearch.delete_index()
            logger.info("Successfully deleted from OpenSearch")
        except Exception as e:
            logger.error(f"Error deleting from OpenSearch: {e}")
            return False
        
        # Delete from RDS registry
        logger.info("Deleting index registry entry from RDS...")
        try:
            session.query(OpenSearchIndexORM).filter_by(
                name=index_name
            ).delete()
            logger.info("Successfully deleted from RDS registry")
        except Exception as e:
            logger.error(f"Error deleting from RDS: {e}")
            return False
    
    logger.info(f"Index '{index_name}' successfully deleted from both OpenSearch and RDS")
    return True


def interactive_cleanup():
    """Interactive index cleanup with user selection.
    
    Lists all indices and allows user to select one for deletion.
    """
    indices = list_all_indices()
    
    if not indices:
        logger.warning("No indices found in registry")
        return
    
    logger.info("=== OpenSearch Indices ===\n")
    
    for i, index in enumerate(indices, 1):
        logger.info(f"[{i}] {index.name}")
        logger.info(f"    Status: {index.status}\n")
    
    while True:
        try:
            choice = input(f"Select index to delete (1-{len(indices)}) or 0 to cancel: ").strip()
            
            if choice == '0':
                logger.info("Cancelled")
                return
            
            index_num = int(choice)
            if 1 <= index_num <= len(indices):
                selected_index = indices[index_num - 1]
                cleanup_index(selected_index.name)
                break
            else:
                logger.warning(f"Invalid choice. Please select 1-{len(indices)}")
        except ValueError:
            logger.warning("Invalid input. Please enter a number")
        except KeyboardInterrupt:
            logger.info("Cancelled")
            return


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Backfill RDO entries to RDS and OpenSearch"
    )
    parser.add_argument(
        "--new",
        action="store_true",
        help="Create a new OpenSearch index instead of updating the latest"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of worker processes (default: {DEFAULT_WORKERS})"
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="reduced-rdo-index_",
        help="OpenSearch index name prefix (default: reduced-rdo-index_)"
    )
    parser.add_argument(
        "--rds-only",
        action="store_true",
        help="Only index to RDS, skip OpenSearch"
    )
    parser.add_argument(
        "--opensearch-only",
        action="store_true",
        help="Only index to OpenSearch, skip RDS"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete an OpenSearch index (interactive selection with confirmation)"
    )
    parser.add_argument(
        "--observer-id",
        type=str,
        help="Observer UUID to limit the backfill to a single observer"
    )
    parser.add_argument(
        "--observation-id",
        type=str,
        help="Observation AD id to limit to a single ad (requires --observer-id)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be indexed without making changes"
    )
    
    args = parser.parse_args()
    
    if args.cleanup:
        interactive_cleanup()
    else:
        if args.rds_only and args.opensearch_only:
            logger.error("Cannot specify both --rds-only and --opensearch-only")
            exit(1)

        if args.observation_id and not args.observer_id:
            logger.error("--observation-id requires --observer-id to be specified")
            exit(1)
        
        try:
            backfill_rdo(
                new_index=args.new,
                index_prefix=args.prefix,
                max_workers=args.workers,
                rds_only=args.rds_only,
                opensearch_only=args.opensearch_only,
                dry_run=args.dry_run,
                observer_id=args.observer_id,
                observation_id=args.observation_id,
            )
        except Exception as e:
            logger.error(f"Backfill failed: {e}")
            exit(1)
