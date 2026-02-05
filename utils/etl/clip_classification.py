#!/usr/bin/env python3
"""
ETL script for processing clip classification data from S3 and storing it in RDS.

This script reads clip classification JSON files from the S3 bucket 
(fta-mobile-observations-v2/{observer_uuid}/clip_classifications/{observation_uuid}.json)
and stores the composite classification data in the ad_classifications RDS table.

This module uses DELETE+INSERT for idempotency - existing classifications for an
observation are deleted before inserting new ones, ensuring consistent state.
"""

import time
from multiprocessing import Pool, cpu_count
from typing import List, Optional, Tuple
from uuid import uuid4

from tqdm import tqdm

from db.database import get_session
from models.clip_classification import ClipClassificationORM, CompositeClassification
from models.observation import ObservationORM
import utils.observations_sub_bucket as observations_sub_bucket

# Default number of worker processes
DEFAULT_WORKERS = min(8, cpu_count())


def read_clip_classification_from_s3(observer_id: str, observation_id: str) -> Optional[dict]:
    """Read a clip classification JSON file from S3.
    
    Args:
        observer_id: The observer UUID
        observation_id: The observation UUID
        
    Returns:
        The parsed JSON data or None if the file doesn't exist
    """
    path = f"{observer_id}/clip_classifications/{observation_id}.json"
    return observations_sub_bucket.read_json_file(path)


def parse_composite_classifications(data: dict) -> List[CompositeClassification]:
    """Parse composite classification data from the S3 JSON.
    
    Args:
        data: The parsed JSON data from S3
        
    Returns:
        List of CompositeClassification objects
    """
    composite_data = data.get('composite_classification', [])
    classifications = []
    
    for item in composite_data:
        classification = CompositeClassification(
            ranking=item.get('ranking', 0),
            label=item.get('label', ''),
            score_normalized=item.get('score_normalized', 0.0)
        )
        classifications.append(classification)
    
    return classifications


def store_classifications(
    observation_id: str, 
    classifications: List[CompositeClassification],
    current_time: int
) -> int:
    """Store clip classifications in RDS.
    
    Args:
        observation_id: The observation UUID
        classifications: List of CompositeClassification objects
        current_time: Current timestamp for created_at/updated_at
        
    Returns:
        Number of classifications stored
    """
    classification_orms = []
    
    for classification in classifications:
        orm = ClipClassificationORM(
            id=str(uuid4()),
            observation_id=observation_id,
            label=classification.label,
            score=classification.score_normalized,
            created_at=current_time,
            updated_at=current_time
        )
        classification_orms.append(orm)
    
    with get_session() as session:
        session.add_all(classification_orms)
    
    return len(classification_orms)


def delete_existing_classifications(observation_id: str) -> int:
    """Delete existing classifications for an observation.
    
    Args:
        observation_id: The observation UUID
        
    Returns:
        Number of classifications deleted
    """
    with get_session() as session:
        deleted = session.query(ClipClassificationORM).filter_by(
            observation_id=observation_id
        ).delete()
        return deleted


def _process_observation_worker(args: Tuple[str, str]) -> Tuple[bool, int]:
    """Worker function for multiprocessing to process a single observation.
    
    This function is designed to be called by a multiprocessing Pool.
    Each worker uses the shared database connection pool.
    
    Args:
        args: Tuple of (observer_id, observation_id)
        
    Returns:
        Tuple of (success: bool, classifications_count: int)
    """
    observer_id, observation_id = args
    
    try:
        # Read classification data from S3
        data = read_clip_classification_from_s3(observer_id, observation_id)
        if not data:
            return (False, 0)
        
        # Parse the composite classifications
        classifications = parse_composite_classifications(data)
        if not classifications:
            return (False, 0)
        
        # Delete existing classifications for this observation (for idempotency)
        delete_existing_classifications(observation_id)
        
        # Store the new classifications
        current_time = int(time.time() * 1000)
        count = store_classifications(observation_id, classifications, current_time)
        
        return (True, count)
            
    except Exception as e:
        # Silently handle errors in workers, they'll be counted as failures
        return (False, 0)


def process_single_ad(observer_id: str, timestamp: str, observation_id: str) -> bool:
    """Process the clip classification for a single observation.
    
    This will allow for processing of individual observations as needed 
    (e.g., backfilling missing data, or processing new observations as they come in).
    
    Uses DELETE+INSERT for idempotency - existing classifications are deleted
    before inserting new ones.
    
    Args:
        observer_id: The observer UUID
        timestamp: The observation timestamp (unused, kept for interface consistency)
        observation_id: The observation UUID
        
    Returns:
        True if classification was processed successfully, False otherwise
    """
    try:
        # Read classification data from S3
        data = read_clip_classification_from_s3(observer_id, observation_id)
        if not data:
            print(f"No clip classification found for {observer_id}/{observation_id}")
            return False
        
        # Parse the composite classifications
        classifications = parse_composite_classifications(data)
        if not classifications:
            print(f"No composite classifications in file for {observer_id}/{observation_id}")
            return False
        
        # Delete existing classifications for this observation (for idempotency)
        delete_existing_classifications(observation_id)
        
        # Store the new classifications
        current_time = int(time.time() * 1000)  # Milliseconds since epoch
        count = store_classifications(observation_id, classifications, current_time)
        
        print(f"Stored {count} classifications for observation {observation_id}")
        return True
        
    except Exception as e:
        print(f"Error processing {observer_id}/{observation_id}: {e}")
        return False


def list_observations_from_rds() -> List[tuple]:
    """List all observations from RDS.
    
    Returns:
        List of tuples (observer_id, observation_id)
    """
    with get_session() as session:
        observations = session.query(ObservationORM).all()
        return [(obs.observer_id, obs.observation_id) for obs in observations]


def list_clip_classification_files_for_observer(observer_id: str) -> List[str]:
    """List all clip classification files for an observer.
    
    Args:
        observer_id: The observer UUID
        
    Returns:
        List of observation IDs that have clip classification files
    """
    path = f"{observer_id}/clip_classifications/"
    try:
        files = observations_sub_bucket.list_dir(path, list_all=True)
        # Extract observation IDs from file paths
        # Path format: {observer_id}/clip_classifications/{observation_id}.json
        observation_ids = []
        for file_path in files:
            if file_path.endswith('.json'):
                filename = file_path.split('/')[-1]
                observation_id = filename.replace('.json', '')
                observation_ids.append(observation_id)
        return observation_ids
    except Exception as e:
        print(f"Error listing clip classifications for {observer_id}: {e}")
        return []


def list_all_observers() -> List[str]:
    """List all observer IDs from S3.
    
    Returns:
        List of observer UUIDs
    """
    dirs = observations_sub_bucket.list_dir("", list_all=True)
    # Filter to only include directories (ending with /)
    # and extract the observer ID (first part of the path)
    observer_ids = set()
    for d in dirs:
        if d.endswith('/'):
            observer_id = d.rstrip('/').split('/')[0]
            # Basic validation that it looks like a UUID
            if len(observer_id) == 36 and observer_id.count('-') == 4:
                observer_ids.add(observer_id)
    return list(observer_ids)


def process_all_ads(clear_existing: bool = False, num_workers: int = DEFAULT_WORKERS) -> dict:
    """Process clip classifications for all observations using multiprocessing.
    
    This should be run to import all existing clip classifications into RDS.
    Uses a process pool to parallelize processing of observations.
    
    Uses DELETE+INSERT for idempotency - existing classifications for each
    observation are deleted before inserting new ones.
    
    Args:
        clear_existing: If True, clear all existing classifications before processing
        num_workers: Number of worker processes to use (default: min(8, cpu_count))
        
    Returns:
        Dictionary with processing statistics
    """
    stats = {
        'observers_processed': 0,
        'observations_found': 0,
        'classifications_stored': 0,
        'errors': 0
    }
    
    # Optionally clear existing classifications
    if clear_existing:
        print("Clearing existing classifications...")
        with get_session() as session:
            deleted = session.query(ClipClassificationORM).delete()
            print(f"Deleted {deleted} existing classifications")
    
    # Get all observers
    print("Listing all observers from S3...")
    observers = list_all_observers()
    print(f"Found {len(observers)} observers")
    
    # Collect all work items (observer_id, observation_id) pairs
    print("Collecting observation files from S3...")
    work_items: List[Tuple[str, str]] = []
    
    for observer_id in tqdm(observers, desc="Scanning observers"):
        stats['observers_processed'] += 1
        observation_ids = list_clip_classification_files_for_observer(observer_id)
        for observation_id in observation_ids:
            work_items.append((observer_id, observation_id))
    
    stats['observations_found'] = len(work_items)
    print(f"Found {len(work_items)} clip classification files to process")
    
    if not work_items:
        print("No observations to process")
        return stats
    
    # Process observations in parallel using multiprocessing
    print(f"Processing observations using {num_workers} worker processes...")
    
    with Pool(processes=num_workers) as pool:
        # Use imap_unordered for better performance with progress bar
        results = list(tqdm(
            pool.imap_unordered(_process_observation_worker, work_items, chunksize=10),
            total=len(work_items),
            desc="Processing observations"
        ))
    
    # Aggregate results
    for success, count in results:
        if success:
            stats['classifications_stored'] += count
        else:
            stats['errors'] += 1
    
    print("\n=== Processing Complete ===")
    print(f"Observers processed: {stats['observers_processed']}")
    print(f"Observations found: {stats['observations_found']}")
    print(f"Classifications stored: {stats['classifications_stored']}")
    print(f"Errors: {stats['errors']}")
    
    return stats


def process_observer_parallel(observer_id: str, num_workers: int = DEFAULT_WORKERS) -> dict:
    """Process all observations for a specific observer using multiprocessing.
    
    Uses DELETE+INSERT for idempotency - existing classifications for each
    observation are deleted before inserting new ones.
    
    Args:
        observer_id: The observer UUID
        num_workers: Number of worker processes to use
        
    Returns:
        Dictionary with processing statistics
    """
    stats = {
        'observations_found': 0,
        'classifications_stored': 0,
        'errors': 0
    }
    
    observation_ids = list_clip_classification_files_for_observer(observer_id)
    print(f"Found {len(observation_ids)} clip classification files for observer {observer_id}")
    
    if not observation_ids:
        print("No observations to process")
        return stats
    
    stats['observations_found'] = len(observation_ids)
    
    # Create work items
    work_items = [(observer_id, obs_id) for obs_id in observation_ids]
    
    # Process in parallel
    print(f"Processing observations using {num_workers} worker processes...")
    
    with Pool(processes=num_workers) as pool:
        results = list(tqdm(
            pool.imap_unordered(_process_observation_worker, work_items, chunksize=10),
            total=len(work_items),
            desc="Processing observations"
        ))
    
    # Aggregate results
    for success, count in results:
        if success:
            stats['classifications_stored'] += count
        else:
            stats['errors'] += 1
    
    print(f"\nProcessed {stats['observations_found']} observations")
    print(f"Classifications stored: {stats['classifications_stored']}")
    print(f"Errors: {stats['errors']}")
    
    return stats


if __name__ == "__main__":
    """
    Usage:
    python -m utils.etl.clip_classification [--clear] [--workers N] [--observer-id OBSERVER_ID] [--observation-id OBSERVATION_ID]
    
    Options:
    --clear                             Clear existing classifications before processing all observations
    --workers N                         Number of worker processes to use (default: min(8, cpu_count))
    --observer-id OBSERVER_ID           Process only a specific observer. When provided, processes all observations for that observer.
    --observation-id OBSERVATION_ID     Process only a specific observation (requires --observer-id)
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Process clip classifications from S3 to RDS")
    parser.add_argument(
        "--clear", 
        action="store_true", 
        help="Clear existing classifications before processing"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of worker processes (default: {DEFAULT_WORKERS})"
    )
    parser.add_argument(
        "--observer-id",
        type=str,
        help="Process only a specific observer"
    )
    parser.add_argument(
        "--observation-id",
        type=str,
        help="Process only a specific observation (requires --observer-id)"
    )
    
    args = parser.parse_args()
    
    if args.observation_id:
        if not args.observer_id:
            print("Error: --observation-id requires --observer-id")
            exit(1)
        # Process single observation (no multiprocessing needed)
        success = process_single_ad(args.observer_id, "", args.observation_id)
        exit(0 if success else 1)
    elif args.observer_id:
        # Process all observations for a specific observer using multiprocessing
        process_observer_parallel(args.observer_id, num_workers=args.workers)
    else:
        # Process all observations using multiprocessing
        process_all_ads(clear_existing=args.clear, num_workers=args.workers)
