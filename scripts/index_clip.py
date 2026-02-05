#!/usr/bin/env python3
"""Backfill script for CLIP classification entries to RDS.

This script handles:
- Scanning S3 for all clip classification files
- Upserting classifications to RDS (ad_classifications table)
- Does NOT delete existing entries, only updates or inserts

Usage:
    python -m scripts.index_clip [--workers N] [--observer-id UUID] [--dry-run]
    
Options:
    --workers N              Number of worker processes (default: 8)
    --observer-id UUID       Process only a specific observer
    --observation-id UUID    Process only a specific observation (requires --observer-id)
    --dry-run                Preview what would be indexed without making changes
"""

import time
from multiprocessing import Pool, cpu_count
from typing import List, Optional, Tuple
from uuid import uuid4
import logging

from tqdm import tqdm

from db.database import get_session
from models.clip_classification import ClipClassificationORM, CompositeClassification
from utils import observations_sub_bucket
from utils.logging import get_backfill_logger

logger = get_backfill_logger("clip")


DEFAULT_WORKERS = min(8, cpu_count())


def list_all_observers() -> List[str]:
    """List all observer IDs from S3.
    
    Returns:
        List of observer UUIDs
    """
    observers = []
    dirs = observations_sub_bucket.list_dir("", list_all=True)
    for d in dirs:
        if d.endswith('/'):
            observer_id = d.rstrip('/').split('/')[0]
            # Basic UUID validation
            if len(observer_id) == 36 and observer_id.count('-') == 4:
                observers.append(observer_id)
    return observers


def list_clip_files_for_observer(observer_id: str) -> List[str]:
    """List all clip classification files for an observer.
    
    Args:
        observer_id: The observer UUID
        
    Returns:
        List of observation IDs that have clip classification files
    """
    path = f"{observer_id}/clip_classifications/"
    try:
        files = observations_sub_bucket.list_dir(path, list_all=True)
        observation_ids = []
        for file_path in files:
            if file_path.endswith('.json'):
                filename = file_path.split('/')[-1]
                observation_id = filename.replace('.json', '')
                observation_ids.append(observation_id)
        return observation_ids
    except Exception as e:
        logger.error(f"Error listing clip classifications for {observer_id}: {e}")
        return []


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


def upsert_classifications(
    observation_id: str, 
    classifications: List[CompositeClassification]
) -> Tuple[int, int]:
    """Upsert clip classifications in RDS.
    
    This function updates existing classifications if they exist (matched by
    observation_id + label), or inserts new ones if they don't. It does NOT
    delete any existing classifications.
    
    Args:
        observation_id: The observation UUID
        classifications: List of CompositeClassification objects
        
    Returns:
        Tuple of (inserted_count, updated_count)
    """
    current_time = int(time.time() * 1000)
    inserted = 0
    updated = 0
    
    with get_session() as session:
        for classification in classifications:
            # Try to find existing classification by observation_id + label
            existing = session.query(ClipClassificationORM).filter_by(
                observation_id=observation_id,
                label=classification.label
            ).first()
            
            if existing:
                # Update existing record
                existing.score = classification.score_normalized
                existing.updated_at = current_time
                updated += 1
            else:
                # Insert new record
                new_classification = ClipClassificationORM(
                    id=str(uuid4()),
                    observation_id=observation_id,
                    label=classification.label,
                    score=classification.score_normalized,
                    created_at=current_time,
                    updated_at=current_time
                )
                session.add(new_classification)
                inserted += 1
    
    return (inserted, updated)


def _process_observation_worker(args: Tuple[str, str]) -> Tuple[bool, int, int]:
    """Worker function for multiprocessing to process a single observation.
    
    Args:
        args: Tuple of (observer_id, observation_id)
        
    Returns:
        Tuple of (success: bool, inserted_count: int, updated_count: int)
    """
    observer_id, observation_id = args
    
    try:
        # Read classification data from S3
        data = read_clip_classification_from_s3(observer_id, observation_id)
        if not data:
            return (False, 0, 0)
        
        # Parse the composite classifications
        classifications = parse_composite_classifications(data)
        if not classifications:
            return (False, 0, 0)
        
        # Upsert classifications (no delete!)
        inserted, updated = upsert_classifications(observation_id, classifications)
        
        return (True, inserted, updated)
            
    except Exception as e:
        # Silently handle errors in workers
        return (False, 0, 0)


def process_single_observation(observer_id: str, observation_id: str) -> bool:
    """Process the clip classification for a single observation.
    
    Args:
        observer_id: The observer UUID
        observation_id: The observation UUID
        
    Returns:
        True if classification was processed successfully, False otherwise
    """
    try:
        # Read classification data from S3
        data = read_clip_classification_from_s3(observer_id, observation_id)
        if not data:
            logger.warning(f"No clip classification found for {observer_id}/{observation_id}")
            return False
        
        # Parse the composite classifications
        classifications = parse_composite_classifications(data)
        if not classifications:
            logger.warning(f"No composite classifications in file for {observer_id}/{observation_id}")
            return False
        
        # Upsert classifications
        inserted, updated = upsert_classifications(observation_id, classifications)
        
        logger.info(f"Observation {observation_id}: {inserted} inserted, {updated} updated")
        return True
        
    except Exception as e:
        logger.error(f"Error processing {observer_id}/{observation_id}: {e}")
        return False


def backfill_clip_classifications(
    observer_id: Optional[str] = None,
    num_workers: int = DEFAULT_WORKERS,
    dry_run: bool = False
) -> dict:
    """Backfill clip classifications for all or specific observers.
    
    Args:
        observer_id: If provided, only process this observer
        num_workers: Number of worker processes
        dry_run: If True, preview changes without making them
        
    Returns:
        Statistics dictionary
    """
    stats = {
        'observers_processed': 0,
        'observations_found': 0,
        'classifications_inserted': 0,
        'classifications_updated': 0,
        'errors': 0
    }
    
    if dry_run:
        logger.warning("*** DRY-RUN MODE: No changes will be made ***")
    
    # Determine which observers to process
    if observer_id:
        observers = [observer_id]
    else:
        logger.info("Listing all observers from S3...")
        observers = list_all_observers()
        logger.info(f"Found {len(observers)} observers")
    
    # Collect all work items
    logger.info("Collecting clip classification files from S3...")
    work_items: List[Tuple[str, str]] = []
    
    for obs_id in tqdm(observers, desc="Scanning observers"):
        stats['observers_processed'] += 1
        observation_ids = list_clip_files_for_observer(obs_id)
        for observation_id in observation_ids:
            work_items.append((obs_id, observation_id))
    
    stats['observations_found'] = len(work_items)
    logger.info(f"Found {len(work_items)} clip classification files to process")
    
    if not work_items:
        logger.warning("No observations to process")
        return stats
    
    if dry_run:
        # Show sample of work items
        logger.info(f"Preview: Would process {len(work_items)} classification files")
        logger.info("Sample files (first 10):")
        for i, (obs_id, obs_file) in enumerate(work_items[:10], 1):
            logger.info(f"  {i}. Observer: {obs_id}, File: {obs_file}")
        if len(work_items) > 10:
            logger.info(f"  ... and {len(work_items) - 10} more files")
        return stats
    
    # Process observations in parallel
    logger.info(f"Processing observations using {num_workers} worker processes...")
    
    with Pool(processes=num_workers) as pool:
        results = list(tqdm(
            pool.imap_unordered(_process_observation_worker, work_items, chunksize=10),
            total=len(work_items),
            desc="Processing observations"
        ))
    
    # Aggregate results
    for success, inserted, updated in results:
        if success:
            stats['classifications_inserted'] += inserted
            stats['classifications_updated'] += updated
        else:
            stats['errors'] += 1
    
    logger.info("=== CLIP Classification Backfill Complete ===")
    logger.info(f"Observers processed: {stats['observers_processed']}")
    logger.info(f"Observations found: {stats['observations_found']}")
    logger.info(f"Classifications inserted: {stats['classifications_inserted']}")
    logger.info(f"Classifications updated: {stats['classifications_updated']}")
    logger.info(f"Errors: {stats['errors']}")
    
    return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Backfill CLIP classifications from S3 to RDS"
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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be indexed without making changes"
    )
    
    args = parser.parse_args()
    
    if args.observation_id:
        if not args.observer_id:
            logger.error("--observation-id requires --observer-id")
            exit(1)
        # Process single observation
        if args.dry_run:
            logger.warning("*** DRY-RUN MODE: Previewing single observation ***")
            logger.info(f"Would process: {args.observer_id}/{args.observation_id}")
            exit(0)
        success = process_single_observation(args.observer_id, args.observation_id)
        exit(0 if success else 1)
    else:
        # Process all or specific observer
        try:
            backfill_clip_classifications(
                observer_id=args.observer_id,
                num_workers=args.workers,
                dry_run=args.dry_run
            )
        except Exception as e:
            logger.error(f"Backfill failed: {e}")
            exit(1)
