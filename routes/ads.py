import concurrent.futures
import datetime
import hashlib
import json

import dateutil.tz
from tqdm import tqdm

from db.clients.rds_storage_client import RdsStorageClient
from db.shared_repositories import observations_repository
from enricher import RdoBuilder
from middlewares.authenticate import authenticate
from middlewares.authorise import Role, authorise
from models.ad_tag import AdTag, AdTagORM
from models.attribute import AdAttributeORM
from models.observation import Observation, ObservationORM
from routes import route
from routes.ad_attributes import ad_attributes_repository
from utils import Response, use
from utils.indexer.indexer import Indexer
from utils.opensearch import AdQuery
from utils.opensearch.rdo_open_search import LATEST_READY_INDEX, AdWithRDO, RdoOpenSearch
import utils.observations_sub_bucket as observations_sub_bucket
import utils.metadata_sub_bucket as metadata
from multiprocessing import Process, Pipe
from .tags import ads_tags_repository


def parse_ad_path(ad_path):
    """Parse the ad path into its components: observer_id, timestamp, and ad_id.

    Args:
        ad_path (str): The full ad path in format "observer_id/temp/timestamp.ad_id"

    Returns:
        dict: Dictionary containing observer_id, timestamp, and ad_id

    Raises:
        ValueError: If ad_path is None or has invalid format
    """
    if ad_path is None:
        raise ValueError("ad_path is None")

    parts = [part for part in ad_path.split('/') if part]
    if len(parts) < 3:
        raise ValueError(f"Invalid ad path format: {ad_path}")

    observer_id = parts[0]
    rest = parts[2]  # Skip the 'temp' part

    ad_parts = rest.split('.')
    if len(ad_parts) < 2:
        raise ValueError(f"Invalid ad path format: {ad_path}")

    ad_id = ad_parts[-1]
    timestamp = ".".join(ad_parts[:-1])

    return {
        'observer_id': observer_id,
        'timestamp': timestamp,
        'ad_id': ad_id
    }


class Enricher:
    """Handles the enrichment of ads with metadata and other relevant information."""

    def __init__(self, ad=None):
        """Initialize the enricher with ad information.

        Args:
            ad (dict, optional): Dictionary containing observer_id, timestamp, and ad_id
        """
        if ad is not None:
            self.observer_id = ad.get("observer_id")
            self.timestamp = ad.get("timestamp")
            self.ad_id = ad.get("ad_id")

        if hasattr(self, 'observer_id') and self.observer_id is not None:
            self.observer = observations_sub_bucket.Observer(self.observer_id)
        else:
            raise ValueError("observer_id is required")

        self.execution_time_ms = 0

    def timeit(func):
        """Decorator to time the execution of a function."""

        def wrapper(self, *args, **kwargs):
            start_at = datetime.datetime.now(tz=dateutil.tz.tzutc())
            result = func(self, *args, **kwargs)
            end_at = datetime.datetime.now(tz=dateutil.tz.tzutc())
            self.execution_time_ms += (end_at -
                                       start_at).total_seconds() * 1000
            return result
        return wrapper

    @timeit
    def attach_attributes(self, include=None):
        """Fetch the attributes metadata from the repository"""
        observation_id = f"{self.observer_id}_{self.timestamp}.{self.ad_id}"
        try:
            # Get attributes from the repository
            with ad_attributes_repository.create_session() as session:
                attributes = session.get({"observation_id": observation_id})
                if attributes is None:
                    attributes = []

                # Convert to the expected format (key-value dictionary)
                attributes_dict = {}
                for attr in attributes:
                    attributes_dict[attr.key] = {
                        "value": attr.value,
                        "created_at": attr.created_at,
                        "created_by": attr.created_by,
                        "modified_at": attr.modified_at,
                        "modified_by": attr.modified_by
                    }

            self.attributes = attributes_dict
        except Exception as e:
            print(f"Error fetching attributes for {observation_id}: {e}")
            self.attributes = {}
        return self

    def attach_tags(self):
        """Attach tags to the ad."""
        # Fetch the tags for the ad
        try:
            with ads_tags_repository.create_session() as session:
                tags: list[AdTag] = session.get({"observation_id": self.ad_id})
                tag_ids = [tag.tag_id for tag in tags]

                self.tags = tag_ids
        except Exception as e:
            print(
                f"Error fetching tags for {self.observer_id}/{self.timestamp}.{self.ad_id}: {e}")
            self.tags = []
        return self

    @timeit
    def attach_rdo(self):
        # Get the RDO for the ad
        rdo = self.observer.get_pre_constructed_rdo(self.timestamp, self.ad_id)
        if rdo is not None:
            self.rdo = rdo
        else:
            self.rdo = {}
        return self

    def to_dict(self):
        """Convert the enriched ad to a dictionary."""
        start_at = datetime.datetime.now(tz=dateutil.tz.tzutc())
        data = {
            "observer_id": self.observer_id,
            "ad_id": self.ad_id,
            "timestamp": self.timestamp,
            "metadata": {}
        }
        if hasattr(self, 'attributes'):
            data['metadata']['attributes'] = self.attributes
        if hasattr(self, 'tags'):
            data['metadata']['tags'] = self.tags
        if hasattr(self, 'rdo'):
            data['metadata']['rdo'] = self.rdo
        end_at = datetime.datetime.now(tz=dateutil.tz.tzutc())
        self.execution_time_ms += (end_at - start_at).total_seconds() * 1000
        return data


class BatchEnricher:
    """Handles batch enrichment of multiple ads with metadata and other relevant information."""

    def __init__(self, ads: list[dict] = None, max_workers=128, parallel=False):
        """Initialize the batch enricher.

        Args:
            ads (list[dict], optional): List of ad dictionaries to enrich
            max_workers (int): Maximum number of worker processes for parallel processing
            parallel (bool): Whether to use parallel processing
        """
        if ads is None:
            ads = []
        self.parallel = parallel
        self.enrichers = [Enricher(ad) for ad in ads]
        self.max_workers = min(max_workers, len(self.enrichers))
        self.execution_time_ms = 0

    def _attach_chunk(self, target, chunk, conn, **kwargs):
        """Attach attributes to a chunk of ads in parallel."""
        for enricher in chunk:
            target(enricher, **kwargs)
            conn.send(enricher)
        conn.send(None)

    def attach_enrichment(self, enricher_target, **kwargs):
        def chunk_target(chunk, conn): return self._attach_chunk(
            enricher_target, chunk, conn, **kwargs)
        start_at = datetime.datetime.now(tz=dateutil.tz.tzutc())
        if self.parallel:
            # Split the work into chunks equal to the number of workers
            # with at least one enricher per worker
            chunk_size = max(len(self.enrichers) // self.max_workers, 1)
            chunks = [self.enrichers[i:i + chunk_size]
                      for i in range(0, len(self.enrichers), chunk_size)]
            processes = []
            parent_connections = []
            results = []

            # Prepare the processes and connections
            for chunk in chunks:
                parent_conn, child_conn = Pipe()
                parent_connections.append(parent_conn)
                process = Process(target=chunk_target,
                                  args=(chunk, child_conn))
                processes.append(process)

            # Start the processes
            for process in processes:
                process.start()

            # Collect the results from each process
            for parent_conn in parent_connections:
                while True:
                    try:
                        # Receive the result from the child process
                        result = parent_conn.recv()
                        # If the result is None, the process has finished (as defined in _attach_attributes_chunk)
                        if result is None:
                            break
                        results.append(result)
                    except EOFError:
                        # If the connection is closed, break the loop
                        break
            # Wait for all processes to finish
            for process in processes:
                process.join()

            # Close the connections
            for parent_conn in parent_connections:
                parent_conn.close()

            # Update the enrichers with the results
            self.enrichers = results
        else:
            for enricher in self.enrichers:
                enricher_target(enricher, **kwargs)
        end_at = datetime.datetime.now(tz=dateutil.tz.tzutc())
        self.execution_time_ms += (end_at - start_at).total_seconds() * 1000
        return self

    def attach_attributes(self):
        """Batch attach attributes to all enrichers using RDS client directly."""
        rds_client: RdsStorageClient = ad_attributes_repository._client
        rds_client.connect()

        def get_observation_id(enricher):
            return f"{enricher.observer_id}_{enricher.timestamp}.{enricher.ad_id}"

        def get_ad_id(observation_id):
            parts = observation_id.split('.')
            if len(parts) < 2:
                raise ValueError(
                    f"Invalid observation_id format: {observation_id}")
            return parts[-1]

        observation_ids = [get_observation_id(
            enricher) for enricher in self.enrichers]
        results = {}
        with rds_client.session_maker() as session:
            query = session.query(AdAttributeORM).filter(
                AdAttributeORM.observation_id.in_(observation_ids)).all()
            print(
                f"[BatchEnricher] Found {len(query)} ads with attributes for {len(observation_ids)} observations: {observation_ids[:5]}.")
            for result in query:
                observation_id = result.observation_id
                ad_id = get_ad_id(observation_id)
                if observation_id not in results:
                    results[ad_id] = {}
                results[ad_id][result.key] = {
                    "value": result.value,
                    "created_at": result.created_at,
                    "created_by": result.created_by,
                    "modified_at": result.modified_at,
                    "modified_by": result.modified_by
                }

        for enricher in tqdm(self.enrichers):
            enricher.attributes = results.get(enricher.ad_id, {})
        return self

    def attach_tags(self):
        """Batch attach tags to all enrichers using RDS client directly."""
        rds_client: RdsStorageClient = ads_tags_repository._client
        rds_client.connect()

        observations = [enricher.ad_id for enricher in self.enrichers]
        results = {}
        with rds_client.session_maker() as session:
            query = session.query(AdTagORM).filter(
                AdTagORM.observation_id.in_(observations)).all()
            print(
                f"[BatchEnricher] Found {len(query)} ads with tags for {len(observations)} observations.")
            for result in query:
                observation_id = result.observation_id
                if observation_id not in results:
                    results[observation_id] = []
                results[observation_id].append(result.tag_id)

        for enricher in tqdm(self.enrichers):
            enricher.tags = results.get(enricher.ad_id, [])
        return self

    def attach_rdo(self):
        def enricher_target(enricher): return enricher.attach_rdo()
        return self.attach_enrichment(enricher_target)

    def to_dict(self):
        return [enricher.to_dict() for enricher in self.enrichers]


# get-access-cache?observer_id=5ea80108-154d-4a7f-8189-096c0641cd87
@route('ads/{observer_id}', 'GET')
@use(authenticate)
def get_access_cache(event, response):
    """Retrieve the access cache for an observer and the ads that passed RDO construction.

    Retrieve the quick access cache for the specified observer.
    ---
    tags:
        - ads
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            data:
                                type: object
                            ads:
                                type: array
                                items:
                                    type: string
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'OBSERVER_NOT_PROVIDED_IN_PATH'
        404:
            description: No cache found for observer
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'NO_CACHE_FOUND_FOR_OBSERVER'
    """
    observer_id = event['pathParameters'].get('observer_id', None)
    if observer_id is None:
        return response.status(400).json({
            'success': False,
            'comment': 'OBSERVER_NOT_PROVIDED_IN_PATH'
        })
    if observer_id.endswith('/'):
        observer_id = observer_id[:-1]

    activation_code = observer_id[-7:-1]

    # Query by observer_id
    ad_query = AdQuery()
    ads_for_observer = ad_query.query_all({
        'method': 'OBSERVER_ID_CONTAINS',
        'args': [activation_code],
    })

    return {
        'success': True,
        'ads': ads_for_observer
    }


def get_stitched_frame_from_rdo(observer_id, timestamp, ad_id):
    """Get the stitched frames from RDO for a specific ad."""
    observer = observations_sub_bucket.Observer(observer_id)
    rdo = observer.get_pre_constructed_rdo(timestamp, ad_id)
    if rdo is None:
        raise Exception(f"RDO not found for {observer_id}/{timestamp}.{ad_id}")
    media: list[str] = rdo['media']
    # temp-v2 is the new version of stitching
    stitched_frames = [
        frame for frame in media if 'stitching' in frame or 'temp-v2' in frame]
    return stitched_frames


@route('ads/{observer_id}/recent', 'GET')
# Note: This purposely has no @use(authenticate) decorator, making it an open endpoint
def get_observer_seen_ads_last_7_days(event, response: Response):
    """
    Retrieve a list of ads that passed RDO construction by a specific observer in the last 7 days.
    This is an open endpoint "authenticated" by the observer_id in the path.
    ---
    tags:
        - ads
    responses:
        200:
            description: A list of ad paths seen by the observer in the last 7 days.
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: True
                            ads:
                                type: array
                                items:
                                    type: string
                                example: ["observer-id-123/temp/1678886400000.ad-id-abc", "observer-id-123/temp/1678972800000.ad-id-def"]
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'OBSERVER_NOT_PROVIDED_IN_PATH'
        404:
            description: No observations found for observer
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'NO_OBSERVATIONS_FOUND_FOR_OBSERVER'
    """
    observer_id = event['pathParameters'].get('observer_id', None)

    if not observer_id:
        return response.status(400).json({
            'success': False,
            'comment': 'OBSERVER_ID_NOT_PROVIDED_IN_PATH'
        })

    # Calculate seven days ago timestamp in milliseconds
    now_utc = datetime.datetime.now(tz=dateutil.tz.tzutc())
    seven_days_ago_utc = now_utc - datetime.timedelta(days=7)
    seven_days_ago_timestamp_ms = int(seven_days_ago_utc.timestamp() * 1000)

    try:
        recent_ads = AdQuery().query_all({
            'method': 'AND',
            'args': [
                {
                    'method': 'OBSERVER_ID_EQUALS',
                    'args': [observer_id]
                },
                {
                    'method': 'DATETIME_AFTER',
                    'args': [str(seven_days_ago_timestamp_ms)]
                }
            ]
        })

        return {
            'success': True,
            'ads': recent_ads
        }

    except Exception as e:
        print(
            f"Error querying observations for observer {observer_id}: {str(e)}")
        return response.status(500).json({
            'success': False,
            'comment': 'FAILED_TO_QUERY_OBSERVATIONS',
            'error': str(e)
        })


@route('ads/batch', 'POST')
@use(authenticate)
def get_batch_ads(event, response: Response):
    """
    Retrieve metadata associated with a batch of ads.

    Given a list of ads in the request body (with observer_id, timestamp and ad_id fields for each ad), as well as a list of metadata types to include, return the metadata for each ad in the list.
    ---
    tags:
        - ads
    requestBody:
        required: true
        content:
            application/json:
                schema:
                    type: object
                    properties:
                        ads:
                            type: array
                            items:
                                type: object
                                properties:
                                    observer_id:
                                        type: string
                                    timestamp:
                                        type: string
                                    ad_id:
                                        type: string
                        metadata_types:
                            type: array
                            items:
                                type: string
                                enum: ['attributes', 'tags', 'rdo']
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            ads:
                                type: array
                                items:
                                    type: object
                                    properties:
                                        observer_id:
                                            type: string
                                        timestamp:
                                            type: string
                                        ad_id:
                                            type: string
                                        metadata:
                                            type: object
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'Invalid request'
    """
    body = event['body']
    ads = body.get('ads', [])
    metadata_types = body.get('metadata_types', [])
    if not ads or not metadata_types:
        return response.status(400).json({
            'success': False,
            'comment': 'Invalid request, missing ads or metadata_types'
        })

    batch_enricher = BatchEnricher(ads, parallel=True)
    if 'attributes' in metadata_types:
        batch_enricher.attach_attributes()

    if 'tags' in metadata_types:
        batch_enricher.attach_tags()

    if 'rdo' in metadata_types:
        batch_enricher.attach_rdo()

    enriched_ads = batch_enricher.to_dict()
    return {
        'success': True,
        'ads': enriched_ads
    }


@route('ads/batch/presign', 'POST')
@use(authenticate)
def get_batch_ads_presign(event, response: Response):
    """
    Attach metadata to a batch of ads and return a presigned URL for the batch.

    Behaves like the ads/batch endpoint, but returns a presigned URL for the batch instead of the metadata itself.
    ---
    tags:
        - ads
    requestBody:
        required: true
        content:
            application/json:
                schema:
                    type: object
                    properties:
                        ads:
                            type: array
                            items:
                                type: object
                                properties:
                                    observer_id:
                                        type: string
                                    timestamp:
                                        type: string
                                    ad_id:
                                        type: string
                        metadata_types:
                            type: array
                            items:
                                type: string
                                enum: ['attributes', 'tags', 'rdo']
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            presigned_url:
                                type: string
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'Invalid request'
    """
    body = event['body']
    ads = body.get('ads', [])
    metadata_types = body.get('metadata_types', [])
    if not ads or not metadata_types:
        return response.status(400).json({
            'success': False,
            'comment': 'Invalid request, missing ads or metadata_types'
        })

    # Ensure metadata_types is a list of strings
    if not isinstance(metadata_types, list) or not all(isinstance(m, str) for m in metadata_types):
        return response.status(400).json({
            'success': False,
            'comment': 'Invalid request, metadata_types must be a list of strings'
        })

    ACCEPTED_METADATA_TYPES = ['attributes', 'tags', 'rdo']
    for metadata_type in metadata_types:
        if metadata_type not in ACCEPTED_METADATA_TYPES:
            return response.status(400).json({
                'success': False,
                'comment': f'Invalid metadata type: {metadata_type}. Accepted types are: {ACCEPTED_METADATA_TYPES}'
            })
    caller = event['user']
    if caller is None or caller.id is None:
        return response.status(400).json({
            'success': False,
            'comment': 'Invalid request, caller is not authenticated'
        })

    # Save the enriched ads to a file and return a presigned URL
    user_id = caller.id

    # Hash the body to create a unique key for the batch to allow for caching
    key = hashlib.sha256(json.dumps({"ads": sorted(ads, key=lambda ad: ad.get(
        'ad_id', None)), "types": sorted(metadata_types)}).encode('utf-8')).hexdigest()
    filename = f'{key}.json'
    path = f'batch_ads/{user_id}/{filename}'

    # Check if the batch already exists in the metadata bucket
    use_cache = 'rdo' in metadata_types
    if use_cache:
        # If the batch already exists, return the presigned URL for the existing batch
        # and skip the enrichment process
        try:
            existing_batch = metadata.get_object(path, read_body=False)
            print(f"Found existing batch: {path}")
        except Exception as e:
            existing_batch = None
            print(f"Batch not found: {path}")

        if existing_batch is not None:
            # Only return the presigned URL if the batch is not older than 1 hour
            last_modified = existing_batch['LastModified']
            now = datetime.datetime.now(tz=dateutil.tz.tzutc())
            cache_age = now - last_modified
            if cache_age < datetime.timedelta(hours=1):
                url = metadata.generate_presigned_url(
                    key=path,
                    expiration=3600,
                    prefer_cache=True
                )
                return response.status(200).json({
                    'success': True,
                    'presigned_url': url,
                })

    # Enrich then cache the batch

    batch_enricher = BatchEnricher(ads, parallel=True)
    if 'attributes' in metadata_types:
        batch_enricher.attach_attributes()

    if 'tags' in metadata_types:
        batch_enricher.attach_tags()

    if 'rdo' in metadata_types:
        batch_enricher.attach_rdo()

    enriched_ads = batch_enricher.to_dict()

    metadata.put_object(
        key=path,
        data=json.dumps(enriched_ads).encode('utf-8')
    )

    presigned_url = metadata.generate_presigned_url(
        key=path,
        expiration=3600
    )

    return {
        'success': True,
        'presigned_url': presigned_url
    }


# get-stiching-frames?path=5ea80108-154d-4a7f-8189-096c0641cd87/temp/1729261457039.c979d19c-0546-412b-a2d9-63a247d7c250
@route('ads/{observer_id}/{timestamp}.{ad_id}/stitching/frames', 'GET')
@use(authenticate)
def get_stitching_frames_presigned(event, response):
    """Get the presigned URLs for the frames of an ad's stitching.

    Retrieve presigned URLs for the frames of the specified ad's stitching, which is the cropped version of the ad that removes the irrelevant parts.
    ---
    tags:
        - ads
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            frame_paths:
                                type: array
                                items:
                                    type: string
                            presigned_urls:
                                type: array
                                items:
                                    type: string
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'PATH_NOT_PROVIDED'
    """
    observer_id = event['pathParameters'].get('observer_id', None)
    timestamp = event['pathParameters'].get('timestamp', None)
    ad_id = event['pathParameters'].get('ad_id', None)
    path = f'{observer_id}/stitching/{timestamp}.{ad_id}'

    if path is None:
        return response.status(400).json({
            'success': False,
            'comment': 'PATH_NOT_PROVIDED'
        })

    if not path.endswith('/'):
        path += '/'
    # # List the frames at the path
    # frames = observations_sub_bucket.list_dir(path)
    # # print(s3.list_dir(f'{observer_id}/temp'))
    # frame_paths = [f'{frame}' for frame in frames]
    frame_paths = get_stitched_frame_from_rdo(observer_id, timestamp, ad_id)

    # Generate presigned URLs for each frame
    presigned_urls = []
    image_extensions = ['.jpg', '.jpeg', '.png']
    for frame_path in frame_paths:
        if not any([frame_path.endswith(ext) for ext in image_extensions]):
            continue
        presigned_url = observations_sub_bucket.client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': observations_sub_bucket.MOBILE_OBSERVATIONS_BUCKET,
                'Key': frame_path
            }
        )
        presigned_urls.append(presigned_url)

    return {
        'success': True,
        'frame_paths': frame_paths,
        'presigned_urls': presigned_urls
    }


# get-frames?path=5ea80108-154d-4a7f-8189-096c0641cd87/temp/1729261457039.c979d19c-0546-412b-a2d9-63a247d7c250
@route('ads/{observer_id}/{timestamp}.{ad_id}/frames', 'GET')
@use(authenticate)
def get_frames_presigned(event, response):
    """Get the presigned URLs for the frames of an ad.

    Retrieve presigned URLs for the frames of the specified ad.
    ---
    tags:
        - ads
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            frame_paths:
                                type: array
                                items:
                                    type: string
                            presigned_urls:
                                type: array
                                items:
                                    type: string
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'PATH_NOT_PROVIDED'
    """
    observer_id = event['pathParameters'].get('observer_id', None)
    timestamp = event['pathParameters'].get('timestamp', None)
    ad_id = event['pathParameters'].get('ad_id', None)
    path = f'{observer_id}/temp/{timestamp}.{ad_id}'

    if path is None:
        return response.status(400).json({
            'success': False,
            'comment': 'PATH_NOT_PROVIDED'
        })

    if not path.endswith('/'):
        path += '/'

    # List the frames at the path
    frames = observations_sub_bucket.list_dir(path)
    frame_paths = [f'{frame}' for frame in frames]

    # Generate presigned URLs for each frame
    presigned_urls = []
    image_extensions = ['.jpg', '.jpeg', '.png']
    for frame_path in frame_paths:
        if not any([frame_path.endswith(ext) for ext in image_extensions]):
            continue
        presigned_url = observations_sub_bucket.client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': observations_sub_bucket.MOBILE_OBSERVATIONS_BUCKET,
                'Key': frame_path
            }
        )
        presigned_urls.append(presigned_url)

    return {
        'success': True,
        'frame_paths': frame_paths,
        'presigned_urls': presigned_urls
    }


INDEX_FILENAME = 'ads_stream.json'


def try_compute_ads_list(prefer_cache=True):
    """List all ads from RDS and save it to the ads_stream.json file in the root of the bucket.

    Does not run if the ads_stream.json file already exists and not older than 1 hour.
    """
    # Check if the ads_stream.json file exists and is not older than given cache age
    index_obj = observations_sub_bucket.try_get_object(key=INDEX_FILENAME)
    if index_obj is not None:
        # Terminate if the file is not older than given cache age
        last_modified = index_obj['LastModified']
        now = datetime.datetime.now(tz=dateutil.tz.tzutc())
        cache_age = now - last_modified
        print(f'Found cache file. Cache age: {cache_age}')
        if cache_age < datetime.timedelta(hours=0) and prefer_cache:
            print('Using cached ads stream index.')
            return observations_sub_bucket.read_json_file(INDEX_FILENAME)

    print('Cache file not found or too old, recomputing ads stream index...')

    # Query all ads from RDS
    with observations_repository.create_session() as session:
        observations: Observation = session.list()
        index = [
            f"{obs.observer_id}/temp/{obs.timestamp}.{obs.observation_id}" for obs in observations]

    observations_sub_bucket.client.put_object(
        Bucket=observations_sub_bucket.MOBILE_OBSERVATIONS_BUCKET,
        Key=INDEX_FILENAME,
        Body=json.dumps(index).encode('utf-8')
    )

    print(f"Saved {len(index)} ads to {INDEX_FILENAME}")

    return index


@route('ads', 'GET')
@use(authenticate)
def get_ads_stream_index(event, response):
    """Retrieve the ads stream index as a presigned URL and the ads that passed RDO construction.

    Retrieve the ads stream index from the S3 bucket, or recompute it if it is older than 24 hours.
    ---
    tags:
        - ads
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            presigned_url:
                                type: string
                            ads:
                                type: array
                                items:
                                    type: string
        500:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'FAILED_TO_COMPUTE_ADS_STREAM_INDEX'
                            error:
                                type: string
    """
    try:
        try_compute_ads_list()
        index = observations_sub_bucket.read_json_file(INDEX_FILENAME)
        if index is None:
            return response.status(500).json({
                'success': False,
                'comment': 'FAILED_TO_GET_ADS_STREAM_INDEX',
                'error': 'ads_stream.json not found'
            })

        # Generate a presigned URL for the ads_passed_rdo_construction.json file
        presigned_url = observations_sub_bucket.client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': observations_sub_bucket.MOBILE_OBSERVATIONS_BUCKET,
                'Key': INDEX_FILENAME
            },
        )

        return {
            'success': True,
            'presigned_url': presigned_url
        }
    except Exception as e:
        raise e
        return response.status(500).json({
            'success': False,
            'comment': f'FAILED_TO_QUERY_OBSERVATIONS',
            'error': str(e)
        })

# Rich Data Object composer


def parse_ad_params(event, response, context):
    observer_id = event['pathParameters'].get('observer_id', None)
    timestamp = event['pathParameters'].get('timestamp', None)
    ad_id = event['pathParameters'].get('ad_id', None)
    if observer_id is None or timestamp is None or ad_id is None:
        missing_params = []
        if observer_id is None:
            missing_params.append('observer_id')
        if timestamp is None:
            missing_params.append('timestamp')
        if ad_id is None:
            missing_params.append('ad_id')
        response.status(400).json({
            'success': False,
            'comment': f'MISSING_PARAMETERS: {", ".join(missing_params)}'
        })
        return event, response, context
    event['ad_params'] = [observer_id, timestamp, ad_id]
    return event, response, context


@route('ads/{observer_id}/{timestamp}.{ad_id}/rdo', 'GET')
@use(authenticate)
@use(parse_ad_params)
def get_ad_rdo(event, response):
    """Retrieve the full Rich Data Object for an ad.

    The Rich Data Object (RDO) is a comprehensive data object that includes all the information, including any enrichments, for the specified ad.
    ---
    tags:
        - rdo
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: True
                            data:
                                type: object
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'MISSING_PARAMETERS: observer_id, timestamp, ad_id'
    """
    observer_id, timestamp, ad_id = event['ad_params']
    observer = observations_sub_bucket.Observer(observer_id)
    return {
        'success': True,
        'data': observer.get_pre_constructed_rdo(timestamp, ad_id)
    }


@route('ads/{observer_id}/{timestamp}.{ad_id}/rdo/ocr_data', 'GET')
@use(authenticate)
@use(parse_ad_params)
def get_ad_ocr_data(event, response):
    """Retrieve OCR data for an ad, relative to the stitched frames.

    Retrieve OCR data for the specified ad, including text matches and their positions.
    ---
    tags:
        - rdo
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            ocr_data:
                                type: array
                                items:
                                    type: object
                                    properties:
                                        screenshot_cropped:
                                            type: string
                                        y_offset:
                                            type: integer
                                        observed_at:
                                            type: string
                                        ocr_data:
                                            type: array
                                            items:
                                                type: object
                                                properties:
                                                    text:
                                                        type: string
                                                    x:
                                                        type: integer
                                                    y:
                                                        type: integer
                                                    width:
                                                        type: integer
                                                    height:
                                                        type: integer
                                                    confidence:
                                                        type: number
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'MISSING_PARAMETERS: observer_id, timestamp, ad_id'
    """
    observer_id, timestamp, ad_id = event['ad_params']
    observer = observations_sub_bucket.Observer(observer_id)
    rdo_builder = RdoBuilder(observer)
    ocr_data = rdo_builder.get_ocr_data(timestamp, ad_id)
    return {
        'success': True,
        'ocr_data': ocr_data
    }


@route('ads/{observer_id}/{timestamp}.{ad_id}/rdo/ocr_data/raw', 'GET')
@use(authenticate)
@use(parse_ad_params)
def get_ad_raw_ocr_data(event, response):
    """Retrieve OCR data for an ad, relative to the raw frames.

    Retrieve raw OCR data for the specified ad, including text matches and their positions relative to the raw frames. This may include personal or irrelevant information.
    ---
    tags:
        - rdo
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            ocr_data:
                                type: array
                                items:
                                    type: object
                                    properties:
                                        screenshot_cropped:
                                            type: string
                                        observed_at:
                                            type: string
                                        ocr_data:
                                            type: array
                                            items:
                                                type: object
                                                properties:
                                                    text:
                                                        type: string
                                                    x:
                                                        type: integer
                                                    y:
                                                        type: integer
                                                    width:
                                                        type: integer
                                                    height:
                                                        type: integer
                                                    confidence:
                                                        type: number
                                        y_source:
                                            type: object
                                            properties:
                                                t:
                                                    type: integer
                                                b:
                                                    type: integer
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'MISSING_PARAMETERS: observer_id, timestamp, ad_id'
    """
    observer_id, timestamp, ad_id = event['ad_params']
    observer = observations_sub_bucket.Observer(observer_id)
    rdo_builder = RdoBuilder(observer)
    ocr_data = rdo_builder.get_raw_ocr_data(timestamp, ad_id)
    return {
        'success': True,
        'ocr_data': ocr_data
    }


@route('ads/{observer_id}/{timestamp}.{ad_id}/rdo/dimensions', 'GET')
@use(authenticate)
@use(parse_ad_params)
def get_ad_dimensions(event, response):
    """Retrieve dimensions for an ad.

    Retrieve the width and height of the specified ad.
    ---
    tags:
        - rdo
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            dimensions:
                                type: object
                                properties:
                                    w:
                                        type: integer
                                    h:
                                        type: integer
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'MISSING_PARAMETERS: observer_id, timestamp, ad_id'
    """
    observer_id, timestamp, ad_id = event['ad_params']
    observer = observations_sub_bucket.Observer(observer_id)
    rdo_builder = RdoBuilder(observer)
    dimensions = rdo_builder.get_ad_dimensions(timestamp, ad_id)
    return {
        'success': True,
        'dimensions': dimensions
    }


@route('ads/{observer_id}/{timestamp}.{ad_id}/rdo/candidates', 'GET')
@use(authenticate)
@use(parse_ad_params)
def get_meta_candidates(event, response):
    """Retrieve meta candidates for an ad.

    Retrieve meta candidates for the specified ad, including media paths and rankings.
    ---
    tags:
        - rdo
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            candidates:
                                type: array
                                items:
                                    type: object
                            media_paths:
                                type: object
                                additionalProperties:
                                    type: string
                            rankings:
                                type: object
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'MISSING_PARAMETERS: observer_id, timestamp, ad_id'
    """
    observer_id, timestamp, ad_id = event['ad_params']
    observer = observations_sub_bucket.Observer(observer_id)
    rdo_builder = RdoBuilder(observer)
    candidates = rdo_builder.get_candidates(timestamp, ad_id)
    media_paths = rdo_builder.get_downloaded_media(timestamp, ad_id)
    rankings = rdo_builder.get_rankings(timestamp, ad_id)
    return {
        'success': True,
        'candidates': candidates,
        'media_paths': media_paths,
        'rankings': rankings
    }


@route('ads/{observer_id}/{timestamp}.{ad_id}/request_index', 'GET')
@use(parse_ad_params)
def request_index(event, response):
    """Request that a specific ad be indexed for querying. This should be triggered automatically when an ad has been RDO-processed.

    This endpoint should not be called manually, but is triggered when the RDO construction is complete for the specified ad.
    ---
    tags:
        - rdo
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'MISSING_PARAMETERS: observer_id, timestamp, ad_id'
    """
    observer_id, timestamp, ad_id = event['ad_params']
    try:
        indexer = Indexer(stage='staging', skip_on_error=False,
                          index_name=LATEST_READY_INDEX)
        indexer.put(observer_id=observer_id, timestamp=timestamp, ad_id=ad_id)
        return {
            'success': True
        }
    except Exception as e:
        if 'already exists' in str(e).lower():
            return {
                'success': True,
                'comment': 'Ad already indexed'
            }

        return response.status(400).json({
            'success': False,
            'comment': f"Error indexing ad: {observer_id}/{timestamp}.{ad_id} - {str(e)}"
        })


@route('ads/{observer_id}/{timestamp}.{ad_id}/request_unindex', 'GET')
@use(authenticate)
@use(authorise(Role.USER, Role.ADMIN))
@use(parse_ad_params)
def request_unindex(event, response):
    """Request that a specific ad be indexed for querying. This should be triggered automatically when an ad has been RDO-processed.

    This endpoint should not be called manually, but is triggered when the RDO construction is complete for the specified ad.
    ---
    tags:
        - rdo
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'MISSING_PARAMETERS: observer_id, timestamp, ad_id'
    """
    observer_id, timestamp, ad_id = event['ad_params']
    try:
        indexer = Indexer(stage='staging', skip_on_error=False,
                          index_name=LATEST_READY_INDEX)
        indexer.delete(observer_id=observer_id,
                       timestamp=timestamp, ad_id=ad_id)
        # It is important that deleted ads are removed from the cache to avoid
        # appearing in obsolete search results
        indexer.clear_open_search_cache(skip_on_error=True)
        return {
            'success': True,
        }
    except Exception as e:
        if 'No objects found with keys' in str(e):
            return {
                'success': True,
                'comment': 'Ad is already unindexed'
            }

        return response.status(400).json({
            'success': False,
            'comment': f"Error unindexing ad: {observer_id}/{timestamp}.{ad_id} - {str(e)}"
        })


@route('ads/query/new-session', 'GET')
@use(authenticate)
def new_query_session(event, response):
    """Create a new query session for ads.

    This endpoint is used to create a new query session for ads, allowing users to perform queries on ads.
    ---
    tags:
        - ads
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            session_id:
                                type: string
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'SESSION_CREATION_FAILED'
    """
    try:
        ad_query = AdQuery()
        session_id = ad_query.create_session()
        return {
            'success': True,
            'session_id': session_id
        }
    except Exception as e:
        return response.status(400).json({
            'success': False,
            'comment': f"Error creating query session: {str(e)}"
        })


@route('ads/query', 'POST')
@use(authenticate)
def query(event, response):
    """Query ads based on specified criteria and return the matching ad paths and the expanded ads.

    Perform a query on ads using the specified criteria and return the matching ad paths.
    ---
    tags:
        - ads
    requestBody:
        required: true
        content:
            application/json:
                schema:
                    type: object
                    properties:
                        method:
                            type: string
                            example: OR
                        args:
                            type: array
                            items:
                                type: object
                        session_id:
                            type: string
                            example: 'my-session-id'
                        context:
                            type: object
                            description: Additional context for the query.
                            properties:
                                continuation_key:
                                    type: string
                                    description: The continuation key for the next page of results, if applicable.
                                    example: '1729261457039'
                                page_size:
                                    type: integer
                                    description: The number of results to return per page.
                                    example: 1000
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            result:
                                type: array
                                items:
                                    type: string
                            expand:
                                type: array
                                items:
                                    $ref: '#/components/schemas/Ad'
                            context:
                                type: object
                                properties:
                                    continuation_key:
                                        type: string
                                        description: The continuation key for the next page of results, if applicable.
                                        example: 1729261457039
                                    total_results:
                                        type: integer
                                        description: The total number of results available for the query.
                                        example: 10000
        400:
            description: A failed response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'INVALID_QUERY'
    """
    method = event['body'].get('method', None)
    args = event['body'].get('args', None)
    if method is None or args is None:
        return response.status(400).json({
            'success': False,
            'comment': 'INVALID_QUERY: method and args are required'
        })
    query_dict = {
        'method': method,
        'args': args
    }

    session_id = event['body'].get('session_id', None)
    context = event['body'].get('context', {})

    ad_query = AdQuery()
    try:
        # result = ad_query.query_all(query_dict) # List of ad paths that satisfy the query

        if session_id is not None:
            result, last_key, total_results = ad_query.query_paginated(
                query_dict=query_dict,
                session_id=session_id,
                page_size=context.get('page_size', 1000),
                search_after=context.get('continuation_key', None)
            )
        else:
            result = ad_query.query_all(query_dict)

        return {
            'success': True,
            'result': result,
            'expand': [],  # Keeping for legacy type support
            'context': {
                'continuation_key': last_key if session_id is not None else None,
                'total_results': total_results if session_id is not None else len(result)
            }
        }
    except Exception as e:
        return response.status(400).json({
            'success': False,
            'comment': f"Error executing query: {str(e)}"
        })
