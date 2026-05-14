"""CCL (Commercial Content Library) API endpoints.

Provides endpoints for querying advertising entities and advertisement
snapshots extracted from CCL enrichments.
"""

from sqlalchemy.orm import Session

from db.clients.rds_storage_client import RdsSession, db_url

_ccl_rds = RdsSession(db_url)
from middlewares.authenticate import authenticate
from models.advertising_entity import AdvertisingEntityORM
from models.advertisement_snapshot import AdvertisementSnapshotORM
from models.commercial_content_enrichment import CommercialContentEnrichmentORM
from models.observation import ObservationORM
from routes import route
from utils import Response, use
from utils.presign import replace_s3_uris


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MAX_LIMIT = 1000
DEFAULT_LIMIT = 100

def _get_observations_for_snapshots(session: Session, snapshot_source_ids: list[str]) -> dict[str, ObservationORM]:
    """Fetch related observation ids for a list of snapshot IDs."""
    
    results = session.query(
        AdvertisementSnapshotORM.source_id,
        CommercialContentEnrichmentORM.observation_id,
    ).join(
        CommercialContentEnrichmentORM,
        AdvertisementSnapshotORM.ccl_enrichment_id == CommercialContentEnrichmentORM.id,
    ).filter(
        AdvertisementSnapshotORM.source_id.in_(snapshot_source_ids)
    ).all()
    
    # Group by source_id to handle multiple snapshots from the same observation
    observations_by_source_id = {}
    for source_id, observation_id in results:
        if source_id not in observations_by_source_id:
            observations_by_source_id[source_id] = set()
        observations_by_source_id[source_id].add(observation_id)
        
    # Convert sets to lists for JSON serialization
    for source_id in observations_by_source_id:
        observations_by_source_id[source_id] = list(observations_by_source_id[source_id])
        
    return observations_by_source_id

def _get_observations_for_entities(session: Session, entity_source_ids: list[str]) -> dict[str, ObservationORM]:
    """Fetch related observation ids for a list of entity source IDs."""
    results = session.query(
        AdvertisingEntityORM.source_id,
        CommercialContentEnrichmentORM.observation_id,
    ).join(
        CommercialContentEnrichmentORM,
        AdvertisingEntityORM.ccl_enrichment_id == CommercialContentEnrichmentORM.id,
    ).filter(
        AdvertisingEntityORM.source_id.in_(entity_source_ids)
    ).all()
    
    # Group by source_id to handle multiple entities from the same observation
    observations_by_source_id = {}
    for source_id, observation_id in results:
        if source_id not in observations_by_source_id:
            observations_by_source_id[source_id] = set()
        observations_by_source_id[source_id].add(observation_id)
        
    # Convert sets to lists for JSON serialization
    for source_id in observations_by_source_id:
        observations_by_source_id[source_id] = list(observations_by_source_id[source_id])
        
    return observations_by_source_id

def _parse_pagination_params(event: dict) -> tuple[int, str | None]:
    """Extract and validate ``limit`` and ``cursor`` from query parameters.

    Returns:
        (limit, cursor) where *cursor* is ``None`` when absent.
    """
    qs = event.get("queryStringParameters", {}) or {}
    try:
        limit = int(qs.get("limit", DEFAULT_LIMIT))
    except (ValueError, TypeError):
        limit = DEFAULT_LIMIT
    limit = max(1, min(limit, MAX_LIMIT))

    cursor = qs.get("cursor", None)
    return limit, cursor


def _parse_filter_params(event: dict) -> dict:
    """Extract recognised filter parameters from the query string.

    Supported filters:
        observation_id, observer_id, platform, type
    """
    qs = event.get("queryStringParameters", {}) or {}
    filters = {}
    for key in (
        "observation_id",
        "observer_id",
        "platform",
        "type",
    ):
        value = qs.get(key, None)
        if value is not None:
            filters[key] = value
    return filters


def _needs_enrichment_join(filters: dict) -> bool:
    """Return True when filters require a join to the enrichments table."""
    return any(
        key in filters
        for key in (
            "observation_id",
            "observer_id",
            "platform",
        )
    )


def _needs_observation_join(filters: dict) -> bool:
    """Return True when we need to further join to the observations table."""
    return "observer_id" in filters


def _apply_enrichment_filters(query, filters: dict):
    """Apply filters that target CommercialContentEnrichmentORM columns."""
    if "observation_id" in filters:
        query = query.filter(
            CommercialContentEnrichmentORM.observation_id == filters["observation_id"]
        )
    if "platform" in filters:
        query = query.filter(
            CommercialContentEnrichmentORM.platform == filters["platform"]
        )
    return query


def _apply_observation_filters(query, filters: dict):
    """Apply filters that target ObservationORM columns."""
    if "observer_id" in filters:
        query = query.filter(
            ObservationORM.observer_id == filters["observer_id"]
        )
    return query


def _serialize_entity(entity: AdvertisingEntityORM, *, s3_client=None) -> dict:
    """Convert an AdvertisingEntityORM instance to a response dict.

    Any ``s3://`` URIs inside the JSONB *data* field are replaced with
    pre-signed HTTPS URLs.
    """
    data = replace_s3_uris(entity.data or {}, client=s3_client)
    return {
        "id": entity.id,
        "ccl_enrichment_id": entity.ccl_enrichment_id,
        "source_id": entity.source_id,
        "type": entity.type,
        "name": data.get("name"),
        "data": data,
    }


def _serialize_snapshot(snapshot: AdvertisementSnapshotORM, *, s3_client=None) -> dict:
    """Convert an AdvertisementSnapshotORM instance to a response dict.

    Any ``s3://`` URIs inside the JSONB *data* field are replaced with
    pre-signed HTTPS URLs.
    """
    return {
        "id": snapshot.id,
        "ccl_enrichment_id": snapshot.ccl_enrichment_id,
        "source_id": snapshot.source_id,
        "data": replace_s3_uris(snapshot.data or {}, client=s3_client),
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@route("ccl/entities", "GET")
@use(authenticate)
def get_ccl_entities(event, response: Response):
    """Retrieve a paginated list of advertising entities from CCL enrichments.

    Supports filtering by observation_id, observer_id, platform and entity type. Uses
    cursor-based pagination with the entity ID as the cursor.
    ---
    tags:
        - ccl
    parameters:
        -   in: query
            name: limit
            schema:
                type: integer
            required: false
            description: Number of items per page (default 100, max 1000)
        -   in: query
            name: cursor
            schema:
                type: string
            required: false
            description: Entity ID to start after (for cursor-based pagination)
        -   in: query
            name: observation_id
            schema:
                type: string
            required: false
        -   in: query
            name: observer_id
            schema:
                type: string
            required: false
        -   in: query
            name: platform
            schema:
                type: string
            required: false
        -   in: query
            name: type
            schema:
                type: string
            required: false
            description: Entity type filter
    responses:
        200:
            description: A paginated list of advertising entities
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            entities:
                                type: array
                                items:
                                    type: object
                            pagination:
                                type: object
                                properties:
                                    next_cursor:
                                        type: string
                                        nullable: true
                                        description: Cursor for the next page (null if no more results)
        500:
            description: Internal server error
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: false
                            comment:
                                type: string
                            error:
                                type: string
    """
    limit, cursor = _parse_pagination_params(event)
    filters = _parse_filter_params(event)

    try:
        with _ccl_rds.session_maker() as session:
            query = session.query(AdvertisingEntityORM)

            # Join to enrichments table when needed for filters
            if _needs_enrichment_join(filters):
                query = query.join(
                    CommercialContentEnrichmentORM,
                    AdvertisingEntityORM.ccl_enrichment_id == CommercialContentEnrichmentORM.id,
                )
                query = _apply_enrichment_filters(query, filters)

                # Further join to observations for observer_id
                if _needs_observation_join(filters):
                    query = query.join(
                        ObservationORM,
                        CommercialContentEnrichmentORM.observation_id == ObservationORM.observation_id,
                    )
                    query = _apply_observation_filters(query, filters)

            # Entity-level filters
            if "type" in filters:
                query = query.filter(AdvertisingEntityORM.type == filters["type"])

            # Cursor-based pagination: return rows with id > cursor
            if cursor is not None:
                query = query.filter(AdvertisingEntityORM.id > cursor)

            query = query.order_by(AdvertisingEntityORM.id.asc())
            results = query.limit(limit).all()

            entities = [_serialize_entity(e) for e in results]
            next_cursor = entities[-1]["id"] if len(entities) == limit else None

            # Add related observation ids to the response
            source_ids = [e["source_id"] for e in entities]
            observations_by_source_id = _get_observations_for_entities(session, source_ids)
            for entity in entities:
                entity["observed_in"] = observations_by_source_id.get(entity["source_id"], [])

        return {
            "success": True,
            "entities": entities,
            "pagination": {
                "next_cursor": next_cursor,
            },
        }
    except Exception as e:
        print(f"[CCL] Error querying entities: {e}")
        return response.status(500).json({
            "success": False,
            "comment": "FAILED_TO_QUERY_ENTITIES",
            "error": str(e),
        })


@route("ccl/snapshots", "GET")
@use(authenticate)
def get_ccl_snapshots(event, response: Response):
    """Retrieve a paginated list of advertisement snapshots from CCL enrichments.

    Supports filtering by observation_id, observer_id and platform. Uses
    cursor-based pagination with the snapshot ID as the cursor.
    ---
    tags:
        - ccl
    parameters:
        -   in: query
            name: limit
            schema:
                type: integer
            required: false
            description: Number of items per page (default 100, max 1000)
        -   in: query
            name: cursor
            schema:
                type: string
            required: false
            description: Snapshot ID to start after (for cursor-based pagination)
        -   in: query
            name: observation_id
            schema:
                type: string
            required: false
        -   in: query
            name: observer_id
            schema:
                type: string
            required: false
        -   in: query
            name: platform
            schema:
                type: string
            required: false
    responses:
        200:
            description: A paginated list of advertisement snapshots
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            snapshots:
                                type: array
                                items:
                                    type: object
                            pagination:
                                type: object
                                properties:
                                    next_cursor:
                                        type: string
                                        nullable: true
                                        description: Cursor for the next page (null if no more results)
        500:
            description: Internal server error
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: false
                            comment:
                                type: string
                            error:
                                type: string
    """
    limit, cursor = _parse_pagination_params(event)
    filters = _parse_filter_params(event)

    try:
        with _ccl_rds.session_maker() as session:
            query = session.query(AdvertisementSnapshotORM)

            # Join to enrichments table when needed for filters
            if _needs_enrichment_join(filters):
                query = query.join(
                    CommercialContentEnrichmentORM,
                    AdvertisementSnapshotORM.ccl_enrichment_id == CommercialContentEnrichmentORM.id,
                )
                query = _apply_enrichment_filters(query, filters)

                # Further join to observations for observer_id
                if _needs_observation_join(filters):
                    query = query.join(
                        ObservationORM,
                        CommercialContentEnrichmentORM.observation_id == ObservationORM.observation_id,
                    )
                    query = _apply_observation_filters(query, filters)

            # Cursor-based pagination
            if cursor is not None:
                query = query.filter(AdvertisementSnapshotORM.id > cursor)

            query = query.order_by(AdvertisementSnapshotORM.id.asc())
            results = query.limit(limit).all()

            snapshots = [_serialize_snapshot(s) for s in results]
            next_cursor = snapshots[-1]["id"] if len(snapshots) == limit else None

            # Add related observation ids to the response
            source_ids = [s["source_id"] for s in snapshots]
            observations_by_source_id = _get_observations_for_snapshots(session, source_ids)
            for snapshot in snapshots:
                snapshot["observed_in"] = observations_by_source_id.get(snapshot["source_id"], [])

        return {
            "success": True,
            "snapshots": snapshots,
            "pagination": {
                "next_cursor": next_cursor,
            },
        }
    except Exception as e:
        print(f"[CCL] Error querying snapshots: {e}")
        return response.status(500).json({
            "success": False,
            "comment": "FAILED_TO_QUERY_SNAPSHOTS",
            "error": str(e),
        })
