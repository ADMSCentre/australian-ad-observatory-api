# AAO Ingestion Service

A standalone Python service for handling data ingestion in the Australian Ad Observatory pipeline. This service processes S3 data for clip classifications and RDO (Rich Data Object) outputs, indexing them to both PostgreSQL (RDS) and OpenSearch using backfill scripts.

## Features

- **Data Processing**: Processes S3 files for clip classifications and RDO outputs
  - Clip classification files (`{observer_id}/clip_classifications/{observation_id}.json`)
  - RDO output files (`{observer_id}/rdo/{timestamp}.{observation_id}/output.json`)
- **Dual Indexing**: Indexes observations to both RDS and OpenSearch
- **Backfill Scripts**: Tools for processing historical data
- **Index Management**: Registry for tracking OpenSearch index versions

## Usage

The **preferred method** to run the ingestion service is using Docker containers for consistent, isolated execution.

### Docker Usage (Recommended)

The service can be run directly from Docker for easy deployment and testing.

#### Prerequisites

- Docker installed
- Access to required AWS resources (S3, OpenSearch, PostgreSQL)

#### Pull Latest Image

> Note: Ensure you have access to the `registry.rc.nectar.org.au/aio2024-private` registry to pull the image. To log in, use:
>
> ```bash
> docker login registry.rc.nectar.org.au -u <username> -p <password>
> ```

```bash
docker pull registry.rc.nectar.org.au/aio2024-private/aao-enrichment-ingestion:latest
docker tag registry.rc.nectar.org.au/aio2024-private/aao-enrichment-ingestion:latest aao-enrichment-ingestion:latest
```

#### Run with Environment Variables

```bash
# Set environment variables inline or use --env-file
docker run --rm --env-file .env -v $(pwd)/logs:/app/logs aao-enrichment-ingestion scripts.index_rdo --new --workers 10
```

#### Common Examples

```bash
# RDO backfill to new index
docker run --rm --env-file .env aao-enrichment-ingestion scripts.index_rdo --new --workers 50

# CLIP classification backfill
docker run --rm --env-file .env aao-enrichment-ingestion scripts.index_clip --workers 8

# Interactive cleanup (requires -it)
docker run -it --rm --env-file .env aao-enrichment-ingestion scripts.index_rdo --cleanup

# Dry-run for validation
docker run --rm --env-file .env aao-enrichment-ingestion scripts.index_rdo --dry-run
```

**Number of Workers** The optimal number of workers typically ranges from 2-4x the number of CPU cores available, depending on memory, network I/O, and system resources. For example, on a 4-core system, try 8-16 workers. Monitor system load and adjust accordingly.

#### Docker Configuration Notes

- **Environment Variables**: Pass via `--env-file .env` or individual `--env` flags
- **Log Persistence**: Mount `/app/logs` volume to persist logs: `-v $(pwd)/logs:/app/logs`
- **Interactive Mode**: Use `-it` for interactive commands like `--cleanup`
- **Resource Limits**: Add `--cpus=2 --memory=2g` for resource constraints in production

### Backfill Scripts (Alternative)

If you prefer to run directly with Python, all backfill scripts are located in the `/scripts` directory.

#### RDO Backfill (Observations + OpenSearch)

Backfill observations to RDS and reduced RDOs to OpenSearch:

```bash
# Update the latest ready index
python -m scripts.index_rdo

# Create a new OpenSearch index
python -m scripts.index_rdo --new

# RDS only (observations table)
python -m scripts.index_rdo --rds-only

# OpenSearch only
python -m scripts.index_rdo --opensearch-only

# Specify number of workers and index prefix
python -m scripts.index_rdo --new --workers 100 --prefix my-index_

# Delete an OpenSearch index (interactive with confirmation)
python -m scripts.index_rdo --cleanup
```

**Workers Note:** The optimal number of workers typically ranges from 2-4x the number of CPU cores available, depending on memory, network I/O, and system resources. For example, on a 4-core system, try 8-16 workers. Monitor system load and adjust accordingly.

The `--cleanup` flag provides an interactive interface to:
- List all available OpenSearch indices with their status
- Select an index for deletion
- View the index details before deletion
- Confirm deletion (requires typing 'yes')
- Delete the index from both OpenSearch and the RDS registry

#### CLIP Classification Backfill

Backfill clip classifications to RDS. Uses UPSERT logic - updates existing 
classifications by `observation_id + label`, or inserts new ones. Does NOT 
delete existing entries.

```bash
# Process all clip classifications
python -m scripts.index_clip

# Specify number of workers
python -m scripts.index_clip --workers 16

**Workers Note:** The optimal number of workers typically ranges from 2-4x the number of CPU cores available, depending on memory, network I/O, and system resources. For example, on a 4-core system, try 8-16 workers. Monitor system load and adjust accordingly.

# Process specific observer
python -m scripts.index_clip --observer-id <uuid>

# Process specific observation
python -m scripts.index_clip --observer-id <uuid> --observation-id <uuid>
```

#### ETL Module (Programmatic Use)

The ETL module at `utils/etl/clip_classification.py` provides programmatic access for processing clip classifications. It uses DELETE+INSERT for idempotency - existing classifications for an observation are deleted before inserting new ones, ensuring consistent state on reprocessing.

```bash
# Process all clip classifications (with optional clear)
python -m utils.etl.clip_classification [--clear] [--workers N]

# Process specific observer
python -m utils.etl.clip_classification --observer-id <uuid>

# Process specific observation
python -m utils.etl.clip_classification --observer-id <uuid> --observation-id <uuid>
```

## Development

### Architecture

```
S3 Bucket (fta-mobile-observations-v2)
    │
    ├── Clip Classifications ──┐
    │                          │
    └── RDO Outputs ───────────┼──► Python Scripts
                                 │        │
                                 │        ├──► PostgreSQL (observations, ad_classifications)
                                 │        │
                                 │        └──► OpenSearch (reduced RDO index)
```

### Project Structure

```
.
├── config.py                 # Environment-based configuration
├── scripts/
│   ├── __init__.py
│   ├── index_rdo.py          # RDO backfill to RDS + OpenSearch
│   └── index_clip.py         # CLIP classification backfill (upsert)
├── db/
│   ├── __init__.py
│   └── database.py           # SQLAlchemy session management
├── models/
│   ├── base.py               # SQLAlchemy Base
│   ├── observation.py        # Observation ORM model
│   ├── clip_classification.py# Clip classification ORM model
│   └── open_search_index.py  # Index registry ORM model
└── utils/
    ├── etl/
    │   └── clip_classification.py  # ETL for clip classifications (delete+insert)
    ├── indexer/
    │   ├── indexer.py        # Dual RDS/OpenSearch indexer
    │   └── registry.py       # Index lifecycle management
    ├── opensearch/
    │   └── rdo_open_search.py# OpenSearch client wrapper
    ├── reduce_rdo/           # RDO transformation logic
    └── observations_sub_bucket.py  # S3 utilities
```

### Setup

#### Prerequisites

- Python 3.10+
- PostgreSQL database
- OpenSearch cluster
- AWS credentials with S3 access
- Access to S3 bucket `fta-mobile-observations-v2`

#### Environment Variables

Create a `.env` file based on `.env.example` in the project root:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=ap-southeast-2

# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=aao
POSTGRES_USERNAME=postgres
POSTGRES_PASSWORD=your_password

# OpenSearch Configuration
OPENSEARCH_ENDPOINT=https://your-opensearch-endpoint

# S3 Buckets
S3_OBSERVATIONS_BUCKET=fta-mobile-observations-v2
S3_METADATA_BUCKET=your-metadata-bucket
```

**Automatic .env Loading:**

The configuration system automatically loads environment variables from a `.env` file if present in the project root. This happens when any script imports the `config` module.

**Environment Variable Validation:**

The configuration system automatically validates environment variables when loaded:

- **Critical Variables (errors if missing)**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `OPENSEARCH_ENDPOINT`, `POSTGRES_PASSWORD`
- **Optional Variables (warnings if missing)**: `S3_METADATA_BUCKET`
- **Default Values**: Variables like `AWS_REGION`, `POSTGRES_HOST`, `POSTGRES_PORT`, etc. have sensible defaults

When you run any script, you'll see:
- **Warnings** for optional variables that are not set (uses default values)
- **Errors** for critical variables that are not set (prevents execution)

Example output when running a script:
```
config.py:170: UserWarning: Missing optional environment variables (using defaults):
  - S3_METADATA_BUCKET
```

#### Installation

```bash
# Create virtual environment
python -m venv .venv

# Activate (Windows)
.\.venv\Scripts\activate

# Activate (Linux/MacOS)
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```


### Deployment

The service can be deployed to a container registry using the automated deployment scripts.

#### Prerequisites

- Docker installed and configured
- Access to `registry.rc.nectar.org.au/aio2024-private` registry
- Version number stored in `version` file

#### Automated Deployment

Use the provided scripts to build and deploy the service:

**Windows (PowerShell):**
```powershell
.\scripts\bump.ps1
```

**Linux/macOS (Bash):**
```bash
./scripts/bump.sh
```

These scripts will:
1. Read the current version from the `version` file
2. Build the Docker image with the version tag
3. Tag the image for the registry with version and `latest` tags
4. Push both tags to the registry

#### Manual Deployment

If you prefer to run the commands manually:

```bash
# Read current version
version=$(cat version)

# Build and tag (replace VERSION with actual version)
docker build . -t aao-enrichment-ingestion:VERSION
docker tag aao-enrichment-ingestion:VERSION registry.rc.nectar.org.au/aio2024-private/aao-enrichment-ingestion:VERSION
docker tag aao-enrichment-ingestion:VERSION registry.rc.nectar.org.au/aio2024-private/aao-enrichment-ingestion:latest

# Push images
docker push registry.rc.nectar.org.au/aio2024-private/aao-enrichment-ingestion:VERSION
docker push registry.rc.nectar.org.au/aio2024-private/aao-enrichment-ingestion:latest

echo "Pushed VERSION"
```

#### Version Management

- Update the `version` file with the new version number before deployment
- Follow semantic versioning (MAJOR.MINOR.PATCH)
- The automated scripts use the version from the file
