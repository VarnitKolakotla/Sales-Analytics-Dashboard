"""Central configuration. Every value can be overridden with an environment variable,
so the SAME code runs against a local emulated bucket, MinIO, AWS S3 or any
S3-compatible cloud storage (Google Cloud Storage interoperability mode, Wasabi, etc.).
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# ---- Cloud storage ---------------------------------------------------------
# local -> folder on disk that behaves like a bucket (zero setup, great for demos)
# s3    -> real object storage through the S3 API (AWS S3, MinIO, GCS-interop ...)
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "local").lower()
BUCKET_NAME = os.getenv("BUCKET_NAME", "sales-analytics-lake")
LOCAL_BUCKET_ROOT = Path(os.getenv("LOCAL_BUCKET_ROOT", BASE_DIR / "cloud_storage"))

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")          # e.g. http://minio:9000 (empty for AWS)
S3_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")  # Mumbai region
# credentials are read by boto3 from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY

# ---- Local cache used when the bucket is remote -----------------------------
CACHE_DIR = Path(os.getenv("CACHE_DIR", BASE_DIR / ".cache"))

# ---- ETL --------------------------------------------------------------------
ETL_WORKERS = int(os.getenv("ETL_WORKERS", max(1, (os.cpu_count() or 2))))

# ---- API --------------------------------------------------------------------
API_CACHE_SIZE = int(os.getenv("API_CACHE_SIZE", 512))
MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", 50))
PORT = int(os.getenv("PORT", 5000))

# Data-lake zones (folders / key prefixes inside the bucket)
ZONE_RAW = "raw/"              # bronze  - files exactly as they arrived
ZONE_CURATED = "curated/"      # silver  - cleaned, typed, partitioned Parquet
ZONE_QUARANTINE = "quarantine/"  # rejected rows with the reason
ZONE_META = "_meta/"           # pipeline run manifests
