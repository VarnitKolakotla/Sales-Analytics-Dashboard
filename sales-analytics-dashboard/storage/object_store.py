"""Cloud-storage abstraction (the heart of the 'Cloud Storage' requirement).

Both back-ends expose the exact same object-storage API (put / get / list / delete)
so the rest of the project never knows where the bytes really live.

    LocalObjectStore  - a folder that behaves like a bucket (key -> file)
    S3ObjectStore     - AWS S3 / MinIO / any S3-compatible service through boto3
"""
from __future__ import annotations

import hashlib
import json
import shutil
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

import config


class ObjectStore(ABC):
    kind = "abstract"

    # ---- writes --------------------------------------------------------
    @abstractmethod
    def put_file(self, local_path: str | Path, key: str) -> None: ...

    def put_bytes(self, data: bytes, key: str) -> None:
        tmp = config.CACHE_DIR / "_upload_tmp"
        tmp.mkdir(parents=True, exist_ok=True)
        f = tmp / hashlib.md5(key.encode()).hexdigest()
        f.write_bytes(data)
        try:
            self.put_file(f, key)
        finally:
            f.unlink(missing_ok=True)

    def put_json(self, obj, key: str) -> None:
        self.put_bytes(json.dumps(obj, indent=2, default=str).encode(), key)

    @abstractmethod
    def delete_prefix(self, prefix: str) -> int: ...

    # ---- reads ---------------------------------------------------------
    @abstractmethod
    def download_file(self, key: str, local_path: str | Path) -> None: ...

    @abstractmethod
    def get_bytes(self, key: str) -> bytes: ...

    def get_json(self, key: str, default=None):
        try:
            return json.loads(self.get_bytes(key))
        except Exception:
            return default

    @abstractmethod
    def list_objects(self, prefix: str = "") -> list[dict]:
        """[{key, size, last_modified(iso str)}] sorted by key."""

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def materialize(self, prefix: str) -> Path:
        """Return a local directory that contains every object under `prefix`
        (relative layout preserved) so a query engine can read it quickly."""

    @abstractmethod
    def describe(self) -> dict: ...


# ======================================================================
class LocalObjectStore(ObjectStore):
    kind = "local"

    def __init__(self, root: Path, bucket: str):
        self.bucket = bucket
        self.root = Path(root) / bucket
        self.root.mkdir(parents=True, exist_ok=True)

    def _p(self, key: str) -> Path:
        p = (self.root / key).resolve()
        if self.root.resolve() not in p.parents and p != self.root.resolve():
            raise ValueError("key escapes bucket")
        return p

    def put_file(self, local_path, key):
        dst = self._p(key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_name(dst.name + ".part")
        shutil.copyfile(local_path, tmp)
        tmp.replace(dst)  # atomic publish, like an S3 PUT

    def delete_prefix(self, prefix):
        n = 0
        base = self._p(prefix) if prefix else self.root
        if base.is_file():
            base.unlink(); return 1
        if base.exists():
            n = sum(1 for p in base.rglob("*") if p.is_file())
            shutil.rmtree(base)
        return n

    def download_file(self, key, local_path):
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self._p(key), local_path)

    def get_bytes(self, key):
        return self._p(key).read_bytes()

    def list_objects(self, prefix=""):
        base = self.root
        out = []
        for p in sorted(base.rglob("*")):
            if p.is_file() and not p.name.endswith(".part"):
                key = p.relative_to(base).as_posix()
                if key.startswith(prefix):
                    st = p.stat()
                    out.append({"key": key, "size": st.st_size,
                                "last_modified": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat()})
        return out

    def exists(self, key):
        return self._p(key).is_file()

    def materialize(self, prefix):
        # Zero-copy: the bucket already lives on local disk.
        return self.root

    def describe(self):
        return {"backend": "Local object store (S3 emulation)", "kind": "local",
                "bucket": self.bucket, "location": str(self.root), "region": "local-disk"}


# ======================================================================
class S3ObjectStore(ObjectStore):
    kind = "s3"

    def __init__(self, bucket: str, endpoint_url: str | None, region: str):
        import boto3
        from botocore.client import Config
        from botocore.exceptions import ClientError

        self._ClientError = ClientError
        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self.region = region
        self.s3 = boto3.client("s3", endpoint_url=endpoint_url, region_name=region,
                               config=Config(signature_version="s3v4", retries={"max_attempts": 5}))
        try:
            self.s3.head_bucket(Bucket=bucket)
        except ClientError:
            kw = {} if region == "us-east-1" or endpoint_url else {
                "CreateBucketConfiguration": {"LocationConstraint": region}}
            self.s3.create_bucket(Bucket=bucket, **kw)

    def put_file(self, local_path, key):
        self.s3.upload_file(str(local_path), self.bucket, key)  # multipart for big files

    def delete_prefix(self, prefix):
        n = 0
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if objs:
                self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": objs})
                n += len(objs)
        return n

    def download_file(self, key, local_path):
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        self.s3.download_file(self.bucket, key, str(local_path))

    def get_bytes(self, key):
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def list_objects(self, prefix=""):
        out = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                out.append({"key": o["Key"], "size": o["Size"], "last_modified": o["LastModified"].isoformat()})
        return out

    def exists(self, key):
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except self._ClientError:
            return False

    def materialize(self, prefix):
        """Incremental sync of `prefix` into a local cache (only new/changed objects are downloaded)."""
        cache = config.CACHE_DIR / "bucket" / self.bucket
        cache.mkdir(parents=True, exist_ok=True)
        manifest_path = config.CACHE_DIR / f"{self.bucket}.manifest.json"
        manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
        remote = {o["key"]: o for o in self.list_objects(prefix)}
        for key, o in remote.items():
            sig = [o["size"], o["last_modified"]]
            if manifest.get(key) != sig or not (cache / key).exists():
                self.download_file(key, cache / key)
                manifest[key] = sig
        for key in list(manifest):                      # drop objects deleted in the cloud
            if key.startswith(prefix) and key not in remote:
                (cache / key).unlink(missing_ok=True)
                manifest.pop(key)
        manifest_path.write_text(json.dumps(manifest))
        return cache

    def describe(self):
        return {"backend": "S3-compatible object storage", "kind": "s3", "bucket": self.bucket,
                "location": self.endpoint_url or f"s3.{self.region}.amazonaws.com", "region": self.region}


# ======================================================================
_store: ObjectStore | None = None


def get_store() -> ObjectStore:
    """Factory driven by configuration (STORAGE_BACKEND=local|s3)."""
    global _store
    if _store is None:
        if config.STORAGE_BACKEND == "s3":
            _store = S3ObjectStore(config.BUCKET_NAME, config.S3_ENDPOINT_URL, config.S3_REGION)
        else:
            _store = LocalObjectStore(config.LOCAL_BUCKET_ROOT, config.BUCKET_NAME)
    return _store
