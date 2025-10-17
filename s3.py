#!/usr/bin/env python3
"""
MinIO S3 helper: create/upload/download/list + delete-file/delete-bucket with progress bars.

Requirements:
  pip install minio tqdm

Env:
  MINIO_ENDPOINT   (e.g. "localhost:9000")
  MINIO_ACCESS_KEY
  MINIO_SECRET_KEY
  MINIO_SECURE     ("true" or "false", defaults to false)
  MINIO_REGION     (optional)
"""

import argparse
import json
import os
import stat
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from minio import Minio
from minio.error import S3Error
from tqdm import tqdm



# ---------- Helpers ----------

class ProgressFile(object):
    """Wrap a file object to report read() progress to a tqdm bar."""
    def __init__(self, path, bar):
        self._f = open(path, "rb")
        self._bar = bar
        self.closed = False
    def read(self, n):
        chunk = self._f.read(n)
        if chunk:
            self._bar.update(len(chunk))
        return chunk
    def seek(self, *args, **kwargs):
        return self._f.seek(*args, **kwargs)
    def tell(self):
        return self._f.tell()
    def close(self):
        if not self.closed:
            try:
                self._f.close()
            finally:
                self._bar.close()
                self.closed = True


def get_client():
    endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")
    secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"

    missing = [k for k, v in [
        ("MINIO_ENDPOINT", endpoint),
        ("MINIO_ACCESS_KEY", access_key),
        ("MINIO_SECRET_KEY", secret_key),
    ] if not v]
    if missing:
        print("Missing required env var(s): {}".format(", ".join(missing)), file=sys.stderr)
        sys.exit(2)

    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def _resolve_setting(env_key: str, settings: Dict[str, Any], default: Optional[str] = None) -> Optional[str]:
    env_val = os.environ.get(env_key)
    if env_val:
        return env_val
    value = settings.get(env_key)
    if value:
        return str(value)
    return default


def get_client3():
    settings = load_settings()

    provider = settings.get("PROVIDER", "aws")
    region = _resolve_setting("AWS_REGION", settings)
    endpoint_default = f"s3.{region}.amazonaws.com" if region else None
    endpoint = _resolve_setting("AWS_S3_ENDPOINT", settings, endpoint_default)
    access_key = _resolve_setting("AWS_ACCESS_KEY_ID", settings)
    secret_key = _resolve_setting("AWS_SECRET_ACCESS_KEY", settings)
    secure_setting = _resolve_setting("AWS_S3_SECURE", settings, "true")
    secure = str(secure_setting).lower() not in ("0", "false", "no")
    path_style = str(_resolve_setting("AWS_S3_PATH_STYLE", settings, "false")).lower() in ("1", "true", "yes")
    ca_cert = None  # AWS public CAs — no custom CA needed

    required = [
        ("AWS_ACCESS_KEY_ID", access_key),
        ("AWS_SECRET_ACCESS_KEY", secret_key),
        ("AWS_S3_ENDPOINT", endpoint),
    ]
    if provider == "aws":
        required.append(("AWS_REGION", region))
    missing = [k for k, v in required if not v]
    if missing:
        print("Missing required configuration value(s): " + ", ".join(missing), file=sys.stderr)
        sys.exit(2)

    http_client = None
    if secure and ca_cert:
        try:
            from urllib import PoolManager
            import ssl
        except Exception:
            http_client = None
        else:
            http_client = PoolManager(cert_reqs=ssl.CERT_REQUIRED,ca_certs=ca_cert)


    minio_kwargs = dict(
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        region=region or None,
        http_client=http_client,
    )

    bucket_lookup_mode = "path" if path_style else "auto"

    try:
        return Minio(endpoint, bucket_lookup=bucket_lookup_mode, **minio_kwargs)
    except TypeError:
        # Older MinIO SDKs (< 7.2.5) do not accept the bucket_lookup argument.
        return Minio(endpoint, **minio_kwargs)


def ensure_bucket(client, bucket, region=None):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket, location=region)
        print("Created bucket: {}".format(bucket))
    else:
        print("Bucket exists: {}".format(bucket))


# ---------- Commands ----------

def cmd_create_bucket(args):
    client = get_client()
    region = os.environ.get("MINIO_REGION")
    try:
        ensure_bucket(client, args.bucket, region)
    except S3Error as e:
        print("Error creating bucket: {}".format(e), file=sys.stderr)
        sys.exit(1)


def cmd_upload(args):
    client = get_client()
    region = os.environ.get("MINIO_REGION")

    src = Path(args.file).expanduser()
    if not src.is_file():
        print("File not found: {}".format(src), file=sys.stderr)
        sys.exit(2)

    try:
        if args.create_if_missing:
            ensure_bucket(client, args.bucket, region)
        elif not client.bucket_exists(args.bucket):
            print("Bucket does not exist: {}".format(args.bucket), file=sys.stderr)
            sys.exit(2)

        object_name = args.key if args.key else src.name
        content_type = args.content_type if args.content_type else None
        file_size = src.stat().st_size

        if args.progress:
            with tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024,
                      desc="Uploading {}".format(object_name)) as bar:
                data = ProgressFile(str(src), bar)
                try:
                    result = client.put_object(
                        args.bucket,
                        object_name,
                        data,
                        length=file_size,
                        content_type=content_type
                    )
                finally:
                    data.close()
        else:
            result = client.fput_object(
                args.bucket, object_name, str(src), content_type=content_type
            )

        print("Uploaded: bucket={} key={} etag={} version_id={}".format(
            args.bucket, object_name, result.etag, result.version_id))
    except S3Error as e:
        print("Upload failed: {}".format(e), file=sys.stderr)
        sys.exit(1)


def cmd_download(args):
    client = get_client()
    try:
        out_path = Path(args.out).expanduser()
        if out_path.exists() and out_path.is_dir():
            filename = Path(args.key).name
            out_file = out_path / filename
        else:
            if out_path.suffix == "":
                out_path.mkdir(parents=True, exist_ok=True)
                out_file = out_path / Path(args.key).name
            else:
                out_file = out_path
                out_file.parent.mkdir(parents=True, exist_ok=True)

        if args.progress:
            # get total size for progress bar
            stat = client.stat_object(args.bucket, args.key)
            total = getattr(stat, "size", None)

            response = client.get_object(args.bucket, args.key)
            try:
                with open(str(out_file), "wb") as f, tqdm(
                    total=total, unit="B", unit_scale=True, unit_divisor=1024,
                    desc="Downloading {}".format(args.key)
                ) as bar:
                    for data in iter(lambda: response.read(32 * 1024), b""):
                        f.write(data)
                        bar.update(len(data))
            finally:
                response.close()
                response.release_conn()
        else:
            client.fget_object(args.bucket, args.key, str(out_file))

        print("Downloaded to: {}".format(out_file))
    except S3Error as e:
        print("Download failed: {}".format(e), file=sys.stderr)
        sys.exit(1)


def cmd_ls(args):
    client = get_client()
    try:
        if not client.bucket_exists(args.bucket):
            print("Bucket does not exist: {}".format(args.bucket), file=sys.stderr)
            sys.exit(2)
        recursive = args.recursive
        prefix = args.prefix if args.prefix else None
        for obj in client.list_objects(args.bucket, prefix=prefix, recursive=recursive):
            size = obj.size
            key = obj.object_name
            print("{:>12}  {}".format(size, key))
    except S3Error as e:
        print("List failed: {}".format(e), file=sys.stderr)
        sys.exit(1)


# ---------- Deletion helpers & commands ----------

def iter_objects(client, bucket, prefix=None, include_versions=False):
    """Yield objects (and versions if supported) under prefix."""
    try:
        iterator = client.list_objects(
            bucket, prefix=prefix, recursive=True, include_version=include_versions
        )
    except TypeError:
        iterator = client.list_objects(bucket, prefix=prefix, recursive=True)
    for obj in iterator:
        yield obj

def empty_bucket(client, bucket, include_versions=False, prefix=None):
    """Delete all objects in a bucket (optionally all versions)."""
    removed = 0
    errors = 0
    for obj in iter_objects(client, bucket, prefix=prefix, include_versions=include_versions):
        version_id = getattr(obj, "version_id", None) if include_versions else None
        try:
            client.remove_object(bucket, obj.object_name, version_id=version_id)
            removed += 1
        except S3Error as e:
            errors += 1
            print("Failed to delete {} (version {}): {}".format(obj.object_name, version_id, e), file=sys.stderr)
    print("Emptied objects: {}, errors: {}".format(removed, errors))
    return errors == 0

def cmd_delete_file(args):
    client = get_client()
    try:
        if args.all_versions:
            count = 0
            errs = 0
            for obj in iter_objects(client, args.bucket, prefix=args.key, include_versions=True):
                if obj.object_name != args.key:
                    continue
                vid = getattr(obj, "version_id", None)
                try:
                    client.remove_object(args.bucket, args.key, version_id=vid)
                    count += 1
                except S3Error as e:
                    errs += 1
                    print("Failed to delete version {} of {}: {}".format(vid, args.key, e), file=sys.stderr)
            if count == 0 and errs == 0:
                print("No versions found for key: {}".format(args.key))
            else:
                print("Deleted {} version(s) of {}".format(count, args.key))
            if errs:
                sys.exit(1)
        else:
            client.remove_object(args.bucket, args.key)
            print("Deleted object: {}".format(args.key))
    except S3Error as e:
        print("Delete failed: {}".format(e), file=sys.stderr)
        sys.exit(1)

def cmd_delete_bucket(args):
    client = get_client()
    try:
        if not client.bucket_exists(args.bucket):
            print("Bucket does not exist: {}".format(args.bucket), file=sys.stderr)
            sys.exit(2)

        if args.force:
            ok = empty_bucket(client, args.bucket, include_versions=args.include_versions)
            if not ok:
                print("Warning: some objects failed to delete; proceeding to remove bucket may fail.", file=sys.stderr)

        client.remove_bucket(args.bucket)
        print("Deleted bucket: {}".format(args.bucket))
    except S3Error as e:
        print("Delete bucket failed: {}".format(e), file=sys.stderr)
        sys.exit(1)


# ---------- CLI ----------

def build_parser():
    p = argparse.ArgumentParser(description="MinIO S3 bucket & object helper")
    sub = p.add_subparsers(dest="cmd", required=True)

    # create-bucket
    p_create = sub.add_parser("create-bucket", help="Create a bucket if it doesn't exist")
    p_create.add_argument("bucket", help="Bucket name")
    p_create.set_defaults(func=cmd_create_bucket)

    # upload
    p_up = sub.add_parser("upload", help="Upload a local file to a bucket")
    p_up.add_argument("bucket", help="Bucket name")
    p_up.add_argument("file", help="Path to local file")
    p_up.add_argument("--key", help="Object key (defaults to file's basename)")
    p_up.add_argument("--content-type", help="Optional content-type")
    p_up.add_argument("--create-if-missing", action="store_true", help="Create bucket if it doesn't exist")
    p_up.add_argument("--progress", action="store_true", help="Show progress bar during upload")
    p_up.set_defaults(func=cmd_upload)

    # download
    p_down = sub.add_parser("download", help="Download an object to a local path or directory")
    p_down.add_argument("bucket", help="Bucket name")
    p_down.add_argument("key", help="Object key to download")
    p_down.add_argument("--out", required=True, help="Destination file or directory")
    p_down.add_argument("--progress", action="store_true", help="Show progress bar during download")
    p_down.set_defaults(func=cmd_download)

    # ls
    p_ls = sub.add_parser("ls", help="List objects in a bucket (optional prefix, recursive)")
    p_ls.add_argument("bucket", help="Bucket name")
    p_ls.add_argument("--prefix", help="List only keys under this prefix")
    p_ls.add_argument("--recursive", action="store_true", help="Recurse into prefixes")
    p_ls.set_defaults(func=cmd_ls)

    # delete-file
    p_df = sub.add_parser("delete-file", help="Delete an object (optionally all versions)")
    p_df.add_argument("bucket", help="Bucket name")
    p_df.add_argument("key", help="Object key to delete")
    p_df.add_argument("--all-versions", action="store_true", help="Delete all versions of the object (if supported)")
    p_df.set_defaults(func=cmd_delete_file)

    # delete-bucket
    p_db = sub.add_parser("delete-bucket", help="Delete a bucket (optionally empty it first)")
    p_db.add_argument("bucket", help="Bucket name")
    p_db.add_argument("--force", action="store_true", help="Empty the bucket before deleting")
    p_db.add_argument("--include-versions", action="store_true", help="Also try deleting all object versions")
    p_db.set_defaults(func=cmd_delete_bucket)

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
CONFIG_ENV_PATH = "S3_MANAGER_CONFIG_PATH"
DEFAULT_CONFIG_FILENAME = ".s3_minio_manager.json"
CONFIG_PATH = Path(os.environ.get(CONFIG_ENV_PATH, str(Path.home() / DEFAULT_CONFIG_FILENAME)))


def load_settings() -> Dict[str, Any]:
    """Load persisted connection settings."""
    try:
        if CONFIG_PATH.is_file():
            raw = CONFIG_PATH.read_text(encoding="utf-8")
            data = json.loads(raw) if raw.strip() else {}
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def save_settings(data: Dict[str, Any]) -> None:
    """Persist connection settings to disk."""
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, indent=2)
    CONFIG_PATH.write_text(payload, encoding="utf-8")
    try:
        CONFIG_PATH.chmod(stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        pass
