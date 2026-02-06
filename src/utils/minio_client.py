"""
MinIO Client Utility
Gère la connexion et les opérations avec MinIO (Data Lake).

Configuration via variables d'environnement:
    MINIO_ENDPOINT  : Endpoint MinIO (default: localhost:9002)
    MINIO_ACCESS_KEY: Access key    (default: minioadmin)
    MINIO_SECRET_KEY: Secret key    (default: minioadmin)
"""

import os
from minio import Minio
from minio.error import S3Error


# --- Configuration ---

BUCKET_MAP = {
    "raw": "telco-raw",
    "staging": "telco-staging",
    "curated": "telco-curated",
}


def load_config() -> dict:
    """Retourne la configuration MinIO depuis les variables d'environnement."""
    return {
        "minio": {
            "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            "secure": False,
            "buckets": BUCKET_MAP,
        }
    }


def get_minio_client() -> Minio:
    """Crée et retourne un client MinIO."""
    config = load_config()["minio"]
    return Minio(
        endpoint=config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        secure=config["secure"],
    )


def create_buckets(client: Minio):
    """Crée les buckets nécessaires (Bronze / Silver / Gold)."""
    for layer, bucket_name in BUCKET_MAP.items():
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"  ✅ Bucket '{bucket_name}' créé ({layer})")
        else:
            print(f"  ℹ️  Bucket '{bucket_name}' existe déjà ({layer})")


def upload_data(client: Minio, bucket_name: str, object_name: str, data, length: int,
                content_type: str = "application/octet-stream"):
    """Upload des données (bytes/stream) dans un bucket MinIO."""
    try:
        client.put_object(bucket_name, object_name, data, length, content_type=content_type)
        print(f"  ✅ '{object_name}' uploadé dans '{bucket_name}'")
    except S3Error as e:
        print(f"  ❌ Erreur upload: {e}")
        raise
