"""
Load to MinIO (Data Lake)
Charge les données brutes et transformées dans MinIO.

Architecture Data Lake (Medallion):
    🥉 Bronze (telco-raw)     : Données brutes telles quelles
    🥈 Silver (telco-staging)  : Données nettoyées
    🥇 Gold (telco-curated)    : Données transformées prêtes pour le warehouse
"""

import pandas as pd
import io
from src.utils.minio_client import get_minio_client, create_buckets, upload_data, load_config


def load_df_to_minio(df: pd.DataFrame, bucket_key: str, object_name: str):
    """
    Charge un DataFrame (format Parquet) dans un bucket MinIO.

    Args:
        df: DataFrame pandas à charger
        bucket_key: Clé du bucket dans la config ('raw', 'staging', 'curated')
        object_name: Nom de l'objet dans le bucket (ex: 'customers.parquet')
    """
    print(f"\n📤 LOAD TO MINIO ({bucket_key}): {object_name}")

    config = load_config()
    client = get_minio_client()
    create_buckets(client)

    bucket = config["minio"]["buckets"][bucket_key]

    # Convertir le DataFrame en Parquet (format optimal pour le Data Lake)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)

    upload_data(
        client, bucket, object_name,
        parquet_buffer, len(parquet_buffer.getvalue()),
        content_type="application/octet-stream"
    )

    print(f"  📊 {len(df)} lignes chargées en Parquet")
