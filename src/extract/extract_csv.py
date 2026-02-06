"""
Extract CSV - Extraction des données CSV depuis MinIO S3
Source: Bucket telco-raw / csv/telco_churn_with_all_feedback.csv
"""

import pandas as pd
import io
from src.utils.minio_client import get_minio_client, load_config


def extract_csv_from_minio(bucket_name: str = None, object_name: str = "csv/telco_churn_with_all_feedback.csv") -> pd.DataFrame:
    """
    Extrait les données CSV directement depuis MinIO S3.

    Args:
        bucket_name: Nom du bucket (par défaut: telco-raw depuis config)
        object_name: Chemin de l'objet dans le bucket

    Returns:
        DataFrame pandas avec les données extraites
    """
    print(f"\n📥 EXTRACT CSV depuis MinIO S3: {object_name}")

    config = load_config()
    client = get_minio_client()

    if bucket_name is None:
        bucket_name = config["minio"]["buckets"]["raw"]

    # Télécharger le CSV depuis MinIO
    response = client.get_object(bucket_name, object_name)
    csv_data = response.read()
    response.close()
    response.release_conn()

    # Parser le CSV
    df = pd.read_csv(io.BytesIO(csv_data))

    print(f"  ✅ {len(df)} lignes extraites depuis s3://{bucket_name}/{object_name}")
    print(f"  📊 Colonnes: {list(df.columns)}")
    print(f"  🎯 Churn distribution:")
    print(f"     {df['Churn'].value_counts().to_dict()}")

    return df
