"""
Extract JSON - Extraction des données depuis MinIO S3
Source: Bucket telco-raw / json/synthetic_telco_data.json

Les données sont déjà uploadées dans MinIO S3.
On lit directement depuis le Data Lake (pas de fichier local).
"""

import pandas as pd
import json
import io
from src.utils.minio_client import get_minio_client, load_config


def extract_json_from_minio(bucket_name: str = None, object_name: str = "json/synthetic_telco_data.json") -> pd.DataFrame:
    """
    Extrait les données JSON directement depuis MinIO S3.

    Args:
        bucket_name: Nom du bucket (par défaut: telco-raw depuis config)
        object_name: Chemin de l'objet dans le bucket

    Returns:
        DataFrame pandas avec les données extraites
    """
    print(f"\n📥 EXTRACT JSON depuis MinIO S3: {object_name}")

    config = load_config()
    client = get_minio_client()

    if bucket_name is None:
        bucket_name = config["minio"]["buckets"]["raw"]

    # Télécharger le JSON depuis MinIO
    response = client.get_object(bucket_name, object_name)
    json_data = response.read()
    response.close()
    response.release_conn()

    # Parser le JSON (détection automatique de l'encodage)
    for encoding in ("utf-8", "utf-16", "latin-1"):
        try:
            data = json.loads(json_data.decode(encoding))
            break
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    else:
        raise ValueError("Impossible de décoder le fichier JSON")

    # Extraire les résultats
    results = data.get("results", [])
    df = pd.json_normalize(results)

    # Renommer les colonnes metadata
    if "metadata.source" in df.columns:
        df.rename(columns={
            "metadata.source": "source",
            "metadata.timestamp": "source_timestamp"
        }, inplace=True)

    print(f"  ✅ {len(df)} lignes extraites depuis s3://{bucket_name}/{object_name}")
    print(f"  📊 Colonnes: {list(df.columns)}")
    print(f"  📡 Source: {data.get('api_version', 'N/A')}")
    print(f"  🎯 Churn distribution:")
    print(f"     {df['Churn'].value_counts().to_dict()}")

    return df
