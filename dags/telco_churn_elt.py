"""
🚀 TELCO CHURN ELT - DAG Airflow
==================================

Pipeline ELT orchestré par Apache Airflow.

Architecture:
    MinIO S3 (Bronze) → Extract → Transform → Load (Silver/Gold + PostgreSQL) → Grafana

Tasks:
    1. extract_csv         : Extraction CSV depuis MinIO S3
    2. extract_json        : Extraction JSON depuis MinIO S3
    3. clean_csv           : Nettoyage des données CSV
    4. clean_json          : Nettoyage des données JSON
    5. load_silver         : Chargement Silver (Parquet) dans MinIO
    6. feature_engineering : Feature engineering + insights
    7. load_gold           : Chargement Gold (Parquet) dans MinIO
    8. load_warehouse      : Chargement dans PostgreSQL (staging + dimensions + facts + insights + features)
"""

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ajouter src au path pour les imports
sys.path.insert(0, "/opt/airflow")


# ============================================
# FONCTIONS DES TASKS
# ============================================

def task_extract_csv(**context):
    """Étape 1a: Extraction CSV depuis MinIO S3."""
    from src.extract.extract_csv import extract_csv_from_minio

    df = extract_csv_from_minio()
    # Stocker en XCom via fichier temporaire parquet
    path = "/tmp/df_csv_raw.parquet"
    df.to_parquet(path, index=False)
    return path


def task_extract_json(**context):
    """Étape 1b: Extraction JSON depuis MinIO S3."""
    from src.extract.extract_json import extract_json_from_minio

    df = extract_json_from_minio()
    # Standardiser les types mixtes avant sérialisation Parquet
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str)
    path = "/tmp/df_json_raw.parquet"
    df.to_parquet(path, index=False)
    return path


def task_clean_csv(**context):
    """Étape 2a: Nettoyage des données CSV."""
    import pandas as pd
    from src.transform.transform_data import clean_data

    ti = context["ti"]
    raw_path = ti.xcom_pull(task_ids="extract_csv")
    df = pd.read_parquet(raw_path)
    df_clean = clean_data(df, source="csv")

    path = "/tmp/df_csv_clean.parquet"
    df_clean.to_parquet(path, index=False)
    return path


def task_clean_json(**context):
    """Étape 2b: Nettoyage des données JSON."""
    import pandas as pd
    from src.transform.transform_data import clean_data

    ti = context["ti"]
    raw_path = ti.xcom_pull(task_ids="extract_json")
    df = pd.read_parquet(raw_path)
    df_clean = clean_data(df, source="json")

    path = "/tmp/df_json_clean.parquet"
    df_clean.to_parquet(path, index=False)
    return path


def task_load_silver(**context):
    """Étape 3: Chargement des données nettoyées dans MinIO Silver."""
    import pandas as pd
    from src.load.load_to_minio import load_df_to_minio

    ti = context["ti"]
    csv_path = ti.xcom_pull(task_ids="clean_csv")
    json_path = ti.xcom_pull(task_ids="clean_json")

    df_csv = pd.read_parquet(csv_path)
    df_json = pd.read_parquet(json_path)

    load_df_to_minio(df_csv, "staging", "csv/telco_churn_clean.parquet")
    load_df_to_minio(df_json, "staging", "json/telco_synthetic_clean.parquet")


def task_feature_engineering(**context):
    """Étape 4: Feature engineering + création d'insights."""
    import pandas as pd
    from src.transform.create_insights import add_engineered_features, create_churn_insights
    from src.transform.transform_data import (
        create_dim_customer, create_dim_service,
        create_dim_contract, create_fact_churn,
    )

    ti = context["ti"]
    csv_path = ti.xcom_pull(task_ids="clean_csv")
    json_path = ti.xcom_pull(task_ids="clean_json")

    df_csv = pd.read_parquet(csv_path)
    df_json = pd.read_parquet(json_path)

    # Feature Engineering
    df_csv_feat = add_engineered_features(df_csv)
    df_json_feat = add_engineered_features(df_json)

    # Insights
    insights_csv = create_churn_insights(df_csv_feat, source="csv")
    insights_json = create_churn_insights(df_json_feat, source="json")

    # Dimensions & Facts (CSV)
    dim_customer_csv = create_dim_customer(df_csv, source="csv")
    dim_service_csv = create_dim_service(df_csv)
    dim_contract_csv = create_dim_contract(df_csv)
    fact_churn_csv = create_fact_churn(df_csv, source="csv")

    # Dimensions & Facts (JSON)
    dim_customer_json = create_dim_customer(df_json, source="json")
    fact_churn_json = create_fact_churn(df_json, source="json")

    # Sauvegarder tous les DataFrames intermédiaires
    df_csv_feat.to_parquet("/tmp/df_csv_feat.parquet", index=False)
    df_json_feat.to_parquet("/tmp/df_json_feat.parquet", index=False)
    insights_csv.to_parquet("/tmp/insights_csv.parquet", index=False)
    insights_json.to_parquet("/tmp/insights_json.parquet", index=False)
    dim_customer_csv.to_parquet("/tmp/dim_customer_csv.parquet", index=False)
    dim_customer_json.to_parquet("/tmp/dim_customer_json.parquet", index=False)
    dim_service_csv.to_parquet("/tmp/dim_service_csv.parquet", index=False)
    dim_contract_csv.to_parquet("/tmp/dim_contract_csv.parquet", index=False)
    fact_churn_csv.to_parquet("/tmp/fact_churn_csv.parquet", index=False)
    fact_churn_json.to_parquet("/tmp/fact_churn_json.parquet", index=False)

    return "features_done"


def task_load_gold(**context):
    """Étape 5: Chargement Gold (Parquet) dans MinIO curated."""
    import pandas as pd
    from src.load.load_to_minio import load_df_to_minio

    files_to_load = {
        "features/customer_features_csv.parquet": "/tmp/df_csv_feat.parquet",
        "features/customer_features_json.parquet": "/tmp/df_json_feat.parquet",
        "insights/churn_insights_csv.parquet": "/tmp/insights_csv.parquet",
        "insights/churn_insights_json.parquet": "/tmp/insights_json.parquet",
        "dimensions/dim_customer_csv.parquet": "/tmp/dim_customer_csv.parquet",
        "dimensions/dim_customer_json.parquet": "/tmp/dim_customer_json.parquet",
        "facts/fact_churn_csv.parquet": "/tmp/fact_churn_csv.parquet",
        "facts/fact_churn_json.parquet": "/tmp/fact_churn_json.parquet",
    }

    for obj_name, local_path in files_to_load.items():
        df = pd.read_parquet(local_path)
        load_df_to_minio(df, "curated", obj_name)


def task_load_warehouse(**context):
    """Étape 6: Chargement dans PostgreSQL Data Warehouse."""
    import pandas as pd
    from src.load.load_to_warehouse import (
        load_csv_to_staging, load_json_to_staging,
        load_to_dimensions, load_to_facts,
        load_insights, load_features,
    )

    ti = context["ti"]
    csv_path = ti.xcom_pull(task_ids="clean_csv")
    json_path = ti.xcom_pull(task_ids="clean_json")

    df_csv = pd.read_parquet(csv_path)
    df_json = pd.read_parquet(json_path)

    # Staging
    load_csv_to_staging(df_csv)
    load_json_to_staging(df_json)

    # Dimensions & Facts
    dim_customer_csv = pd.read_parquet("/tmp/dim_customer_csv.parquet")
    dim_service_csv = pd.read_parquet("/tmp/dim_service_csv.parquet")
    dim_contract_csv = pd.read_parquet("/tmp/dim_contract_csv.parquet")
    fact_churn_csv = pd.read_parquet("/tmp/fact_churn_csv.parquet")

    load_to_dimensions(dim_customer_csv, dim_service_csv, dim_contract_csv)
    load_to_facts(fact_churn_csv)

    # Insights
    insights_csv = pd.read_parquet("/tmp/insights_csv.parquet")
    insights_json = pd.read_parquet("/tmp/insights_json.parquet")
    load_insights(insights_csv)
    load_insights(insights_json)

    # Features
    df_csv_feat = pd.read_parquet("/tmp/df_csv_feat.parquet")
    df_json_feat = pd.read_parquet("/tmp/df_json_feat.parquet")

    df_csv_feat_wh = df_csv_feat.copy()
    df_csv_feat_wh["customer_id"] = df_csv_feat_wh["customerID"]
    df_csv_feat_wh["has_churned"] = df_csv_feat_wh["Churn"].map({"Yes": True, "No": False}).fillna(False).astype(bool)
    df_csv_feat_wh["data_source"] = "csv"
    load_features(df_csv_feat_wh)

    df_json_feat_wh = df_json_feat.copy()
    df_json_feat_wh["customer_id"] = df_json_feat_wh["customerID"]
    df_json_feat_wh["has_churned"] = df_json_feat_wh["Churn"].map({"Yes": True, "No": False}).fillna(False).astype(bool)
    df_json_feat_wh["data_source"] = "json"
    load_features(df_json_feat_wh)

    print("\n✅ PIPELINE ELT TERMINÉ AVEC SUCCÈS!")


# ============================================
# DAG DEFINITION
# ============================================

default_args = {
    "owner": "telco-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="telco_churn_elt_pipeline",
    default_args=default_args,
    description="Pipeline ELT Telco Churn: MinIO S3 → Transform → PostgreSQL → Grafana",
    schedule_interval=None,  # Déclenché manuellement
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["telco", "elt", "churn", "data-engineering"],
) as dag:

    # --- Tasks ---
    t_extract_csv = PythonOperator(
        task_id="extract_csv",
        python_callable=task_extract_csv,
    )

    t_extract_json = PythonOperator(
        task_id="extract_json",
        python_callable=task_extract_json,
    )

    t_clean_csv = PythonOperator(
        task_id="clean_csv",
        python_callable=task_clean_csv,
    )

    t_clean_json = PythonOperator(
        task_id="clean_json",
        python_callable=task_clean_json,
    )

    t_load_silver = PythonOperator(
        task_id="load_silver",
        python_callable=task_load_silver,
    )

    t_feature_eng = PythonOperator(
        task_id="feature_engineering",
        python_callable=task_feature_engineering,
    )

    t_load_gold = PythonOperator(
        task_id="load_gold",
        python_callable=task_load_gold,
    )

    t_load_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=task_load_warehouse,
    )

    # --- Dépendances (DAG Graph) ---
    #
    #   extract_csv  ──→  clean_csv  ──┐
    #                                  ├──→ load_silver ──→ feature_engineering ──┬──→ load_gold
    #   extract_json ──→  clean_json ──┘                                         └──→ load_warehouse
    #

    t_extract_csv >> t_clean_csv
    t_extract_json >> t_clean_json
    [t_clean_csv, t_clean_json] >> t_load_silver >> t_feature_eng
    t_feature_eng >> [t_load_gold, t_load_warehouse]
