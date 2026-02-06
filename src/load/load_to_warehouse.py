"""
Load to Data Warehouse (PostgreSQL)
Charge les données transformées dans le Data Warehouse.

Architecture:
    staging.*   → Données brutes (depuis le Data Lake)
    warehouse.* → Données transformées (modèle dimensionnel)
"""

import pandas as pd
from src.utils.db_client import get_db_connection, insert_dataframe


def load_csv_to_staging(df: pd.DataFrame):
    """
    Charge les données CSV dans la table de staging.
    """
    print("\n📤 LOAD TO WAREHOUSE (Staging): CSV → staging.telco_churn_raw")

    conn = get_db_connection()
    try:
        # Sélectionner les colonnes attendues par la table
        staging_cols = [
            "customerID", "gender", "SeniorCitizen", "Partner", "Dependents",
            "tenure", "PhoneService", "MultipleLines", "InternetService",
            "OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport",
            "StreamingTV", "StreamingMovies", "Contract", "PaperlessBilling",
            "PaymentMethod", "MonthlyCharges", "TotalCharges", "Churn",
            "PromptInput", "CustomerFeedback"
        ]
        df_staging = df[[c for c in staging_cols if c in df.columns]].copy()
        insert_dataframe(conn, "staging.telco_churn_raw", df_staging)
    finally:
        conn.close()


def load_json_to_staging(df: pd.DataFrame):
    """
    Charge les données JSON dans la table de staging.
    """
    print("\n📤 LOAD TO WAREHOUSE (Staging): JSON → staging.telco_synthetic_raw")

    conn = get_db_connection()
    try:
        staging_cols = [
            "customerID", "gender", "SeniorCitizen", "Partner", "Dependents",
            "tenure", "PhoneService", "MultipleLines", "InternetService",
            "OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport",
            "StreamingTV", "StreamingMovies", "Contract", "PaperlessBilling",
            "PaymentMethod", "MonthlyCharges", "TotalCharges", "Churn",
            "CustomerFeedback", "source", "source_timestamp"
        ]
        df_staging = df[[c for c in staging_cols if c in df.columns]].copy()
        insert_dataframe(conn, "staging.telco_synthetic_raw", df_staging)
    finally:
        conn.close()


def load_to_dimensions(df_customers: pd.DataFrame, df_services: pd.DataFrame, df_contracts: pd.DataFrame):
    """
    Charge les dimensions transformées dans le warehouse.
    """
    print("\n📤 LOAD TO WAREHOUSE: Dimensions")

    conn = get_db_connection()
    try:
        insert_dataframe(conn, "warehouse.dim_customer", df_customers)
        insert_dataframe(conn, "warehouse.dim_service", df_services)
        insert_dataframe(conn, "warehouse.dim_contract", df_contracts)
    finally:
        conn.close()


def load_to_facts(df_facts: pd.DataFrame):
    """
    Charge la table de faits dans le warehouse.
    """
    print("\n📤 LOAD TO WAREHOUSE: Facts")

    conn = get_db_connection()
    try:
        insert_dataframe(conn, "warehouse.fact_churn", df_facts)
    finally:
        conn.close()


def load_insights(df_insights: pd.DataFrame):
    """
    Charge les insights agrégés dans le warehouse (pour Grafana).
    """
    print("\n📤 LOAD TO WAREHOUSE: Insights → warehouse.churn_insights")

    conn = get_db_connection()
    try:
        insert_dataframe(conn, "warehouse.churn_insights", df_insights)
    finally:
        conn.close()


def load_features(df_features: pd.DataFrame):
    """
    Charge les features engineerées dans le warehouse.
    """
    print("\n📤 LOAD TO WAREHOUSE: Features → warehouse.customer_features")

    conn = get_db_connection()
    try:
        feature_cols = [
            "customer_id", "tenure_group", "monthly_charges_group",
            "total_services", "has_streaming", "has_security",
            "is_high_value", "avg_monthly_spend", "contract_risk_score",
            "has_churned", "data_source"
        ]
        df_feat = df_features[[c for c in feature_cols if c in df_features.columns]].copy()
        insert_dataframe(conn, "warehouse.customer_features", df_feat)
    finally:
        conn.close()
