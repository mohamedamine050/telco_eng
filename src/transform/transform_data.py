"""
Transform Data - Transformations et nettoyage des données Telco Churn

Transformations appliquées:
    1. Nettoyage des types de données
    2. Gestion des valeurs manquantes
    3. Standardisation des colonnes
    4. Création des dimensions (Customer, Service, Contract)
    5. Création de la table de faits (Fact Churn)
"""

import pandas as pd
import numpy as np


def clean_data(df: pd.DataFrame, source: str = "csv") -> pd.DataFrame:
    """
    Nettoie et standardise les données brutes.

    Transformations:
        - Convertir TotalCharges en numérique (valeurs vides → NaN)
        - Standardiser SeniorCitizen en 0/1
        - Supprimer les doublons
        - Gérer les valeurs manquantes
    """
    print(f"\n🔄 TRANSFORM: Nettoyage des données ({source})")

    df_clean = df.copy()

    # 1. Nettoyer TotalCharges (gestion des valeurs corrompues "$xx$xx...")
    def _parse_total_charges(val):
        """Extrait la première valeur numérique d'un champ TotalCharges potentiellement corrompu."""
        if pd.isna(val) or val == "" or val == " ":
            return np.nan
        s = str(val).strip()
        # Si c'est déjà un nombre propre
        try:
            return float(s)
        except ValueError:
            pass
        # Extraire le premier montant d'une chaîne "$xx.xx$xx.xx..."
        s = s.lstrip("$")
        if "$" in s:
            s = s.split("$")[0]
        try:
            return float(s)
        except ValueError:
            return np.nan

    df_clean["TotalCharges"] = df_clean["TotalCharges"].apply(_parse_total_charges)

    # 2. Nettoyer MonthlyCharges (gestion des valeurs avec "$")
    df_clean["MonthlyCharges"] = df_clean["MonthlyCharges"].apply(_parse_total_charges)

    # 3. Convertir tenure en numérique
    df_clean["tenure"] = pd.to_numeric(df_clean["tenure"], errors="coerce").fillna(0).astype(int)

    # 4. Remplir les TotalCharges manquants avec MonthlyCharges * tenure
    mask = df_clean["TotalCharges"].isna()
    df_clean.loc[mask, "TotalCharges"] = (
        df_clean.loc[mask, "MonthlyCharges"] * df_clean.loc[mask, "tenure"]
    )

    # 3. Standardiser SeniorCitizen (gestion Yes/No et 0/1)
    if df_clean["SeniorCitizen"].dtype == object:
        df_clean["SeniorCitizen"] = df_clean["SeniorCitizen"].map(
            {"Yes": 1, "No": 0, "1": 1, "0": 0}
        ).fillna(0).astype(int)
    else:
        df_clean["SeniorCitizen"] = df_clean["SeniorCitizen"].astype(int)

    # 4. Supprimer les doublons
    initial_len = len(df_clean)
    df_clean.drop_duplicates(subset=["customerID"], keep="first", inplace=True)
    if len(df_clean) < initial_len:
        print(f"  🗑️  {initial_len - len(df_clean)} doublons supprimés")

    # 6. Standardiser les valeurs Yes/No (gestion booléens, NaN, variations de casse)
    for col in ["Partner", "Dependents", "PhoneService", "PaperlessBilling", "Churn"]:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(
                lambda x: "Yes" if str(x).strip().lower() in ["yes", "true", "1"]
                else ("No" if str(x).strip().lower() in ["no", "false", "0"]
                      else str(x).strip().capitalize() if pd.notna(x) else "No")
            )

    # 6. Standardiser le genre
    if "gender" in df_clean.columns:
        df_clean["gender"] = df_clean["gender"].str.strip().str.capitalize()

    print(f"  ✅ {len(df_clean)} lignes après nettoyage")
    print(f"  📊 Valeurs manquantes restantes: {df_clean.isnull().sum().sum()}")

    return df_clean


def create_dim_customer(df: pd.DataFrame, source: str = "csv") -> pd.DataFrame:
    """
    Crée la dimension Client à partir des données nettoyées.

    Colonnes:
        customer_id, gender, is_senior_citizen, has_partner, has_dependents, data_source
    """
    print(f"\n🔄 TRANSFORM: Création dim_customer ({source})")

    dim_customer = pd.DataFrame({
        "customer_id": df["customerID"],
        "gender": df["gender"],
        "is_senior_citizen": df["SeniorCitizen"].astype(bool),
        "has_partner": df["Partner"].map({"Yes": True, "No": False}),
        "has_dependents": df["Dependents"].map({"Yes": True, "No": False}),
        "data_source": source,
    })

    print(f"  ✅ {len(dim_customer)} clients créés")
    return dim_customer


def create_dim_service(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée la dimension Service.

    Colonnes:
        customer_id, phone_service, multiple_lines, internet_service,
        online_security, online_backup, device_protection, tech_support,
        streaming_tv, streaming_movies
    """
    print("\n🔄 TRANSFORM: Création dim_service")

    dim_service = pd.DataFrame({
        "customer_id": df["customerID"],
        "phone_service": df["PhoneService"].map({"Yes": True, "No": False}),
        "multiple_lines": df["MultipleLines"],
        "internet_service": df["InternetService"],
        "online_security": df["OnlineSecurity"],
        "online_backup": df["OnlineBackup"],
        "device_protection": df["DeviceProtection"],
        "tech_support": df["TechSupport"],
        "streaming_tv": df["StreamingTV"],
        "streaming_movies": df["StreamingMovies"],
    })

    print(f"  ✅ {len(dim_service)} services créés")
    return dim_service


def create_dim_contract(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée la dimension Contrat.

    Colonnes:
        customer_id, contract_type, paperless_billing, payment_method
    """
    print("\n🔄 TRANSFORM: Création dim_contract")

    dim_contract = pd.DataFrame({
        "customer_id": df["customerID"],
        "contract_type": df["Contract"],
        "paperless_billing": df["PaperlessBilling"].map({"Yes": True, "No": False}),
        "payment_method": df["PaymentMethod"],
    })

    print(f"  ✅ {len(dim_contract)} contrats créés")
    return dim_contract


def create_fact_churn(df: pd.DataFrame, source: str = "csv") -> pd.DataFrame:
    """
    Crée la table de faits Churn.

    Colonnes:
        tenure_months, monthly_charges, total_charges,
        has_churned, customer_feedback, data_source
    """
    print(f"\n🔄 TRANSFORM: Création fact_churn ({source})")

    fact_churn = pd.DataFrame({
        "tenure_months": df["tenure"],
        "monthly_charges": df["MonthlyCharges"],
        "total_charges": df["TotalCharges"],
        "has_churned": df["Churn"].map({"Yes": True, "No": False}),
        "customer_feedback": df.get("CustomerFeedback", pd.Series(dtype=str)),
        "data_source": source,
    })

    churn_rate = fact_churn["has_churned"].mean() * 100
    avg_monthly = fact_churn["monthly_charges"].mean()
    avg_tenure = fact_churn["tenure_months"].mean()

    print(f"  ✅ {len(fact_churn)} faits créés")
    print(f"  📊 Taux de Churn: {churn_rate:.1f}%")
    print(f"  💰 Charges mensuelles moyennes: ${avg_monthly:.2f}")
    print(f"  📅 Tenure moyenne: {avg_tenure:.1f} mois")

    return fact_churn
