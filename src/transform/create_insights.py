"""
Create Insights - Génération de features et insights pour Grafana

Insights créés:
    1. churn_by_contract      : Taux de churn par type de contrat
    2. churn_by_internet      : Taux de churn par type d'internet
    3. churn_by_payment       : Taux de churn par méthode de paiement
    4. churn_by_tenure_group  : Taux de churn par tranche de tenure
    5. churn_by_gender        : Taux de churn par genre
    6. revenue_by_contract    : Revenu moyen par type de contrat
    7. service_adoption       : Taux d'adoption des services
    8. monthly_charges_dist   : Distribution des charges mensuelles
    9. high_risk_customers    : Clients à haut risque de churn
    10. customer_lifetime_value: CLV par segment
"""

import pandas as pd
import numpy as np


def add_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ajoute des features engineerées au DataFrame nettoyé.

    Nouvelles features:
        - tenure_group         : Tranche de tenure (0-12, 13-24, 25-48, 49-60, 61+)
        - monthly_charges_group: Tranche de charges mensuelles
        - total_services       : Nombre total de services souscrits
        - has_streaming        : Client a un service streaming
        - has_security         : Client a sécurité/backup/protection
        - is_high_value        : Client à forte valeur (charges > médiane)
        - avg_monthly_spend    : Dépense moyenne par mois (TotalCharges/tenure)
        - contract_risk_score  : Score de risque basé sur le contrat
    """
    print("\n🔄 TRANSFORM: Ajout de features engineerées")

    df_feat = df.copy()

    # 1. Tenure Group
    df_feat["tenure_group"] = pd.cut(
        df_feat["tenure"],
        bins=[0, 12, 24, 48, 60, 100],
        labels=["0-12 mois", "13-24 mois", "25-48 mois", "49-60 mois", "61+ mois"],
        include_lowest=True
    ).astype(str)

    # 2. Monthly Charges Group
    df_feat["monthly_charges_group"] = pd.cut(
        df_feat["MonthlyCharges"],
        bins=[0, 30, 50, 70, 90, 200],
        labels=["0-30$", "31-50$", "51-70$", "71-90$", "91+$"],
        include_lowest=True
    ).astype(str)

    # 3. Total Services Count
    service_cols = ["PhoneService", "MultipleLines", "InternetService",
                    "OnlineSecurity", "OnlineBackup", "DeviceProtection",
                    "TechSupport", "StreamingTV", "StreamingMovies"]

    df_feat["total_services"] = 0
    for col in service_cols:
        if col in df_feat.columns:
            df_feat["total_services"] += (
                df_feat[col].apply(lambda x: 1 if str(x) in ["Yes", "DSL", "Fiber optic"] else 0)
            )

    # 4. Has Streaming
    df_feat["has_streaming"] = (
        (df_feat.get("StreamingTV", "No") == "Yes") |
        (df_feat.get("StreamingMovies", "No") == "Yes")
    )

    # 5. Has Security Bundle
    df_feat["has_security"] = (
        (df_feat.get("OnlineSecurity", "No") == "Yes") |
        (df_feat.get("OnlineBackup", "No") == "Yes") |
        (df_feat.get("DeviceProtection", "No") == "Yes")
    )

    # 6. Is High Value
    median_charges = df_feat["MonthlyCharges"].median()
    df_feat["is_high_value"] = df_feat["MonthlyCharges"] > median_charges

    # 7. Average Monthly Spend
    df_feat["avg_monthly_spend"] = np.where(
        df_feat["tenure"] > 0,
        df_feat["TotalCharges"] / df_feat["tenure"],
        df_feat["MonthlyCharges"]
    )

    # 8. Contract Risk Score (Month-to-month = high risk)
    contract_risk = {
        "Month-to-month": 3,
        "One year": 2,
        "Two year": 1
    }
    df_feat["contract_risk_score"] = df_feat["Contract"].map(contract_risk).fillna(2)

    print(f"  ✅ {len(df_feat)} lignes avec {len(df_feat.columns)} features")
    print(f"  📊 Nouvelles features: tenure_group, monthly_charges_group, total_services,")
    print(f"     has_streaming, has_security, is_high_value, avg_monthly_spend, contract_risk_score")

    return df_feat


def create_churn_insights(df: pd.DataFrame, source: str = "csv") -> pd.DataFrame:
    """
    Crée une table d'insights agrégés pour Grafana.

    Chaque ligne = un insight avec:
        - insight_name: Nom de l'insight
        - dimension: La dimension analysée
        - category: La catégorie (ex: "Month-to-month")
        - total_customers: Nombre total de clients
        - churned_customers: Nombre de clients partis
        - churn_rate: Taux de churn (%)
        - avg_monthly_charges: Charges mensuelles moyennes
        - avg_tenure: Tenure moyenne
        - avg_total_charges: Charges totales moyennes
        - data_source: Source des données
    """
    print(f"\n🔄 TRANSFORM: Création des insights ({source})")

    insights = []

    # Convertir Churn en booléen
    df_work = df.copy()
    if df_work["Churn"].dtype == object:
        df_work["has_churned"] = df_work["Churn"].map({"Yes": True, "No": False})
    else:
        df_work["has_churned"] = df_work["Churn"].astype(bool)

    # --- Insight 1: Churn by Contract ---
    for contract, group in df_work.groupby("Contract"):
        insights.append({
            "insight_name": "churn_by_contract",
            "dimension": "Contract",
            "category": str(contract),
            "total_customers": len(group),
            "churned_customers": int(group["has_churned"].sum()),
            "churn_rate": round(group["has_churned"].mean() * 100, 2),
            "avg_monthly_charges": round(group["MonthlyCharges"].mean(), 2),
            "avg_tenure": round(group["tenure"].mean(), 1),
            "avg_total_charges": round(group["TotalCharges"].mean(), 2),
            "data_source": source,
        })

    # --- Insight 2: Churn by Internet Service ---
    for internet, group in df_work.groupby("InternetService"):
        insights.append({
            "insight_name": "churn_by_internet",
            "dimension": "InternetService",
            "category": str(internet),
            "total_customers": len(group),
            "churned_customers": int(group["has_churned"].sum()),
            "churn_rate": round(group["has_churned"].mean() * 100, 2),
            "avg_monthly_charges": round(group["MonthlyCharges"].mean(), 2),
            "avg_tenure": round(group["tenure"].mean(), 1),
            "avg_total_charges": round(group["TotalCharges"].mean(), 2),
            "data_source": source,
        })

    # --- Insight 3: Churn by Payment Method ---
    for payment, group in df_work.groupby("PaymentMethod"):
        insights.append({
            "insight_name": "churn_by_payment",
            "dimension": "PaymentMethod",
            "category": str(payment),
            "total_customers": len(group),
            "churned_customers": int(group["has_churned"].sum()),
            "churn_rate": round(group["has_churned"].mean() * 100, 2),
            "avg_monthly_charges": round(group["MonthlyCharges"].mean(), 2),
            "avg_tenure": round(group["tenure"].mean(), 1),
            "avg_total_charges": round(group["TotalCharges"].mean(), 2),
            "data_source": source,
        })

    # --- Insight 4: Churn by Tenure Group ---
    if "tenure_group" in df_work.columns:
        for tgroup, group in df_work.groupby("tenure_group"):
            insights.append({
                "insight_name": "churn_by_tenure_group",
                "dimension": "tenure_group",
                "category": str(tgroup),
                "total_customers": len(group),
                "churned_customers": int(group["has_churned"].sum()),
                "churn_rate": round(group["has_churned"].mean() * 100, 2),
                "avg_monthly_charges": round(group["MonthlyCharges"].mean(), 2),
                "avg_tenure": round(group["tenure"].mean(), 1),
                "avg_total_charges": round(group["TotalCharges"].mean(), 2),
                "data_source": source,
            })

    # --- Insight 5: Churn by Gender ---
    for gender, group in df_work.groupby("gender"):
        insights.append({
            "insight_name": "churn_by_gender",
            "dimension": "gender",
            "category": str(gender),
            "total_customers": len(group),
            "churned_customers": int(group["has_churned"].sum()),
            "churn_rate": round(group["has_churned"].mean() * 100, 2),
            "avg_monthly_charges": round(group["MonthlyCharges"].mean(), 2),
            "avg_tenure": round(group["tenure"].mean(), 1),
            "avg_total_charges": round(group["TotalCharges"].mean(), 2),
            "data_source": source,
        })

    # --- Insight 6: Churn by Senior Citizen ---
    for senior, group in df_work.groupby("SeniorCitizen"):
        insights.append({
            "insight_name": "churn_by_senior",
            "dimension": "SeniorCitizen",
            "category": "Senior" if senior == 1 else "Non-Senior",
            "total_customers": len(group),
            "churned_customers": int(group["has_churned"].sum()),
            "churn_rate": round(group["has_churned"].mean() * 100, 2),
            "avg_monthly_charges": round(group["MonthlyCharges"].mean(), 2),
            "avg_tenure": round(group["tenure"].mean(), 1),
            "avg_total_charges": round(group["TotalCharges"].mean(), 2),
            "data_source": source,
        })

    # --- Insight 7: Churn by Monthly Charges Group ---
    if "monthly_charges_group" in df_work.columns:
        for mgroup, group in df_work.groupby("monthly_charges_group"):
            insights.append({
                "insight_name": "churn_by_charges_group",
                "dimension": "monthly_charges_group",
                "category": str(mgroup),
                "total_customers": len(group),
                "churned_customers": int(group["has_churned"].sum()),
                "churn_rate": round(group["has_churned"].mean() * 100, 2),
                "avg_monthly_charges": round(group["MonthlyCharges"].mean(), 2),
                "avg_tenure": round(group["tenure"].mean(), 1),
                "avg_total_charges": round(group["TotalCharges"].mean(), 2),
                "data_source": source,
            })

    # --- Insight 8: Overall Summary ---
    insights.append({
        "insight_name": "overall_summary",
        "dimension": "ALL",
        "category": "Total",
        "total_customers": len(df_work),
        "churned_customers": int(df_work["has_churned"].sum()),
        "churn_rate": round(df_work["has_churned"].mean() * 100, 2),
        "avg_monthly_charges": round(df_work["MonthlyCharges"].mean(), 2),
        "avg_tenure": round(df_work["tenure"].mean(), 1),
        "avg_total_charges": round(df_work["TotalCharges"].mean(), 2),
        "data_source": source,
    })

    df_insights = pd.DataFrame(insights)

    print(f"  ✅ {len(df_insights)} insights créés")
    print(f"  📊 Types d'insights: {df_insights['insight_name'].unique().tolist()}")

    return df_insights
