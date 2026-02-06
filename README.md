# 🚀 Telco Customer Churn - Data Engineering Pipeline

Pipeline ELT complet orchestré par **Apache Airflow** pour l'analyse du churn client dans le secteur télécom.

## 📐 Architecture

```
MinIO S3 (Bronze)  ──→  Extract  ──→  Transform  ──→  Load  ──→  Grafana
   CSV + JSON           Airflow        Pandas         MinIO       Dashboards
                         DAG          + Feature       + PostgreSQL
                                     Engineering
```

### Medallion Architecture (Data Lake)
| Couche | Bucket | Description |
|--------|--------|-------------|
| 🥉 Bronze | `telco-raw` | Données brutes (CSV + JSON) |
| 🥈 Silver | `telco-staging` | Données nettoyées (Parquet) |
| 🥇 Gold | `telco-curated` | Features, insights, dimensions (Parquet) |

### Star Schema (Data Warehouse)
| Table | Type | Description |
|-------|------|-------------|
| `dim_customer` | Dimension | Genre, senior, partner, dependents |
| `dim_service` | Dimension | Services souscrits |
| `dim_contract` | Dimension | Type contrat, paiement |
| `fact_churn` | Fait | Tenure, charges, churn |
| `churn_insights` | Insight | KPIs agrégés pour Grafana |
| `customer_features` | Feature | Features engineerées |

## 🛠️ Stack Technique

| Service | Rôle | Port |
|---------|------|------|
| **Apache Airflow** | Orchestration ELT | `8080` |
| **MinIO** | Data Lake (S3) | `9002` (API) / `9003` (Console) |
| **PostgreSQL 15** | Data Warehouse | `5433` |
| **pgAdmin 4** | Administration BDD | `5050` |
| **Grafana** | Visualisation | `3000` |

## 📁 Structure du Projet

```
telco-data-eng/
├── dags/
│   └── telco_churn_elt.py          # DAG Airflow (orchestration)
├── src/
│   ├── extract/
│   │   ├── extract_csv.py          # Extraction CSV depuis MinIO S3
│   │   └── extract_json.py         # Extraction JSON depuis MinIO S3
│   ├── transform/
│   │   ├── transform_data.py       # Nettoyage + dimensions + facts
│   │   └── create_insights.py      # Feature engineering + insights
│   ├── load/
│   │   ├── load_to_minio.py        # Chargement Data Lake (Parquet)
│   │   └── load_to_warehouse.py    # Chargement PostgreSQL
│   └── utils/
│       ├── minio_client.py         # Client MinIO (env vars)
│       └── db_client.py            # Client PostgreSQL (env vars)
├── docker/
│   ├── docker-compose.yml          # Tous les services
│   ├── init_db.sql                 # Schéma PostgreSQL
│   └── grafana/                    # Provisioning Grafana
├── requirements.txt
└── README.md
```

## 🚀 Démarrage Rapide

### 1. Lancer les services
```bash
cd docker
docker-compose up -d
```

### 2. Accéder aux services
| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | `admin` / `admin` |
| **MinIO** | http://localhost:9003 | `minioadmin` / `minioadmin` |
| **pgAdmin** | http://localhost:5050 | `admin@telco.com` / `admin` |
| **Grafana** | http://localhost:3000 | `admin` / `admin` |

### 3. Lancer le pipeline
1. Ouvrir **Airflow** → http://localhost:8080
2. Trouver le DAG `telco_churn_elt_pipeline`
3. Cliquer sur **Trigger DAG** ▶️

### 4. Voir les résultats
- **Grafana** → Dashboard "Telco Customer Churn - Insights"
- **MinIO** → Buckets Bronze / Silver / Gold

## 🔄 DAG Airflow

```
extract_csv  ──→  clean_csv  ──┐
                               ├──→ load_silver ──→ feature_engineering ──┬──→ load_gold
extract_json ──→  clean_json ──┘                                         └──→ load_warehouse
```

| Task | Description |
|------|-------------|
| `extract_csv` | Extraction CSV depuis MinIO S3 |
| `extract_json` | Extraction JSON depuis MinIO S3 |
| `clean_csv` | Nettoyage données CSV |
| `clean_json` | Nettoyage données JSON |
| `load_silver` | Chargement Parquet dans MinIO Silver |
| `feature_engineering` | 8 features + insights agrégés |
| `load_gold` | Chargement Parquet dans MinIO Gold |
| `load_warehouse` | Chargement PostgreSQL (staging + star schema) |

## 📊 Features Engineerées

| Feature | Description |
|---------|-------------|
| `tenure_group` | Tranche de tenure (0-12, 13-24, 25-48, 49-60, 61+) |
| `monthly_charges_group` | Tranche de charges mensuelles |
| `total_services` | Nombre de services souscrits |
| `has_streaming` | Client streaming (TV/Movies) |
| `has_security` | Client avec sécurité/backup |
| `is_high_value` | Charges > médiane |
| `avg_monthly_spend` | Dépense moyenne mensuelle |
| `contract_risk_score` | Score de risque contrat (1-3) |
