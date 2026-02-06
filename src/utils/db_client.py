"""
Database Client Utility
Gère la connexion et les opérations avec PostgreSQL (Data Warehouse).

Configuration via variables d'environnement:
    PG_HOST    : Hostname PostgreSQL  (default: localhost)
    PG_PORT    : Port PostgreSQL      (default: 5433)
    PG_DATABASE: Nom de la base       (default: telco_warehouse)
    PG_USER    : Utilisateur          (default: telco_admin)
    PG_PASSWORD: Mot de passe         (default: telco_pass)
"""

import os
import psycopg2
import pandas as pd


def get_db_connection():
    """Crée et retourne une connexion PostgreSQL."""
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5433")),
        database=os.getenv("PG_DATABASE", "telco_warehouse"),
        user=os.getenv("PG_USER", "telco_admin"),
        password=os.getenv("PG_PASSWORD", "telco_pass"),
    )
    return conn


def insert_dataframe(conn, table_name: str, df: pd.DataFrame):
    """Insère un DataFrame pandas dans une table PostgreSQL."""
    if df.empty:
        print(f"  ⚠️  DataFrame vide, rien à insérer dans {table_name}")
        return

    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    values = [tuple(row) for row in df.values]

    query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
    """

    cursor = conn.cursor()
    try:
        cursor.executemany(query, values)
        conn.commit()
        print(f"  ✅ {len(values)} lignes insérées dans {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"  ❌ Erreur insertion dans {table_name}: {e}")
        raise
    finally:
        cursor.close()
