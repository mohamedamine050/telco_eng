-- ============================================
-- TELCO DATA WAREHOUSE - Schema Initialization
-- ============================================

-- Schema pour les données brutes (staging)
CREATE SCHEMA IF NOT EXISTS staging;

-- Schema pour les données transformées (warehouse)
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ============================================
-- STAGING TABLES (données brutes depuis le Data Lake)
-- ============================================

-- Table staging pour les données CSV
CREATE TABLE IF NOT EXISTS staging.telco_churn_raw (
    customerID VARCHAR(20) PRIMARY KEY,
    gender VARCHAR(10),
    SeniorCitizen INTEGER,
    Partner VARCHAR(5),
    Dependents VARCHAR(5),
    tenure INTEGER,
    PhoneService VARCHAR(5),
    MultipleLines VARCHAR(20),
    InternetService VARCHAR(20),
    OnlineSecurity VARCHAR(25),
    OnlineBackup VARCHAR(25),
    DeviceProtection VARCHAR(25),
    TechSupport VARCHAR(25),
    StreamingTV VARCHAR(25),
    StreamingMovies VARCHAR(25),
    Contract VARCHAR(20),
    PaperlessBilling VARCHAR(5),
    PaymentMethod VARCHAR(40),
    MonthlyCharges DECIMAL(10,2),
    TotalCharges VARCHAR(20),
    Churn VARCHAR(5),
    PromptInput TEXT,
    CustomerFeedback TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table staging pour les données JSON (synthétiques)
CREATE TABLE IF NOT EXISTS staging.telco_synthetic_raw (
    customerID VARCHAR(20) PRIMARY KEY,
    gender VARCHAR(10),
    SeniorCitizen VARCHAR(5),
    Partner VARCHAR(5),
    Dependents VARCHAR(5),
    tenure INTEGER,
    PhoneService VARCHAR(5),
    MultipleLines VARCHAR(20),
    InternetService VARCHAR(20),
    OnlineSecurity VARCHAR(25),
    OnlineBackup VARCHAR(25),
    DeviceProtection VARCHAR(25),
    TechSupport VARCHAR(25),
    StreamingTV VARCHAR(25),
    StreamingMovies VARCHAR(25),
    Contract VARCHAR(20),
    PaperlessBilling VARCHAR(5),
    PaymentMethod VARCHAR(40),
    MonthlyCharges DECIMAL(10,2),
    TotalCharges VARCHAR(20),
    Churn VARCHAR(5),
    CustomerFeedback TEXT,
    source VARCHAR(30),
    source_timestamp TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- WAREHOUSE TABLES (données transformées)
-- ============================================

-- Dimension Client
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) UNIQUE NOT NULL,
    gender VARCHAR(10),
    is_senior_citizen BOOLEAN,
    has_partner BOOLEAN,
    has_dependents BOOLEAN,
    data_source VARCHAR(20),  -- 'csv' ou 'json'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension Service
CREATE TABLE IF NOT EXISTS warehouse.dim_service (
    service_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    phone_service BOOLEAN,
    multiple_lines VARCHAR(20),
    internet_service VARCHAR(20),
    online_security VARCHAR(25),
    online_backup VARCHAR(25),
    device_protection VARCHAR(25),
    tech_support VARCHAR(25),
    streaming_tv VARCHAR(25),
    streaming_movies VARCHAR(25),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension Contrat
CREATE TABLE IF NOT EXISTS warehouse.dim_contract (
    contract_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    contract_type VARCHAR(20),
    paperless_billing BOOLEAN,
    payment_method VARCHAR(40),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table de Faits - Churn
CREATE TABLE IF NOT EXISTS warehouse.fact_churn (
    fact_key SERIAL PRIMARY KEY,
    customer_key INTEGER REFERENCES warehouse.dim_customer(customer_key),
    service_key INTEGER REFERENCES warehouse.dim_service(service_key),
    contract_key INTEGER REFERENCES warehouse.dim_contract(contract_key),
    tenure_months INTEGER,
    monthly_charges DECIMAL(10,2),
    total_charges DECIMAL(10,2),
    has_churned BOOLEAN,
    customer_feedback TEXT,
    data_source VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INSIGHTS TABLE (pour Grafana dashboards)
-- ============================================

CREATE TABLE IF NOT EXISTS warehouse.churn_insights (
    insight_key SERIAL PRIMARY KEY,
    insight_name VARCHAR(50) NOT NULL,
    dimension VARCHAR(30) NOT NULL,
    category VARCHAR(50) NOT NULL,
    total_customers INTEGER,
    churned_customers INTEGER,
    churn_rate DECIMAL(5,2),
    avg_monthly_charges DECIMAL(10,2),
    avg_tenure DECIMAL(5,1),
    avg_total_charges DECIMAL(10,2),
    data_source VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- ENGINEERED FEATURES TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS warehouse.customer_features (
    feature_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    tenure_group VARCHAR(20),
    monthly_charges_group VARCHAR(20),
    total_services INTEGER,
    has_streaming BOOLEAN,
    has_security BOOLEAN,
    is_high_value BOOLEAN,
    avg_monthly_spend DECIMAL(10,2),
    contract_risk_score INTEGER,
    has_churned BOOLEAN,
    data_source VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
