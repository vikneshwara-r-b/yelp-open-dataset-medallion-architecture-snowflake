# Yelp Dataset Medallion Architecture with Snowflake Dynamic Tables

## 📋 Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup Guide](#setup-guide)
- [Data Processing Workflow](#data-processing-workflow)
- [Usage Examples](#usage-examples)
- [Features](#features)
- [Licensing & Compliance](#licensing--compliance)

## 🌟 Overview

This repository implements a **medallion architecture data pipeline** for the Yelp Open Dataset using **Snowflake Dynamic Tables**. The project demonstrates modern data engineering best practices by processing raw JSON data through Bronze, Silver, and Gold layers to create analytics-ready datasets.

### Key Highlights
- **Medallion Architecture**: Bronze (Raw) → Silver (Cleaned) → Gold (Business Logic)
- **Snowflake Dynamic Tables**: Automated refresh management and incremental processing
- **Dimensional Modeling**: Star schema design optimized for analytics
- **Advanced SQL Transformations**: JSON processing, array operations, and business logic
- **Production-Ready**: Complete audit trails, error handling, and performance optimization

## 🏗️ Architecture

### Data Flow Overview
```
Raw Yelp JSON Files
        ↓
Python Preprocessing (data_preparation.py)
        ↓
Snowflake Internal Stage
        ↓
Bronze Layer (Raw VARIANT storage)
        ↓
Silver Layer (Structured & Cleaned) [Dynamic Tables]
        ↓
Gold Layer (Dimensional Model) [Dynamic Tables]
        ↓
Business Intelligence & Analytics
```

### Layer Descriptions

#### 🥉 Bronze Layer
- **Purpose**: Raw data ingestion and storage
- **Tables**: `business_raw`, `reviews_raw`, `users_raw`
- **Data Type**: `VARIANT` columns for flexible JSON storage
- **Features**: 
  - Audit columns for data lineage
  - File metadata tracking
  - Error handling with `ON_ERROR = CONTINUE`

#### 🥈 Silver Layer (Dynamic Tables)
- **Purpose**: Data cleaning, validation, and structuring
- **Refresh**: `TARGET_LAG = 'DOWNSTREAM'` with incremental mode
- **Transformations**:
  - JSON field extraction and type conversion
  - Data quality enhancements
  - Calculated fields (review analytics, user metrics)
  - Array processing for categories and friends

#### 🥇 Gold Layer (Dynamic Tables)
- **Purpose**: Business logic and dimensional modeling
- **Design**: Star schema with facts and dimensions
- **Tables**:
  - `DIM_BUSINESS`: Business attributes with categorical intelligence
  - `DIM_USER`: User profiles with influence scoring
  - `DIM_DATE`: Standard date dimension
  - `FACT_REVIEW`: Review facts with engagement metrics

## 📋 Prerequisites

### Snowflake Requirements
- Snowflake account with `SYSADMIN` role access
- Warehouse creation privileges
- Database creation privileges
- Dynamic Tables feature enabled

### Local Environment
- Python 3.7+ with standard libraries
- Access to Yelp Open Dataset (see [Licensing](#licensing--compliance))
- Snowflake connectivity (SnowSQL, Python connector, or Snowsight)

## 📁 Project Structure

```
yelp-open-dataset-medallion-architecture-snowflake/
├── README.md                          # This comprehensive guide
├── LICENSE                            # MIT license for code
├── data_license.md                    # ⚠️ IMPORTANT: Yelp dataset licensing
├── data_preparation.py                # Python tool for JSONL processing
└── snowflake_scripts/
    ├── 0_db_and_schema_creation.sql   # Infrastructure setup
    ├── 1_bronze_layer_data_load.sql   # Raw data ingestion
    ├── 2_silver_layer_data_load.sql   # Dynamic Tables (Silver)
    ├── 3_gold_layer_data_load.sql     # Dynamic Tables (Gold)
    └── analytical_queries.sql         # Sample business intelligence queries
```

## 🚀 Setup Guide

### 1. Download Yelp Dataset
⚠️ **CRITICAL**: Read [`data_license.md`](./data_license.md) first!

1. Visit [Yelp Open Dataset](https://www.yelp.com/dataset)
2. Register and agree to Terms of Use
3. Download the dataset (academic use only)

### 2. Prepare Data Files (Optional)
If you need to split large files for easier processing:

```bash
# Split large JSONL files into smaller chunks
python data_preparation.py yelp_academic_dataset_business.json -o business_chunks -n 50000
python data_preparation.py yelp_academic_dataset_review.json -o review_chunks -n 100000
python data_preparation.py yelp_academic_dataset_user.json -o user_chunks -n 75000
```

### 3. Execute Snowflake Scripts

Run the SQL scripts in order:

```sql
-- 1. Setup infrastructure
-- Execute: snowflake_scripts/0_db_and_schema_creation.sql

-- 2. Upload data files to @source.landing_zone stage
-- Use Snowsight UI or SnowSQL PUT commands
PUT file://path/to/business/*.json @source.landing_zone/business;
PUT file://path/to/reviews/*.json @source.landing_zone/reviews;
PUT file://path/to/users/*.json @source.landing_zone/users;

-- 3. Load Bronze layer
-- Execute: snowflake_scripts/1_bronze_layer_data_load.sql

-- 4. Create Silver Dynamic Tables
-- Execute: snowflake_scripts/2_silver_layer_data_load.sql

-- 5. Create Gold Dynamic Tables
-- Execute: snowflake_scripts/3_gold_layer_data_load.sql
```

## 📊 Data Processing Workflow

### Bronze Layer Processing
```sql
-- Raw JSON storage with audit trails
CREATE TABLE bronze.business_raw (
    raw_data VARIANT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP,
    _stg_file_md5 TEXT,
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### Silver Layer Transformations
```sql
-- Structured data with enhanced analytics
CREATE DYNAMIC TABLE silver.business
TARGET_LAG = 'DOWNSTREAM'
AS
SELECT 
    RAW_DATA:business_id::VARCHAR AS BUSINESS_ID,
    RAW_DATA:stars::DECIMAL(2,1) AS STARS,
    STRTOK_TO_ARRAY(RAW_DATA:categories, ',') AS CATEGORIES,
    -- ... additional transformations
FROM bronze.business_raw;
```

### Gold Layer Business Logic
```sql
-- Dimensional modeling with KPIs
CREATE DYNAMIC TABLE gold.dim_business
AS
SELECT 
    business_id,
    business_name,
    CASE WHEN stars >= 4.5 THEN 'Excellent'
         WHEN stars >= 4.0 THEN 'Very Good'
         -- ... rating categories
    END AS star_rating_category,
    -- Business type intelligence
    (categories ILIKE '%restaurant%') AS is_restaurant
FROM silver.business;
```

## 💡 Usage Examples

### Business Intelligence Queries

```sql
-- Top performing businesses by category
SELECT 
    db.PRIMARY_CATEGORY,
    db.BUSINESS_NAME,
    db.CURRENT_STARS,
    db.CURRENT_REVIEW_COUNT
FROM YELP_DB.GOLD.DIM_BUSINESS db
WHERE db.IS_OPEN = TRUE
ORDER BY db.CURRENT_STARS DESC, db.CURRENT_REVIEW_COUNT DESC;

-- User engagement analysis
SELECT 
    du.USER_ENGAGEMENT_TIER,
    COUNT(*) AS user_count,
    AVG(du.USER_INFLUENCE_SCORE) AS avg_influence_score
FROM YELP_DB.GOLD.DIM_USER du
GROUP BY du.USER_ENGAGEMENT_TIER;

-- Review trends over time
SELECT 
    fr.REVIEW_YEAR,
    fr.REVIEW_QUARTER,
    COUNT(*) AS total_reviews,
    AVG(fr.STARS) AS avg_rating
FROM YELP_DB.GOLD.FACT_REVIEW fr
GROUP BY fr.REVIEW_YEAR, fr.REVIEW_QUARTER
ORDER BY fr.REVIEW_YEAR, fr.REVIEW_QUARTER;
```

See [`snowflake_scripts/analytical_queries.sql`](./snowflake_scripts/analytical_queries.sql) for more examples.

## ✨ Features

### Advanced SQL Capabilities
- **JSON Processing**: Complex VARIANT data extraction and transformation
- **Array Operations**: Category parsing, friend networks, elite year processing
- **String Analytics**: Review text analysis (length, word count, engagement scoring)
- **Temporal Intelligence**: Date dimensions and time-based aggregations

### Performance Optimization
- **Dynamic Tables**: Automated incremental refresh management
- **Warehouse Management**: Auto-suspend/resume for cost optimization
- **Compressed Storage**: Efficient JSON storage with audit trails
- **Scalable Architecture**: Designed for large dataset processing

### Data Quality & Governance
- **Audit Trails**: Complete data lineage from source to gold
- **Error Handling**: Graceful handling of malformed records
- **Data Validation**: NOT NULL constraints and data type enforcement
- **Metadata Tracking**: File-level processing information

## ⚖️ Licensing & Compliance

### 🚨 IMPORTANT: Data Licensing
**READ THIS FIRST**: [`data_license.md`](./data_license.md)

The Yelp Open Dataset has **strict licensing restrictions**:
- ❌ **NOT included** in this repository
- ❌ **Academic use ONLY**
- ❌ **Cannot be redistributed**
- ✅ Must be obtained directly from Yelp

### Code Licensing
- **This Repository**: MIT License (see [`LICENSE`](./LICENSE))
- **Yelp Dataset**: Governed by Yelp's Terms of Use

### Compliance Checklist
- [ ] Downloaded dataset from official Yelp source
- [ ] Agreed to Yelp Dataset Terms of Use
- [ ] Using data only for academic/educational purposes
- [ ] Not redistributing or sharing dataset files
- [ ] Proper citation in any published research

## 📈 Business Value

This architecture provides:
- **360° Business Intelligence**: Complete view of businesses, users, and reviews
- **Scalable Analytics**: Dimensional design supports complex analytical workloads
- **Real-time Insights**: Dynamic Tables provide fresh data with minimal latency
- **Cost Optimization**: Automated warehouse management and incremental processing
- **Enterprise-Ready**: Production-grade data governance and quality controls

---

**Ready to explore Yelp data insights? Start with the [Setup Guide](#setup-guide) and remember to review the [data licensing requirements](./data_license.md)!**
