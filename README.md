# Declarative Data Pipeline via Snowflake Dynamic Tables
An automated, infrastructure-minded data engineering pipeline deployed within the Snowflake Data Cloud utilizing a declarative architectural framework and Directed Acyclic Graphs (DAG).

## 🏗️ Pipeline Architecture

### 1. Data Ingestion & Staging
* **Semi-Structured JSON Unpacking**: Engineered staging environments (`STG_ORDERS_DT`) to parse raw JSON payloads, casting nested key-value pairs (`purchase:"prodid"`) directly into explicit relational datatypes.
* **Schema Standardization**: Constructed custom staging blocks to rename, type-cast, and sanitize raw records (`STG_CUSTOMERS_DT`) for downstream analytics.

### 2. Multi-Stage Pipeline Chaining (DAG)
* **Automated Dependency Discovery**: Chained multiple upstream staging tables into a centralized analytical fact model (`FCT_CUSTOMER_ORDERS_DT`) using relational left joins.
* **Declarative Scheduling**: Implemented the `TARGET_LAG=DOWNSTREAM` protocol, enabling Snowflake to automatically discover structural dependencies and build a visual Directed Acyclic Graph (DAG) for automated refresh scheduling.

### 3. Data Quality & Observability
* **Governance Enforcement**: Authored targeted data quality filters (`WHERE product_id IS NOT NULL`) directly into the dynamic table definitions to systematically eliminate processing anomalies.
* **Pipeline Observability**: Leveraged `information_schema.dynamic_table_refresh_history()` and Snowsight monitoring tools to audit query execution times, refresh health, and system performance.

## 🛠️ Technical Stack
* **Cloud Data Platform**: Snowflake Data Cloud
* **Infrastructure Provider**: Amazon Web Services (AWS)
* **Language**: SQL (Data Definition Language / Data Manipulation Language)
* **Pipeline Design**: Declarative Directed Acyclic Graphs (DAG)
