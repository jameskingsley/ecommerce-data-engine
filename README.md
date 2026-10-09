# E-Commerce Data Engine

An end-to-end, production-ready ELT data pipeline designed to ingest public e-commerce API data, run transformation logic with local unit and integration tests, orchestrate workflows with **Dagster**, land aggregated models in **Google BigQuery**, and visualize key metrics in **Looker Studio**.


The pipeline follows a modular Medallion-style architecture:

1. **Bronze Layer (`raw_orders_bronze`):** Ingests and flattens nested cart and product JSON payloads from the public DummyJSON API into daily-partitioned tabular records in BigQuery.
2. **Silver Layer (`daily_sales_summary`):** Aggregates raw transactional order events into daily business KPIs (Gross Revenue, Total Orders, Average Order Value, Unique Customers, Units Sold).
3. **Local Testing & Quality Control:** Uses **DuckDB** and **`pytest`** to test extraction schema shapes and transformation SQL logic in-memory before materializing upstream to cloud environments.

---

## 🧰 Tech Stack

| Component | Technology | Description |
| :--- | :--- | :--- |
| **Orchestration** | [Dagster](https://dagster.io/) | Asset-based data orchestration & asset materialization. |
| **Data Extraction** | Python (`requests`, `pandas`) | HTTP extraction, nested payload flattening, and typing. |
| **Data Warehouse** | [Google BigQuery](https://cloud.google.com/bigquery) | Partitioned cloud data warehouse storage & analytics execution. |
| **Local Testing** | [DuckDB](https://duckdb.org/) + `pytest` | In-memory relational engine for transformation test suite execution.

---