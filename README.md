# **🛡️ Cloud Bill Hunter (FinOps Intelligence Platform)**

![Python](https://img.shields.io/badge/Python-3.10-blue?style=for-the-badge&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Microservices-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-High_Performance-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)

**An event-driven FinOps observability engine designed to detect, isolate, and quantify "Zombie Infrastructure" (idle cloud assets) using a Medallion Architecture.**

## **1️⃣ Executive Summary**

**Cloud Bill Hunter** is a specialized data platform built to solve the "Cloud Waste" problem. It ingests raw AWS Cost & Usage Reports (CUR), transforms them into actionable insights using an embedded OLAP engine (**DuckDB**), and serves data via a decoupled API and Dashboard.

Unlike static reporting tools, the platform operates as an **event-driven cost intelligence engine**, reacting to billing data arrival and producing near-real-time waste insights with deterministic guarantees: it watches for data arrival, triggers an automated ETL pipeline (Bronze $\\to$ Silver $\\to$ Gold), and pushes updates to the warehouse in real-time, enabling sub-second anomaly detection on billing datasets up to 10GB.

## **2️⃣ Problem Statement & Constraints**

### **The "Silent Waste" Problem**

As cloud infrastructure scales, decentralized teams often provision resources (EC2 instances, EBS volumes, Load Balancers) that eventually become orphaned or idle.

* **The Cost:** Industry averages suggest **20-30%** of cloud spend is waste.  
* **The Technical Challenge:** Detecting this requires joining **Billing Data** (Finance) with **Usage Metrics** (Observability), which typically live in data silos with different schemas and granularity.

### **System Constraints & SLOs**

* **Latency:** Analysis must complete within \< 5 seconds of file ingestion.  
* **Reliability:** Pipeline must be idempotent; re-uploading the same bill should not duplicate costs.  
* **Portability:** Must run entirely in Docker (Air-gapped friendly) without external SaaS dependencies.

## **3️⃣ High-Level Architecture**

The system follows a **Microservices Pattern** orchestrated via Docker Compose.

``` mermaid
graph LR  
    subgraph External\_World  
        User\[User / CI Pipeline\]  
        S3\[S3 Bucket / Landing Zone\]  
    end

    subgraph Cloud\_Bill\_Hunter\_Platform  
        Watcher\[Watchdog Service\]  
        API\[FastAPI Gateway\]  
        Engine\[Compute Engine\]  
        DB\[(DuckDB Warehouse)\]  
        Dash\[Streamlit Dashboard\]  
    end

    User \-- POST /upload \--\> API  
    S3 \-- File Event \--\> Watcher

    API \-- Trigger \--\> Engine  
    Watcher \-- Trigger \--\> Engine

    Engine \-- ETL Process \--\> DB

    DB \-- Read-Only Queries \--\> Dash  
    DB \-- Read-Only Queries \--\> API
```

**Design Principle:** All compute is stateless; durability and analytical truth live exclusively in the warehouse.

### **Component Responsibilities**

| Component | Role | Design Decision |
| :---- | :---- | :---- |
| **FastAPI Gateway** | Ingestion & Serving | Decoupled from the UI to allow headless integration with CI/CD pipelines (e.g., stopping a deploy if waste is high). |
| **Compute Engine** | ETL & Business Logic | Encapsulates the "Medallion" transformation logic. Stateless design. |
| **DuckDB Warehouse** | Storage & Analytics | Chosen over Postgres for its **Columnar Storage** efficiency on analytical queries (OLAP). |
| **Watchdog Service** | Event Listener | Enables the "Drop and Forget" pattern, simulating AWS Lambda triggers. |

## **4️⃣ End-to-End Data Flow (Medallion Architecture)**

The pipeline implements a strict **Bronze-Silver-Gold** data progression to ensure data quality and auditability.

### **🥉 Bronze Layer (Raw Ingestion)**

* **Input:** Raw CSV files from AWS CUR.  
* **Action:** Schema-on-read ingestion into DuckDB using read\_csv\_auto, preserving raw fidelity while deferring type enforcement.  
* **Goal:** Immutable record of what was received. No transformations.

### **🥈 Silver Layer (Cleaning & Normalization)**

* **Action:** \* Casting string types to strict numeric/date types.  
  * Standardizing column names (e.g., LineItem/UnblendedCost $\\to$ cost).  
  * **Star Schema Transformation:** Splitting data into Fact\_Usage and Dim\_Resource.  
* **Goal:** Clean data ready for multiple downstream use cases.

### **🥇 Gold Layer (Business Value)**

* **Action:** Applying the "Zombie Heuristics":  
  * Cost \> $0.00 (Asset is billing)  
  * AND Usage \== 0.00 (Asset is idle)  
* **Goal:** High-value, aggregated table (gold\_zombie\_report) optimized for the Dashboard API.

## **5️⃣ Engineering Decisions & Trade-offs**

### **1\. DuckDB vs. Postgres/MySQL**

* **Decision:** Used DuckDB (In-process OLAP).  
* **Reasoning:** Billing data is analytical (SUMs, GROUP BYs), not transactional. DuckDB is orders of magnitude faster for aggregations and requires zero management overhead (Serverless).  
* **Trade-off:** DuckDB is single-writer. I implemented a read\_only=True connection pattern in the Dashboard and manual connection closing in the Engine to handle concurrency.

### **2\. Event-Driven vs. Cron Schedule**

* **Decision:** Implemented watchdog file listeners.  
* **Reasoning:** Polling (Cron) creates latency. Event-driven architecture ensures the dashboard reflects the state of the system the millisecond a file arrives.

### **3\. API-First vs. UI-First**

* **Decision:** Logic resides in the API/Engine, UI is just a consumer.  
* **Reasoning:** Allows the tool to be integrated into Slack bots or Jira workflows later without refactoring the core logic.

## **6️⃣ Reliability & Observability**

### **Idempotency**

Each ingestion run is fingerprinted using file hash \+ billing period to guarantee deterministic reprocessing.  
The ingestion pipeline is designed to be idempotent.

* The Bronze layer uses CREATE OR REPLACE patterns (or specific partition overwrites) to prevent duplicate data processing if a file is re-uploaded.

### **Observability**

* **Structured Logging:** All services emit standard logs with context (\[WATCHER\], \[API\]).  
* **Health Checks:** API exposes a / endpoint for liveness probes.  
* **Error Handling:** Input files are validated for CSV integrity; path traversal attacks are prevented using os.path.basename.

## **7️⃣ Local Development & Setup**

### **Prerequisites**

* Docker & Docker Compose

### **Quick Start (Production Mode)**

The entire platform is containerized. Run one command to spin up the fleet.

\# 1\. Build and Launch
make up

\# 2\. Access Interfaces
\# Dashboard: <http://localhost:8501>
\# API Docs:  <http://localhost:8000/docs>

### **Simulating Data Events**

The system watches data/landing\_zone.

\# Generate synthetic billing data
make data

\# Trigger the pipeline via file drop
cp data/raw/aws\_billing\_data.csv data/landing\_zone/

## **8️⃣ Testing Strategy**

Testing focuses on logic correctness and API contract verification.

* **Unit Tests (tests/test\_core.py):** Mocks an in-memory database to verify the SQL logic correctly identifies "Zombies" vs "Active" resources.  
* **Integration Tests (tests/test\_api.py):** Spins up a TestClient to ensure the API correctly handles file uploads and returns the expected JSON structure.

make test

## **9️⃣ Future Roadmap (Scalability)**

| Horizon | Bottleneck | Proposed Solution |
| :---- | :---- | :---- |
| **10GB+ Data** | Single-node DuckDB RAM usage | Migrate storage to **S3 \+ Parquet** and compute to **Spark/PySpark**. |
| **Multi-Tenancy** | Single Warehouse file | Implement **Row-Level Security (RLS)** or separate DB files per Tenant ID. |
| **Real-Time Cost** | Batch CSV latency | Shift ingestion to **Kafka Connect** reading directly from AWS Cost Explorer API. |
| **Org Scale** | Ad-hoc heuristics | Externalize rules into a policy engine (YAML/SQL-based) |

## **👤 Author**

Emmanuel Richard

Data Engineer focused on Building Resilient Systems.
