# Big Data Climate Monitoring & Predictive Analytics

An end-to-end Big Data pipeline leveraging the **Lambda Architecture** to ingest, process, and analyze global weather and air quality data in real-time. The system is designed to be highly scalable, fault-tolerant, and cloud-native via Kubernetes.

## 🎯 Project Objectives
1. **High-Throughput Ingestion:** Simulate and ingest IoT sensor data (10,000+ records/interval) utilizing Data Augmentation.
2. **Real-time Stream Processing:** Calculate sliding windows, detect anomalies, and compute physical atmospheric features on the fly.
3. **Physics-Informed Feature Engineering:** Transform standard API data into deep thermodynamic and kinematic features (e.g., Dew Point, Wind Vectors) without relying on black-box ML alone.
4. **Predictive Analytics:** Train time-series machine learning models (Gradient-Boosted Trees) on batch data and deploy them to the stream for real-time forecasting.

---

## 🏗️ System Architecture (Lambda)

The pipeline processes data through parallel Speed and Batch layers:
![Data architect](./images/Data-pipeline.drawio.png)

  * **Data Sources:** Open-Meteo & OpenWeatherMap APIs (Simulated IoT Network).
  * **Ingestion:** Python, Apache Airflow, Apache Kafka.
  * **Storage (Batch Layer):** Hadoop HDFS (Parquet format).
  * **Processing (Speed & Batch):** Apache Spark (Structured Streaming & Spark SQL/MLlib).
  * **Serving:** MongoDB / Apache Cassandra.
  * **Deployment:** Kubernetes.

-----

## 📂 Repository Layout

```text
.
├── Ingestion/                  # Kafka-based ingestion for weather & air-quality data
│   ├── airflow/                # DAG orchestration for scheduling
│   ├── collectors/             # Batch and stream data collectors
│   ├── config/                 # Kafka and environment configuration
│   ├── docs/                   # Requirements and design notes
│   ├── great_expectations/     # Data quality validation suites
│   ├── producers/              # Kafka and DLQ producer helpers
│   ├── samples/                # Minimal normalized JSON examples
│   ├── scripts/                # Setup, smoke tests, and GE validations
│   ├── tests/                  # Automated testing suites
│   ├── utils/                  # Shared helpers (logging, serialization)
│   ├── validators/             # Normalization and blocking validation logic
│   └── docker-compose.yml      # Local Kafka stack infrastructure
│
├── Processing/                 # Spark Structured Streaming & Batch jobs
│   ├── streaming/              # Speed Layer (Real-time UDFs, Windowing)
│   ├── batch/                  # Batch Layer (Aggregations, ML Training)
│   └── common/                 # Shared data schemas and utilities
│
├── Storage/                    # HDFS & NoSQL schema configurations
│   ├── hdfs/                   # Distributed storage setup scripts
│   └── mongodb/                # NoSQL collection and indexing scripts
│
├── Deployment/                 # Kubernetes manifests & Helm charts
│   ├── docker-compose.yml      # Full local development environment
│   └── helm/                   # K8s charts for Kafka, Spark, and MongoDB
│
└── README.md
-----
```

## 🚀 Module Status & Details

### 1\. Ingestion Layer

*Status: Completed* | [Go to Ingestion README](Ingestion/README.md)

Implemented in the ingestion module:

  - Kafka stack and topic setup (`weather_stream`, `air_quality_stream`, `dlq`).
  - Batch and stream collectors (Python `requests`).
  - Data Augmentation (simulating 10k+ IoT sensors).
  - Normalization, validation, and DLQ (Dead Letter Queue) handling.
  - Checkpoint-based duplicate prevention & metadata enrichment.
  - Airflow DAG scaffolding for 5-minute interval scheduling.
  - Downstream-ready quality rules for Great Expectations.

### 2\. Processing Layer (Spark)

*Status: In Progress* | `Processing/`

**Speed Layer (Real-time):**

  - **Windowing:** 1-hour and 24-hour sliding averages for AQI and PM2.5.
  - **Physics-Informed UDFs:** Vectorizing Native Spark SQL to compute:
      - *Thermodynamics:* Vapor Pressure ($e$), Air Density ($\rho$).
      - *Kinematics:* Wind U/V Vector components.
      - *Temporal Dynamics:* Pressure tendency ($\Delta P$ over 3 hours) for storm prediction.
  - **Stateful Processing:** Watermarking for late API payloads and state management for prolonged health hazard alerts.
  - **Real-time Inference:** Applying pre-trained ML models to streaming data.

**Batch Layer (Historical & ML):**

  - Master dataset persistence to HDFS using optimized **Parquet** columnar formats.
  - Spatio-temporal aggregations and baseline climate modeling.
  - **Machine Learning Pipeline:** Training Gradient-Boosted Trees (GBT) Regressor using chronological splits to prevent data leakage.

### 3\. Serving & Visualization Layer

*Status: Planned* | `Storage/`

  - **Real-time Views:** Upserting streaming results into **MongoDB** for sub-second latency access.
  - **Batch Views:** Indexed historical tables for BI Dashboard connections.

-----

## ☸️ Kubernetes Deployment

The entire pipeline is containerized and orchestrated using K8s to ensure production-level resilience.

  - **StatefulSets:** Apache Kafka, Zookeeper, HDFS, MongoDB.
  - **Deployments:** Spark Driver/Executors, Ingestion CronJobs, Dashboard UI.
  - **Networking:** ClusterIP Services and Secrets management for API keys.

-----

## 🛠️ How to Run (Local Development)

**1. Clone the repository:**

```bash
git clone [https://github.com/YOUR_USERNAME/weather-air-quality-pipeline.git](https://github.com/YOUR_USERNAME/weather-air-quality-pipeline.git)
cd weather-air-quality-pipeline
```

**2. Start the Local Infrastructure (Ingestion):**
Navigate to the Ingestion module and follow the specific setup guide to start the Kafka broker.

```bash
cd Ingestion
docker compose -f docker-compose.yml up -d
```

**3. Run Modules:**
Check module-specific documentation (e.g., `Ingestion/README.md`) for commands to trigger collectors, create topics, or run Airflow DAGs.