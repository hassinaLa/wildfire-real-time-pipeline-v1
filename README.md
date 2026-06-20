🌍 Final Year Project (PFE)
Design and Implementation of a Real-Time Forest Fire Prediction System Based on a Big Data Architecture
👨‍🎓 Prepared by:
Mezrouh Hanine
Lakehali Hassina
👩‍🏫 Supervised by:
Mme Asma Belaroussi

## 📌 Overview
This project presents the design and implementation of a real-time forest fire prediction system leveraging a big data distributed architecture. The system integrates stream processing, machine learning, and scalable data pipelines to enable real-time environmental risk prediction.

It combines:

* ⚡ Apache Flink (stream processing)
* 📡 Kafka (event streaming)
* 🧠 Machine Learning inference services
* 🗄️ PostgreSQL (data persistence)
* 📊 Grafana (monitoring & visualization)
* 🐳 Docker (containerized deployment)

The system supports both **real-time (live)** and **batch (historical)** execution modes.

---

## 🏗️ System Architecture

The platform is composed of the following components:

* **Data Simulation Layer** → Generates or replays environmental data
* **Streaming Layer (Kafka)** → Event-driven message broker
* **Processing Layer (Flink)** → Real-time computation & feature engineering
* **ML Inference Layer** → Predicts wildfire risk scores
* **Storage Layer (PostgreSQL)** → Stores predictions and features
* **Monitoring Layer (Grafana)** → Visual analytics dashboard

---

## ⚙️ Prerequisites

Ensure the following tools are installed:

* Docker ≥ 20
* Docker Compose v2
* Git

Verify installation:

```bash
docker --version
docker compose version
```

---

## 📥 Installation

Clone the repository:

```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo
```

---

## 🚀 Execution Modes

The system supports two execution environments:

---

### 🔴 Live Mode (Real-Time Processing)

Runs the pipeline using streaming data sources:

```bash
docker compose --env-file .env.live -f docker/docker-compose.yml up --build
```

---

### 🟡 Historical Mode (Batch Processing)

Runs the pipeline using stored historical datasets:

```bash
docker compose --env-file .env.historical -f docker/docker-compose.yml up --build
```

---

## 🌐 Service Endpoints

After deployment, the following services are available:

| Component                  | URL                                            |
| -------------------------- | ---------------------------------------------- |
| 🧠 Flink Dashboard         | [http://localhost:8082](http://localhost:8082) |
| 📊 Kafka UI                | [http://localhost:8085](http://localhost:8085) |
| 🧾 pgAdmin                 | [http://localhost:8081](http://localhost:8081) |
| 📈 Grafana                 | [http://localhost:3001](http://localhost:3001) |
| 🐘 PostgreSQL              | localhost:5432                                 |
| 🔵 Zookeeper               | localhost:2181                                 |
| 📡 Kafka Broker (external) | localhost:29092                                |

---

## 🔐 Access Credentials

### pgAdmin

* Email: `hassinalakehali123@gmail.com`
* Password: `admin`

---

## 🔄 Kafka Connectivity

Kafka exposes two listeners:

* Internal (Docker network):

  ```
  kafka:9092
  ```

* External (Host machine):

  ```
  localhost:29092
  ```

---

## 📊 Flink Configuration

* Flink JobManager UI is mapped to:

  ```
  http://localhost:8082
  ```

* Jobs are automatically submitted via `flink-job-submit` container.

---

## 🧪 System Verification

Check running services:

```bash
docker ps
```

Expected running components:

* Kafka + Zookeeper
* Flink JobManager + TaskManager
* PostgreSQL + pgAdmin
* Grafana
* Simulator
* ML inference services

---

## 🛑 Shutdown

To stop all services:

```bash
docker compose -f docker/docker-compose.yml down
```

---

## 🧹 Full Cleanup (Optional)

Remove containers, networks, and volumes:

```bash
docker compose -f docker/docker-compose.yml down -v
```

---

## 🧠 Project Highlights

* Event-driven architecture
* Real-time stream processing with Apache Flink
* Dual-mode execution (Live & Historical)
* Scalable microservices design
* Containerized deployment (fully reproducible)
* Monitoring and visualization via Grafana

---
