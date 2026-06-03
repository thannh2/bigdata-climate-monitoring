# Kubernetes Deployment

This folder contains a compact Kubernetes deployment for the climate monitoring stack.
It is intended for a local or demo cluster such as Minikube, k3d, kind, or k3s.

## What This Deploys

- Kafka in KRaft mode
- Schema Registry
- Kafka Connect
- AKHQ Kafka UI
- Redis
- MongoDB
- Airflow Postgres
- Airflow webserver and scheduler
- MLflow server
- Streamlit dashboard
- Streaming scorer
- Ingestion CronJobs
- Kafka topic initialization Job

## Image Requirements

Public infrastructure images are pulled from Docker Hub.

The project code images must exist in your Kubernetes cluster before applying:

- `air-quality-ml:local`
- `climate-ingestion:local`
- `climate-airflow:local`

`air-quality-ml:local` already has a Dockerfile at `air-quality-ml/Dockerfile`.
The ingestion and Airflow images are placeholders because the current repo does not yet include Dockerfiles for those modules.

For a local cluster, build or load images into the cluster before deploy. Example for k3d:

```powershell
docker build -t air-quality-ml:local .\air-quality-ml
k3d image import air-quality-ml:local -c climate-dev
```

After you add Dockerfiles for ingestion and Airflow, build and import:

```powershell
docker build -t climate-ingestion:local -f deployment\images\ingestion.Dockerfile .
docker build -t climate-airflow:local -f deployment\images\airflow.Dockerfile .
k3d image import climate-ingestion:local climate-airflow:local -c climate-dev
```

## Configure Secrets

Edit `02-secrets.example.yaml` before applying:

```yaml
OWM_API_KEY: "your-openweathermap-key"
AQICN_API_KEY: "your-aqicn-key"
IQAIR_API_KEY: "your-iqair-key"
```

For a real environment, create secrets with `kubectl create secret` instead of committing secret values.

## Deploy

Apply all manifests:

```powershell
kubectl apply -k deployment\kubernetes
```

Wait for workloads:

```powershell
kubectl -n climate-monitoring get pods
kubectl -n climate-monitoring get svc
```

Run the topic init job again if needed:

```powershell
kubectl -n climate-monitoring delete job kafka-topic-init
kubectl apply -f deployment\kubernetes\21-kafka-topic-job.yaml
```

## Access UIs

NodePort services:

- AKHQ: `http://<node-ip>:30080`
- Airflow: `http://<node-ip>:30088`
- Dashboard: `http://<node-ip>:30501`

For Minikube:

```powershell
minikube service akhq -n climate-monitoring
minikube service airflow-webserver -n climate-monitoring
minikube service dashboard -n climate-monitoring
```

For port-forwarding:

```powershell
kubectl -n climate-monitoring port-forward svc/akhq 8080:8080
kubectl -n climate-monitoring port-forward svc/airflow-webserver 8088:8080
kubectl -n climate-monitoring port-forward svc/dashboard 8501:8501
```

## Important Notes

This is a demo deployment, not a production-hardened chart.

- Kafka is single-replica.
- MongoDB and Redis are single-replica.
- Secrets are example values unless replaced.
- Airflow and ingestion require custom images that contain this repository's Python code.
- Production Kubernetes should add resource limits, probes, ingress TLS, image registry tags, backup policy, and stronger secret management.
