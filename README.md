Description:
Developed a real-time fraud detection platform that analyzes streaming financial transactions using Machine Learning and big-data technologies. The system detects suspicious activities, generates instant fraud alerts, calculates risk scores, blocks high-risk accounts, and visualizes live transaction analytics through an interactive monitoring dashboard.

Technologies Used:

Python
Apache Kafka
Apache Spark
FastAPI
React.js
PostgreSQL / SQLite
Scikit-learn
Docker
MLflow (MLOps)
Chart.js / Recharts


Key Highlights:

Real-time transaction processing
Machine Learning-based fraud detection
Live risk score calculation
Automated fraud alerts
Account blocking mechanism
Interactive analytics dashboard
Kafka-Spark streaming pipeline
MLOps model monitoring
ATS Resume Version (2 Lines)


**Stack**
- Apache Kafka — transaction event bus
- Apache Spark Structured Streaming — real-time enrichment & scoring
- PostgreSQL — feature store + alerts
- scikit-learn (IsolationForest + GradientBoosting) — fraud model
- FastAPI — low-latency inference API
- React + Vite + Recharts — live dashboard

## Run
```bash
docker compose -f docker/docker-compose.yml up -d   # kafka, postgres
python ml/train.py                                  # trains model -> ml/model.pkl
uvicorn api.main:app --reload --port 8000           # inference API
python kafka/producer.py                            # streams synthetic txns
spark-submit spark/stream_job.py                    # scoring pipeline
cd dashboard && npm i && npm run dev                # live dashboard
```

## Architecture
```
Producer -> Kafka(transactions) -> Spark Structured Streaming
              |                          |-> FastAPI /score (ML model)
              |                          |-> Postgres (alerts, features)
              v
        Dashboard <- FastAPI (alerts, metrics, SSE stream)
```


docker compose up -d
docker ps
>uvicorn api.main:app --reload
>npm run dev
>python consumer_sink.py
>python producer.py
