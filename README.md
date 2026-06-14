# Project 4 — Real-Time Fraud Detection System

End-to-end reference implementation.

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