"""FastAPI inference + dashboard read API with SSE live stream (SQLite Advanced Version)."""
import asyncio, json, os, joblib, numpy as np, sqlite3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

MODEL = joblib.load(os.getenv("MODEL_PATH", "ml/model.pkl"))
DB_FILE = "fraud.db"

app = FastAPI(title="Fraud Detection API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_id TEXT UNIQUE,
                user_id TEXT,
                amount REAL,
                merchant TEXT,
                country TEXT,
                device TEXT,
                ts TEXT,
                risk_score REAL,
                is_fraud INTEGER
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_id TEXT,
                reason TEXT,
                risk_score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # NEW: Blacklist table for our Kill Switch feature
        cur.execute("""
            CREATE TABLE IF NOT EXISTS blacklist (
                user_id TEXT PRIMARY KEY,
                blocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

init_db()

class Tx(BaseModel):
    tx_id: str; user_id: str; amount: float
    merchant: str | None = None
    country: str | None = None
    device:  str | None = None
    ts: str | None = None

class BlacklistRequest(BaseModel):
    user_id: str

def _score(tx: Tx):
    # NEW FEATURE: Direct blacklist check (Instant Auto-Decline)
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM blacklist WHERE user_id = ?", (tx.user_id,))
        if cur.fetchone():
            return 1.0, True, "CRITICAL: Blacklisted User Attempt"

    enc, model = MODEL["encoder"], MODEL["model"]
    X = enc.transform([[tx.country or "NA", tx.device or "NA", tx.merchant or "NA"]])
    feats = np.hstack([[[tx.amount]], X.toarray()])
    p = float(model.predict_proba(feats)[0,1])
    reason = []
    if tx.amount > 1500: reason.append("high amount")
    if tx.country in ("RU","NG","BR"): reason.append("rare geo")
    return p, p > 0.7, ", ".join(reason) or "model signal"

@app.post("/score")
def score(tx: Tx):
    p, is_fraud, reason = _score(tx)
    return {"tx_id": tx.tx_id, "risk_score": round(p,4), "is_fraud": is_fraud, "reason": reason}

# NEW FEATURE: Endpoint to block a malicious user
@app.post("/blacklist")
def blacklist_user(req: BlacklistRequest):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO blacklist (user_id) VALUES (?)", (req.user_id,))
        conn.commit()
    return {"status": "success", "message": f"User {req.user_id} has been blacklisted."}

# NEW FEATURE: Endpoint to fetch the active blacklist
@app.get("/blacklist")
def get_blacklist():
    return q("SELECT * FROM blacklist ORDER BY blocked_at DESC")

def q(sql, args=()):
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(sql, args)
        return [dict(row) for row in cur.fetchall()]

@app.get("/transactions")
def txs(limit: int = 50):
    return q("SELECT tx_id,user_id,amount,merchant,country,device,ts,risk_score,is_fraud "
             "FROM transactions ORDER BY ts DESC LIMIT ?", (limit,))

@app.get("/alerts")
def alerts(limit: int = 50):
    return q("SELECT tx_id, reason, risk_score FROM alerts ORDER BY created_at DESC LIMIT ?", (limit,))

@app.get("/metrics")
def metrics():
    # 1. Get transaction and fraud counts
    res = q("""SELECT COUNT(*) AS total,
                      SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS frauds,
                      AVG(risk_score) AS avg_risk
               FROM transactions""")
    
    # 2. Get the total number of blocked users from the blacklist table
    blacklist_res = q("SELECT COUNT(*) AS total_blocked FROM blacklist")
    blocked_count = blacklist_res[0]["total_blocked"] if blacklist_res else 0

    if res and res[0]["total"] > 0:
        metrics_data = res[0]
        metrics_data["blocked"] = blocked_count
        return metrics_data
        
    return {"total": 0, "frauds": 0, "avg_risk": 0, "blocked": blocked_count}

@app.get("/stream")
async def stream():
    async def gen():
        last = 0
        while True:
            rows = q("SELECT id,tx_id,amount,country,risk_score,is_fraud,ts "
                     "FROM transactions WHERE id > ? ORDER BY id ASC LIMIT 100", (last,))
            for r in rows:
                last = max(last, r["id"])
                yield f"data: {json.dumps(r, default=str)}\n\n"
            await asyncio.sleep(1)
    return StreamingResponse(gen(), media_type="text/event-stream")