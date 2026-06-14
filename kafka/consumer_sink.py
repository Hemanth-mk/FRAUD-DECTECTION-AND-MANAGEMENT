"""Lightweight consumer that scores via FastAPI, alerts Discord, and writes to SQLite."""
import json
import os
import requests
import sqlite3
from kafka import KafkaConsumer

API = os.getenv("API_URL", "http://localhost:8000/score")

# OPTIONAL: Put your Discord webhook URL string here to receive instant phone alerts
DISCORD_WEBHOOK_URL = "" 

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, "fraud.db")

def send_discord_alert(tx, reason, score):
    if not DISCORD_WEBHOOK_URL:
        return
    payload = {
        "content": f"🚨 **CRITICAL FRAUD ALERT** 🚨\n"
                   f"• **User ID:** `{tx['user_id']}`\n"
                   f"• **Amount:** `${tx['amount']:,}`\n"
                   f"• **Location:** 📍 `{tx['country']}` ({tx['device']})\n"
                   f"• **Flagged Reason:** `{reason}`\n"
                   f"• **Model Risk Score:** `{score}`"
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=3)
    except Exception as e:
        print(f"⚠️ Failed sending Discord notification: {e}")

def main():
    c = KafkaConsumer("transactions",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda b: json.loads(b.decode()),
        auto_offset_reset="latest")
    
    print(f"📥 Consumer is listening to Kafka and monitoring system threat triggers...")
    
    for msg in c:
        tx = msg.value
        try:
            r = requests.post(API, json=tx, timeout=10).json()
            
            with sqlite3.connect(DB_FILE) as conn:
                cur = conn.cursor()
                
                cur.execute("""
                    INSERT OR IGNORE INTO transactions(tx_id, user_id, amount, merchant, country, device, ts, risk_score, is_fraud)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    tx["tx_id"], tx["user_id"], tx["amount"], tx["merchant"], 
                    tx["country"], tx["device"], tx["ts"], r["risk_score"], 1 if r["is_fraud"] else 0
                ))
                
                if r["is_fraud"]:
                    cur.execute("INSERT INTO alerts(tx_id, reason, risk_score) VALUES(?, ?, ?)",
                                (tx["tx_id"], r["reason"], r["risk_score"]))
                    
                    # TRIGGER DISCORD REAL-TIME NOTIFICATION
                    send_discord_alert(tx, r["reason"], r["risk_score"])
                    
                conn.commit()
                print(f"✅ Processed {tx['tx_id']} | User: {tx['user_id']} | Fraud: {r['is_fraud']}")
                
        except Exception as e:
            print(f"❌ Error processing transaction: {e}")

if __name__ == "__main__":
    main()