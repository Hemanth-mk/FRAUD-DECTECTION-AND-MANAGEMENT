import React, { useEffect, useState } from "react";
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

const API = "http://localhost:8000";

export default function App() {
  const [txs, setTxs] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [metrics, setMetrics] = useState({ total: 0, frauds: 0, blocked: 0, avg_risk: 0 });
  const [series, setSeries] = useState([]);

  const tick = async () => {
    try {
      const [t, a, m] = await Promise.all([
        fetch(`${API}/transactions?limit=25`).then(r => r.json()),
        fetch(`${API}/alerts?limit=15`).then(r => r.json()),
        fetch(`${API}/metrics`).then(r => r.json()),
      ]);
      setTxs(t); setAlerts(a); setMetrics(m);
      setSeries(s => [...s.slice(-29), { t: new Date().toLocaleTimeString(), risk: m.avg_risk || 0 }]);
    } catch (err) {
      console.error("Error pulling data from backend hub:", err);
    }
  };

  useEffect(() => {
    tick(); 
    const id = setInterval(tick, 2000); 
    return () => clearInterval(id);
  }, []);

  // KILL SWITCH: Action execution handler to black-list users instantly
  const handleBlockUser = async (userId) => {
    if (window.confirm(`Are you sure you want to completely ban User ${userId}?`)) {
      try {
        const res = await fetch(`${API}/blacklist`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ user_id: userId })
        }).then(r => r.json());
        alert(res.message);
        tick();
      } catch (err) {
        alert("Failed to block user.");
      }
    }
  };

  return (
    <div style={{ padding: 24, maxWidth: 1300, margin: "0 auto", fontFamily: "sans-serif", background: "#020617", color: "#f8fafc", minHeight: "100vh" }}>
      <h1 style={{ marginBottom: 4, color: "#f1f5f9" }}>Real-Time Fraud Detection Command Center</h1>
      <p style={{ opacity: .6, marginTop: 0 }}>Kafka · Spark · SQLite · MLOps · FastAPI · React</p>

      {/* UPDATED: 4-Card layout containing both Frauds and Account Blocked numbers */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 16, margin: "24px 0" }}>
        <Card label="Tx (1h)"          value={metrics.total ?? 0}/>
        <Card label="Frauds Flagged"   value={metrics.frauds ?? 0} accent="#ef4444"/>
        <Card label="Accounts Blocked" value={metrics.blocked ?? 0} accent="#a855f7"/>
        <Card label="System Avg Risk"  value={(metrics.avg_risk ?? 0).toFixed(3)} accent="#f59e0b"/>
      </div>

      <Panel title="Average Risk Matrix Trend">
        <div style={{ height: 220 }}>
          <ResponsiveContainer>
            <LineChart data={series}>
              <XAxis dataKey="t" stroke="#64748b"/>
              <YAxis stroke="#64748b" domain={[0,1]}/>
              <Tooltip 
                contentStyle={{ background:"#0f172a", border:"1px solid #1e293b" }}
                formatter={(value) => [Number(value).toFixed(3), "Risk Factor"]}
              />
              <Line type="monotone" dataKey="risk" stroke="#22d3ee" dot={false} strokeWidth={2}/>
            </LineChart>
          </ResponsiveContainer>
        </div>
      </Panel>

      <div style={{ display:"grid", gridTemplateColumns:"1.6fr 1.1fr", gap:16, marginTop:16 }}>
        <Panel title="Live Operational Transactions">
          <div style={{ overflow:"auto", maxHeight:360 }}>
            <table style={{ width:"100%", fontSize:12, borderCollapse:"collapse" }}>
              <thead>
                <tr>
                  {["tx_id","user_id","amount","country","risk","fraud","action"].map(c=><th key={c} style={{textAlign:"left",padding:8,opacity:.6}}>{c}</th>)}
                </tr>
              </thead>
              <tbody>
                {txs.map((r,i)=>(
                  <tr key={i} style={{ borderTop:"1px solid #1e293b", color: r.is_fraud?"#fca5a5":"inherit" }}>
                    <td style={{padding:8}}>{r.tx_id.slice(0,10)}...</td>
                    <td style={{padding:8, fontWeight:600}}>{r.user_id}</td>
                    <td style={{padding:8}}>${r.amount}</td>
                    <td style={{padding:8}}>📍 {r.country}</td>
                    <td style={{padding:8}}>{r.risk_score}</td>
                    <td style={{padding:8}}>{r.is_fraud ? "🚨 YES" : "✅ NO"}</td>
                    <td style={{padding:8}}>
                      <button 
                        onClick={() => handleBlockUser(r.user_id)}
                        style={{ background: "#311212", color: "#f87171", border: "1px solid #991b1b", padding: "2px 6px", borderRadius: 4, cursor: "pointer", fontSize: 10 }}
                      >
                        🚫 Block
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel title="System Critical Alerts">
          <div style={{ overflow:"auto", maxHeight:360 }}>
            <table style={{ width:"100%", fontSize:12, borderCollapse:"collapse" }}>
              <thead><tr>{["tx_id","reason","risk"].map(c=><th key={c} style={{textAlign:"left",padding:8,opacity:.6}}>{c}</th>)}</tr></thead>
              <tbody>
                {alerts.map((r,i)=>(
                  <tr key={i} style={{ borderTop:"1px solid #1e293b", color: "#fca5a5" }}>
                    <td style={{padding:8}}>{r.tx_id.slice(0,10)}...</td>
                    <td style={{padding:8}}>{r.reason}</td>
                    <td style={{padding:8, fontWeight:700}}>{r.risk_score}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      </div>
    </div>
  );
}

const Card = ({label,value,accent="#22d3ee"}) => (
  <div style={{ background:"#0f172a", border:"1px solid #1e293b", borderRadius:12, padding:16 }}>
    <div style={{ fontSize:12, opacity:.6, marginBottom:4 }}>{label}</div>
    <div style={{ fontSize:32, fontWeight:700, color:accent }}>{value}</div>
  </div>
);
const Panel = ({title,children}) => (
  <div style={{ background:"#0f172a", border:"1px solid #1e293b", borderRadius:12, padding:16 }}>
    <div style={{ fontWeight:600, marginBottom:12, fontSize:14, borderBottom:"1px solid #1e293b", paddingBottom:6 }}>{title}</div>{children}
  </div>
);