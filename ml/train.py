"""Train fraud detection model (GradientBoosting + OneHot)."""
import numpy as np, pandas as pd, joblib, random
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score

rng = np.random.default_rng(42)
N = 50_000
countries = ["US","GB","DE","IN","BR","NG","RU","JP","FR"]
devices   = ["ios","android","web","pos"]
merchants = ["Amazon","Uber","Steam","Apple","Walmart","Shell","Netflix","Airbnb"]

def synth():
    rows=[]
    for _ in range(N):
        fraud = rng.random() < 0.06
        rows.append({
            "amount":   rng.uniform(2000,9000) if fraud else rng.uniform(1,400),
            "country":  rng.choice(countries) if fraud else rng.choice(["US","GB","DE"]),
            "device":   rng.choice(devices),
            "merchant": rng.choice(merchants),
            "label":    int(fraud),
        })
    return pd.DataFrame(rows)

df = synth()
enc = OneHotEncoder(handle_unknown="ignore").fit(df[["country","device","merchant"]])
X = np.hstack([df[["amount"]].values, enc.transform(df[["country","device","merchant"]]).toarray()])
y = df["label"].values

Xtr,Xte,ytr,yte = train_test_split(X,y,test_size=.2,stratify=y,random_state=42)
clf = GradientBoostingClassifier(n_estimators=200, max_depth=3, random_state=42).fit(Xtr,ytr)
p = clf.predict_proba(Xte)[:,1]
print(classification_report(yte, p>0.5))
print("AUC", roc_auc_score(yte, p))

joblib.dump({"model":clf, "encoder":enc}, "ml/model.pkl")
print("saved -> ml/model.pkl")
