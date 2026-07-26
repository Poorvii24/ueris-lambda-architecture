"""
ai_layer/anomaly_ensemble.py
=============================
UERIS — 3-Model Anomaly Detection Ensemble

Models: Isolation Forest + Local Outlier Factor + One-Class SVM
Decision: majority voting (≥2 of 3 = anomaly)
Output: is_anomaly, anomaly_score [0-1], severity, confidence, feature_zscores
"""

import warnings
import numpy as np
from typing import List
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")

SEVERITY_THRESHOLDS = [(0.9,"Critical"),(0.7,"High"),(0.5,"Medium"),(0.3,"Low"),(0.0,"Normal")]
FEATURE_NAMES = ["aqi","temperature","humidity","usi"]


def _score_to_severity(score):
    for t, label in SEVERITY_THRESHOLDS:
        if score >= t: return label
    return "Normal"


def _score_to_confidence(score, n_flagged, n_models):
    return min(99, max(1, int(score*70 + (n_flagged/n_models)*30)))


class AnomalyEnsemble:
    MODEL_NAMES = ["IsolationForest","LOF","OneClassSVM"]

    def __init__(self, contamination=0.05, random_state=42):
        self.contamination   = contamination
        self.random_state    = random_state
        self._scaler         = StandardScaler()
        self._models         = {}
        self._fitted         = False
        self._n_train        = 0
        self._feature_stats  = {}
        self._feat_names     = FEATURE_NAMES

    def fit(self, X: np.ndarray, feature_names: List[str] = None) -> "AnomalyEnsemble":
        self._n_train    = len(X)
        self._feat_names = feature_names or FEATURE_NAMES[:X.shape[1]]
        self._feature_stats = {
            name: {"mean": float(X[:,i].mean()), "std": float(X[:,i].std())}
            for i, name in enumerate(self._feat_names)
        }
        X_scaled = self._scaler.fit_transform(X)
        self._models["IsolationForest"] = IsolationForest(
            contamination=self.contamination, n_estimators=200,
            random_state=self.random_state, n_jobs=-1,
        ).fit(X_scaled)
        self._models["LOF"] = LocalOutlierFactor(
            contamination=self.contamination,
            n_neighbors=min(20, max(5, self._n_train//10)),
            novelty=True, n_jobs=-1,
        ).fit(X_scaled)
        self._models["OneClassSVM"] = OneClassSVM(
            kernel="rbf", nu=self.contamination, gamma="scale",
        ).fit(X_scaled)
        self._fitted = True
        return self

    def predict_single(self, aqi, temperature, humidity, usi) -> dict:
        X = np.array([[aqi, temperature, humidity, usi]], dtype=np.float32)
        return self.predict_batch(X)[0]

    def predict_batch(self, X: np.ndarray) -> List[dict]:
        if not self._fitted:
            return [self._threshold_fallback(row) for row in X]
        X_scaled = self._scaler.transform(X)
        preds, scores = {}, {}
        for name, model in self._models.items():
            try:
                preds[name]  = model.predict(X_scaled)
                scores[name] = model.decision_function(X_scaled)
            except Exception as e:
                preds[name]  = np.ones(len(X))
                scores[name] = np.zeros(len(X))

        results = []
        for i in range(len(X)):
            flagged    = [n for n in self.MODEL_NAMES if n in preds and preds[n][i] == -1]
            n_flagged  = len(flagged)
            is_anomaly = n_flagged >= 2

            raw_scores = [scores[n][i] for n in self.MODEL_NAMES if n in scores]
            if raw_scores:
                mean_score = float(np.mean(raw_scores))
                anom_score = float(np.clip(1.0/(1.0+np.exp(mean_score*2)), 0, 1))
            else:
                anom_score = 0.5 if is_anomaly else 0.1

            feat_zscores = {}
            for j, fname in enumerate(self._feat_names):
                if j < X.shape[1]:
                    stat = self._feature_stats.get(fname, {"mean":0,"std":1})
                    std  = stat["std"] if stat["std"] > 0 else 1
                    feat_zscores[fname] = round((float(X[i,j]) - stat["mean"]) / std, 2)

            severity   = _score_to_severity(anom_score)
            confidence = _score_to_confidence(anom_score, n_flagged, len(self.MODEL_NAMES))
            results.append({
                "is_anomaly":       is_anomaly,
                "anomaly_score":    round(anom_score, 4),
                "confidence_pct":   confidence,
                "severity":         severity if is_anomaly else "Normal",
                "models_flagged":   flagged,
                "n_models_flagged": n_flagged,
                "method":           "ensemble",
                "feature_zscores":  feat_zscores,
            })
        return results

    def _threshold_fallback(self, row: np.ndarray) -> dict:
        aqi        = float(row[0]) if len(row) > 0 else 0
        is_anomaly = aqi > 200
        score      = min(aqi/300, 1.0)
        return {
            "is_anomaly":       is_anomaly,
            "anomaly_score":    round(score, 4),
            "confidence_pct":   40,
            "severity":         _score_to_severity(score) if is_anomaly else "Normal",
            "models_flagged":   ["threshold"] if is_anomaly else [],
            "n_models_flagged": 1 if is_anomaly else 0,
            "method":           "threshold",
            "feature_zscores":  {},
        }

    def evaluate(self, X: np.ndarray, y_true: np.ndarray = None) -> dict:
        predictions = self.predict_batch(X)
        y_pred      = np.array([1 if r["is_anomaly"] else 0 for r in predictions])
        scores      = np.array([r["anomaly_score"] for r in predictions])
        stats = {
            "total_samples":    len(X),
            "anomalies_found":  int(y_pred.sum()),
            "anomaly_rate_pct": round(float(y_pred.mean())*100, 2),
            "mean_score":       round(float(scores.mean()), 4),
            "max_score":        round(float(scores.max()), 4),
        }
        if y_true is not None:
            try:
                from sklearn.metrics import precision_score, recall_score, f1_score
                stats["precision"] = round(precision_score(y_true, y_pred, zero_division=0), 4)
                stats["recall"]    = round(recall_score(y_true, y_pred, zero_division=0), 4)
                stats["f1"]        = round(f1_score(y_true, y_pred, zero_division=0), 4)
            except Exception as e:
                stats["eval_error"] = str(e)
        return stats
