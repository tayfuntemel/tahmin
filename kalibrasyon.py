# -*- coding: utf-8 -*-
from typing import Dict, List, Tuple, Optional
import numpy as np
import pandas as pd
from scipy.special import logit
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, brier_score_loss


def ece_hesapla(y_true: np.ndarray, y_prob: np.ndarray, n_bins: int = 10) -> Tuple[float, List[Dict[str, float]]]:
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    bin_indices = np.digitize(y_prob, bins) - 1
    ece = 0.0
    diyagram = []
    n_total = len(y_true)

    for b in range(n_bins):
        mask = bin_indices == b
        count = int(mask.sum())
        if count > 0:
            avg_prob = float(np.mean(y_prob[mask]))
            avg_true = float(np.mean(y_true[mask]))
            ece += (count / n_total) * abs(avg_true - avg_prob)
            diyagram.append({
                "bin": f"{bins[b]:.1f}-{bins[b+1]:.1f}",
                "adet": count,
                "ort_tahmin": round(avg_prob, 4),
                "gercek_oran": round(avg_true, 4),
                "fark": round(avg_true - avg_prob, 4)
            })
    return float(ece), diyagram


class MarketKalibratoru:
    def __init__(self, market_adi: str, min_isotonic_orneklem: int = 400):
        self.market_adi = market_adi
        self.min_isotonic_orneklem = min_isotonic_orneklem
        self.yontem: str = "platt"
        self.iso_model: Optional[IsotonicRegression] = None
        self.platt_model: Optional[LogisticRegression] = None
        self.baz_oran: float = 0.5

    def fit(self, oof_probs: np.ndarray, y_true: np.ndarray) -> None:
        mask = (~np.isnan(oof_probs)) & (~np.isnan(y_true))
        p = np.clip(oof_probs[mask], 1e-4, 1.0 - 1e-4)
        y = y_true[mask].astype(int)
        n = len(y)

        if n < 30 or len(np.unique(y)) < 2:
            self.yontem = "identity"
            return

        self.baz_oran = float(np.mean(y))

        if n >= self.min_isotonic_orneklem:
            self.yontem = "isotonic"
            self.iso_model = IsotonicRegression(y_min=0.01, y_max=0.99, out_of_bounds="clip")
            self.iso_model.fit(p, y)
        else:
            self.yontem = "platt"
            X_logit = logit(p).reshape(-1, 1)
            self.platt_model = LogisticRegression(C=1.0, solver="lbfgs")
            self.platt_model.fit(X_logit, y)

    def transform(self, probs: np.ndarray) -> np.ndarray:
        p = np.clip(np.asarray(probs, dtype=float), 1e-4, 1.0 - 1e-4)
        if self.yontem == "isotonic" and self.iso_model is not None:
            p_cal = self.iso_model.predict(p)
        elif self.yontem == "platt" and self.platt_model is not None:
            X_logit = logit(p).reshape(-1, 1)
            p_cal = self.platt_model.predict_proba(X_logit)[:, 1]
        else:
            p_cal = p

        return np.clip(0.85 * p_cal + 0.15 * p, 0.01, 0.99)


class CanliKalibrasyonIzleyici:
    def __init__(self, pencere: int = 100):
        self.pencere = pencere

    def market_saglik_kontrolu(self, conn) -> Dict[str, Dict[str, object]]:
        rapor: Dict[str, Dict[str, object]] = {}
        with conn.cursor() as cur:
            cur.execute("""
                SELECT secilen_tahmin, COALESCE(p_kalibre, olasilik) AS p_kal, sonuc, tarih
                FROM `tahminler`
                WHERE sonuc IN ('KAZANDI', 'KAYBETTI')
                  AND COALESCE(p_kalibre, olasilik) IS NOT NULL
                ORDER BY tarih DESC, id DESC
            """)
            rows = cur.fetchall()

        if not rows:
            return rapor

        df = pd.DataFrame(rows)
        for mkt, grp in df.groupby("secilen_tahmin"):
            son_n = grp.head(self.pencere)
            if len(son_n) < 20:
                rapor[mkt] = {"durum": "AKTIF_YETERSIZ_CANLI_VERI", "orneklem": len(son_n), "drift_alarmi": False}
                continue

            y = (son_n["sonuc"] == "KAZANDI").astype(float).values
            p = np.clip(son_n["p_kal"].astype(float).values, 1e-4, 1.0 - 1e-4)
            p_baz = np.full_like(p, np.clip(np.mean(y), 1e-4, 1.0 - 1e-4))

            ll_model = float(log_loss(y, p, labels=[0, 1]))
            ll_baz = float(log_loss(y, p_baz, labels=[0, 1]))
            brier_model = float(brier_score_loss(y, p))
            brier_baz = float(brier_score_loss(y, p_baz))

            drift_var = (ll_model > ll_baz) or (brier_model > brier_baz)
            rapor[mkt] = {
                "durum": "DEVRE_DISI_DRIFT" if drift_var else "SAGLIKLI",
                "orneklem": len(son_n),
                "rolling_log_loss": round(ll_model, 4),
                "baz_log_loss": round(ll_baz, 4),
                "rolling_brier": round(brier_model, 4),
                "baz_brier": round(brier_baz, 4),
                "drift_alarmi": bool(drift_var)
            }
        return rapor
