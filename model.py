# -*- coding: utf-8 -*-
from typing import Dict, List, Tuple, Optional
import numpy as np
import pandas as pd
from scipy.stats import poisson, nbinom
from sklearn.linear_model import Ridge, LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import log_loss

ADAY_MARKETLER = [
    "maç sonu 1", "maç sonu 2", "1x çifte şans", "x2 çifte şans",
    "1.5 gol üst", "2.5 gol üst", "2.5 gol alt", "3.5 gol alt",
    "karşılıklı gol var", "kg yok", "kg var ve 2.5 üst",
    "ms 1 ve 1.5 üst", "ms 2 ve 1.5 üst", "1x ve 1.5 üst", "x2 ve 1.5 üst",
    "ms 1 ve kg var", "ms 2 ve kg var", "ms 1 ve 2.5 üst", "ms 2 ve 2.5 üst",
    "ev 1.5 gol üst", "dep 1.5 gol üst",
    "ilk yarı 0.5 üst", "ilk yarı 1.5 üst", "ilk yarı kg var",
    "ilk yarı 1", "ilk yarı 2", "2. yarı 1.5 üst", "iki yarıda da kg var",
    "iy/ms 1/1", "iy/ms 2/2", "iy/ms x/1", "iy/ms x/2", "iy/ms x/x",
    "iy/ms 1/x", "iy/ms 2/x", "iy/ms 2/1", "iy/ms 1/2",
    "8.5 korner üst", "9.5 korner üst", "10.5 korner üst",
    "3.5 kart üst", "4.5 kart üst",
    "kırmızı kart çıkar", "penaltı olur"
]

OZELLIK_KOLONLARI = [
    "dc_lambda_ev", "dc_lambda_dep", "ev_hucum_gucu", "ev_savunma_zaafi",
    "dep_hucum_gucu", "dep_savunma_zaafi", "elo_fark", "elo_p_ev",
    "dinlenme_farki", "ev_form", "dep_form",
    "ev_sut_lehe", "dep_sut_lehe", "ev_isabetli_lehe", "dep_isabetli_lehe",
    "ev_korner_lehe", "ev_korner_aleyhe", "dep_korner_lehe", "dep_korner_aleyhe",
    "ev_sari_lehe", "ev_sari_aleyhe", "dep_sari_lehe", "dep_sari_aleyhe",
    "ev_faul_lehe", "dep_faul_lehe", "ev_top_oynama", "dep_top_oynama",
    "ev_pas_yuzde", "dep_pas_yuzde", "iy_gol_payi",
    "lig_gol_ort", "lig_korner_ort", "lig_sari_ort"
]

# Hedef önbelleği (Walk-forward sırasında tekrar tekrar hesaplamayı önler)
_HEDEF_ONBELLEK: Dict[Tuple[int, str], float] = {}


def dixon_coles_matrisi_uret(
    lam_ev: float,
    lam_dep: float,
    rho: float = -0.09,
    max_gol: int = 9
) -> np.ndarray:
    lam_ev = float(np.clip(lam_ev, 0.15, 5.5))
    lam_dep = float(np.clip(lam_dep, 0.15, 5.5))
    max_neg_rho = -0.95 / max(1.0, lam_ev * lam_dep)
    rho = float(np.clip(rho, max_neg_rho, 0.25))

    p_ev = poisson.pmf(np.arange(max_gol + 1), lam_ev)
    p_dep = poisson.pmf(np.arange(max_gol + 1), lam_dep)
    matris = np.outer(p_ev, p_dep)

    matris[0, 0] *= (1.0 - lam_ev * lam_dep * rho)
    matris[0, 1] *= (1.0 + lam_ev * rho)
    matris[1, 0] *= (1.0 + lam_dep * rho)
    matris[1, 1] *= (1.0 - rho)

    matris = np.clip(matris, 0.0, None)
    toplam = matris.sum()
    if toplam > 0:
        matris /= toplam
    return matris


def skor_matrisinden_gol_marketlerini_turet(
    lam_ev: float,
    lam_dep: float,
    iy_payi: float = 0.44,
    rho: float = -0.09
) -> Tuple[Dict[str, float], np.ndarray]:
    iy_payi = float(np.clip(iy_payi, 0.35, 0.55))
    lam_ev_iy = lam_ev * iy_payi
    lam_dep_iy = lam_dep * iy_payi
    lam_ev_2y = lam_ev * (1.0 - iy_payi)
    lam_dep_2y = lam_dep * (1.0 - iy_payi)

    M_ms = dixon_coles_matrisi_uret(lam_ev, lam_dep, rho=rho, max_gol=9)
    M_iy = dixon_coles_matrisi_uret(lam_ev_iy, lam_dep_iy, rho=rho * 0.5, max_gol=5)
    M_2y = dixon_coles_matrisi_uret(lam_ev_2y, lam_dep_2y, rho=rho * 0.5, max_gol=6)

    n = M_ms.shape[0]
    ev_idx, dep_idx = np.meshgrid(np.arange(n), np.arange(n), indexing="ij")
    top_gol = ev_idx + dep_idx

    m_ms1 = ev_idx > dep_idx
    m_ms2 = dep_idx > ev_idx
    m_1x = ev_idx >= dep_idx
    m_x2 = dep_idx >= ev_idx
    m_15_ust = top_gol >= 2
    m_25_ust = top_gol >= 3
    m_25_alt = top_gol <= 2
    m_35_alt = top_gol <= 3
    m_kg_var = (ev_idx > 0) & (dep_idx > 0)
    m_kg_yok = ~m_kg_var

    n_iy = M_iy.shape[0]
    e_iy, d_iy = np.meshgrid(np.arange(n_iy), np.arange(n_iy), indexing="ij")
    p_iy_1 = float(M_iy[e_iy > d_iy].sum())
    p_iy_2 = float(M_iy[d_iy > e_iy].sum())
    p_iy_05_ust = float(M_iy[(e_iy + d_iy) >= 1].sum())
    p_iy_15_ust = float(M_iy[(e_iy + d_iy) >= 2].sum())
    p_iy_kg_var = float(M_iy[(e_iy > 0) & (d_iy > 0)].sum())

    n_2y = M_2y.shape[0]
    e_2y, d_2y = np.meshgrid(np.arange(n_2y), np.arange(n_2y), indexing="ij")
    p_2y_15_ust = float(M_2y[(e_2y + d_2y) >= 2].sum())
    p_2y_kg_var = float(M_2y[(e_2y > 0) & (d_2y > 0)].sum())

    iy_ms_probs = {
        "iy/ms 1/1": 0.0, "iy/ms 1/x": 0.0, "iy/ms 1/2": 0.0,
        "iy/ms x/1": 0.0, "iy/ms x/x": 0.0, "iy/ms x/2": 0.0,
        "iy/ms 2/1": 0.0, "iy/ms 2/x": 0.0, "iy/ms 2/2": 0.0,
    }
    for i1 in range(n_iy):
        for j1 in range(n_iy):
            p1 = M_iy[i1, j1]
            if p1 < 1e-7:
                continue
            iy_etiket = "1" if i1 > j1 else ("2" if j1 > i1 else "x")
            for i2 in range(n_2y):
                for j2 in range(n_2y):
                    p_birlesik = p1 * M_2y[i2, j2]
                    ms_ev, ms_dep = i1 + i2, j1 + j2
                    ms_etiket = "1" if ms_ev > ms_dep else ("2" if ms_dep > ms_ev else "x")
                    iy_ms_probs[f"iy/ms {iy_etiket}/{ms_etiket}"] += float(p_birlesik)

    market_olasiliklari = {
        "maç sonu 1": float(M_ms[m_ms1].sum()),
        "maç sonu 2": float(M_ms[m_ms2].sum()),
        "1x çifte şans": float(M_ms[m_1x].sum()),
        "x2 çifte şans": float(M_ms[m_x2].sum()),
        "1.5 gol üst": float(M_ms[m_15_ust].sum()),
        "2.5 gol üst": float(M_ms[m_25_ust].sum()),
        "2.5 gol alt": float(M_ms[m_25_alt].sum()),
        "3.5 gol alt": float(M_ms[m_35_alt].sum()),
        "karşılıklı gol var": float(M_ms[m_kg_var].sum()),
        "kg yok": float(M_ms[m_kg_yok].sum()),
        "kg var ve 2.5 üst": float(M_ms[m_kg_var & m_25_ust].sum()),
        "ms 1 ve 1.5 üst": float(M_ms[m_ms1 & m_15_ust].sum()),
        "ms 2 ve 1.5 üst": float(M_ms[m_ms2 & m_15_ust].sum()),
        "1x ve 1.5 üst": float(M_ms[m_1x & m_15_ust].sum()),
        "x2 ve 1.5 üst": float(M_ms[m_x2 & m_15_ust].sum()),
        "ms 1 ve kg var": float(M_ms[m_ms1 & m_kg_var].sum()),
        "ms 2 ve kg var": float(M_ms[m_ms2 & m_kg_var].sum()),
        "ms 1 ve 2.5 üst": float(M_ms[m_ms1 & m_25_ust].sum()),
        "ms 2 ve 2.5 üst": float(M_ms[m_ms2 & m_25_ust].sum()),
        "ev 1.5 gol üst": float(M_ms[ev_idx >= 2].sum()),
        "dep 1.5 gol üst": float(M_ms[dep_idx >= 2].sum()),
        "ilk yarı 0.5 üst": p_iy_05_ust,
        "ilk yarı 1.5 üst": p_iy_15_ust,
        "ilk yarı kg var": p_iy_kg_var,
        "ilk yarı 1": p_iy_1,
        "ilk yarı 2": p_iy_2,
        "2. yarı 1.5 üst": p_2y_15_ust,
        "iki yarıda da kg var": p_iy_kg_var * p_2y_kg_var,
        **iy_ms_probs
    }
    return market_olasiliklari, M_ms


def ayni_mac_ortak_olasilik(M_ms: np.ndarray, bacak_1: str, bacak_2: str) -> float:
    n = M_ms.shape[0]
    ev, dep = np.meshgrid(np.arange(n), np.arange(n), indexing="ij")
    top = ev + dep

    def _maske(mkt: str) -> Optional[np.ndarray]:
        harita = {
            "maç sonu 1": ev > dep,
            "maç sonu 2": dep > ev,
            "1x çifte şans": ev >= dep,
            "x2 çifte şans": dep >= ev,
            "1.5 gol üst": top >= 2,
            "2.5 gol üst": top >= 3,
            "2.5 gol alt": top <= 2,
            "3.5 gol alt": top <= 3,
            "karşılıklı gol var": (ev > 0) & (dep > 0),
            "kg yok": (ev == 0) | (dep == 0),
            "ev 1.5 gol üst": ev >= 2,
            "dep 1.5 gol üst": dep >= 2,
        }
        return harita.get(mkt)

    m1, m2 = _maske(bacak_1), _maske(bacak_2)
    if m1 is not None and m2 is not None:
        return float(M_ms[m1 & m2].sum())
    return 0.0


def negatif_binom_ust_olasiligi(mu: float, alpha_disp: float, esik: float) -> float:
    mu = float(np.clip(mu, 0.5, 25.0))
    if alpha_disp <= 1e-5:
        return float(1.0 - poisson.cdf(int(np.floor(esik)), mu))
    r = 1.0 / alpha_disp
    p = r / (r + mu)
    return float(1.0 - nbinom.cdf(int(np.floor(esik)), r, p))


class BirlesikQuantModel:
    def __init__(self):
        self.ev_gol_model = Pipeline([("imp", SimpleImputer(strategy="median")), ("sc", StandardScaler()), ("reg", Ridge(alpha=25.0))])
        self.dep_gol_model = Pipeline([("imp", SimpleImputer(strategy="median")), ("sc", StandardScaler()), ("reg", Ridge(alpha=25.0))])
        self.korner_model = Pipeline([("imp", SimpleImputer(strategy="median")), ("sc", StandardScaler()), ("reg", Ridge(alpha=30.0))])
        self.kart_model = Pipeline([("imp", SimpleImputer(strategy="median")), ("sc", StandardScaler()), ("reg", Ridge(alpha=30.0))])

        self.alpha_korner: float = 0.08
        self.alpha_kart: float = 0.14
        self.dc_rho: float = -0.09

        self.ml_modeller: Dict[str, object] = {}
        self.ensemble_agirliklari: Dict[str, float] = {}

    @staticmethod
    def hedef_uret(df: pd.DataFrame, market: str) -> pd.Series:
        from guncelle import mac_sonucu_degerlendir
        sonuclar = []
        for r in df.to_dict("records"):
            anahtar = (int(r["id"]), market)
            if anahtar not in _HEDEF_ONBELLEK:
                res = mac_sonucu_degerlendir(market, r)
                if res == "KAZANDI":
                    _HEDEF_ONBELLEK[anahtar] = 1.0
                elif res == "KAYBETTI":
                    _HEDEF_ONBELLEK[anahtar] = 0.0
                else:
                    _HEDEF_ONBELLEK[anahtar] = np.nan
            sonuclar.append(_HEDEF_ONBELLEK[anahtar])
        return pd.Series(sonuclar, index=df.index)

    def fit(self, train_df: pd.DataFrame) -> None:
        ft_df = train_df[train_df["durum_temiz"].str.contains("FT", na=False)].copy()
        if len(ft_df) < 100:
            return

        split_idx = int(len(ft_df) * 0.80)
        tr_sub = ft_df.iloc[:split_idx]
        val_sub = ft_df.iloc[split_idx:]

        X_tr = tr_sub[OZELLIK_KOLONLARI]
        X_full = ft_df[OZELLIK_KOLONLARI]

        self.ev_gol_model.fit(X_tr, tr_sub["ev_skor"].values)
        self.dep_gol_model.fit(X_tr, tr_sub["dep_skor"].values)

        kor_mask_tr = tr_sub["ev_korner"].notna() & tr_sub["dep_korner"].notna()
        if kor_mask_tr.sum() >= 50:
            y_kor = (tr_sub.loc[kor_mask_tr, "ev_korner"] + tr_sub.loc[kor_mask_tr, "dep_korner"]).values
            self.korner_model.fit(X_tr.loc[kor_mask_tr], y_kor)
            mu_hat = np.clip(self.korner_model.predict(X_tr.loc[kor_mask_tr]), 4.0, 18.0)
            var_res = np.mean((y_kor - mu_hat) ** 2 - mu_hat)
            self.alpha_korner = float(np.clip(var_res / np.mean(mu_hat ** 2), 0.01, 0.35))

        kart_mask_tr = tr_sub["ev_sari"].notna() & tr_sub["dep_sari"].notna()
        if kart_mask_tr.sum() >= 50:
            y_kart = (tr_sub.loc[kart_mask_tr, "ev_sari"] + tr_sub.loc[kart_mask_tr, "dep_sari"]).values
            self.kart_model.fit(X_tr.loc[kart_mask_tr], y_kart)
            mu_k_hat = np.clip(self.kart_model.predict(X_tr.loc[kart_mask_tr]), 1.5, 10.0)
            var_k_res = np.mean((y_kart - mu_k_hat) ** 2 - mu_k_hat)
            self.alpha_kart = float(np.clip(var_k_res / np.mean(mu_k_hat ** 2), 0.02, 0.50))

        param_val_preds = self._parametrik_tahmin_coklu(val_sub)

        for mkt in ADAY_MARKETLER:
            y_tr_mkt = self.hedef_uret(tr_sub, mkt)
            y_val_mkt = self.hedef_uret(val_sub, mkt)
            gecerli_tr = y_tr_mkt.notna()
            gecerli_val = y_val_mkt.notna()

            if gecerli_tr.sum() < 80 or gecerli_val.sum() < 25:
                self.ensemble_agirliklari[mkt] = 1.0
                continue

            y_tr_clean = y_tr_mkt[gecerli_tr].values
            if len(np.unique(y_tr_clean)) < 2:
                self.ensemble_agirliklari[mkt] = 1.0
                continue

            clf_lr = Pipeline([
                ("imp", SimpleImputer(strategy="median")),
                ("sc", StandardScaler()),
                ("lr", LogisticRegression(C=0.2, max_iter=500))
            ])
            clf_lr.fit(X_tr.loc[gecerli_tr], y_tr_clean)
            p_ml_val = clf_lr.predict_proba(val_sub.loc[gecerli_val, OZELLIK_KOLONLARI])[:, 1]

            p_par_val = param_val_preds.loc[gecerli_val, mkt].values
            en_iyi_w, en_iyi_loss = 1.0, 999.0
            for w in np.linspace(0.0, 1.0, 11):
                p_karisim = np.clip(w * p_par_val + (1.0 - w) * p_ml_val, 1e-4, 1.0 - 1e-4)
                loss = log_loss(y_val_mkt[gecerli_val], p_karisim, labels=[0, 1])
                if loss < en_iyi_loss:
                    en_iyi_loss = loss
                    en_iyi_w = float(w)

            self.ensemble_agirliklari[mkt] = en_iyi_w

            y_full_mkt = self.hedef_uret(ft_df, mkt)
            gecerli_full = y_full_mkt.notna()
            if gecerli_full.sum() >= 80 and len(np.unique(y_full_mkt[gecerli_full])) == 2:
                clf_lr.fit(X_full.loc[gecerli_full], y_full_mkt[gecerli_full].values)
                self.ml_modeller[mkt] = clf_lr

        self.ev_gol_model.fit(X_full, ft_df["ev_skor"].values)
        self.dep_gol_model.fit(X_full, ft_df["dep_skor"].values)
        kor_full = ft_df["ev_korner"].notna() & ft_df["dep_korner"].notna()
        if kor_full.sum() >= 50:
            self.korner_model.fit(X_full.loc[kor_full], (ft_df.loc[kor_full, "ev_korner"] + ft_df.loc[kor_full, "dep_korner"]).values)
        kart_full = ft_df["ev_sari"].notna() & ft_df["dep_sari"].notna()
        if kart_full.sum() >= 50:
            self.kart_model.fit(X_full.loc[kart_full], (ft_df.loc[kart_full, "ev_sari"] + ft_df.loc[kart_full, "dep_sari"]).values)

    def _parametrik_tahmin_coklu(self, df: pd.DataFrame) -> pd.DataFrame:
        X = df[OZELLIK_KOLONLARI]
        lam_ev_reg = np.clip(self.ev_gol_model.predict(X), 0.25, 4.2)
        lam_dep_reg = np.clip(self.dep_gol_model.predict(X), 0.20, 3.8)

        lam_ev_vec = 0.55 * df["dc_lambda_ev"].values + 0.45 * lam_ev_reg
        lam_dep_vec = 0.55 * df["dc_lambda_dep"].values + 0.45 * lam_dep_reg

        try:
            mu_kor_vec = np.clip(self.korner_model.predict(X), 5.0, 16.0)
        except Exception:
            mu_kor_vec = df["lig_korner_ort"].values

        try:
            mu_kart_vec = np.clip(self.kart_model.predict(X), 2.0, 8.5)
        except Exception:
            mu_kart_vec = df["lig_sari_ort"].values

        kayitlar = []
        for idx, r in enumerate(df.to_dict("records")):
            gol_mkt, _ = skor_matrisinden_gol_marketlerini_turet(
                lam_ev_vec[idx], lam_dep_vec[idx], iy_payi=r.get("iy_gol_payi", 0.44), rho=self.dc_rho
            )
            mu_kor = 0.5 * mu_kor_vec[idx] + 0.5 * (r["ev_korner_lehe"] + r["dep_korner_lehe"])
            mu_kart = 0.5 * mu_kart_vec[idx] + 0.5 * (r["ev_sari_lehe"] + r["dep_sari_lehe"])

            gol_mkt["8.5 korner üst"] = negatif_binom_ust_olasiligi(mu_kor, self.alpha_korner, 8.5)
            gol_mkt["9.5 korner üst"] = negatif_binom_ust_olasiligi(mu_kor, self.alpha_korner, 9.5)
            gol_mkt["10.5 korner üst"] = negatif_binom_ust_olasiligi(mu_kor, self.alpha_korner, 10.5)
            gol_mkt["3.5 kart üst"] = negatif_binom_ust_olasiligi(mu_kart, self.alpha_kart, 3.5)
            gol_mkt["4.5 kart üst"] = negatif_binom_ust_olasiligi(mu_kart, self.alpha_kart, 4.5)
            gol_mkt["kırmızı kart çıkar"] = float(np.clip(r.get("baz_kirmizi_oran", 0.16), 0.05, 0.35))
            gol_mkt["penaltı olur"] = float(np.clip(r.get("baz_penalti_oran", 0.28), 0.10, 0.45))
            kayitlar.append(gol_mkt)

        return pd.DataFrame(kayitlar, index=df.index)

    def predict_proba_df(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[int, np.ndarray]]:
        param_df = self._parametrik_tahmin_coklu(df)
        X = df[OZELLIK_KOLONLARI]

        sonuc_df = param_df.copy()
        for mkt in ADAY_MARKETLER:
            w = self.ensemble_agirliklari.get(mkt, 1.0)
            if w < 1.0 and mkt in self.ml_modeller:
                p_ml = self.ml_modeller[mkt].predict_proba(X)[:, 1]
                sonuc_df[mkt] = np.clip(w * param_df[mkt].values + (1.0 - w) * p_ml, 1e-4, 1.0 - 1e-4)

        matrisler: Dict[int, np.ndarray] = {}
        for r in df.to_dict("records"):
            matrisler[int(r["id"])] = dixon_coles_matrisi_uret(r["dc_lambda_ev"], r["dc_lambda_dep"], rho=self.dc_rho)

        return sonuc_df, matrisler
