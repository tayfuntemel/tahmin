# -*- coding: utf-8 -*-
import json
import logging
from typing import Dict, Tuple
import numpy as np
import pandas as pd
from sklearn.metrics import log_loss, brier_score_loss

from model import BirlesikQuantModel, ADAY_MARKETLER
from kalibrasyon import MarketKalibratoru, ece_hesapla

logger = logging.getLogger("backtest")


def bootstrap_fark_guven_araligi(
    loss_baz: np.ndarray,
    loss_model: np.ndarray,
    n_boot: int = 1000,
    seed: int = 42
) -> Tuple[float, float, float]:
    rng = np.random.default_rng(seed)
    fark = loss_baz - loss_model
    n = len(fark)
    if n < 20:
        return 0.0, -1.0, 1.0
    idx = rng.integers(0, n, size=(n_boot, n))
    boot_ort = np.mean(fark[idx], axis=1)
    ort = float(np.mean(fark))
    ci_low, ci_high = np.percentile(boot_ort, [2.5, 97.5])
    return ort, float(ci_low), float(ci_high)


def walk_forward_backtest_calistir(
    df_ozellikli: pd.DataFrame,
    marketler_json_yolu: str = "marketler.json"
) -> Tuple[pd.DataFrame, Dict[str, MarketKalibratoru], Dict[str, dict]]:
    ft_df = df_ozellikli[df_ozellikli["durum_temiz"].str.contains("FT", na=False)].copy()
    ft_df = ft_df.sort_values(["tarih", "id"]).reset_index(drop=True)

    n_toplam = len(ft_df)
    isinma_bitis = int(n_toplam * 0.25)
    holdout_baslangic = int(n_toplam * 0.80)

    # Hızlı çalışması için 2 haftalık pencerelerle walk-forward yeniden eğitim
    ft_df["donem_kodu"] = (ft_df.index // 90)
    donemler = ft_df.iloc[isinma_bitis:]["donem_kodu"].unique()

    oof_tahmin_listesi = []
    model = BirlesikQuantModel()

    for donem in donemler:
        donem_mask = ft_df["donem_kodu"] == donem
        ilk_idx = ft_df[donem_mask].index.min()

        egitim_kesiti = ft_df.iloc[:ilk_idx]
        test_kesiti = ft_df.loc[donem_mask]

        model.fit(egitim_kesiti)
        preds_df, _ = model.predict_proba_df(test_kesiti)
        preds_df["id"] = test_kesiti["id"].values
        preds_df["tarih"] = test_kesiti["tarih"].values
        preds_df["lig"] = test_kesiti["lig"].values
        preds_df["bolum"] = np.where(test_kesiti.index >= holdout_baslangic, "HOLDOUT_TEST", "VAL_OOF")
        oof_tahmin_listesi.append(preds_df)

    oof_full = pd.concat(oof_tahmin_listesi, axis=0)
    degerlendirme_df = pd.merge(ft_df.iloc[isinma_bitis:], oof_full, on=["id", "tarih", "lig"], how="inner")

    val_df = degerlendirme_df[degerlendirme_df["bolum"] == "VAL_OOF"].copy()
    holdout_df = degerlendirme_df[degerlendirme_df["bolum"] == "HOLDOUT_TEST"].copy()

    kalibratorler: Dict[str, MarketKalibratoru] = {}
    market_kararlari: Dict[str, dict] = {}
    rapor_satirlari = []

    for mkt in ADAY_MARKETLER:
        y_val = BirlesikQuantModel.hedef_uret(val_df, mkt).values
        p_val_ham = val_df[mkt].values
        g_val = (~np.isnan(y_val)) & (~np.isnan(p_val_ham))

        if g_val.sum() < 100:
            market_kararlari[mkt] = {"aktif": False, "sebep": "Yetersiz örneklem (<100)"}
            continue

        kal = MarketKalibratoru(mkt)
        p_val_cal = np.zeros_like(p_val_ham)
        orta = int(g_val.sum() * 0.5)
        gecerli_idx = np.where(g_val)[0]
        idx1, idx2 = gecerli_idx[:orta], gecerli_idx[orta:]

        kal1, kal2 = MarketKalibratoru(mkt), MarketKalibratoru(mkt)
        kal1.fit(p_val_ham[idx1], y_val[idx1])
        kal2.fit(p_val_ham[idx2], y_val[idx2])
        p_val_cal[idx2] = kal1.transform(p_val_ham[idx2])
        p_val_cal[idx1] = kal2.transform(p_val_ham[idx1])

        kal.fit(p_val_ham[g_val], y_val[g_val])
        kalibratorler[mkt] = kal

        y_seri = pd.Series(y_val)
        lig_baz_val = (
            val_df.assign(_y=y_seri)
            .groupby("lig")["_y"]
            .transform(lambda s: s.expanding().mean().shift(1))
            .fillna(float(np.nanmean(y_val[g_val])))
            .values
        )
        lig_baz_val = np.clip(lig_baz_val, 0.01, 0.99)

        yv = y_val[g_val]
        pv = np.clip(p_val_cal[g_val], 1e-4, 1.0 - 1e-4)
        bv = np.clip(lig_baz_val[g_val], 1e-4, 1.0 - 1e-4)

        ll_vec_model = -(yv * np.log(pv) + (1.0 - yv) * np.log(1.0 - pv))
        ll_vec_baz = -(yv * np.log(bv) + (1.0 - yv) * np.log(1.0 - bv))

        fark_ort, ci_low, ci_high = bootstrap_fark_guven_araligi(ll_vec_baz, ll_vec_model)
        brier_m = float(brier_score_loss(yv, pv))
        brier_b = float(brier_score_loss(yv, bv))
        ece_val, rel_bins = ece_hesapla(yv, pv)

        y_ho = BirlesikQuantModel.hedef_uret(holdout_df, mkt).values
        p_ho_ham = holdout_df[mkt].values
        g_ho = (~np.isnan(y_ho)) & (~np.isnan(p_ho_ham))
        if g_ho.sum() >= 30:
            yh = y_ho[g_ho]
            ph = kal.transform(p_ho_ham[g_ho])
            bh = np.full_like(ph, float(np.mean(yv)))
            ho_ll_m = float(log_loss(yh, ph, labels=[0, 1]))
            ho_ll_b = float(log_loss(yh, bh, labels=[0, 1]))
            ho_brier_m = float(brier_score_loss(yh, ph))
            ho_ece, _ = ece_hesapla(yh, ph)
        else:
            ho_ll_m, ho_ll_b, ho_brier_m, ho_ece = np.nan, np.nan, np.nan, np.nan

        aktif = bool((g_val.sum() >= 300) and (ci_low > 0.0) and (brier_m < brier_b) and (ece_val <= 0.045))
        baz_oran = float(np.mean(yv))
        min_guven = float(np.clip(max(0.32, baz_oran + 0.05), 0.32, 0.68))

        karar_bilgisi = {
            "aktif": aktif,
            "orneklem_val": int(g_val.sum()),
            "orneklem_holdout": int(g_ho.sum()),
            "baz_oran": round(baz_oran, 4),
            "val_log_loss": round(float(np.mean(ll_vec_model)), 4),
            "baz_log_loss": round(float(np.mean(ll_vec_baz)), 4),
            "delta_ll_ort": round(fark_ort, 4),
            "delta_ll_ci95": [round(ci_low, 4), round(ci_high, 4)],
            "val_brier": round(brier_m, 4),
            "baz_brier": round(brier_b, 4),
            "val_ece": round(ece_val, 4),
            "holdout_log_loss": round(ho_ll_m, 4) if not np.isnan(ho_ll_m) else None,
            "holdout_baz_ll": round(ho_ll_b, 4) if not np.isnan(ho_ll_b) else None,
            "holdout_brier": round(ho_brier_m, 4) if not np.isnan(ho_brier_m) else None,
            "holdout_ece": round(ho_ece, 4) if not np.isnan(ho_ece) else None,
            "min_orneklem_esigi": 300,
            "min_guven_olasiligi": round(min_guven, 3),
            "min_edge": 0.045,
            "kalibrasyon_yontemi": kal.yontem
        }
        market_kararlari[mkt] = karar_bilgisi
        rapor_satirlari.append({"market": mkt, **karar_bilgisi})

    with open(marketler_json_yolu, "w", encoding="utf-8") as f:
        json.dump(market_kararlari, f, ensure_ascii=False, indent=2)

    return pd.DataFrame(rapor_satirlari), kalibratorler, market_kararlari
