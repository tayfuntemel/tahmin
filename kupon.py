# -*- coding: utf-8 -*-
from itertools import combinations
from typing import Dict, List, Optional
import numpy as np
import pandas as pd

from model import ayni_mac_ortak_olasilik


def marj_arindir_shin(oranlar: List[float], yontem: str = "shin") -> List[float]:
    inv = np.array([1.0 / o for o in oranlar if o and o > 1.0], dtype=float)
    s = float(inv.sum())
    if s <= 1.0 or yontem == "orantili":
        return (inv / s).tolist()

    z = 0.02
    for _ in range(50):
        kokler = np.sqrt(z ** 2 + 4.0 * (1.0 - z) * (inv ** 2) / s)
        p_shin = (kokler - z) / (2.0 * (1.0 - z))
        fark = float(p_shin.sum() - 1.0)
        if abs(fark) < 1e-7:
            break
        z = float(np.clip(z + fark * 0.5, 1e-5, 0.30))
    return (p_shin / p_shin.sum()).tolist()


def clv_hesapla(alinan_oran: float, kapanis_orani: Optional[float]) -> Optional[float]:
    if not kapanis_orani or kapanis_orani <= 1.0 or not alinan_oran or alinan_oran <= 1.0:
        return None
    return float((alinan_oran / kapanis_orani) - 1.0)


def fractional_kelly_stake(
    p_kupon: float,
    toplam_oran: float,
    kelly_kesri: float = 0.25,
    max_stake_yuzde: float = 0.02
) -> float:
    b = toplam_oran - 1.0
    if b <= 0:
        return 0.0
    ev = p_kupon * toplam_oran - 1.0
    if ev <= 0:
        return 0.0
    tam_kelly = ev / b
    stake = tam_kelly * kelly_kesri
    return float(np.clip(stake, 0.0, max_stake_yuzde))


def kupon_olasiligi_hesapla(
    bacaklar: List[dict],
    skor_matrisleri: Dict[int, np.ndarray]
) -> float:
    mac_gruplari: Dict[int, List[dict]] = {}
    for b in bacaklar:
        mac_gruplari.setdefault(int(b["fikstur_id"]), []).append(b)

    p_toplam = 1.0
    for fid, grup in mac_gruplari.items():
        if len(grup) == 1:
            p_toplam *= float(grup[0]["p_kalibre"])
        elif len(grup) == 2:
            M = skor_matrisleri.get(fid)
            if M is None:
                return 0.0
            p_ortak = ayni_mac_ortak_olasilik(M, grup[0]["secilen_tahmin"], grup[1]["secilen_tahmin"])
            min_tekil = min(grup[0]["p_kalibre"], grup[1]["p_kalibre"])
            if p_ortak <= 0.0 or abs(p_ortak - min_tekil) < 0.01:
                return 0.0
            p_toplam *= p_ortak
        else:
            return 0.0

    return float(p_toplam)


def gunluk_kupon_olustur(
    gunun_maclari_df: pd.DataFrame,
    kalibre_olasiliklar_df: pd.DataFrame,
    skor_matrisleri: Dict[int, np.ndarray],
    market_kurallari: Dict[str, dict],
    mevcut_oranlar: Dict[int, Dict[str, Dict[str, float]]],
    min_edge: float = 0.045,
    hedef_min_oran: float = 4.00,
    min_bacak: int = 2,
    max_bacak: int = 4,
    gunluk_zarar_limiti_doldu: bool = False
) -> Dict[str, object]:
    if gunluk_zarar_limiti_doldu:
        return {"durum": "BUGÜN OYNAMA", "sebep": "Günlük/Haftalık maksimum zarar limiti doldu."}

    if gunun_maclari_df.empty:
        return {"durum": "BUGÜN OYNAMA", "sebep": "Bugün için uygun fikstür bulunamadı."}

    oran_var_mi = len(mevcut_oranlar) > 0
    aday_bacaklar = []

    for idx, mac in gunun_maclari_df.iterrows():
        fid = int(mac["id"])
        if int(mac.get("min_mac_sayisi", 0)) < 5:
            continue

        mac_oranlari = mevcut_oranlar.get(fid, {})

        for mkt, kural in market_kurallari.items():
            if not kural.get("aktif", False):
                continue

            p_kal = float(kalibre_olasiliklar_df.loc[idx, mkt])
            p_ham = float(mac.get(f"ham_{mkt}", p_kal))
            min_guven = float(kural.get("min_guven_olasiligi", 0.45))

            if p_kal < min_guven:
                continue

            adil_oran = round(1.0 / p_kal, 3)
            min_degerli_oran = round((1.0 + min_edge) / p_kal, 3)

            if oran_var_mi:
                mkt_oran_bilgi = mac_oranlari.get(mkt)
                if not mkt_oran_bilgi or not mkt_oran_bilgi.get("oran"):
                    continue
                gercek_oran = float(mkt_oran_bilgi["oran"])
                edge = p_kal * gercek_oran - 1.0
                if edge < min_edge:
                    continue

                aday_bacaklar.append({
                    "fikstur_id": fid,
                    "mac_adi": f"{mac['ev_sahibi']} - {mac['deplasman']}",
                    "lig": mac["lig"],
                    "tarih": str(mac["tarih"].date()),
                    "secilen_tahmin": mkt,
                    "p_model": round(p_ham, 4),
                    "p_kalibre": round(p_kal, 4),
                    "adil_oran": adil_oran,
                    "min_degerli_oran": min_degerli_oran,
                    "oran": gercek_oran,
                    "edge": round(edge, 4),
                    "mod": "GERCEK_ORAN"
                })
            else:
                aday_bacaklar.append({
                    "fikstur_id": fid,
                    "mac_adi": f"{mac['ev_sahibi']} - {mac['deplasman']}",
                    "lig": mac["lig"],
                    "tarih": str(mac["tarih"].date()),
                    "secilen_tahmin": mkt,
                    "p_model": round(p_ham, 4),
                    "p_kalibre": round(p_kal, 4),
                    "adil_oran": adil_oran,
                    "min_degerli_oran": min_degerli_oran,
                    "oran": adil_oran,
                    "edge": 0.0,
                    "mod": "ADIL_ORAN"
                })

    if len(aday_bacaklar) < min_bacak:
        return {
            "durum": "BUGÜN OYNAMA",
            "mod": "GERCEK_ORAN" if oran_var_mi else "ADIL_ORAN",
            "sebep": f"Yeterli sayıda değerli bacak bulunamadı (Bulunan aday: {len(aday_bacaklar)})."
        }

    aday_bacaklar = sorted(
        aday_bacaklar,
        key=lambda x: (x["edge"], x["p_kalibre"]),
        reverse=True
    )[:18]

    en_iyi_kupon = None
    uygun_kombinasyon_sayisi = 0

    for r in range(min_bacak, min(max_bacak, len(aday_bacaklar)) + 1):
        for komb in combinations(aday_bacaklar, r):
            fids = [b["fikstur_id"] for b in komb]
            if len(set(fids)) < len(fids):
                p_kup = kupon_olasiligi_hesapla(list(komb), skor_matrisleri)
            else:
                p_kup = float(np.prod([b["p_kalibre"] for b in komb]))

            if p_kup <= 0.0:
                continue

            if oran_var_mi:
                toplam_oran = float(np.prod([b["oran"] for b in komb]))
            else:
                toplam_oran = float(np.prod([b["adil_oran"] for b in komb]))

            if toplam_oran < hedef_min_oran or toplam_oran > 12.0:
                continue

            uygun_kombinasyon_sayisi += 1
            ev_kupon = p_kup * toplam_oran - 1.0

            if oran_var_mi and ev_kupon <= 0.08:
                continue

            aday_skor = (round(ev_kupon, 4), round(p_kup, 4))
            mevcut_skor = (round(en_iyi_kupon["ev_kupon"], 4), round(en_iyi_kupon["p_kupon"], 4)) if en_iyi_kupon else (-999.0, -999.0)

            if aday_skor > mevcut_skor:
                stake = fractional_kelly_stake(p_kup, toplam_oran) if oran_var_mi else 0.0
                en_iyi_kupon = {
                    "durum": "KUPON_HAZIR" if oran_var_mi else "ADIL_ORAN_ONERISI_ORAN_GIRINIZ",
                    "mod": "GERCEK_ORAN" if oran_var_mi else "ADIL_ORAN",
                    "uyari": None if oran_var_mi else (
                        "Veritabanında gerçek oran bulunamadı! Kupon ADİL ORAN (1/p) modunda üretildi. "
                        "Oynamadan önce her bacağın büro oranının `min_degerli_oran` değerinden yüksek olduğunu doğrulayın."
                    ),
                    "bacak_sayisi": r,
                    "toplam_oran": round(toplam_oran, 3),
                    "adil_kupon_orani": round(1.0 / p_kup, 3),
                    "p_kupon": round(p_kup, 4),
                    "ev_kupon": round(ev_kupon, 4),
                    "onerilen_stake_yuzde": round(stake * 100.0, 2),
                    "incelenen_uygun_kombinasyon": uygun_kombinasyon_sayisi,
                    "bacaklar": list(komb)
                }

    if en_iyi_kupon is None:
        return {
            "durum": "BUGÜN OYNAMA",
            "mod": "GERCEK_ORAN" if oran_var_mi else "ADIL_ORAN",
            "sebep": f"Toplam oranı >= {hedef_min_oran} ve pozitif EV sağlayan kombinasyon bulunamadı."
        }

    return en_iyi_kupon
