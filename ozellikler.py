# -*- coding: utf-8 -*-
"""
ozellikler.py
SIZINTI (LEAKAGE) KURALI:
Bir maçın (t) özelliklerini üretirken YALNIZCA tarih < t_mac olan ve durumu 'FT'
olan maçlar kullanılır. Aynı gün oynanan maçların bile birbirine sızmaması için
gün bazlı (strictly prior date) güncelleme kuyruğu uygulanır.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd


@dataclass
class TakimDurumu:
    """Bir takımın o ana kadarki üstel ağırlıklı (EWM) istatistik durumu."""
    elo: float = 1500.0
    son_mac_tarihi: Optional[pd.Timestamp] = None
    son_sezon: Optional[str] = None
    ev_mac_sayisi: int = 0
    dep_mac_sayisi: int = 0
    toplam_mac: int = 0

    # Üstel ağırlıklı ortalamalar (ev ve deplasman ayrı sözlüklerde)
    ev_ewm: Dict[str, float] = field(default_factory=dict)
    dep_ewm: Dict[str, float] = field(default_factory=dict)
    genel_ewm: Dict[str, float] = field(default_factory=dict)

    # Son 5 maç puan formu
    son_puanlar: List[float] = field(default_factory=list)


class OzellikMotoru:
    """
    Walk-forward, sızıntısız (leakage-free) özellik çıkarım motoru.
    - Yarı ömür (half-life) = 5.5 maç -> alpha = 1 - exp(-ln(2)/5.5) ≈ 0.1184
    - Empirical-Bayes Shrinkage: n / (n + k), k = 8.0 maç
    - Sezon geçişinde merkeze çekme (reversion): gamma = 0.65
    """

    ISTATISTIK_ANAHTARLARI = [
        "gol_atilan", "gol_yenen", "iy_gol_atilan", "iy_gol_yenen",
        "sut_lehe", "sut_aleyhe", "isabetli_lehe", "isabetli_aleyhe",
        "korner_lehe", "korner_aleyhe", "sari_lehe", "sari_aleyhe",
        "faul_lehe", "faul_aleyhe", "top_oynama", "pas_yuzde",
        "engel_sut_lehe", "kurtaris_lehe"
    ]

    VARSAYILAN_LIG_ORT = {
        "gol_atilan": 1.38, "gol_yenen": 1.38,
        "iy_gol_atilan": 0.61, "iy_gol_yenen": 0.61,
        "sut_lehe": 12.8, "sut_aleyhe": 12.8,
        "isabetli_lehe": 4.4, "isabetli_aleyhe": 4.4,
        "korner_lehe": 5.0, "korner_aleyhe": 5.0,
        "sari_lehe": 2.15, "sari_aleyhe": 2.15,
        "faul_lehe": 12.2, "faul_aleyhe": 12.2,
        "top_oynama": 0.50, "pas_yuzde": 0.81,
        "engel_sut_lehe": 3.4, "kurtaris_lehe": 2.9,
        "ev_avantaji_gol": 1.12,
        "kirmizi_oran": 0.16,
        "penalti_oran": 0.28
    }

    def __init__(self, yari_omur: float = 5.5, shrinkage_k: float = 8.0, sezon_reversion: float = 0.65):
        self.alpha = 1.0 - np.exp(-np.log(2.0) / yari_omur)
        self.shrinkage_k = shrinkage_k
        self.sezon_reversion = sezon_reversion
        self.takimlar: Dict[str, TakimDurumu] = {}
        self.lig_gecmisi: Dict[str, Dict[str, float]] = {}
        self.lig_mac_sayisi: Dict[str, int] = {}

    def _lig_ort_al(self, lig: str, stat: str) -> float:
        """Lig bazlı kümülatif ortalamayı (shrinkage ile global ortalamaya çekilmiş) döndürür."""
        n_lig = self.lig_mac_sayisi.get(lig, 0)
        global_val = self.VARSAYILAN_LIG_ORT[stat]
        if n_lig == 0 or lig not in self.lig_gecmisi or stat not in self.lig_gecmisi[lig]:
            return global_val
        lig_val = self.lig_gecmisi[lig][stat]
        w = n_lig / (n_lig + 30.0)
        return w * lig_val + (1.0 - w) * global_val

    def _sezon_gecisi_kontrol(self, t_durum: TakimDurumu, yeni_sezon: str, lig: str) -> None:
        """Sezon değiştiğinde Elo ve takım istatistiklerini kısmen lig ortalamasına sıfırlar."""
        if t_durum.son_sezon is not None and yeni_sezon != "-" and yeni_sezon != t_durum.son_sezon:
            g = self.sezon_reversion
            t_durum.elo = g * t_durum.elo + (1.0 - g) * 1500.0
            t_durum.ev_mac_sayisi = int(t_durum.ev_mac_sayisi * g)
            t_durum.dep_mac_sayisi = int(t_durum.dep_mac_sayisi * g)
            t_durum.toplam_mac = int(t_durum.toplam_mac * g)
            for st in self.ISTATISTIK_ANAHTARLARI:
                lo = self._lig_ort_al(lig, st)
                if st in t_durum.ev_ewm:
                    t_durum.ev_ewm[st] = g * t_durum.ev_ewm[st] + (1.0 - g) * lo
                if st in t_durum.dep_ewm:
                    t_durum.dep_ewm[st] = g * t_durum.dep_ewm[st] + (1.0 - g) * lo
                if st in t_durum.genel_ewm:
                    t_durum.genel_ewm[st] = g * t_durum.genel_ewm[st] + (1.0 - g) * lo
        if yeni_sezon != "-":
            t_durum.son_sezon = yeni_sezon

    def _shrikned_deger(self, t_durum: TakimDurumu, saha: str, stat: str, lig: str) -> float:
        """
        Empirical-Bayes Shrinkage:
        1) Saha (ev/dep) EWM'sini takımın genel EWM'si ile birleştirir.
        2) Takımın maç sayısına göre (n / (n + k)) lig ortalamasına çeker.
        """
        lig_ort = self._lig_ort_al(lig, stat)
        n_saha = t_durum.ev_mac_sayisi if saha == "ev" else t_durum.dep_mac_sayisi
        saha_dict = t_durum.ev_ewm if saha == "ev" else t_durum.dep_ewm

        genel_val = t_durum.genel_ewm.get(stat, lig_ort)
        saha_val = saha_dict.get(stat, genel_val)

        # Saha vs Genel karışımı
        w_saha = n_saha / (n_saha + 5.0)
        ham_tahmin = 0.65 * (w_saha * saha_val + (1.0 - w_saha) * genel_val) + 0.35 * genel_val

        # Empirical-Bayes lig ortalamasına çekme (Shrinkage)
        w_shrink = t_durum.toplam_mac / (t_durum.toplam_mac + self.shrinkage_k)
        return float(w_shrink * ham_tahmin + (1.0 - w_shrink) * lig_ort)

    def mac_ozelliklerini_cikar(self, mac: pd.Series) -> Dict[str, float]:
        """
        Tek bir maç için SADECE geçmiş duruma bakarak özellik vektörü üretir.
        (Maçın kendi istatistiklerine ASLA dokunmaz).
        """
        lig = str(mac["lig"])
        sezon = str(mac.get("sezon", "-"))
        tarih = mac["tarih"]
        ev_isim = str(mac["ev_sahibi"])
        dep_isim = str(mac["deplasman"])

        ev_d = self.takimlar.setdefault(ev_isim, TakimDurumu())
        dep_d = self.takimlar.setdefault(dep_isim, TakimDurumu())

        self._sezon_gecisi_kontrol(ev_d, sezon, lig)
        self._sezon_gecisi_kontrol(dep_d, sezon, lig)

        # Dinlenme günleri (tavan = 14 gün)
        ev_dinlenme = float(np.clip((tarih - ev_d.son_mac_tarihi).days, 2, 14)) if ev_d.son_mac_tarihi is not None else 7.0
        dep_dinlenme = float(np.clip((tarih - dep_d.son_mac_tarihi).days, 2, 14)) if dep_d.son_mac_tarihi is not None else 7.0

        lig_gol_ort = max(0.5, self._lig_ort_al(lig, "gol_atilan"))
        ev_avantaji = self._lig_ort_al(lig, "ev_avantaji_gol")

        # Shrink edilmiş gol beklentisi bileşenleri
        ev_atilan = self._shrikned_deger(ev_d, "ev", "gol_atilan", lig)
        ev_yenen = self._shrikned_deger(ev_d, "ev", "gol_yenen", lig)
        dep_atilan = self._shrikned_deger(dep_d, "dep", "gol_atilan", lig)
        dep_yenen = self._shrikned_deger(dep_d, "dep", "gol_yenen", lig)

        # Dixon-Coles tarzı normalize hücum (alpha) ve savunma (beta) güçleri
        ev_hucum_gucu = ev_atilan / lig_gol_ort
        ev_savunma_zaafi = ev_yenen / lig_gol_ort
        dep_hucum_gucu = dep_atilan / lig_gol_ort
        dep_savunma_zaafi = dep_yenen / lig_gol_ort

        # İY gol payı (shrinked)
        ev_iy_pay = np.clip(self._shrikned_deger(ev_d, "ev", "iy_gol_atilan", lig) / max(0.3, ev_atilan), 0.35, 0.55)
        dep_iy_pay = np.clip(self._shrikned_deger(dep_d, "dep", "iy_gol_atilan", lig) / max(0.3, dep_atilan), 0.35, 0.55)
        iy_gol_orani = float(0.5 * (ev_iy_pay + dep_iy_pay))

        # Elo farkı ve beklenen puan
        elo_fark = (ev_d.elo + 55.0) - dep_d.elo
        elo_p_ev = 1.0 / (1.0 + 10.0 ** (-elo_fark / 400.0))

        # Son 5 maç form ortalaması (puan / 3)
        ev_form = float(np.mean(ev_d.son_puanlar[-5:]) / 3.0) if ev_d.son_puanlar else 0.45
        dep_form = float(np.mean(dep_d.son_puanlar[-5:]) / 3.0) if dep_d.son_puanlar else 0.45

        ozellikler = {
            "fikstur_id": int(mac["id"]),
            "lig": lig,
            "ev_mac_sayisi": ev_d.toplam_mac,
            "dep_mac_sayisi": dep_d.toplam_mac,
            "min_mac_sayisi": min(ev_d.toplam_mac, dep_d.toplam_mac),
            "lig_gol_ort": lig_gol_ort,
            "ev_avantaji": ev_avantaji,
            "ev_hucum_gucu": ev_hucum_gucu,
            "ev_savunma_zaafi": ev_savunma_zaafi,
            "dep_hucum_gucu": dep_hucum_gucu,
            "dep_savunma_zaafi": dep_savunma_zaafi,
            "dc_lambda_ev": float(np.clip(lig_gol_ort * ev_hucum_gucu * dep_savunma_zaafi * np.sqrt(ev_avantaji), 0.25, 4.2)),
            "dc_lambda_dep": float(np.clip(lig_gol_ort * dep_hucum_gucu * ev_savunma_zaafi / np.sqrt(ev_avantaji), 0.20, 3.8)),
            "iy_gol_payi": iy_gol_orani,
            "elo_ev": ev_d.elo,
            "elo_dep": dep_d.elo,
            "elo_fark": elo_fark,
            "elo_p_ev": elo_p_ev,
            "ev_dinlenme": ev_dinlenme,
            "dep_dinlenme": dep_dinlenme,
            "dinlenme_farki": ev_dinlenme - dep_dinlenme,
            "ev_form": ev_form,
            "dep_form": dep_form,
            # Şut ve İsabetli Şut
            "ev_sut_lehe": self._shrikned_deger(ev_d, "ev", "sut_lehe", lig),
            "ev_sut_aleyhe": self._shrikned_deger(ev_d, "ev", "sut_aleyhe", lig),
            "dep_sut_lehe": self._shrikned_deger(dep_d, "dep", "sut_lehe", lig),
            "dep_sut_aleyhe": self._shrikned_deger(dep_d, "dep", "sut_aleyhe", lig),
            "ev_isabetli_lehe": self._shrikned_deger(ev_d, "ev", "isabetli_lehe", lig),
            "ev_isabetli_aleyhe": self._shrikned_deger(ev_d, "ev", "isabetli_aleyhe", lig),
            "dep_isabetli_lehe": self._shrikned_deger(dep_d, "dep", "isabetli_lehe", lig),
            "dep_isabetli_aleyhe": self._shrikned_deger(dep_d, "dep", "isabetli_aleyhe", lig),
            # Korner (Lehe / Aleyhe)
            "ev_korner_lehe": self._shrikned_deger(ev_d, "ev", "korner_lehe", lig),
            "ev_korner_aleyhe": self._shrikned_deger(ev_d, "ev", "korner_aleyhe", lig),
            "dep_korner_lehe": self._shrikned_deger(dep_d, "dep", "korner_lehe", lig),
            "dep_korner_aleyhe": self._shrikned_deger(dep_d, "dep", "korner_aleyhe", lig),
            "lig_korner_ort": self._lig_ort_al(lig, "korner_lehe") * 2.0,
            # Kart ve Faul
            "ev_sari_lehe": self._shrikned_deger(ev_d, "ev", "sari_lehe", lig),
            "ev_sari_aleyhe": self._shrikned_deger(ev_d, "ev", "sari_aleyhe", lig),
            "dep_sari_lehe": self._shrikned_deger(dep_d, "dep", "sari_lehe", lig),
            "dep_sari_aleyhe": self._shrikned_deger(dep_d, "dep", "sari_aleyhe", lig),
            "ev_faul_lehe": self._shrikned_deger(ev_d, "ev", "faul_lehe", lig),
            "dep_faul_lehe": self._shrikned_deger(dep_d, "dep", "faul_lehe", lig),
            "lig_sari_ort": self._lig_ort_al(lig, "sari_lehe") * 2.0,
            # Top Oynama, Pas %, Engellenen Şut, Kurtarış
            "ev_top_oynama": self._shrikned_deger(ev_d, "ev", "top_oynama", lig),
            "dep_top_oynama": self._shrikned_deger(dep_d, "dep", "top_oynama", lig),
            "ev_pas_yuzde": self._shrikned_deger(ev_d, "ev", "pas_yuzde", lig),
            "dep_pas_yuzde": self._shrikned_deger(dep_d, "dep", "pas_yuzde", lig),
            "ev_engel_sut": self._shrikned_deger(ev_d, "ev", "engel_sut_lehe", lig),
            "dep_engel_sut": self._shrikned_deger(dep_d, "dep", "engel_sut_lehe", lig),
            "ev_kurtaris": self._shrikned_deger(ev_d, "ev", "kurtaris_lehe", lig),
            "dep_kurtaris": self._shrikned_deger(dep_d, "dep", "kurtaris_lehe", lig),
            # Nadir olaylar (Yalnızca lig baz oranı)
            "baz_kirmizi_oran": self._lig_ort_al(lig, "kirmizi_oran"),
            "baz_penalti_oran": self._lig_ort_al(lig, "penalti_oran"),
        }
        return ozellikler

    def mac_sonucuyla_guncelle(self, mac: pd.Series) -> None:
        """
        Tamamlanmış ('FT') bir maçın istatistikleriyle takım ve lig durumlarını günceller.
        Bu fonksiyon SADECE maçın tahmin özellikleri çıkarıldıktan SONRA çağrılır.
        """
        if "FT" not in str(mac.get("durum_temiz", "")):
            return
        if pd.isna(mac.get("ev_skor")) or pd.isna(mac.get("dep_skor")):
            return

        lig = str(mac["lig"])
        tarih = mac["tarih"]
        ev_isim = str(mac["ev_sahibi"])
        dep_isim = str(mac["deplasman"])

        ev_d = self.takimlar.setdefault(ev_isim, TakimDurumu())
        dep_d = self.takimlar.setdefault(dep_isim, TakimDurumu())

        ev_gol = float(mac["ev_skor"])
        dep_gol = float(mac["dep_skor"])

        # Elo Güncellemesi (K=20, gol farkı çarpanlı)
        elo_fark = (ev_d.elo + 55.0) - dep_d.elo
        beklenen_ev = 1.0 / (1.0 + 10.0 ** (-elo_fark / 400.0))
        gercek_ev = 1.0 if ev_gol > dep_gol else (0.5 if ev_gol == dep_gol else 0.0)
        gol_carpani = np.log(abs(ev_gol - dep_gol) + 1.0) * 0.7 + 0.8
        delta = 20.0 * gol_carpani * (gercek_ev - beklenen_ev)
        ev_d.elo += delta
        dep_d.elo -= delta

        # Son puanlar
        ev_d.son_puanlar.append(3.0 if ev_gol > dep_gol else (1.0 if ev_gol == dep_gol else 0.0))
        dep_d.son_puanlar.append(3.0 if dep_gol > ev_gol else (1.0 if ev_gol == dep_gol else 0.0))

        # EWM Güncelleme yardımcı fonksiyonu (NaN değerleri atlar, sahte sıfır sokmaz!)
        def _ewm_guncelle(sozluk: Dict[str, float], anahtar: str, deger: float):
            if pd.isna(deger):
                return
            if anahtar not in sozluk:
                sozluk[anahtar] = float(deger)
            else:
                sozluk[anahtar] = self.alpha * float(deger) + (1.0 - self.alpha) * sozluk[anahtar]

        ev_obs = {
            "gol_atilan": ev_gol, "gol_yenen": dep_gol,
            "iy_gol_atilan": mac.get("ev_iy_skor"), "iy_gol_yenen": mac.get("dep_iy_skor"),
            "sut_lehe": mac.get("ev_sut"), "sut_aleyhe": mac.get("dep_sut"),
            "isabetli_lehe": mac.get("ev_isabetli"), "isabetli_aleyhe": mac.get("dep_isabetli"),
            "korner_lehe": mac.get("ev_korner"), "korner_aleyhe": mac.get("dep_korner"),
            "sari_lehe": mac.get("ev_sari"), "sari_aleyhe": mac.get("dep_sari"),
            "faul_lehe": mac.get("ev_faul"), "faul_aleyhe": mac.get("dep_faul"),
            "top_oynama": mac.get("ev_top_oynama"), "pas_yuzde": mac.get("ev_pas_yuzde"),
            "engel_sut_lehe": mac.get("ev_engel_sut"), "kurtaris_lehe": mac.get("ev_kurtaris")
        }
        dep_obs = {
            "gol_atilan": dep_gol, "gol_yenen": ev_gol,
            "iy_gol_atilan": mac.get("dep_iy_skor"), "iy_gol_yenen": mac.get("ev_iy_skor"),
            "sut_lehe": mac.get("dep_sut"), "sut_aleyhe": mac.get("ev_sut"),
            "isabetli_lehe": mac.get("dep_isabetli"), "isabetli_aleyhe": mac.get("ev_isabetli"),
            "korner_lehe": mac.get("dep_korner"), "korner_aleyhe": mac.get("ev_korner"),
            "sari_lehe": mac.get("dep_sari"), "sari_aleyhe": mac.get("ev_sari"),
            "faul_lehe": mac.get("dep_faul"), "faul_aleyhe": mac.get("ev_faul"),
            "top_oynama": mac.get("dep_top_oynama"), "pas_yuzde": mac.get("dep_pas_yuzde"),
            "engel_sut_lehe": mac.get("dep_engel_sut"), "kurtaris_lehe": mac.get("dep_kurtaris")
        }

        for k, v in ev_obs.items():
            _ewm_guncelle(ev_d.ev_ewm, k, v)
            _ewm_guncelle(ev_d.genel_ewm, k, v)
        for k, v in dep_obs.items():
            _ewm_guncelle(dep_d.dep_ewm, k, v)
            _ewm_guncelle(dep_d.genel_ewm, k, v)

        ev_d.ev_mac_sayisi += 1
        ev_d.toplam_mac += 1
        ev_d.son_mac_tarihi = tarih

        dep_d.dep_mac_sayisi += 1
        dep_d.toplam_mac += 1
        dep_d.son_mac_tarihi = tarih

        # Lig kümülatif ortalamalarını güncelle
        lig_dict = self.lig_gecmisi.setdefault(lig, {})
        n_l = self.lig_mac_sayisi.get(lig, 0)
        for k in self.ISTATISTIK_ANAHTARLARI:
            v1, v2 = ev_obs.get(k), dep_obs.get(k)
            if pd.notna(v1) and pd.notna(v2):
                ort_mac = 0.5 * (float(v1) + float(v2))
                lig_dict[k] = (lig_dict.get(k, ort_mac) * n_l + ort_mac) / (n_l + 1)

        if dep_gol >= 0 and ev_gol >= 0:
            ev_av = ( ev_gol + 0.1 ) / ( dep_gol + 0.1 )
            ev_av_clipped = float(np.clip(ev_av, 0.5, 2.0))
            lig_dict["ev_avantaji_gol"] = (lig_dict.get("ev_avantaji_gol", 1.12) * n_l + ev_av_clipped) / (n_l + 1)

        if pd.notna(mac.get("ev_kirmizi")) and pd.notna(mac.get("dep_kirmizi")):
            kirmizi_cikti = 1.0 if (mac["ev_kirmizi"] + mac["dep_kirmizi"]) >= 1 else 0.0
            lig_dict["kirmizi_oran"] = (lig_dict.get("kirmizi_oran", 0.16) * n_l + kirmizi_cikti) / (n_l + 1)

        if pd.notna(mac.get("ev_pen_etkin")) and pd.notna(mac.get("dep_pen_etkin")):
            pen_oldu = 1.0 if (mac["ev_pen_etkin"] + mac["dep_pen_etkin"]) >= 1 else 0.0
            lig_dict["penalti_oran"] = (lig_dict.get("penalti_oran", 0.28) * n_l + pen_oldu) / (n_l + 1)

        self.lig_mac_sayisi[lig] = n_l + 1


def tum_veri_icin_ozellik_uret(df: pd.DataFrame) -> pd.DataFrame:
    """
    Kronolojik sırada her tarih grubu için:
    1) O günkü tüm maçların özelliklerini SADECE önceki günlerin verisiyle çıkarır.
    2) Gün bittikten sonra o günün FT maçlarını motora işler (Sıfır sızıntı garantisi).
    """
    motor = OzellikMotoru()
    ozellik_listesi = []

    for tarih, gun_grubu in df.groupby("tarih", sort=True):
        # 1. Önce o günkü tüm maçların özelliklerini çıkar
        for _, mac in gun_grubu.iterrows():
            oz = motor.mac_ozelliklerini_cikar(mac)
            ozellik_listesi.append(oz)
        # 2. Sonra o günün tamamlanmış maçlarıyla durumu güncelle
        for _, mac in gun_grubu.iterrows():
            motor.mac_sonucuyla_guncelle(mac)

    oz_df = pd.DataFrame(ozellik_listesi)
    birlesik = pd.merge(df, oz_df, left_on="id", right_on="fikstur_id", how="left", suffixes=("", "_oz"))
    return birlesik
