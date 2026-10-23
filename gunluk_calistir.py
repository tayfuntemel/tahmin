# -*- coding: utf-8 -*-
import os
import json
import uuid
import logging
from datetime import datetime
import pandas as pd

from veri_hazirla import (
    db_baglantisi_al,
    sema_kontrol_ve_olustur,
    fikstur_verisini_yukle_ve_temizle,
    oranlari_getir
)
from ozellikler import tum_veri_icin_ozellik_uret
from model import BirlesikQuantModel, ADAY_MARKETLER
from kalibrasyon import CanliKalibrasyonIzleyici
from backtest import walk_forward_backtest_calistir
from kupon import gunluk_kupon_olustur

logger = logging.getLogger("gunluk_calistir")


def clv_guncelle(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE `tahminler` t
            JOIN `oranlar` o ON o.fikstur_id = t.fikstur_id AND o.secilen_tahmin = t.secilen_tahmin
            SET t.kapanis_orani = o.kapanis_orani,
                t.clv = (t.oran / o.kapanis_orani) - 1.0
            WHERE o.kapanis_orani IS NOT NULL AND o.kapanis_orani > 1.0
              AND t.oran IS NOT NULL AND t.clv IS NULL
        """)


def kuponu_veritabanina_yaz(conn, kupon_sonucu: dict) -> None:
    if kupon_sonucu.get("durum") not in ("KUPON_HAZIR", "ADIL_ORAN_ONERISI_ORAN_GIRINIZ"):
        logger.info(f"Veritabanına kupon yazılmadı. Durum: {kupon_sonucu.get('durum')} - {kupon_sonucu.get('sebep')}")
        return

    kupon_id = f"KUP_{datetime.now().strftime('%Y%m%d')}_{uuid.uuid4().hex[:6].upper()}"
    stake_yuzde = float(kupon_sonucu.get("onerilen_stake_yuzde", 0.0)) / 100.0
    mod = kupon_sonucu.get("mod", "ADIL_ORAN")

    sql = """
        INSERT INTO `tahminler`
        (`fikstur_id`, `secilen_tahmin`, `sonuc`, `oran`, `olasilik`, `p_model`, `p_kalibre`, `edge`, `kupon_id`, `stake_yuzde`, `mod`, `tarih`)
        VALUES (%s, %s, 'BEKLIYOR', %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        for b in kupon_sonucu["bacaklar"]:
            cur.execute(
                "SELECT id FROM `tahminler` WHERE fikstur_id = %s AND secilen_tahmin = %s",
                (b["fikstur_id"], b["secilen_tahmin"])
            )
            if cur.fetchone() is None:
                cur.execute(sql, (
                    b["fikstur_id"],
                    b["secilen_tahmin"],
                    b["oran"],
                    b["p_kalibre"],
                    b["p_model"],
                    b["p_kalibre"],
                    b["edge"],
                    kupon_id,
                    stake_yuzde,
                    mod,
                    b["tarih"]
                ))
    logger.info(f"Kupon ({kupon_id}) başarıyla `tahminler` tablosuna kaydedildi.")


def ana_akis(haftalik_yeniden_egit: bool = False) -> dict:
    conn = db_baglantisi_al()
    try:
        sema_kontrol_ve_olustur(conn)
        clv_guncelle(conn)

        df_temiz = fikstur_verisini_yukle_ve_temizle(conn)
        df_ozellik = tum_veri_icin_ozellik_uret(df_temiz)

        marketler_json = "marketler.json"
        logger.info("Walk-forward backtest ve kalibratör eğitimi çalıştırılıyor...")
        _, kalibratorler, market_kurallari = walk_forward_backtest_calistir(df_ozellik, marketler_json)

        izleyici = CanliKalibrasyonIzleyici(pencere=100)
        canli_saglik = izleyici.market_saglik_kontrolu(conn)
        for mkt, durum in canli_saglik.items():
            if durum.get("drift_alarmi") and mkt in market_kurallari:
                logger.warning(f"[DRIFT ALARMI] '{mkt}' marketi canlıda baz orandan kötüleşti, otomatik kapatılıyor!")
                market_kurallari[mkt]["aktif"] = False

        model = BirlesikQuantModel()
        model.fit(df_ozellik)

        bugun_str = datetime.now().strftime("%Y-%m-%d")
        aday_maclar = df_ozellik[
            (~df_ozellik["durum_temiz"].str.contains("FT|CANCELED|POSTPONED", na=False)) &
            (df_ozellik["tarih"] >= pd.to_datetime(bugun_str))
        ].copy()

        if aday_maclar.empty:
            sonuc = {"durum": "BUGÜN OYNAMA", "sebep": "Planlanmış (Scheduled) maç bulunamadı."}
            print(json.dumps(sonuc, ensure_ascii=False, indent=2))
            return sonuc

        ham_olasiliklar_df, skor_matrisleri = model.predict_proba_df(aday_maclar)
        kalibre_df = ham_olasiliklar_df.copy()

        for mkt in ADAY_MARKETLER:
            aday_maclar[f"ham_{mkt}"] = ham_olasiliklar_df[mkt].values
            if mkt in kalibratorler:
                kalibre_df[mkt] = kalibratorler[mkt].transform(ham_olasiliklar_df[mkt].values)

        fids = aday_maclar["id"].astype(int).tolist()
        mevcut_oranlar = oranlari_getir(conn, fids)

        kupon_sonucu = gunluk_kupon_olustur(
            gunun_maclari_df=aday_maclar,
            kalibre_olasiliklar_df=kalibre_df,
            skor_matrisleri=skor_matrisleri,
            market_kurallari=market_kurallari,
            mevcut_oranlar=mevcut_oranlar
        )

        kuponu_veritabanina_yaz(conn, kupon_sonucu)
        print(json.dumps(kupon_sonucu, ensure_ascii=False, indent=2))
        return kupon_sonucu
    finally:
        conn.close()


if __name__ == "__main__":
    ana_akis()
