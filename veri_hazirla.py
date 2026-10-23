# -*- coding: utf-8 -*-
import os
import logging
from typing import Optional, Dict, List
import numpy as np
import pandas as pd
import pymysql
import pymysql.cursors

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("veri_hazirla")

CONFIG = {
    "db": {
        "host": os.environ.get("DB_HOST") or "netscout.fun",
        "user": os.environ.get("DB_USER") or "netscout_veri",
        "password": os.environ.get("DB_PASSWORD") or "i.34temel1",
        "database": os.environ.get("DB_NAME") or "netscout_veri",
        "port": int(os.environ.get("DB_PORT") or 3306)
    }
}

LIG_NORMALIZASYON = {
    "ENGLISH-PREMIER-LEAGUE": "ENGLISH-PREMIER-LEAGUE",
    "SPANISH-LALIGA": "LALIGA",
    "LALIGA": "LALIGA",
    "GERMAN-BUNDESLIGA": "GERMAN-BUNDESLIGA",
    "ITALIAN-SERIE-A": "ITALIAN-SERIE-A",
    "FRENCH-LIGUE-1": "LIGUE-1",
    "LIGUE-1": "LIGUE-1",
    "TURKISH-SUPER-LIG": "TURKISH-SUPER-LIG",
    "PORTUGUESE-PRIMEIRA-LIGA": "PORTUGUESE-PRIMEIRA-LIGA",
    "PRIMEIRA-LIGA": "PORTUGUESE-PRIMEIRA-LIGA",
    "BRASILEIRO-SERIE-A": "BRASILEIRO-SERIE-A",
    "DUTCH-EREDIVISIE": "EREDIVISIE",
    "EREDIVISIE": "EREDIVISIE",
}

SAYISAL_KOLONLAR = [
    "ev_skor", "dep_skor", "ev_iy_skor", "dep_iy_skor",
    "ev_top_oynama", "dep_top_oynama", "ev_sut", "dep_sut",
    "ev_isabetli", "dep_isabetli", "ev_korner", "dep_korner",
    "ev_faul", "dep_faul", "ev_sari", "dep_sari",
    "ev_kirmizi", "dep_kirmizi", "ev_ofsayt", "dep_ofsayt",
    "ev_kurtaris", "dep_kurtaris", "ev_sut_yuzde", "dep_sut_yuzde",
    "ev_pen_gol", "dep_pen_gol", "ev_pen_sut", "dep_pen_sut",
    "ev_isabetli_pas", "dep_isabetli_pas", "ev_toplam_pas", "dep_toplam_pas",
    "ev_pas_yuzde", "dep_pas_yuzde", "ev_isabetli_orta", "dep_isabetli_orta",
    "ev_toplam_orta", "dep_toplam_orta", "ev_orta_yuzde", "dep_orta_yuzde",
    "ev_toplam_uzun", "dep_toplam_uzun", "ev_isabet_uzun", "dep_isabet_uzun",
    "ev_uzun_yuzde", "dep_uzun_yuzde", "ev_engel_sut", "dep_engel_sut",
    "ev_basarili_mud", "dep_basarili_mud", "ev_toplam_mud", "dep_toplam_mud",
    "ev_mud_yuzde", "dep_mud_yuzde", "ev_pas_arasi", "dep_pas_arasi",
    "ev_basarili_uzak", "dep_basarili_uzak", "ev_toplam_uzak", "dep_toplam_uzak"
]

DETAY_PAS_SAVUNMA_KOLONLARI = [
    "ev_isabetli_pas", "dep_isabetli_pas", "ev_toplam_pas", "dep_toplam_pas",
    "ev_pas_yuzde", "dep_pas_yuzde", "ev_isabetli_orta", "dep_isabetli_orta",
    "ev_toplam_orta", "dep_toplam_orta", "ev_orta_yuzde", "dep_orta_yuzde",
    "ev_toplam_uzun", "dep_toplam_uzun", "ev_isabet_uzun", "dep_isabet_uzun",
    "ev_uzun_yuzde", "dep_uzun_yuzde", "ev_basarili_mud", "dep_basarili_mud",
    "ev_toplam_mud", "dep_toplam_mud", "ev_mud_yuzde", "dep_mud_yuzde",
    "ev_pas_arasi", "dep_pas_arasi", "ev_basarili_uzak", "dep_basarili_uzak",
    "ev_toplam_uzak", "dep_toplam_uzak", "ev_ofsayt", "dep_ofsayt"
]

TUM_BOXSCORE_KOLONLARI = [
    c for c in SAYISAL_KOLONLAR
    if c not in ("ev_skor", "dep_skor", "ev_iy_skor", "dep_iy_skor")
]


def db_baglantisi_al() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=CONFIG["db"]["host"],
        user=CONFIG["db"]["user"],
        password=CONFIG["db"]["password"],
        database=CONFIG["db"]["database"],
        port=CONFIG["db"]["port"],
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor
    )


def sema_kontrol_ve_olustur(conn: pymysql.connections.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES LIKE 'tahminler'")
        tablo_var = cur.fetchone() is not None

        if not tablo_var:
            logger.info("`tahminler` tablosu bulunamadı, guncelle.py uyumlu oluşturuluyor...")
            cur.execute("""
                CREATE TABLE `tahminler` (
                    `id` INT(11) NOT NULL AUTO_INCREMENT,
                    `fikstur_id` INT(11) NOT NULL,
                    `secilen_tahmin` VARCHAR(100) NOT NULL,
                    `sonuc` VARCHAR(30) DEFAULT 'BEKLIYOR',
                    `oran` DECIMAL(6,3) DEFAULT NULL,
                    `kapanis_orani` DECIMAL(6,3) DEFAULT NULL,
                    `olasilik` DECIMAL(6,4) DEFAULT NULL,
                    `p_model` DECIMAL(6,4) DEFAULT NULL,
                    `p_kalibre` DECIMAL(6,4) DEFAULT NULL,
                    `edge` DECIMAL(6,4) DEFAULT NULL,
                    `clv` DECIMAL(6,4) DEFAULT NULL,
                    `kupon_id` VARCHAR(64) DEFAULT NULL,
                    `stake_yuzde` DECIMAL(5,4) DEFAULT NULL,
                    `mod` VARCHAR(20) DEFAULT 'ADIL_ORAN',
                    `tarih` DATE DEFAULT NULL,
                    `olusturma_zamani` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (`id`),
                    KEY `idx_fikstur_id` (`fikstur_id`),
                    KEY `idx_tarih` (`tarih`),
                    KEY `idx_market_sonuc` (`secilen_tahmin`, `sonuc`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
        else:
            cur.execute("DESCRIBE `tahminler`")
            mevcut_kolonlar = {row["Field"] for row in cur.fetchall()}
            eklenecekler = {
                "oran": "DECIMAL(6,3) DEFAULT NULL",
                "kapanis_orani": "DECIMAL(6,3) DEFAULT NULL",
                "olasilik": "DECIMAL(6,4) DEFAULT NULL",
                "p_model": "DECIMAL(6,4) DEFAULT NULL",
                "p_kalibre": "DECIMAL(6,4) DEFAULT NULL",
                "edge": "DECIMAL(6,4) DEFAULT NULL",
                "clv": "DECIMAL(6,4) DEFAULT NULL",
                "kupon_id": "VARCHAR(64) DEFAULT NULL",
                "stake_yuzde": "DECIMAL(5,4) DEFAULT NULL",
                "mod": "VARCHAR(20) DEFAULT 'ADIL_ORAN'",
                "tarih": "DATE DEFAULT NULL",
            }
            for kol, tip in eklenecekler.items():
                if kol not in mevcut_kolonlar:
                    logger.info(f"`tahminler` tablosuna `{kol}` kolonu ekleniyor...")
                    cur.execute(f"ALTER TABLE `tahminler` ADD COLUMN `{kol}` {tip}")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS `oranlar` (
                `id` INT(11) NOT NULL AUTO_INCREMENT,
                `fikstur_id` INT(11) NOT NULL,
                `secilen_tahmin` VARCHAR(100) NOT NULL,
                `oran` DECIMAL(6,3) NOT NULL,
                `kapanis_orani` DECIMAL(6,3) DEFAULT NULL,
                `kaynak` VARCHAR(50) DEFAULT 'MANUEL',
                `guncelleme_zamani` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_fikstur_market` (`fikstur_id`, `secilen_tahmin`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)


def fikstur_verisini_yukle_ve_temizle(conn: pymysql.connections.Connection) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM `fikstur` ORDER BY `tarih` ASC, `id` ASC")
        rows = cur.fetchall()

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["tarih"] = pd.to_datetime(df["tarih"], errors="coerce")
    df["lig"] = df["lig"].astype(str).str.strip().str.upper()
    df["lig"] = df["lig"].map(lambda x: LIG_NORMALIZASYON.get(x, x))
    df["sezon"] = df["sezon"].astype(str).str.strip()
    df["durum_temiz"] = df["durum"].astype(str).str.upper().str.strip()

    for col in SAYISAL_KOLONLAR:
        if col in df.columns:
            ser = df[col].astype(str).str.replace("%", "", regex=False).str.strip()
            ser = ser.replace({"-": np.nan, "": np.nan, "None": np.nan, "null": np.nan, "nan": np.nan})
            df[col] = pd.to_numeric(ser, errors="coerce")

    komple_bos_mask = (
        (df["ev_top_oynama"].fillna(0) + df["dep_top_oynama"].fillna(0) == 0) &
        (df["ev_sut"].fillna(0) + df["dep_sut"].fillna(0) == 0)
    )
    bos_sayisi = int(komple_bos_mask.sum())
    if bos_sayisi > 0:
        logger.info(f"[TEMİZLİK] Skor var ama tüm istatistikleri '0' basılmış {bos_sayisi} maçta boxscore kolonları NaN yapıldı.")
        df.loc[komple_bos_mask, TUM_BOXSCORE_KOLONLARI] = np.nan

    pas_bos_mask = (df["ev_toplam_pas"].fillna(0) + df["dep_toplam_pas"].fillna(0) == 0)
    pas_bos_sayisi = int(pas_bos_mask.sum())
    if pas_bos_sayisi > 0:
        logger.info(f"[TEMİZLİK] Pas verisi '0' basılmış {pas_bos_sayisi} maçta detay pas/savunma/ofsayt kolonları NaN yapıldı.")
        df.loc[pas_bos_mask, DETAY_PAS_SAVUNMA_KOLONLARI] = np.nan

    for tk in ("ev_top_oynama", "dep_top_oynama"):
        if tk in df.columns:
            buyuk_mask = df[tk] > 1.0
            df.loc[buyuk_mask, tk] = df.loc[buyuk_mask, tk] / 100.0

    oran_ciftleri = [
        ("ev_sut_yuzde", "ev_isabetli", "ev_sut"),
        ("dep_sut_yuzde", "dep_isabetli", "dep_sut"),
        ("ev_pas_yuzde", "ev_isabetli_pas", "ev_toplam_pas"),
        ("dep_pas_yuzde", "dep_isabetli_pas", "dep_toplam_pas"),
        ("ev_orta_yuzde", "ev_isabetli_orta", "ev_toplam_orta"),
        ("dep_orta_yuzde", "dep_isabetli_orta", "dep_toplam_orta"),
        ("ev_uzun_yuzde", "ev_isabet_uzun", "ev_toplam_uzun"),
        ("dep_uzun_yuzde", "dep_isabet_uzun", "dep_toplam_uzun"),
        ("ev_mud_yuzde", "ev_basarili_mud", "ev_toplam_mud"),
        ("dep_mud_yuzde", "dep_basarili_mud", "dep_toplam_mud"),
    ]
    for yuzde_kol, pay_kol, payda_kol in oran_ciftleri:
        gecerli = df[payda_kol].notna() & (df[payda_kol] > 0)
        df.loc[gecerli, yuzde_kol] = (df.loc[gecerli, pay_kol] / df.loc[gecerli, payda_kol]).clip(0.0, 1.0)
        df.loc[~gecerli, yuzde_kol] = np.nan

    iy_hatali = (df["ev_iy_skor"] > df["ev_skor"]) | (df["dep_iy_skor"] > df["dep_skor"])
    if iy_hatali.any():
        df.loc[iy_hatali, ["ev_iy_skor", "dep_iy_skor"]] = np.nan

    pen_var_mask = df["ev_pen_sut"].notna() | df["ev_pen_gol"].notna()
    df["ev_pen_etkin"] = np.where(
        pen_var_mask,
        np.maximum(df["ev_pen_sut"].fillna(0), df["ev_pen_gol"].fillna(0)),
        np.nan
    )
    df["dep_pen_etkin"] = np.where(
        pen_var_mask,
        np.maximum(df["dep_pen_sut"].fillna(0), df["dep_pen_gol"].fillna(0)),
        np.nan
    )

    df = df.sort_values(["tarih", "id"]).reset_index(drop=True)
    return df


def csv_oran_yukle(conn: pymysql.connections.Connection, csv_yolu: str) -> int:
    if not os.path.exists(csv_yolu):
        raise FileNotFoundError(f"Oran CSV dosyası bulunamadı: {csv_yolu}")

    oran_df = pd.read_csv(csv_yolu)
    gerekli = {"fikstur_id", "secilen_tahmin", "oran"}
    if not gerekli.issubset(oran_df.columns):
        raise ValueError(f"CSV kolonları {gerekli} içermelidir.")

    sayac = 0
    sql = """
        INSERT INTO `oranlar` (`fikstur_id`, `secilen_tahmin`, `oran`, `kapanis_orani`, `kaynak`)
        VALUES (%s, %s, %s, %s, 'CSV')
        ON DUPLICATE KEY UPDATE
            `oran` = VALUES(`oran`),
            `kapanis_orani` = COALESCE(VALUES(`kapanis_orani`), `kapanis_orani`)
    """
    with conn.cursor() as cur:
        for _, r in oran_df.iterrows():
            kapanis = float(r["kapanis_orani"]) if "kapanis_orani" in r and pd.notna(r["kapanis_orani"]) else None
            cur.execute(sql, (int(r["fikstur_id"]), str(r["secilen_tahmin"]).strip().lower(), float(r["oran"]), kapanis))
            sayac += 1

    logger.info(f"{sayac} adet oran kaydı `oranlar` tablosuna işlendi.")
    return sayac


def oranlari_getir(conn: pymysql.connections.Connection, fikstur_id_listesi: List[int]) -> Dict[int, Dict[str, Dict[str, float]]]:
    if not fikstur_id_listesi:
        return {}

    format_strings = ",".join(["%s"] * len(fikstur_id_listesi))
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT fikstur_id, secilen_tahmin, oran, kapanis_orani "
            f"FROM `oranlar` WHERE fikstur_id IN ({format_strings})",
            tuple(fikstur_id_listesi)
        )
        rows = cur.fetchall()

    sonuc: Dict[int, Dict[str, Dict[str, float]]] = {}
    for r in rows:
        fid = int(r["fikstur_id"])
        mkt = str(r["secilen_tahmin"]).strip().lower()
        sonuc.setdefault(fid, {})[mkt] = {
            "oran": float(r["oran"]) if r["oran"] is not None else None,
            "kapanis_orani": float(r["kapanis_orani"]) if r["kapanis_orani"] is not None else None
        }
    return sonuc
