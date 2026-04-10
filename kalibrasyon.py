#!/usr/bin/env python3
"""
Model kalibrasyonu: Geçmiş bitmiş maçları analiz ederek,
- xG bias değerlerini (home_xg_bias, away_xg_bias) hesaplar ve günceller.
- Market bazlı Brier, Log Loss, kazanma oranı gibi metrikleri loglar (opsiyonel olarak bir tabloya yazabilir).
- Parametreleri (form çarpanı, dönüşüm etkisi vs.) şimdilik sabit bırakır, ancak istenirse onları da optimize edebiliriz.
"""

import os
import sys
import mysql.connector
import math
import logging
from datetime import datetime, timedelta

# ----------------------------- LOG AYARLARI ---------------------------------
LOG_FILE = "/var/log/kalibrasyon.log"  # İsterseniz değiştirin
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ----------------------------- VERİTABANI ---------------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306))
}

# ----------------------------- FONKSİYONLAR --------------------------------
def get_db_connection():
    """MySQL bağlantısı oluşturur."""
    return mysql.connector.connect(**DB_CONFIG)

def init_calibration_table(cursor):
    """model_calibration tablosu yoksa oluştur."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS model_calibration (
            param_name VARCHAR(64) PRIMARY KEY,
            param_value FLOAT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """)

def update_calibration_param(cursor, param_name, param_value):
    """Tek bir parametreyi günceller."""
    cursor.execute("""
        INSERT INTO model_calibration (param_name, param_value)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE param_value = VALUES(param_value)
    """, (param_name, param_value))

def get_finished_matches(cursor, days_back=90):
    """Son 'days_back' gün içinde bitmiş ve best_market_raw atanmış maçları getirir."""
    query = """
        SELECT mp.event_id, mp.exp_goals_home, mp.exp_goals_away,
               mp.best_market_raw, mp.best_prob,
               r.ft_home, r.ft_away, r.status
        FROM match_predictions mp
        JOIN results_football r ON mp.event_id = r.event_id
        WHERE r.status = 'finished'
          AND mp.best_market_raw IS NOT NULL
          AND mp.best_market_raw != 'NO_BET'
          AND r.start_utc >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
        ORDER BY r.start_utc DESC
    """
    cursor.execute(query, (days_back,))
    return cursor.fetchall()

def calculate_outcome(market, ft_home, ft_away):
    """Market bazında sonucu döndürür (1: kazanç, 0: kayıp)."""
    total = ft_home + ft_away
    if market == 'MS1':
        return 1 if ft_home > ft_away else 0
    elif market == 'MS0':
        return 1 if ft_home == ft_away else 0
    elif market == 'MS2':
        return 1 if ft_home < ft_away else 0
    elif market == 'O15':
        return 1 if total >= 2 else 0
    elif market == 'O25':
        return 1 if total >= 3 else 0
    elif market == 'O35':
        return 1 if total >= 4 else 0
    elif market == 'KG Var':
        return 1 if (ft_home > 0 and ft_away > 0) else 0
    elif market == 'KG Yok':
        return 1 if (ft_home == 0 or ft_away == 0) else 0
    else:
        return None

def compute_bias(cursor):
    """xG bias hesaplar: ortalama (gerçek - tahmin)"""
    cursor.execute("""
        SELECT AVG(ft_home - exp_goals_home) as home_bias,
               AVG(ft_away - exp_goals_away) as away_bias
        FROM match_predictions mp
        JOIN results_football r ON mp.event_id = r.event_id
        WHERE r.status = 'finished' AND mp.exp_goals_home IS NOT NULL
    """)
    row = cursor.fetchone()
    home_bias = row['home_bias'] if row['home_bias'] is not None else 0.0
    away_bias = row['away_bias'] if row['away_bias'] is not None else 0.0
    return home_bias, away_bias

def compute_market_metrics(cursor, matches):
    """Market bazında Brier, Log Loss, kazanma oranı, örnek sayısı hesaplar.
       İsteğe bağlı olarak bir tabloya yazabiliriz."""
    stats = {}
    for m in matches:
        market = m['best_market_raw']
        prob = m['best_prob'] / 100.0 if m['best_prob'] else 0.5
        ft_h = m['ft_home']
        ft_a = m['ft_away']
        outcome = calculate_outcome(market, ft_h, ft_a)
        if outcome is None:
            continue
        if market not in stats:
            stats[market] = {
                'brier_sum': 0.0,
                'logloss_sum': 0.0,
                'cnt': 0,
                'wins': 0
            }
        brier = (prob - outcome) ** 2
        logloss = - (outcome * math.log(max(prob, 1e-15)) + (1-outcome) * math.log(max(1-prob, 1e-15)))
        stats[market]['brier_sum'] += brier
        stats[market]['logloss_sum'] += logloss
        stats[market]['cnt'] += 1
        if outcome == 1:
            stats[market]['wins'] += 1
    # Logla
    logging.info("=== Market Bazlı Kalibrasyon ===")
    for market, s in stats.items():
        brier_avg = s['brier_sum'] / s['cnt']
        logloss_avg = s['logloss_sum'] / s['cnt']
        win_rate = (s['wins'] / s['cnt']) * 100
        logging.info(f"{market}: Brier={brier_avg:.4f}, LogLoss={logloss_avg:.4f}, WinRate={win_rate:.1f}% ({s['cnt']} örnek)")
    # Opsiyonel: Bir tabloya kaydetmek isterseniz aşağıdaki kodu aktifleştirin
    # cursor.execute("CREATE TABLE IF NOT EXISTS market_calibration_log ( ... )")
    return stats

def update_market_sample_counts(cursor):
    """market_sample_counts tablosunu güncelle (en_iyi_tahminleri_kaydet.py'nin kullandığı)"""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_sample_counts (
            market_raw VARCHAR(32) PRIMARY KEY,
            sample_count INT NOT NULL DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        INSERT INTO market_sample_counts (market_raw, sample_count)
        SELECT best_market_raw, COUNT(*) 
        FROM match_predictions mp
        JOIN results_football r ON mp.event_id = r.event_id
        WHERE r.status = 'finished' AND best_market_raw IS NOT NULL AND best_market_raw != 'NO_BET'
        GROUP BY best_market_raw
        ON DUPLICATE KEY UPDATE sample_count = VALUES(sample_count)
    """)
    logging.info("market_sample_counts tablosu güncellendi.")

def main():
    logging.info("Kalibrasyon betiği başladı.")
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        init_calibration_table(cursor)
        
        # 1. Bias hesapla ve güncelle
        home_bias, away_bias = compute_bias(cursor)
        logging.info(f"Hesaplanan bias - Ev: {home_bias:.4f}, Deplasman: {away_bias:.4f}")
        # Bias düzeltmesi = -bias (tahmine ekleyeceğiz)
        update_calibration_param(cursor, 'home_xg_bias', -home_bias)
        update_calibration_param(cursor, 'away_xg_bias', -away_bias)
        logging.info(f"Güncellenen bias değerleri: home_xg_bias = {-home_bias:.4f}, away_xg_bias = {-away_bias:.4f}")
        
        # 2. Geçmiş maçları çek (son 90 gün)
        matches = get_finished_matches(cursor, days_back=90)
        if not matches:
            logging.warning("Hiç bitmiş maç bulunamadı, kalibrasyon yapılamadı.")
        else:
            # Market metriklerini hesapla ve logla
            compute_market_metrics(cursor, matches)
            # market_sample_counts tablosunu güncelle
            update_market_sample_counts(cursor)
        
        conn.commit()
        logging.info("Kalibrasyon başarıyla tamamlandı.")
        
    except Exception as e:
        logging.exception(f"Hata oluştu: {str(e)}")
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
