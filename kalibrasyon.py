#!/usr/bin/env python3
import os
import mysql.connector
import logging
from datetime import datetime, timedelta

# Loglama ayarları
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Veritabanı bağlantı ayarları (GitHub Secrets üzerinden gelir)
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306))
}

def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

def ensure_table():
    """model_calibration tablosunu yoksa oluştur."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS model_calibration (
                param_name VARCHAR(64) PRIMARY KEY,
                param_value FLOAT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        logging.info("model_calibration tablosu kontrol edildi/oluşturuldu.")
    except Exception as e:
        logging.error(f"Tablo oluşturma hatası: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def calculate_bias(days_back=30, min_matches=20):
    """Belirtilen gün sayısına göre bias hesaplar."""
    cutoff_date = (datetime.now() - timedelta(days=days_back)).date()
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        query = """
            SELECT 
                AVG(r.ft_home - mp.exp_goals_home) AS home_bias,
                AVG(r.ft_away - mp.exp_goals_away) AS away_bias,
                COUNT(*) AS total_matches
            FROM match_predictions mp
            JOIN results_football r ON mp.event_id = r.event_id
            WHERE r.status = 'finished'
              AND mp.exp_goals_home IS NOT NULL
              AND mp.exp_goals_away IS NOT NULL
              AND mp.start_utc >= %s
        """
        cursor.execute(query, (cutoff_date,))
        row = cursor.fetchone()
        if row and row['total_matches'] >= min_matches:
            home_bias = float(row['home_bias']) if row['home_bias'] is not None else 0.0
            away_bias = float(row['away_bias']) if row['away_bias'] is not None else 0.0
            logging.info(f"Son {row['total_matches']} maç (son {days_back} gün) -> Ev bias: {home_bias:.3f}, Deplasman bias: {away_bias:.3f}")
            return home_bias, away_bias
        else:
            logging.warning(f"Yeterli veri yok (ihtiyaç: {min_matches}, mevcut: {row['total_matches'] if row else 0})")
            return None, None
    finally:
        cursor.close()
        conn.close()

def update_calibration_params(home_bias, away_bias):
    """Hesaplanan bias değerlerini veritabanına kaydeder."""
    if home_bias is None or away_bias is None:
        return
    
    new_home_bias = -home_bias
    new_away_bias = -away_bias
    
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Emniyet için tabloyu tekrar kontrol et
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS model_calibration (
                param_name VARCHAR(64) PRIMARY KEY,
                param_value FLOAT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        
        # Değerleri güncelle (varsa güncelle, yoksa ekle)
        cursor.execute("""
            INSERT INTO model_calibration (param_name, param_value)
            VALUES (%s, %s), (%s, %s)
            ON DUPLICATE KEY UPDATE param_value = VALUES(param_value)
        """, ('home_xg_bias', new_home_bias, 'away_xg_bias', new_away_bias))
        conn.commit()
        logging.info(f"Kalibrasyon güncellendi: home_xg_bias = {new_home_bias:.3f}, away_xg_bias = {new_away_bias:.3f}")
    except Exception as e:
        logging.error(f"Güncelleme hatası: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    logging.info("Günlük kalibrasyon betiği başladı.")
    
    # 1. Adım: Tablonun var olduğundan emin ol
    try:
        ensure_table()
    except Exception:
        logging.error("Tablo oluşturulamadı, devam ediliyor...")
        
    # 2. Adım: Bias hesapla
    home_bias, away_bias = calculate_bias(days_back=30, min_matches=20)
    
    # 3. Adım: Değerleri kaydet
    if home_bias is not None:
        update_calibration_params(home_bias, away_bias)
    else:
        logging.info("Bias güncellenmedi (yetersiz veri).")
        
    logging.info("Kalibrasyon betiği tamamlandı.")

if __name__ == "__main__":
    main()
