#!/usr/bin/env python3
import os
import mysql.connector
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    """Tablo yoksa oluşturmaya çalış, yetki yoksa sadece uyar."""
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
        cursor.close()
        conn.close()
        logging.info("model_calibration tablosu hazır.")
        return True
    except Exception as e:
        logging.warning(f"Tablo oluşturulamadı (yetki sorunu?): {e}. Lütfen tabloyu manuel oluşturun.")
        return False

def calculate_bias(days_back=30, min_matches=20):
    cutoff_date = (datetime.now() - timedelta(days=days_back)).date()
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
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
    cursor.close()
    conn.close()
    if row and row['total_matches'] >= min_matches:
        home_bias = float(row['home_bias']) if row['home_bias'] is not None else 0.0
        away_bias = float(row['away_bias']) if row['away_bias'] is not None else 0.0
        logging.info(f"Son {row['total_matches']} maç -> Ev bias: {home_bias:.3f}, Deplasman bias: {away_bias:.3f}")
        return home_bias, away_bias
    else:
        logging.warning("Yetersiz veri, bias güncellenmedi.")
        return None, None

def update_calibration_params(home_bias, away_bias):
    if home_bias is None or away_bias is None:
        return
    new_home_bias = -home_bias
    new_away_bias = -away_bias
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO model_calibration (param_name, param_value)
            VALUES ('home_xg_bias', %s), ('away_xg_bias', %s)
            ON DUPLICATE KEY UPDATE param_value = VALUES(param_value)
        """, (new_home_bias, new_away_bias))
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Bias güncellendi: home={new_home_bias:.3f}, away={new_away_bias:.3f}")
    except Exception as e:
        logging.error(f"Bias güncellenemedi (tablo yok veya yetki sorunu): {e}")

def main():
    logging.info("Kalibrasyon başladı.")
    table_ok = ensure_table()
    home_bias, away_bias = calculate_bias()
    if home_bias is not None and table_ok:
        update_calibration_params(home_bias, away_bias)
    elif home_bias is not None and not table_ok:
        logging.warning("Tablo olmadığı için bias güncellenemedi, ancak hesaplama yapıldı.")
    logging.info("Kalibrasyon bitti.")

if __name__ == "__main__":
    main()
