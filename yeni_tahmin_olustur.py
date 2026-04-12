#!/usr/bin/env python3
# yeni_tahmin_olustur.py
import os
import mysql.connector
import pandas as pd
import numpy as np
import joblib
from datetime import datetime, timedelta, timezone

CONFIG = {
    "db": {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306))
    }
}

MODEL_DIR = "models"

def get_connection():
    return mysql.connector.connect(**CONFIG["db"])

def create_table_if_not_exists():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
    CREATE TABLE IF NOT EXISTS `match_predictions` (
      `id` INT AUTO_INCREMENT PRIMARY KEY,
      `event_id` INT NOT NULL UNIQUE,
      `prob_ms1` FLOAT,
      `prob_ms0` FLOAT,
      `prob_ms2` FLOAT,
      `prob_o15` FLOAT,
      `prob_o35` FLOAT,
      `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      FOREIGN KEY (`event_id`) REFERENCES `results_football`(`event_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        cursor.execute(query)
        conn.commit()
        print("Tablo kontrolü başarılı (match_predictions hazır).")
    except Exception as e:
        print(f"Tablo oluşturulurken hata: {e}")
    finally:
        cursor.close()
        conn.close()

def load_models():
    scaler = joblib.load(f"{MODEL_DIR}/scaler.pkl")
    model_result = joblib.load(f"{MODEL_DIR}/result_model.pkl")
    model_o15 = joblib.load(f"{MODEL_DIR}/o15_model.pkl")
    model_u35 = joblib.load(f"{MODEL_DIR}/u35_model.pkl")
    return scaler, model_result, model_o15, model_u35

def get_upcoming_matches():
    conn = get_connection()
    tz_tr = timezone(timedelta(hours=3))
    today = datetime.now(tz_tr).date()
    end_date = today + timedelta(days=2)
    
    query = """
    SELECT r.* FROM results_football r
    LEFT JOIN match_predictions mp ON r.event_id = mp.event_id
    WHERE r.start_utc BETWEEN %s AND %s
      AND r.status IN ('notstarted', 'scheduled')
      AND mp.event_id IS NULL
    ORDER BY r.start_utc, r.start_time_utc
    """
    df = pd.read_sql(query, conn, params=(today, end_date))
    conn.close()
    return df

def get_features_for_match(row):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    def get_stat(team, tour_id, venue, table, fields):
        q = f"SELECT {', '.join(fields)} FROM {table} WHERE team_name = %s AND tournament_id = %s AND venue_type = %s"
        cursor.execute(q, (team, tour_id, venue))
        return cursor.fetchone() or {}
    
    home_team = row['home_team']
    away_team = row['away_team']
    tour_id = row['tournament_id']
    
    home_stats = get_stat(home_team, tour_id, 'Home', 'team_analytics', ['matches_played', 'goals_for', 'goals_against', 'wins', 'draws', 'losses', 'avg_possession', 'avg_shots', 'avg_shots_on'])
    away_stats = get_stat(away_team, tour_id, 'Away', 'team_analytics', ['matches_played', 'goals_for', 'goals_against', 'wins', 'draws', 'losses', 'avg_possession', 'avg_shots', 'avg_shots_on'])
    home_form = get_stat(home_team, tour_id, 'Home', 'team_form_analytics', ['points_last_5'])
    away_form = get_stat(away_team, tour_id, 'Away', 'team_form_analytics', ['points_last_5'])
    home_eff = get_stat(home_team, tour_id, 'Home', 'team_efficiency_analytics', ['conversion_rate_pct', 'save_rate_pct'])
    away_eff = get_stat(away_team, tour_id, 'Away', 'team_efficiency_analytics', ['conversion_rate_pct', 'save_rate_pct'])
    home_ht = get_stat(home_team, tour_id, 'Home', 'team_half_time_analytics', ['ht_btts_yes_pct'])
    away_ht = get_stat(away_team, tour_id, 'Away', 'team_half_time_analytics', ['ht_btts_yes_pct'])
    cursor.execute("SELECT avg_goals_match, home_win_pct, draw_pct FROM league_analytics WHERE tournament_id = %s", (tour_id,))
    league = cursor.fetchone() or {}
    cursor.execute("SELECT avg_goals_match FROM referee_analytics WHERE referee_name = %s", (row['referee'],))
    ref = cursor.fetchone() or {}
    
    cursor.close()
    conn.close()
    
    features = [
        home_stats.get('matches_played', 0), home_stats.get('goals_for', 0), home_stats.get('goals_against', 0),
        home_stats.get('wins', 0), home_stats.get('draws', 0), home_stats.get('losses', 0),
        home_stats.get('avg_possession', 50), home_stats.get('avg_shots', 0), home_stats.get('avg_shots_on', 0),
        away_stats.get('matches_played', 0), away_stats.get('goals_for', 0), away_stats.get('goals_against', 0),
        away_stats.get('wins', 0), away_stats.get('draws', 0), away_stats.get('losses', 0),
        away_stats.get('avg_possession', 50), away_stats.get('avg_shots', 0), away_stats.get('avg_shots_on', 0),
        home_form.get('points_last_5', 0), away_form.get('points_last_5', 0),
        home_eff.get('conversion_rate_pct', 10), away_eff.get('conversion_rate_pct', 10),
        home_eff.get('save_rate_pct', 70), away_eff.get('save_rate_pct', 70),
        home_ht.get('ht_btts_yes_pct', 40), away_ht.get('ht_btts_yes_pct', 40),
        league.get('avg_goals_match', 2.5), league.get('home_win_pct', 45), league.get('draw_pct', 25),
        ref.get('avg_goals_match', 2.5)
    ]
    features = [0 if pd.isna(x) else x for x in features]
    return np.array(features).reshape(1, -1)

def save_predictions(event_id, prob_result, prob_o15, prob_u35):
    conn = get_connection()
    cursor = conn.cursor()
    prob_1x = prob_result[0][1] + prob_result[0][0]
    prob_12 = prob_result[0][1] + prob_result[0][2]
    prob_x2 = prob_result[0][0] + prob_result[0][2]
    
    prob_o15_val = prob_o15[0][1]
    prob_u35_val = 1 - prob_u35[0][1] if prob_u35[0][1] is not None else 0.5
    
    cursor.execute("""
        INSERT INTO match_predictions
        (event_id, prob_ms1, prob_ms0, prob_ms2, prob_o15, prob_o35, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
        prob_ms1 = VALUES(prob_ms1), prob_ms0 = VALUES(prob_ms0), prob_ms2 = VALUES(prob_ms2),
        prob_o15 = VALUES(prob_o15), prob_o35 = VALUES(prob_o35)
    """, (event_id, prob_result[0][1]*100, prob_result[0][0]*100, prob_result[0][2]*100,
          prob_o15_val*100, (1-prob_u35_val)*100))
    conn.commit()
    cursor.close()
    conn.close()

def main():
    create_table_if_not_exists()
    
    scaler, model_result, model_o15, model_u35 = load_models()
    matches = get_upcoming_matches()
    print(f"{len(matches)} yeni maç tahmin edilecek.")
    
    for idx, row in matches.iterrows():
        try:
            X = get_features_for_match(row)
            X_scaled = scaler.transform(X)
            prob_result = model_result.predict_proba(X_scaled)
            prob_o15 = model_o15.predict_proba(X_scaled)
            prob_u35 = model_u35.predict_proba(X_scaled)
            save_predictions(row['event_id'], prob_result, prob_o15, prob_u35)
            print(f"Tahmin edildi: {row['home_team']} - {row['away_team']}")
        except Exception as e:
            print(f"Hata {row['event_id']}: {e}")

if __name__ == "__main__":
    main()
