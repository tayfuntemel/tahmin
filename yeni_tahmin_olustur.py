#!/usr/bin/env python3
# yeni_tahmin_olustur.py - sadece O1.5/O2.5/O3.5, eksik veri yoksa NO_BET
import os
import mysql.connector
import pandas as pd
import numpy as np
import joblib
import sys
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

# Eşik değerleri (deneyimle optimize edilebilir)
THRESHOLD_O15 = 0.62   # %62 üzerinde O1.5 tahmin et
THRESHOLD_O25 = 0.52   # %52 üzerinde O2.5 tahmin et (O3.5 değilse)
THRESHOLD_O35 = 0.42   # %42 üzerinde direkt O3.5 tahmin et

def get_connection():
    return mysql.connector.connect(**CONFIG["db"])

def create_table_if_not_exists():
    print(">>> Tablo kontrolü...")
    conn = get_connection()
    cursor = conn.cursor()
    query = """
    CREATE TABLE IF NOT EXISTS `match_predictions` (
      `id` INT AUTO_INCREMENT PRIMARY KEY,
      `event_id` BIGINT UNSIGNED NOT NULL,
      `predicted_market` VARCHAR(10) NULL,
      `probability` FLOAT NULL,
      `prob_o15` FLOAT NULL,
      `prob_o25` FLOAT NULL,
      `prob_o35` FLOAT NULL,
      `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY `unique_event` (`event_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        cursor.execute(query)
        conn.commit()
        print(">>> Tablo hazır.")
    except Exception as e:
        print(f">>> HATA: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

def load_models():
    print(">>> Modeller yükleniyor...")
    scaler = joblib.load(f"{MODEL_DIR}/scaler.pkl")
    model_o15 = joblib.load(f"{MODEL_DIR}/o15_model.pkl")
    model_o25 = joblib.load(f"{MODEL_DIR}/o25_model.pkl")
    model_o35 = joblib.load(f"{MODEL_DIR}/o35_model.pkl")
    return scaler, model_o15, model_o25, model_o35

def get_upcoming_matches_with_features():
    """Gelecek maçlar için özellikleri tek sorguda çek"""
    print(">>> Gelecek maçlar ve özellikler çekiliyor...")
    conn = get_connection()
    tz_tr = timezone(timedelta(hours=3))
    today = datetime.now(tz_tr).date()
    end_date = today + timedelta(days=2)  # 2 günlük maçlar

    query = """
    SELECT 
        r.event_id, r.home_team, r.away_team, r.tournament_id, r.referee,
        COALESCE(ha.matches_played, ho.matches_played) as home_matches,
        COALESCE(ha.goals_for, ho.goals_for) as home_gf,
        COALESCE(ha.goals_against, ho.goals_against) as home_ga,
        COALESCE(ha.wins, ho.wins) as home_wins,
        COALESCE(ha.draws, ho.draws) as home_draws,
        COALESCE(ha.losses, ho.losses) as home_losses,
        COALESCE(ha.avg_possession, ho.avg_possession) as home_poss,
        COALESCE(ha.avg_shots, ho.avg_shots) as home_shots,
        COALESCE(ha.avg_shots_on, ho.avg_shots_on) as home_sot,
        COALESCE(aa.matches_played, ao.matches_played) as away_matches,
        COALESCE(aa.goals_for, ao.goals_for) as away_gf,
        COALESCE(aa.goals_against, ao.goals_against) as away_ga,
        COALESCE(aa.wins, ao.wins) as away_wins,
        COALESCE(aa.draws, ao.draws) as away_draws,
        COALESCE(aa.losses, ao.losses) as away_losses,
        COALESCE(aa.avg_possession, ao.avg_possession) as away_poss,
        COALESCE(aa.avg_shots, ao.avg_shots) as away_shots,
        COALESCE(aa.avg_shots_on, ao.avg_shots_on) as away_sot,
        hf.points_last_5 as home_form_pts,
        af.points_last_5 as away_form_pts,
        he.conversion_rate_pct as home_conv,
        ae.conversion_rate_pct as away_conv,
        he.save_rate_pct as home_save,
        ae.save_rate_pct as away_save,
        hht.ht_btts_yes_pct as home_ht_btts,
        aht.ht_btts_yes_pct as away_ht_btts,
        la.avg_goals_match as league_avg_goals,
        la.home_win_pct as league_home_win_pct,
        la.draw_pct as league_draw_pct,
        ra.avg_goals_match as ref_avg_goals
    FROM results_football r
    LEFT JOIN team_analytics ha ON r.home_team = ha.team_name AND r.tournament_id = ha.tournament_id AND r.category_id = ha.category_id AND ha.venue_type = 'Home'
    LEFT JOIN team_analytics ho ON r.home_team = ho.team_name AND r.tournament_id = ho.tournament_id AND r.category_id = ho.category_id AND ho.venue_type = 'Overall'
    LEFT JOIN team_analytics aa ON r.away_team = aa.team_name AND r.tournament_id = aa.tournament_id AND r.category_id = aa.category_id AND aa.venue_type = 'Away'
    LEFT JOIN team_analytics ao ON r.away_team = ao.team_name AND r.tournament_id = ao.tournament_id AND r.category_id = ao.category_id AND ao.venue_type = 'Overall'
    LEFT JOIN team_form_analytics hf ON r.home_team = hf.team_name AND r.tournament_id = hf.tournament_id AND hf.venue_type = 'Home'
    LEFT JOIN team_form_analytics af ON r.away_team = af.team_name AND r.tournament_id = af.tournament_id AND af.venue_type = 'Away'
    LEFT JOIN team_efficiency_analytics he ON r.home_team = he.team_name AND r.tournament_id = he.tournament_id AND he.venue_type = 'Home'
    LEFT JOIN team_efficiency_analytics ae ON r.away_team = ae.team_name AND r.tournament_id = ae.tournament_id AND ae.venue_type = 'Away'
    LEFT JOIN team_half_time_analytics hht ON r.home_team = hht.team_name AND r.tournament_id = hht.tournament_id AND hht.venue_type = 'Home'
    LEFT JOIN team_half_time_analytics aht ON r.away_team = aht.team_name AND r.tournament_id = aht.tournament_id AND aht.venue_type = 'Away'
    LEFT JOIN league_analytics la ON r.tournament_id = la.tournament_id
    LEFT JOIN referee_analytics ra ON r.referee = ra.referee_name
    WHERE r.start_utc BETWEEN %s AND %s
      AND r.status IN ('notstarted', 'scheduled')
      AND NOT EXISTS (SELECT 1 FROM match_predictions mp WHERE mp.event_id = r.event_id)
    ORDER BY r.start_utc, r.start_time_utc
    """
    df = pd.read_sql(query, conn, params=(today, end_date))
    conn.close()
    return df

def predict_for_match(row, scaler, model_o15, model_o25, model_o35):
    feature_cols = [
        'home_matches', 'home_gf', 'home_ga', 'home_wins', 'home_draws', 'home_losses',
        'home_poss', 'home_shots', 'home_sot',
        'away_matches', 'away_gf', 'away_ga', 'away_wins', 'away_draws', 'away_losses',
        'away_poss', 'away_shots', 'away_sot',
        'home_form_pts', 'away_form_pts',
        'home_conv', 'away_conv', 'home_save', 'away_save',
        'home_ht_btts', 'away_ht_btts',
        'league_avg_goals', 'league_home_win_pct', 'league_draw_pct',
        'ref_avg_goals'
    ]
    # Eksik değer kontrolü
    for col in feature_cols:
        if pd.isna(row[col]):
            return None, None, None, None
    X = np.array([row[col] for col in feature_cols]).reshape(1, -1)
    X_scaled = scaler.transform(X)
    prob_o15 = model_o15.predict_proba(X_scaled)[0][1]
    prob_o25 = model_o25.predict_proba(X_scaled)[0][1]
    prob_o35 = model_o35.predict_proba(X_scaled)[0][1]
    return prob_o15, prob_o25, prob_o35, X_scaled

def decide_market(prob_o15, prob_o25, prob_o35):
    """Hiyerarşik karar: önce 3.5Ü, sonra 2.5Ü, sonra 1.5Ü"""
    if prob_o35 >= THRESHOLD_O35:
        return 'O3.5', prob_o35
    elif prob_o25 >= THRESHOLD_O25:
        return 'O2.5', prob_o25
    elif prob_o15 >= THRESHOLD_O15:
        return 'O1.5', prob_o15
    else:
        return 'NO_BET', max(prob_o15, prob_o25, prob_o35)

def save_prediction(event_id, predicted_market, probability, prob_o15, prob_o25, prob_o35):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO `match_predictions`
        (event_id, predicted_market, probability, prob_o15, prob_o25, prob_o35, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
        predicted_market = VALUES(predicted_market),
        probability = VALUES(probability),
        prob_o15 = VALUES(prob_o15),
        prob_o25 = VALUES(prob_o25),
        prob_o35 = VALUES(prob_o35)
    """, (event_id, predicted_market, probability, prob_o15, prob_o25, prob_o35))
    conn.commit()
    cursor.close()
    conn.close()

def main():
    create_table_if_not_exists()
    scaler, model_o15, model_o25, model_o35 = load_models()
    matches = get_upcoming_matches_with_features()
    print(f">>> {len(matches)} maç adayı bulundu.")
    
    predicted_count = 0
    for idx, row in matches.iterrows():
        prob_o15, prob_o25, prob_o35, _ = predict_for_match(row, scaler, model_o15, model_o25, model_o35)
        if prob_o15 is None:
            save_prediction(row['event_id'], 'NO_BET', 0, None, None, None)
            print(f"--- Eksik veri, atlandı: {row['home_team']} - {row['away_team']}")
            continue
        market, prob = decide_market(prob_o15, prob_o25, prob_o35)
        save_prediction(row['event_id'], market, prob, prob_o15, prob_o25, prob_o35)
        if market != 'NO_BET':
            predicted_count += 1
            print(f"--- Tahmin: {row['home_team']} - {row['away_team']} -> {market} (%{prob*100:.1f})")
        else:
            print(f"--- Tahmin yok: {row['home_team']} - {row['away_team']} (max=%{max(prob_o15,prob_o25,prob_o35)*100:.1f})")
    
    print(f">>> İşlem tamam. {predicted_count} maça tahmin üretildi.")

if __name__ == "__main__":
    main()
