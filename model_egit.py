#!/usr/bin/env python3
# model_egit.py
import os
import mysql.connector
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.preprocessing import StandardScaler
import pickle
import joblib

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
os.makedirs(MODEL_DIR, exist_ok=True)

def get_connection():
    return mysql.connector.connect(**CONFIG["db"])

def load_training_data():
    conn = get_connection()
    query = """
    SELECT 
        r.event_id,
        r.tournament_id,
        r.ft_home, r.ft_away,
        -- Ev sahibi istatistikleri (Overall + Home karışımı)
        ha.matches_played as home_matches,
        ha.goals_for as home_gf, ha.goals_against as home_ga,
        ha.wins as home_wins, ha.draws as home_draws, ha.losses as home_losses,
        ha.avg_possession as home_poss, ha.avg_shots as home_shots, ha.avg_shots_on as home_sot,
        -- Deplasman istatistikleri
        aa.matches_played as away_matches,
        aa.goals_for as away_gf, aa.goals_against as away_ga,
        aa.wins as away_wins, aa.draws as away_draws, aa.losses as away_losses,
        aa.avg_possession as away_poss, aa.avg_shots as away_shots, aa.avg_shots_on as away_sot,
        -- Form (son 5 maç puanı)
        hf.points_last_5 as home_form_pts, af.points_last_5 as away_form_pts,
        -- Verimlilik
        he.conversion_rate_pct as home_conv, ae.conversion_rate_pct as away_conv,
        he.save_rate_pct as home_save, ae.save_rate_pct as away_save,
        -- İlk yarı BTTS yüzdesi
        hht.ht_btts_yes_pct as home_ht_btts, aht.ht_btts_yes_pct as away_ht_btts,
        -- Lig ortalamaları
        la.avg_goals_match as league_avg_goals,
        la.home_win_pct as league_home_win_pct, la.draw_pct as league_draw_pct,
        -- Hakem
        ra.avg_goals_match as ref_avg_goals
    FROM results_football r
    LEFT JOIN team_analytics ha ON r.home_team = ha.team_name AND r.tournament_id = ha.tournament_id AND ha.venue_type = 'Home'
    LEFT JOIN team_analytics aa ON r.away_team = aa.team_name AND r.tournament_id = aa.tournament_id AND aa.venue_type = 'Away'
    LEFT JOIN team_form_analytics hf ON r.home_team = hf.team_name AND r.tournament_id = hf.tournament_id AND hf.venue_type = 'Home'
    LEFT JOIN team_form_analytics af ON r.away_team = af.team_name AND r.tournament_id = af.tournament_id AND af.venue_type = 'Away'
    LEFT JOIN team_efficiency_analytics he ON r.home_team = he.team_name AND r.tournament_id = he.tournament_id AND he.venue_type = 'Home'
    LEFT JOIN team_efficiency_analytics ae ON r.away_team = ae.team_name AND r.tournament_id = ae.tournament_id AND ae.venue_type = 'Away'
    LEFT JOIN team_half_time_analytics hht ON r.home_team = hht.team_name AND r.tournament_id = hht.tournament_id AND hht.venue_type = 'Home'
    LEFT JOIN team_half_time_analytics aht ON r.away_team = aht.team_name AND r.tournament_id = aht.tournament_id AND aht.venue_type = 'Away'
    LEFT JOIN league_analytics la ON r.tournament_id = la.tournament_id
    LEFT JOIN referee_analytics ra ON r.referee = ra.referee_name
    WHERE r.status IN ('finished','ended')
      AND r.ft_home IS NOT NULL AND r.ft_away IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def prepare_features(df):
    # Hedef değişkenler
    df['result'] = np.where(df['ft_home'] > df['ft_away'], 1,
                    np.where(df['ft_home'] == df['ft_away'], 0, 2))  # 1=Ev, 0=Beraberlik, 2=Deplasman
    df['total_goals'] = df['ft_home'] + df['ft_away']
    df['o15'] = (df['total_goals'] > 1.5).astype(int)
    df['u35'] = (df['total_goals'] <= 3.5).astype(int)
    
    # Eksik değerleri doldur (medyan veya 0)
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())
    
    # Özellik seçimi
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
    # Var olan sütunları al
    exist_features = [c for c in feature_cols if c in df.columns]
    X = df[exist_features]
    return X, df

def train_and_save_models(X, y_result, y_o15, y_u35):
    # Ölçeklendirme (Random Forest için şart değil ama iyi olur)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Model 1: Maç sonucu (1,0,2)
    clf_result = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
    clf_result.fit(X_scaled, y_result)
    # Kalibrasyon
    calibrated_result = CalibratedClassifierCV(clf_result, method='sigmoid', cv=3)
    calibrated_result.fit(X_scaled, y_result)
    
    # Model 2: 1.5 Üst
    clf_o15 = RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42)
    clf_o15.fit(X_scaled, y_o15)
    calibrated_o15 = CalibratedClassifierCV(clf_o15, method='sigmoid', cv=3)
    calibrated_o15.fit(X_scaled, y_o15)
    
    # Model 3: 3.5 Alt
    clf_u35 = RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42)
    clf_u35.fit(X_scaled, y_u35)
    calibrated_u35 = CalibratedClassifierCV(clf_u35, method='sigmoid', cv=3)
    calibrated_u35.fit(X_scaled, y_u35)
    
    # Kaydet
    joblib.dump(scaler, f"{MODEL_DIR}/scaler.pkl")
    joblib.dump(calibrated_result, f"{MODEL_DIR}/result_model.pkl")
    joblib.dump(calibrated_o15, f"{MODEL_DIR}/o15_model.pkl")
    joblib.dump(calibrated_u35, f"{MODEL_DIR}/u35_model.pkl")
    
    print("Modeller başarıyla kaydedildi.")

def main():
    print("Eğitim verisi yükleniyor...")
    df = load_training_data()
    print(f"{len(df)} maç yüklendi.")
    X, df = prepare_features(df)
    print(f"Özellik sayısı: {X.shape[1]}")
    
    train_and_save_models(X, df['result'], df['o15'], df['u35'])
    print("İşlem tamam.")

if __name__ == "__main__":
    main()
