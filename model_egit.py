#!/usr/bin/env python3
# model_egit.py - XGBoost ile eğitim, eksik veri silinir, hiperparametre optimizasyonu
import os
import mysql.connector
import pandas as pd
import numpy as np
import joblib
from sklearn.preprocessing import StandardScaler
from sklearn.calibration import CalibratedClassifierCV
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
import xgboost as xgb
from scipy.stats import uniform, randint

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
    """Tüm özellikleri çeker (Home/Overall düşme mantığı ile)"""
    conn = get_connection()
    query = """
    SELECT 
        r.event_id, r.start_utc,
        r.ft_home, r.ft_away,
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
    WHERE r.status IN ('finished','ended')
      AND r.ft_home IS NOT NULL AND r.ft_away IS NOT NULL
    ORDER BY r.start_utc  -- zamana göre sıralı (backtest için)
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def prepare_features(df):
    """Hedef değişkenler, eksik değer içeren satırları sil"""
    df['total_goals'] = df['ft_home'] + df['ft_away']
    df['o15'] = (df['total_goals'] > 1.5).astype(int)
    df['o25'] = (df['total_goals'] > 2.5).astype(int)
    df['o35'] = (df['total_goals'] > 3.5).astype(int)

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
    X = df[feature_cols].copy()
    # Eksik değer içeren satırları sil
    before = len(X)
    X = X.dropna()
    after = len(X)
    print(f"Eksik veri nedeniyle silinen satır: {before - after}")
    # Hedefleri de aynı indekslerle filtrele
    y_o15 = df.loc[X.index, 'o15']
    y_o25 = df.loc[X.index, 'o25']
    y_o35 = df.loc[X.index, 'o35']
    # Zaman bilgisini de sakla (backtest için)
    dates = df.loc[X.index, 'start_utc']
    return X, y_o15, y_o25, y_o35, dates

def train_xgb_model(X, y, model_name, cv_folds=5):
    """XGBoost hiperparametre optimizasyonu ve kalibrasyon"""
    param_dist = {
        'n_estimators': randint(100, 500),
        'max_depth': randint(3, 10),
        'learning_rate': uniform(0.01, 0.3),
        'subsample': uniform(0.6, 0.4),
        'colsample_bytree': uniform(0.6, 0.4),
        'gamma': uniform(0, 0.5),
        'reg_alpha': uniform(0, 2),
        'reg_lambda': uniform(0, 2)
    }
    # Zaman serisi cross-validation (overfitting'i önlemek için)
    tscv = TimeSeriesSplit(n_splits=cv_folds)
    base_model = xgb.XGBClassifier(objective='binary:logistic', random_state=42, use_label_encoder=False, eval_metric='logloss')
    random_search = RandomizedSearchCV(base_model, param_distributions=param_dist, n_iter=30, cv=tscv, scoring='roc_auc', n_jobs=-1, random_state=42)
    random_search.fit(X, y)
    best_model = random_search.best_estimator_
    print(f"{model_name} best params: {random_search.best_params_}")
    # Kalibrasyon
    calibrated = CalibratedClassifierCV(best_model, method='sigmoid', cv=3)
    calibrated.fit(X, y)
    return calibrated

def main():
    print("Veri yükleniyor...")
    df = load_training_data()
    print(f"Toplam ham maç: {len(df)}")
    X, y_o15, y_o25, y_o35, dates = prepare_features(df)
    print(f"Temizlenmiş maç sayısı: {len(X)}")
    
    # Ölçeklendirici (XGBoost için şart değil ama diğer modeller için kullanılabilir)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    print("Model 1: Over 1.5 eğitiliyor...")
    model_o15 = train_xgb_model(X_scaled, y_o15, "O15")
    print("Model 2: Over 2.5 eğitiliyor...")
    model_o25 = train_xgb_model(X_scaled, y_o25, "O25")
    print("Model 3: Over 3.5 eğitiliyor...")
    model_o35 = train_xgb_model(X_scaled, y_o35, "O35")
    
    # Kaydet
    joblib.dump(scaler, f"{MODEL_DIR}/scaler.pkl")
    joblib.dump(model_o15, f"{MODEL_DIR}/o15_model.pkl")
    joblib.dump(model_o25, f"{MODEL_DIR}/o25_model.pkl")
    joblib.dump(model_o35, f"{MODEL_DIR}/o35_model.pkl")
    print("Tüm modeller ve scaler kaydedildi.")
    
    # Opsiyonel: Feature importance çıktısı
    importances = model_o15.best_estimator_.feature_importances_
    feature_names = X.columns
    fi_df = pd.DataFrame({'feature': feature_names, 'importance': importances}).sort_values('importance', ascending=False)
    print("Feature importance (O15 modeli):\n", fi_df.head(10))

if __name__ == "__main__":
    main()
