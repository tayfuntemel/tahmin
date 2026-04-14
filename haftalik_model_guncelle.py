#!/usr/bin/env python3
import os
import mysql.connector
import pandas as pd
import numpy as np
import joblib
import xgboost as xgb
from sklearn.preprocessing import StandardScaler
from sklearn.calibration import CalibratedClassifierCV
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from scipy.stats import uniform, randint
import warnings

# Gereksiz uyarıları gizle
warnings.filterwarnings('ignore')

# ==========================================
# 0. MANUEL EĞİTİM AYARLARI (BURAYI DEĞİŞTİRECEKSİN)
# ==========================================
# Tahmin yapmak istediğin haftadan BİR ÖNCEKİ haftayı buraya gir.
TARGET_YEAR = 2026
TARGET_WEEK = 10

# ==========================================
# 1. VERİTABANI VE AYARLAR
# ==========================================
CONFIG = {
    "db": {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306))
    }
}

MODEL_DIR = "models_weekly"
os.makedirs(MODEL_DIR, exist_ok=True)

def get_data_from_db(target_year, target_week):
    print(f"[1/4] Veritabanına bağlanılıyor...")
    print(f"      Hedef: En baştan başlayarak {target_year} yılı {target_week}. haftasına (dahil) kadar olan maçlar çekiliyor.")
    
    conn = mysql.connector.connect(**CONFIG["db"])
    
    query = """
        SELECT * FROM results_football 
        WHERE status IN ('finished','ended') 
          AND match_year IS NOT NULL 
          AND match_week IS NOT NULL
          AND (match_year < %(year)s OR (match_year = %(year)s AND match_week <= %(week)s))
        ORDER BY start_utc ASC, start_time_utc ASC
    """
    
    params = {"year": target_year, "week": target_week}
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

# ==========================================
# 2. ÖZELLİK MÜHENDİSLİĞİ
# ==========================================
def create_dynamic_features(df):
    print(f"[2/4] Toplam {len(df)} maç için dinamik istatistikler hesaplanıyor...")
    df = df.copy()
    
    # Hedef Değişkenler
    df['total_goals'] = df['ft_home'] + df['ft_away']
    
    df['target_1x'] = (df['ft_home'] >= df['ft_away']).astype(int)
    df['target_x2'] = (df['ft_away'] >= df['ft_home']).astype(int)
    df['target_12'] = (df['ft_home'] != df['ft_away']).astype(int)
    df['target_o15'] = (df['total_goals'] > 1.5).astype(int)
    df['target_u15'] = (df['total_goals'] <= 1.5).astype(int)
    df['target_o25'] = (df['total_goals'] > 2.5).astype(int)
    df['target_u25'] = (df['total_goals'] <= 2.5).astype(int)
    df['target_o35'] = (df['total_goals'] > 3.5).astype(int)
    df['target_u35'] = (df['total_goals'] <= 3.5).astype(int)
    df['target_btts_yes'] = ((df['ft_home'] > 0) & (df['ft_away'] > 0)).astype(int)
    df['target_btts_no'] = ((df['ft_home'] == 0) | (df['ft_away'] == 0)).astype(int)

    features = ['h_avg_gf', 'h_avg_ga', 'a_avg_gf', 'a_avg_ga', 'h_win_rate', 'a_win_rate']
    for f in features: 
        df[f] = np.nan

    team_history = {}

    for idx, row in df.iterrows():
        h_team = row['home_team']
        a_team = row['away_team']

        if h_team in team_history and len(team_history[h_team]) >= 3:
            last_matches = team_history[h_team][-5:]
            df.at[idx, 'h_avg_gf'] = np.mean([m['gf'] for m in last_matches])
            df.at[idx, 'h_avg_ga'] = np.mean([m['ga'] for m in last_matches])
            df.at[idx, 'h_win_rate'] = np.mean([1 if m['gf'] > m['ga'] else 0 for m in last_matches])

        if a_team in team_history and len(team_history[a_team]) >= 3:
            last_matches = team_history[a_team][-5:]
            df.at[idx, 'a_avg_gf'] = np.mean([m['gf'] for m in last_matches])
            df.at[idx, 'a_avg_ga'] = np.mean([m['ga'] for m in last_matches])
            df.at[idx, 'a_win_rate'] = np.mean([1 if m['gf'] > m['ga'] else 0 for m in last_matches])

        for team, gf, ga in [(h_team, row['ft_home'], row['ft_away']), (a_team, row['ft_away'], row['ft_home'])]:
            if team not in team_history: 
                team_history[team] = []
            team_history[team].append({'gf': gf, 'ga': ga})

    df = df.dropna(subset=['h_avg_gf', 'a_avg_gf'])
    print("[BİLGİ] Özellik mühendisliği tamamlandı.")
    return df

# ==========================================
# 3. GELİŞMİŞ EĞİTİM FONKSİYONU (Optimizasyonlu)
# ==========================================
def train_and_save_model(X, y, label, market_name, feature_names):
    # Zaman serisi çapraz doğrulama (Veri azsa n_splits'i düşürüyoruz)
    cv_folds = 3 if len(X) < 500 else 5
    tscv = TimeSeriesSplit(n_splits=cv_folds)
    
    # Hiperparametre havuzu
    param_dist = {
        'n_estimators': randint(100, 300),
        'max_depth': randint(3, 8),
        'learning_rate': uniform(0.01, 0.2),
        'subsample': uniform(0.6, 0.4),
        'colsample_bytree': uniform(0.6, 0.4),
        'gamma': uniform(0, 0.5),
        'reg_alpha': uniform(0, 2),
        'reg_lambda': uniform(0, 2)
    }
    
    base_model = xgb.XGBClassifier(
        objective='binary:logistic', 
        random_state=42, 
        use_label_encoder=False, 
        eval_metric='logloss'
    )
    
    # Rastgele Arama ile En İyi Parametreleri Bulma
    # İşlem süresini makul tutmak için n_iter=15 yapıldı. (İstersen artırabilirsin)
    random_search = RandomizedSearchCV(
        base_model, 
        param_distributions=param_dist, 
        n_iter=15, 
        cv=tscv, 
        scoring='roc_auc', 
        n_jobs=-1, 
        random_state=42
    )
    
    random_search.fit(X, y)
    best_model = random_search.best_estimator_
    
    # Model Kalibrasyonu (Olasılık Yüzdelerini Gerçekçi Kılmak İçin)
    calibrated_model = CalibratedClassifierCV(best_model, method='sigmoid', cv=3)
    calibrated_model.fit(X, y)
    
    # Modeli Kaydet
    path = f"{MODEL_DIR}/{market_name}_{label}.pkl"
    joblib.dump(calibrated_model, path)
    
    # Özellik önemlerini döndür (Sadece bilgi amaçlı)
    importances = best_model.feature_importances_
    fi_df = pd.DataFrame({'feature': feature_names, 'importance': importances}).sort_values('importance', ascending=False)
    
    return fi_df

# ==========================================
# 4. ANA İŞLEM
# ==========================================
def main():
    df = get_data_from_db(TARGET_YEAR, TARGET_WEEK)
    
    if len(df) == 0:
        print("[HATA] Veritabanında eşleşen maç bulunamadı.")
        return

    df = create_dynamic_features(df)
    
    targets = {
        'o15': 'target_o15', 'o25': 'target_o25', 'btts_yes': 'target_btts_yes'
        # İhtiyacın olan diğer marketleri buraya ekleyebilirsin
    }
    
    feature_cols = [
        'h_avg_gf', 'h_avg_ga', 'a_avg_gf', 'a_avg_ga', 'h_win_rate', 'a_win_rate',
        'odds_1', 'odds_x', 'odds_2', 'odds_o25', 'odds_u25', 'odds_btts_yes'
    ]

    print(f"\n[3/4] Modeller hiperparametre optimizasyonu ile eğitiliyor... (Hedef: {TARGET_YEAR}-W{TARGET_WEEK})")
    
    # Eksik verileri temizle
    df_clean = df.dropna(subset=feature_cols)
    print(f"     Eksik oranlar/istatistikler silindikten sonra kalan maç sayısı: {len(df_clean)}")

    X = df_clean[feature_cols]
    
    # Verileri ölçeklendir
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    label = f"{TARGET_YEAR}_W{TARGET_WEEK}"
    joblib.dump(scaler, f"{MODEL_DIR}/scaler_{label}.pkl")

    for market_label, target_col in targets.items():
        print(f"\n---> {market_label} modeli için en iyi parametreler aranıyor...")
        y = df_clean[target_col]
        
        # Eğit, kaydet ve hangi istatistiklerin önemli olduğunu al
        importance_df = train_and_save_model(X_scaled, y, label, market_label, feature_cols)
        
        print(f"     ✓ {market_label} başarıyla eğitildi!")
        print(f"     En çok dikkat edilen 3 istatistik:\n{importance_df.head(3).to_string(index=False)}")

    print("\n[4/4] İŞLEM BAŞARILI! Modeller ve Scaler 'models_weekly' klasörüne kaydedildi.")

if __name__ == "__main__":
    main()
