#!/usr/bin/env python3
import os
import mysql.connector
import pandas as pd
import numpy as np
import joblib
import xgboost as xgb
from sklearn.preprocessing import StandardScaler
from sklearn.calibration import CalibratedClassifierCV
import warnings

# Gereksiz uyarıları gizle
warnings.filterwarnings('ignore')

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

def get_data_from_db():
    print("[1/4] Veritabanına bağlanılıyor ve geçmiş maçlar çekiliyor...")
    conn = mysql.connector.connect(**CONFIG["db"])
    # Tüm bitmiş maçları en eskiden en yeniye doğru sıralı çekiyoruz
    query = """
        SELECT * FROM results_football 
        WHERE status IN ('finished','ended') 
          AND match_year IS NOT NULL 
          AND match_week IS NOT NULL
        ORDER BY start_utc ASC, start_time_utc ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ==========================================
# 2. ÖZELLİK MÜHENDİSLİĞİ (DİNAMİK HESAPLAMA)
# ==========================================
def create_dynamic_features(df):
    print(f"[2/4] Toplam {len(df)} maç için dinamik istatistikler hesaplanıyor (Bu işlem biraz sürebilir)...")
    df = df.copy()
    
    # Hedef Değişkenler (Marketler) - Kendi istediğin marketlere göre ayarlandı
    df['total_goals'] = df['ft_home'] + df['ft_away']
    
    # Alt / Üst Marketleri
    df['target_o15'] = (df['total_goals'] > 1.5).astype(int)
    df['target_o25'] = (df['total_goals'] > 2.5).astype(int)
    df['target_o35'] = (df['total_goals'] > 3.5).astype(int)
    df['target_u15'] = (df['total_goals'] <= 1.5).astype(int)
    df['target_u25'] = (df['total_goals'] <= 2.5).astype(int)
    df['target_u35'] = (df['total_goals'] <= 3.5).astype(int)
    
    # Karşılıklı Gol (KG) Marketleri
    df['target_btts_yes'] = ((df['ft_home'] > 0) & (df['ft_away'] > 0)).astype(int)
    df['target_btts_no'] = ((df['ft_home'] == 0) | (df['ft_away'] == 0)).astype(int)
    
    # Çifte Şans Marketleri
    df['target_1x'] = (df['ft_home'] >= df['ft_away']).astype(int)
    df['target_x2'] = (df['ft_away'] >= df['ft_home']).astype(int)
    df['target_12'] = (df['ft_home'] != df['ft_away']).astype(int)

    # İstatistikleri tutacağımız yeni sütunlar
    features = ['h_avg_gf', 'h_avg_ga', 'a_avg_gf', 'a_avg_ga', 'h_win_rate', 'a_win_rate']
    for f in features: 
        df[f] = np.nan

    team_history = {}

    for idx, row in df.iterrows():
        h_team = row['home_team']
        a_team = row['away_team']

        # Ev sahibi geçmiş istatistikleri hesapla (Son 5 maç)
        if h_team in team_history and len(team_history[h_team]) >= 3:
            last_matches = team_history[h_team][-5:]
            df.at[idx, 'h_avg_gf'] = np.mean([m['gf'] for m in last_matches])
            df.at[idx, 'h_avg_ga'] = np.mean([m['ga'] for m in last_matches])
            df.at[idx, 'h_win_rate'] = np.mean([1 if m['gf'] > m['ga'] else 0 for m in last_matches])

        # Deplasman geçmiş istatistikleri hesapla (Son 5 maç)
        if a_team in team_history and len(team_history[a_team]) >= 3:
            last_matches = team_history[a_team][-5:]
            df.at[idx, 'a_avg_gf'] = np.mean([m['gf'] for m in last_matches])
            df.at[idx, 'a_avg_ga'] = np.mean([m['ga'] for m in last_matches])
            df.at[idx, 'a_win_rate'] = np.mean([1 if m['gf'] > m['ga'] else 0 for m in last_matches])

        # Maç bitti, sonucu gelecekteki maçlar için hafızaya al
        for team, gf, ga in [(h_team, row['ft_home'], row['ft_away']), (a_team, row['ft_away'], row['ft_home'])]:
            if team not in team_history: 
                team_history[team] = []
            team_history[team].append({'gf': gf, 'ga': ga})

    # Geçmişi olmayan (ilk birkaç maç) satırları veri setinden çıkarıyoruz
    df = df.dropna(subset=['h_avg_gf', 'a_avg_gf'])
    print("[BİLGİ] Özellik mühendisliği tamamlandı.")
    return df

# ==========================================
# 3. EĞİTİM FONKSİYONU
# ==========================================
def train_and_save_model(X, y, week_label, market_name):
    # Temel XGBoost Modeli
    model = xgb.XGBClassifier(
        n_estimators=150, 
        max_depth=4, 
        learning_rate=0.05, 
        random_state=42, 
        eval_metric='logloss',
        use_label_encoder=False
    )
    
    # Çıktıların güvenilir yüzdelere (olasılıklara) dönüşmesi için kalibrasyon kullanıyoruz
    calibrated_model = CalibratedClassifierCV(model, method='sigmoid', cv=3)
    calibrated_model.fit(X, y)
    
    path = f"{MODEL_DIR}/{market_name}_{week_label}.pkl"
    joblib.dump(calibrated_model, path)

# ==========================================
# 4. ANA DÖNGÜ (BACKTEST)
# ==========================================
def main():
    df = get_data_from_db()
    df = create_dynamic_features(df)
    
    # Eğiteceğimiz hedefler (Market Adı -> Sütun Adı)
    targets = {
        '1x': 'target_1x', 'x2': 'target_x2', '12': 'target_12',
        'o15': 'target_o15', 'u15': 'target_u15',
        'o25': 'target_o25', 'u25': 'target_u25',
        'o35': 'target_o35', 'u35': 'target_u35',
        'btts_yes': 'target_btts_yes', 'btts_no': 'target_btts_no'
    }
    
    # Modelin öğrenmesi için kullanacağı kolonlar (Oranlar dahil)
    feature_cols = [
        'h_avg_gf', 'h_avg_ga', 'a_avg_gf', 'a_avg_ga', 'h_win_rate', 'a_win_rate',
        'odds_1', 'odds_x', 'odds_2', 'odds_o25', 'odds_u25', 'odds_btts_yes'
    ]

    # HEDEF HAFTALAR: Senin belirttiğin test haftaları
    target_year = 2026
    target_weeks = [10, 11, 12, 13, 14, 15]

    print(f"[3/4] Modeller eğitiliyor... (Yıl: {target_year}, Haftalar: {target_weeks})")

    for week in target_weeks:
        print(f"\n---> Yıl: {target_year} | Hafta: {week} İçin Veriler Hazırlanıyor...")
        
        # DİKKAT: Veri Sızıntısını Önleme Filtresi!
        # Eğer 11. haftayı eğitiyorsak; sadece geçmiş yılları VEYA 2026'nın 10. haftası ve öncesini alıyoruz.
        train_mask = (df['match_year'] < target_year) | ((df['match_year'] == target_year) & (df['match_week'] < week))
        train_data = df[train_mask].copy()
        
        if len(train_data) < 100:
            print(f"[UYARI] {target_year}-W{week} için yeterli geçmiş veri yok, atlanıyor!")
            continue
            
        print(f"     Eğitim veri seti boyutu: {len(train_data)} maç.")

        # X (Özellikler) ve Na değerleri 0 ile doldurma (Oranı olmayan maçlar için)
        X = train_data[feature_cols].fillna(0)
        
        # Verileri aynı ölçeğe getirme
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Gelecekte tahmin yaparken kullanmak üzere Scaler'ı bu haftaya özel kaydediyoruz
        scaler_path = f"{MODEL_DIR}/scaler_{target_year}_W{week}.pkl"
        joblib.dump(scaler, scaler_path)

        # Seçtiğimiz her market için modeli eğit ve kaydet
        for market_label, target_col in targets.items():
            y = train_data[target_col]
            week_label = f"{target_year}_W{week}"
            
            # Model eğitimi
            train_and_save_model(X_scaled, y, week_label, market_label)
            print(f"     ✓ {market_label} modeli eğitildi ve kaydedildi.")

    print("\n[4/4] İŞLEM BAŞARILI! Tüm modeller 'models_weekly' klasörüne başarıyla oluşturuldu.")

if __name__ == "__main__":
    main()
