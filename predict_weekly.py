#!/usr/bin/env python3
"""
predict_weekly.py
-----------------
10. hafta modeli ile 11. hafta maçlarına,
11. hafta modeli ile 12. hafta maçlarına,
...
15. hafta modeli ile 16. hafta maçlarına tahmin yapar.
Tahminleri 'predictions' tablosuna kaydeder.

Kullanım:
    python predict_weekly.py --model-year 2026 --model-week 10 --target-week 11
    (veya tüm çiftleri otomatik işlemek için --auto)
"""

import os
import sys
import argparse
import mysql.connector
import pandas as pd
import numpy as np
import joblib
from datetime import datetime

# ---------- KONFIG ----------
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

# ---------- VERİTABANI BAĞLANTISI ----------
def get_connection():
    return mysql.connector.connect(**CONFIG["db"])

# ---------- TAHMİN TABLOSUNU OLUŞTUR (eğer yoksa) ----------
def create_predictions_table():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            event_id BIGINT UNSIGNED NOT NULL,
            target_name VARCHAR(20) NOT NULL,
            prediction_prob FLOAT,
            prediction_class INT,
            model_version VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_pred (event_id, target_name, model_version),
            FOREIGN KEY (event_id) REFERENCES results_football(event_id) ON DELETE CASCADE
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("[TABLO] predictions tablosu hazır.")

# ---------- TAHMİN EDİLECEK MAÇLARI VE ÖZELLİKLERİ YÜKLE ----------
def load_match_features(model_year, target_week):
    """
    target_week haftasındaki başlamamış maçların özelliklerini
    team_analytics, league_analytics vb. tablolarla birleştirerek getirir.
    """
    conn = get_connection()
    query = f"""
    SELECT 
        r.event_id,
        r.tournament_id,
        -- Ev sahibi istatistikleri
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
    WHERE r.status IN ('notstarted', 'scheduled')
      AND r.match_year = {model_year}
      AND r.match_week = {target_week}
    """
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"{len(df)} adet tahmin edilecek maç yüklendi (yıl={model_year}, hafta={target_week}).")
    return df

# ---------- ÖZELLİKLERİ HAZIRLA (train_all_models ile aynı) ----------
def prepare_features(df):
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
    exist_features = [c for c in feature_cols if c in df.columns]
    X = df[exist_features].copy()
    # Eksik değerleri doldur (medyan veya 0)
    for col in X.columns:
        if X[col].dtype in ['float64', 'int64']:
            X[col].fillna(X[col].median(), inplace=True)
        else:
            X[col].fillna(0, inplace=True)
    return X

# ---------- TAHMİN YAP VE KAYDET ----------
def predict_and_save(model_year, model_week, target_week):
    """
    model_week ile eğitilmiş modelleri kullanarak target_week maçlarını tahmin eder.
    """
    # 1. Tahmin edilecek maçların özelliklerini yükle
    df_matches = load_match_features(model_year, target_week)
    if df_matches.empty:
        print(f"  -> Hafta {target_week} için tahmin edilecek maç yok.")
        return

    # 2. Özellikleri hazırla
    X = prepare_features(df_matches)
    print(f"  Özellik matrisi boyutu: {X.shape}")

    # 3. Scaler'ı yükle
    scaler_path = f"{MODEL_DIR}/scaler_{model_year}_week{model_week}.pkl"
    if not os.path.exists(scaler_path):
        print(f"  HATA: Scaler dosyası bulunamadı: {scaler_path}")
        return
    scaler = joblib.load(scaler_path)
    X_scaled = scaler.transform(X)

    # 4. Tahmin edilecek hedefler (train_all_models'deki sıra ile aynı)
    targets = [
        'result', 'over15', 'over25', 'over35',
        'under15', 'under25', 'under35',
        'btts', 'dc_1x', 'dc_x2', 'dc_12'
    ]

    conn = get_connection()
    cursor = conn.cursor()

    # Her hedef için modeli yükle ve tahmin yap
    for target in targets:
        model_path = f"{MODEL_DIR}/{target}_model_{model_year}_week{model_week}.pkl"
        if not os.path.exists(model_path):
            print(f"  UYARI: Model dosyası bulunamadı: {model_path}, atlanıyor.")
            continue

        model = joblib.load(model_path)
        # Tahmin olasılıkları
        proba = model.predict_proba(X_scaled)

        # result (multiclass: 0,1,2) için 3 sınıf, diğerleri binary
        if target == 'result':
            # Her maç için en yüksek olasılıklı sınıf ve olasılık değeri
            pred_class = np.argmax(proba, axis=1)
            pred_prob = np.max(proba, axis=1)
        else:
            # Binary: proba[:,1] -> sınıf 1 olasılığı
            pred_prob = proba[:, 1]
            pred_class = (pred_prob >= 0.5).astype(int)

        # Veritabanına kaydet
        model_version = f"{model_year}_week{model_week}"
        for idx, row in df_matches.iterrows():
            event_id = int(row['event_id'])
            prob_val = float(pred_prob[idx])
            class_val = int(pred_class[idx])
            try:
                cursor.execute("""
                    INSERT INTO predictions (event_id, target_name, prediction_prob, prediction_class, model_version)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        prediction_prob = VALUES(prediction_prob),
                        prediction_class = VALUES(prediction_class),
                        created_at = CURRENT_TIMESTAMP
                """, (event_id, target, prob_val, class_val, model_version))
            except Exception as e:
                print(f"    Kayıt hatası (event_id={event_id}, target={target}): {e}")
        conn.commit()
        print(f"  {target} tahminleri kaydedildi.")

    cursor.close()
    conn.close()
    print(f"  -> Hafta {target_week} tahminleri tamamlandı.\n")

# ---------- ANA FONKSİYON ----------
def main():
    parser = argparse.ArgumentParser(description="Football match predictions using weekly models")
    parser.add_argument("--model-year", type=int, default=2026, help="Model eğitim yılı (örn: 2026)")
    parser.add_argument("--model-week", type=int, help="Model haftası (örn: 10). Tek çift için kullanılır.")
    parser.add_argument("--target-week", type=int, help="Tahmin edilecek hafta (model_week+1)")
    parser.add_argument("--auto", action="store_true", help="Otomatik olarak 10->11, 11->12, ..., 15->16 işlemlerini yap")
    args = parser.parse_args()

    # Tahmin tablosunu oluştur
    create_predictions_table()

    if args.auto:
        # Belirtilen aralık: 10'dan 15'e kadar model week
        for mw in range(10, 16):
            tw = mw + 1
            print(f"\n===== Model haftası {mw} -> Tahmin haftası {tw} =====")
            predict_and_save(args.model_year, mw, tw)
    else:
        if args.model_week is None or args.target_week is None:
            print("Lütfen --model-week ve --target-week belirtin veya --auto kullanın.")
            sys.exit(1)
        print(f"\n===== Model haftası {args.model_week} -> Tahmin haftası {args.target_week} =====")
        predict_and_save(args.model_year, args.model_week, args.target_week)

    print("\n✅ Tüm tahminler başarıyla veritabanına kaydedildi.")

if __name__ == "__main__":
    main()
