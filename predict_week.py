#!/usr/bin/env python3
"""
predict_week.py
---------------
Belirtilen yıl ve haftadaki maçları, eğitilmiş Random Forest modelleri kullanarak tahmin eder.
Modellerin ve scaler'ın 'models/' klasöründe olduğu varsayılır.
"""

import os
import sys
import argparse
import mysql.connector
import pandas as pd
import numpy as np
import joblib
from sklearn.preprocessing import StandardScaler

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

def get_connection():
    return mysql.connector.connect(**CONFIG["db"])

def load_models_and_scaler(version_suffix):
    """Tüm modelleri ve scaler'ı yükler."""
    models = {}
    target_names = ['result', 'over15', 'over25', 'over35', 'under15', 'under25', 'under35', 'btts', 'dc_1x', 'dc_x2', 'dc_12']
    for name in target_names:
        model_path = f"{MODEL_DIR}/{name}_model_{version_suffix}.pkl"
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model dosyası bulunamadı: {model_path}")
        models[name] = joblib.load(model_path)
    scaler_path = f"{MODEL_DIR}/scaler_{version_suffix}.pkl"
    if not os.path.exists(scaler_path):
        raise FileNotFoundError(f"Scaler dosyası bulunamadı: {scaler_path}")
    scaler = joblib.load(scaler_path)
    print(f"[Yükleme] {len(models)} model ve scaler yüklendi (versiyon: {version_suffix})")
    return models, scaler

def fetch_matches_for_week(year, week):
    """Belirtilen haftadaki oynanmamış (status != 'finished') maçları getirir."""
    conn = get_connection()
    query = """
    SELECT 
        event_id,
        tournament_id,
        home_team,
        away_team,
        referee,
        start_utc
    FROM results_football
    WHERE match_year = %s AND match_week = %s
      AND (status IS NULL OR status NOT IN ('finished','ended'))
    """
    df = pd.read_sql(query, conn, params=(year, week))
    conn.close()
    print(f"[Veri] {len(df)} maç bulundu (hafta {week}/{year})")
    return df

def build_features_for_matches(matches_df):
    """
    Her maç için gerekli istatistikleri analitik tablolardan çeker.
    Eksik değerler medyan ile doldurulur (train'deki gibi).
    """
    conn = get_connection()
    # Maç listesinden event_id'leri al
    event_ids = tuple(matches_df['event_id']) if len(matches_df) > 0 else (0,)
    # Tek sorgu ile tüm maçların feature'larını çek
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
        -- Form
        hf.points_last_5 as home_form_pts, af.points_last_5 as away_form_pts,
        -- Verimlilik
        he.conversion_rate_pct as home_conv, ae.conversion_rate_pct as away_conv,
        he.save_rate_pct as home_save, ae.save_rate_pct as away_save,
        -- İlk yarı BTTS
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
    WHERE r.event_id IN ({','.join(['%s']*len(event_ids))})
    """
    cursor = conn.cursor()
    cursor.execute(query, event_ids)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    conn.close()
    df_features = pd.DataFrame(data, columns=columns)
    
    # Eksik değerleri medyan ile doldur (train'deki gibi)
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
    for col in feature_cols:
        if col in df_features.columns:
            df_features[col].fillna(df_features[col].median(), inplace=True)
        else:
            df_features[col] = 0  # fallback
    return df_features

def predict_matches(models, scaler, features_df, original_matches_df):
    """Özellikleri kullanarak tüm modellerle tahmin yapar."""
    # Özellik sırasını train ile aynı tut (scaler'ın fit edildiği sıra)
    # Scaler'ın feature_names_in_ attribute'ü varsa kullan, yoksa varsayılan sırayı kullan
    if hasattr(scaler, 'feature_names_in_'):
        expected_cols = scaler.feature_names_in_
    else:
        # fallback: train_all_models.py'deki feature_cols sırası
        expected_cols = [
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
    # Eksik sütunları kontrol et
    for col in expected_cols:
        if col not in features_df.columns:
            features_df[col] = 0
    X = features_df[expected_cols].values
    X_scaled = scaler.transform(X)
    
    predictions = {}
    for name, model in models.items():
        if name == 'result':
            # result: sınıf probları [beraberlik, ev, deplasman] (model sınıfları 0,1,2)
            probs = model.predict_proba(X_scaled)
            # Anlamlı etiketler: 0:Beraberlik, 1:Ev, 2:Deplasman
            predictions[name] = probs
        else:
            # binary: pozitif sınıfın olasılığı (1 olan)
            probs = model.predict_proba(X_scaled)[:, 1]
            predictions[name] = probs
    return predictions

def main():
    parser = argparse.ArgumentParser(description="Belirli bir haftadaki maçları tahmin et")
    parser.add_argument("--year", type=int, required=True, help="Tahmin yapılacak yıl (örn: 2026)")
    parser.add_argument("--week", type=int, required=True, help="Tahmin yapılacak hafta (örn: 11)")
    parser.add_argument("--model-version", type=str, default=None, 
                        help="Model versiyonu (örn: 2026_week10). Varsayılan: {year}_week{week-1}")
    args = parser.parse_args()
    
    if args.model_version is None:
        prev_week = args.week - 1
        if prev_week < 1:
            print("Hata: 1. hafta için model versiyonu otomatik belirlenemez. Lütfen --model-version verin.")
            sys.exit(1)
        model_version = f"{args.year}_week{prev_week}"
    else:
        model_version = args.model_version
    
    print(f"===== TAHMİN BAŞLADI (Yıl: {args.year}, Hafta: {args.week}) =====")
    print(f"Kullanılan model versiyonu: {model_version}")
    
    # 1. Modelleri ve scaler'ı yükle
    try:
        models, scaler = load_models_and_scaler(model_version)
    except FileNotFoundError as e:
        print(f"Hata: {e}")
        sys.exit(1)
    
    # 2. Tahmin yapılacak maçları getir
    matches = fetch_matches_for_week(args.year, args.week)
    if matches.empty:
        print("Tahmin yapılacak maç bulunamadı.")
        return
    
    # 3. Maçlar için feature'ları oluştur
    features_df = build_features_for_matches(matches)
    if features_df.empty:
        print("Feature oluşturulamadı.")
        return
    
    # 4. Tahminleri hesapla
    preds = predict_matches(models, scaler, features_df, matches)
    
    # 5. Sonuçları ekrana yaz ve isteğe bağlı kaydet
    print("\n" + "="*80)
    print(f"TAHMİN SONUÇLARI - {args.year} Hafta {args.week}")
    print("="*80)
    for idx, row in matches.iterrows():
        event_id = row['event_id']
        home = row['home_team']
        away = row['away_team']
        print(f"\n{home} vs {away} (ID: {event_id})")
        # result olasılıkları
        res_probs = preds['result'][idx]
        print(f"  Maç Sonucu: Beraberlik %{res_probs[0]*100:.1f} | Ev %{res_probs[1]*100:.1f} | Deplasman %{res_probs[2]*100:.1f}")
        print(f"  Alt/Üst: 1.5 Üst %{preds['over15'][idx]*100:.1f} | 2.5 Üst %{preds['over25'][idx]*100:.1f} | 3.5 Üst %{preds['over35'][idx]*100:.1f}")
        print(f"  KG (BTTS): Evet %{preds['btts'][idx]*100:.1f}")
        print(f"  Çifte Şans: 1X %{preds['dc_1x'][idx]*100:.1f} | X2 %{preds['dc_x2'][idx]*100:.1f} | 12 %{preds['dc_12'][idx]*100:.1f}")
    print("="*80)

if __name__ == "__main__":
    main()
