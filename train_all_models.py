#!/usr/bin/env python3
"""
train_all_models.py
-------------------
- results_football tablosundaki bitmiş maçları kullanarak
- gerekli analitik tabloları (team_analytics, league_analytics, vs.) oluşturur
- Belirtilen yıl ve haftaya kadarki verileri kullanarak 11 farklı hedef için Random Forest modelleri eğitir
- Modelleri scaler ile birlikte 'models/' klasörüne kaydeder
"""

import os
import sys
import argparse
import mysql.connector
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.preprocessing import StandardScaler
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
os.makedirs(MODEL_DIR, exist_ok=True)

# ---------- VERİTABANI BAĞLANTISI ----------
def get_connection():
    return mysql.connector.connect(**CONFIG["db"])

# ---------- YARDIMCI FONKSİYON: SQL ZAMAN FİLTRESİ ----------
def get_time_filter(target_year, target_week, table_alias=""):
    """
    Belirtilen yıl ve haftaya kadar olan maçları filtrelemek için SQL stringi oluşturur.
    """
    if not target_year or not target_week:
        return ""
    
    prefix = f"{table_alias}." if table_alias else ""
    return f" AND ({prefix}match_year < {target_year} OR ({prefix}match_year = {target_year} AND {prefix}match_week <= {target_week}))"

# ---------- 1. ANALİTİK TABLOLARI OLUŞTUR (eğer yoksa) ----------
def create_analytics_tables():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS team_analytics (
            team_name VARCHAR(128), tournament_id INT, venue_type ENUM('Home','Away'),
            matches_played INT, goals_for INT, goals_against INT, wins INT, draws INT, losses INT,
            avg_possession DECIMAL(5,2), avg_shots DECIMAL(5,2), avg_shots_on DECIMAL(5,2),
            PRIMARY KEY (team_name, tournament_id, venue_type)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS team_form_analytics (
            team_name VARCHAR(128), tournament_id INT, venue_type ENUM('Home','Away'),
            points_last_5 INT, PRIMARY KEY (team_name, tournament_id, venue_type)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS team_efficiency_analytics (
            team_name VARCHAR(128), tournament_id INT, venue_type ENUM('Home','Away'),
            conversion_rate_pct DECIMAL(5,2), save_rate_pct DECIMAL(5,2),
            PRIMARY KEY (team_name, tournament_id, venue_type)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS team_half_time_analytics (
            team_name VARCHAR(128), tournament_id INT, venue_type ENUM('Home','Away'),
            ht_btts_yes_pct DECIMAL(5,2), PRIMARY KEY (team_name, tournament_id, venue_type)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS league_analytics (
            tournament_id INT PRIMARY KEY, avg_goals_match DECIMAL(5,2),
            home_win_pct DECIMAL(5,2), draw_pct DECIMAL(5,2)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS referee_analytics (
            referee_name VARCHAR(128) PRIMARY KEY, avg_goals_match DECIMAL(5,2)
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("[TABLOLAR] Analitik tablolar hazır.")

# ---------- 2. ANALİTİK TABLOLARI DOLDUR ----------
def populate_analytics_tables(target_year=None, target_week=None):
    conn = get_connection()
    cursor = conn.cursor()
    
    # Geçmişe dönük işlem yapıyorsak tabloları temizlemeliyiz ki gelecek verileri karışmasın
    print("[ANALİTİK] Önceki analitik verileri temizleniyor (Geçmiş veri sızıntısını önlemek için)...")
    cursor.execute("TRUNCATE TABLE team_analytics")
    cursor.execute("TRUNCATE TABLE team_form_analytics")
    cursor.execute("TRUNCATE TABLE team_efficiency_analytics")
    cursor.execute("TRUNCATE TABLE team_half_time_analytics")
    cursor.execute("TRUNCATE TABLE league_analytics")
    cursor.execute("TRUNCATE TABLE referee_analytics")
    conn.commit()

    print(f"[ANALİTİK] Tablolar {target_year if target_year else 'günümüz'} yılı {target_week if target_week else 'son'} haftasına göre dolduruluyor...")
    
    time_filter = get_time_filter(target_year, target_week)

    # 1. team_analytics
    query_team = f"""
        INSERT INTO team_analytics (team_name, tournament_id, venue_type, matches_played, goals_for, goals_against, wins, draws, losses, avg_possession, avg_shots, avg_shots_on)
        SELECT team, tournament_id, venue, COUNT(*) as matches_played, SUM(goals_for) as goals_for, SUM(goals_against) as goals_against,
            SUM(CASE WHEN goals_for > goals_against THEN 1 ELSE 0 END) as wins, SUM(CASE WHEN goals_for = goals_against THEN 1 ELSE 0 END) as draws,
            SUM(CASE WHEN goals_for < goals_against THEN 1 ELSE 0 END) as losses, AVG(possession) as avg_possession, AVG(shots) as avg_shots, AVG(shots_on) as avg_shots_on
        FROM (
            SELECT home_team as team, tournament_id, 'Home' as venue, ft_home as goals_for, ft_away as goals_against, poss_h as possession, shot_h as shots, shot_on_h as shots_on
            FROM results_football WHERE status IN ('finished','ended') AND ft_home IS NOT NULL {time_filter}
            UNION ALL
            SELECT away_team as team, tournament_id, 'Away' as venue, ft_away as goals_for, ft_home as goals_against, poss_a as possession, shot_a as shots, shot_on_a as shots_on
            FROM results_football WHERE status IN ('finished','ended') AND ft_away IS NOT NULL {time_filter}
        ) t GROUP BY team, tournament_id, venue
    """
    cursor.execute(query_team)

    # 2. team_form_analytics
    query_form = f"""
        INSERT INTO team_form_analytics (team_name, tournament_id, venue_type, points_last_5)
        SELECT team, tournament_id, venue, SUM(points) as points_last_5
        FROM (
            SELECT home_team as team, tournament_id, 'Home' as venue, start_utc,
                CASE WHEN ft_home > ft_away THEN 3 WHEN ft_home = ft_away THEN 1 ELSE 0 END as points,
                ROW_NUMBER() OVER (PARTITION BY home_team, tournament_id ORDER BY start_utc DESC) as rn
            FROM results_football WHERE status IN ('finished','ended') AND ft_home IS NOT NULL {time_filter}
            UNION ALL
            SELECT away_team as team, tournament_id, 'Away' as venue, start_utc,
                CASE WHEN ft_away > ft_home THEN 3 WHEN ft_away = ft_home THEN 1 ELSE 0 END as points,
                ROW_NUMBER() OVER (PARTITION BY away_team, tournament_id ORDER BY start_utc DESC) as rn
            FROM results_football WHERE status IN ('finished','ended') AND ft_away IS NOT NULL {time_filter}
        ) ranked WHERE rn <= 5 GROUP BY team, tournament_id, venue
    """
    cursor.execute(query_form)

    # 3. team_efficiency_analytics
    query_efficiency = f"""
        INSERT INTO team_efficiency_analytics (team_name, tournament_id, venue_type, conversion_rate_pct, save_rate_pct)
        SELECT team, tournament_id, venue,
            AVG(CASE WHEN shots > 0 THEN (goals * 100.0 / shots) ELSE NULL END) as conv_pct,
            AVG(CASE WHEN shots_on_target_opp > 0 THEN (saves * 100.0 / shots_on_target_opp) ELSE NULL END) as save_pct
        FROM (
            SELECT home_team as team, tournament_id, 'Home' as venue, ft_home as goals, shot_h as shots, shot_on_a as shots_on_target_opp, saves_h as saves
            FROM results_football WHERE status IN ('finished','ended') AND ft_home IS NOT NULL {time_filter}
            UNION ALL
            SELECT away_team as team, tournament_id, 'Away' as venue, ft_away as goals, shot_a as shots, shot_on_h as shots_on_target_opp, saves_a as saves
            FROM results_football WHERE status IN ('finished','ended') AND ft_away IS NOT NULL {time_filter}
        ) t GROUP BY team, tournament_id, venue
    """
    cursor.execute(query_efficiency)

    # 4. team_half_time_analytics
    query_ht_btts = f"""
        INSERT INTO team_half_time_analytics (team_name, tournament_id, venue_type, ht_btts_yes_pct)
        SELECT team, tournament_id, venue, AVG(CASE WHEN ht_home > 0 AND ht_away > 0 THEN 100 ELSE 0 END) as ht_btts_pct
        FROM (
            SELECT home_team as team, tournament_id, 'Home' as venue, ht_home, ht_away
            FROM results_football WHERE status IN ('finished','ended') AND ht_home IS NOT NULL AND ht_away IS NOT NULL {time_filter}
            UNION ALL
            SELECT away_team as team, tournament_id, 'Away' as venue, ht_home, ht_away
            FROM results_football WHERE status IN ('finished','ended') AND ht_home IS NOT NULL AND ht_away IS NOT NULL {time_filter}
        ) t GROUP BY team, tournament_id, venue
    """
    cursor.execute(query_ht_btts)

    # 5. league_analytics
    query_league = f"""
        INSERT INTO league_analytics (tournament_id, avg_goals_match, home_win_pct, draw_pct)
        SELECT tournament_id, AVG(ft_home + ft_away) as avg_goals,
            AVG(CASE WHEN ft_home > ft_away THEN 1 ELSE 0 END) * 100 as home_win_pct,
            AVG(CASE WHEN ft_home = ft_away THEN 1 ELSE 0 END) * 100 as draw_pct
        FROM results_football WHERE status IN ('finished','ended') AND ft_home IS NOT NULL {time_filter}
        GROUP BY tournament_id
    """
    cursor.execute(query_league)

    # 6. referee_analytics
    query_referee = f"""
        INSERT INTO referee_analytics (referee_name, avg_goals_match)
        SELECT referee, AVG(ft_home + ft_away) as avg_goals
        FROM results_football WHERE status IN ('finished','ended') AND referee IS NOT NULL AND ft_home IS NOT NULL {time_filter}
        GROUP BY referee
    """
    cursor.execute(query_referee)

    conn.commit()
    cursor.close()
    conn.close()
    print("[ANALİTİK] Tüm analitik tablolar başarıyla güncellendi.")

# ---------- 3. EĞİTİM VERİSİNİ YÜKLE ----------
def load_training_data(target_year=None, target_week=None):
    conn = get_connection()
    time_filter = get_time_filter(target_year, target_week, table_alias="r")
    
    query = f"""
    SELECT 
        r.event_id, r.tournament_id, r.ft_home, r.ft_away,
        ha.matches_played as home_matches, ha.goals_for as home_gf, ha.goals_against as home_ga, ha.wins as home_wins, ha.draws as home_draws, ha.losses as home_losses, ha.avg_possession as home_poss, ha.avg_shots as home_shots, ha.avg_shots_on as home_sot,
        aa.matches_played as away_matches, aa.goals_for as away_gf, aa.goals_against as away_ga, aa.wins as away_wins, aa.draws as away_draws, aa.losses as away_losses, aa.avg_possession as away_poss, aa.avg_shots as away_shots, aa.avg_shots_on as away_sot,
        hf.points_last_5 as home_form_pts, af.points_last_5 as away_form_pts,
        he.conversion_rate_pct as home_conv, ae.conversion_rate_pct as away_conv, he.save_rate_pct as home_save, ae.save_rate_pct as away_save,
        hht.ht_btts_yes_pct as home_ht_btts, aht.ht_btts_yes_pct as away_ht_btts,
        la.avg_goals_match as league_avg_goals, la.home_win_pct as league_home_win_pct, la.draw_pct as league_draw_pct,
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
    WHERE r.status IN ('finished','ended') AND r.ft_home IS NOT NULL AND r.ft_away IS NOT NULL
    {time_filter}
    """
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"{len(df)} bitmiş maç yüklendi.")
    return df

# ---------- 4. HEDEF DEĞİŞKENLERİ OLUŞTUR ----------
def create_targets(df):
    df = df.copy()
    df['result'] = np.where(df['ft_home'] > df['ft_away'], 1, np.where(df['ft_home'] == df['ft_away'], 0, 2))
    total = df['ft_home'] + df['ft_away']
    df['over15'] = (total > 1.5).astype(int)
    df['over25'] = (total > 2.5).astype(int)
    df['over35'] = (total > 3.5).astype(int)
    df['under15'] = (total < 1.5).astype(int)
    df['under25'] = (total < 2.5).astype(int)
    df['under35'] = (total < 3.5).astype(int)
    df['btts'] = ((df['ft_home'] > 0) & (df['ft_away'] > 0)).astype(int)
    df['dc_1x'] = (df['ft_home'] >= df['ft_away']).astype(int)
    df['dc_x2'] = (df['ft_home'] <= df['ft_away']).astype(int)
    df['dc_12'] = (df['ft_home'] != df['ft_away']).astype(int)
    return df

# ---------- 5. ÖZELLİK HAZIRLAMA ----------
def prepare_features(df):
    feature_cols = [
        'home_matches', 'home_gf', 'home_ga', 'home_wins', 'home_draws', 'home_losses', 'home_poss', 'home_shots', 'home_sot',
        'away_matches', 'away_gf', 'away_ga', 'away_wins', 'away_draws', 'away_losses', 'away_poss', 'away_shots', 'away_sot',
        'home_form_pts', 'away_form_pts', 'home_conv', 'away_conv', 'home_save', 'away_save', 'home_ht_btts', 'away_ht_btts',
        'league_avg_goals', 'league_home_win_pct', 'league_draw_pct', 'ref_avg_goals'
    ]
    exist_features = [c for c in feature_cols if c in df.columns]
    X = df[exist_features].copy()
    for col in X.columns:
        if X[col].dtype in ['float64', 'int64']:
            X[col].fillna(X[col].median(), inplace=True)
        else:
            X[col].fillna(0, inplace=True)
    return X, df

# ---------- 6. MODEL EĞİTİMİ VE KAYDI ----------
def train_and_save_models(X, df, version_suffix):
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    targets = [
        ('result', df['result'], 'multiclass'), ('over15', df['over15'], 'binary'), ('over25', df['over25'], 'binary'),
        ('over35', df['over35'], 'binary'), ('under15', df['under15'], 'binary'), ('under25', df['under25'], 'binary'),
        ('under35', df['under35'], 'binary'), ('btts', df['btts'], 'binary'), ('dc_1x', df['dc_1x'], 'binary'),
        ('dc_x2', df['dc_x2'], 'binary'), ('dc_12', df['dc_12'], 'binary')
    ]
    
    for name, y, mtype in targets:
        print(f"\n>>> Eğitiliyor: {name}")
        clf = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
        clf.fit(X_scaled, y)
        calibrated = CalibratedClassifierCV(clf, method='sigmoid', cv=3)
        calibrated.fit(X_scaled, y)
        
        model_filename = f"{MODEL_DIR}/{name}_model_{version_suffix}.pkl"
        joblib.dump(calibrated, model_filename)
        print(f"   Kaydedildi: {model_filename}")
    
    scaler_filename = f"{MODEL_DIR}/scaler_{version_suffix}.pkl"
    joblib.dump(scaler, scaler_filename)
    print(f"\nScaler kaydedildi: {scaler_filename}")

# ---------- 7. ANA FONKSİYON ----------
def main():
    # Terminal argümanlarını ayarla
    parser = argparse.ArgumentParser(description="Futbol Maçları Tahmin Modeli Eğitici")
    
    # Varsayılan (default) değerler olarak 2026 yılı ve 10. haftayı atadık
    parser.add_argument("--year", type=int, help="Eğitim için son maç yılı sınırı", default=2026)
    parser.add_argument("--week", type=int, help="Eğitim için son maç haftası sınırı", default=10)
    
    args = parser.parse_args()

    print("===== TÜM MODELLERİ EĞİTME SİSTEMİ =====")
    
    # Bilgilendirme mesajı
    print(f"BİLGİ: Modeller {args.year} yılı {args.week}. haftaya kadar olan verilerle eğitiliyor.")
    
    create_analytics_tables()
    populate_analytics_tables(args.year, args.week)
    
    df_raw = load_training_data(args.year, args.week)
    
    if len(df_raw) < 100:
        print("Yetersiz veri (100'den az maç). Eğitim yapılamıyor.")
        sys.exit(0)
    
    df = create_targets(df_raw)
    X, df = prepare_features(df)
    print(f"Özellik sayısı: {X.shape[1]}")
    
    # Model versiyon isimlendirmesi (otomatik olarak 2026_week10 olacak)
    version_suffix = f"{args.year}_week{args.week}"
    print(f"Model versiyonu: {version_suffix}")
    
    train_and_save_models(X, df, version_suffix)
    
    print("\n✅ Tüm modeller başarıyla eğitildi ve kaydedildi.")
    print(f"📁 Klasör: {MODEL_DIR}/")

if __name__ == "__main__":
    main()
