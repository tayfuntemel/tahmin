#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SIZINTISIZ 5 HAFTALIK BACKTEST MOTORU

Mantık:
- START_PREDICT_DATE = 2026-03-15 ise ilk istatistik cutoff'u 2026-03-14'tür.
- İlk gün cutoff tarihine kadar tüm geçmişten cache tabloları kurulur.
- Sonraki günlerde sadece bir önceki günün maçlarından etkilenen lig/takım/hakem cacheleri güncellenir.
- Sonra sadece o günün maçlarına tahmin oluşturulur.
- predict_date/prediction_date CURDATE() değil, döngüdeki gerçek backtest günüdür.
- H2H, lig, takım formu, hakem, takım tahmin başarı oranı hiçbir şekilde target date veya sonrasını görmez.

Kullanım:
    python3 backtest_5hafta_sizintisiz.py

Ayarları aşağıdaki CONFIG bölümünden değiştirebilirsin.
"""

import os
import datetime as dt
from typing import Optional, Tuple, List, Dict, Any

import mysql.connector


# =========================
# BACKTEST AYARLARI
# =========================
START_PREDICT_DATE = dt.date(2026, 3, 15)
BACKTEST_DAYS = 35  # yaklaşık 5 hafta

# Aynı tarih aralığında önceden üretilmiş tahminler varsa temizleyip baştan yazsın mı?
# Güvenli deneme için True öneririm; canlı tahminleri değil sadece bu tarih aralığındaki prediction_date kayıtlarını siler.
CLEAR_EXISTING_BACKTEST_PREDICTIONS = True

# Tahmin seçilecek turnuva/kategori listesi.
MAJOR_TOURNAMENT_IDS = {
    1, 2, 3, 72, 84, 36, 37, 3739, 33, 34, 7372, 42, 41, 8343, 810,
    4, 5397, 62, 101, 39, 40, 38, 692, 280, 127, 83, 1449,
    169352, 5071, 28, 6720, 18, 3397, 3708, 82, 3034, 3284, 6230,
    54, 64, 29, 1060, 219, 652, 144, 1339, 1340, 1341,
    5, 6, 12, 13, 19, 24, 27, 30, 31, 48, 49, 50, 52, 53, 55, 79,
    102, 232, 384, 681, 877, 1061, 1107, 1427, 10812, 16753, 19232,
    34363, 51702, 52653, 58560, 64475, 71900, 71901, 72112, 78740,
    92016, 92614, 143625,
}

VALUE_EDGE_THRESHOLD = 0
TEAM_SUCCESS_WARN_RATE = 60.0
TEAM_SUCCESS_WARN_MIN_SAMPLE = 12

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "charset": "utf8mb4",
    "use_unicode": True,
    "collation": "utf8mb4_unicode_ci",
}


def avg_clean(values, fallback=0.0):
    clean = [float(v) for v in values if v is not None]
    return (sum(clean) / len(clean)) if clean else float(fallback)


def bool_to_int(value: bool) -> int:
    return 1 if value else 0


class BacktestDB:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = mysql.connector.connect(**DB_CONFIG)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor(dictionary=True)
        self.create_tables()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def create_tables(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS league_stats (
                category_id INT PRIMARY KEY,
                avg_goals FLOAT,
                avg_shot_on FLOAT,
                avg_total_shots FLOAT,
                avg_corners FLOAT,
                btts_ratio FLOAT,
                over25_ratio FLOAT,
                avg_first_half_goals FLOAT,
                zero_zero_comeback_ratio FLOAT,
                match_count INT,
                last_updated DATE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS team_dna (
                team_key VARCHAR(255) PRIMARY KEY,
                avg_goals FLOAT,
                avg_shot_on FLOAT,
                avg_corners FLOAT,
                total_matches INT,
                last_updated DATE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS team_form_cache (
                team_key VARCHAR(255) PRIMARY KEY,
                last_10_avg_goals FLOAT,
                last_10_avg_shot_on FLOAT,
                last_10_avg_corners FLOAT,
                last_10_btts_ratio FLOAT,
                last_10_avg_first_half_goals FLOAT,
                zero_zero_comeback_ratio FLOAT,
                home_last_5_avg_goals FLOAT,
                away_last_5_avg_goals FLOAT,
                last_3_avg_shot_on FLOAT,
                last_3_avg_total_shots FLOAT,
                last_3_avg_corners FLOAT,
                last_3_avg_possession FLOAT,
                last_match_shot_on INT,
                last_updated DATE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS referee_stats (
                referee_name VARCHAR(128) PRIMARY KEY,
                avg_goals FLOAT,
                match_count INT,
                last_updated DATE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                event_id BIGINT UNSIGNED NOT NULL,
                prediction_date DATE NOT NULL,
                match_date DATE NOT NULL,
                home_team VARCHAR(128) NOT NULL,
                away_team VARCHAR(128) NOT NULL,
                category_id INT NULL,
                model_over_prob FLOAT,
                model_btts_prob FLOAT,
                odds_over FLOAT,
                odds_btts FLOAT,
                edge_over FLOAT,
                edge_btts FLOAT,
                play_over BOOLEAN,
                play_btts BOOLEAN,
                form_score FLOAT,
                pressure_score FLOAT,
                h2h_score FLOAT,
                early_bonus INT,
                second_bonus INT,
                referee_penalty FLOAT,
                net_total_score FLOAT,
                actual_ft_home INT NULL,
                actual_ft_away INT NULL,
                actual_over25 BOOLEAN NULL,
                actual_btts BOOLEAN NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_prediction (event_id, prediction_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        new_columns = [
            ("model_over15_prob", "FLOAT"),
            ("odds_over15", "FLOAT"),
            ("edge_over15", "FLOAT"),
            ("play_over15", "BOOLEAN"),
            ("home_team_success_rate", "FLOAT NULL"),
            ("home_team_success_sample", "INT NOT NULL DEFAULT 0"),
            ("away_team_success_rate", "FLOAT NULL"),
            ("away_team_success_sample", "INT NOT NULL DEFAULT 0"),
            ("team_success_rate", "FLOAT NULL"),
            ("team_success_sample", "INT NOT NULL DEFAULT 0"),
            ("team_success_warning", "BOOLEAN NOT NULL DEFAULT FALSE"),
            ("team_success_note", "VARCHAR(255) NULL"),
        ]
        for col, typ in new_columns:
            try:
                self.cursor.execute(f"ALTER TABLE predictions ADD COLUMN {col} {typ}")
            except mysql.connector.Error as err:
                if err.errno != 1060:
                    print(f"[DB] Sütun uyarısı {col}: {err}")
        try:
            self.cursor.execute("CREATE INDEX idx_event_prediction_date ON predictions (event_id, prediction_date)")
        except mysql.connector.Error as err:
            if err.errno != 1061:
                print(f"[DB] İndeks uyarısı: {err}")


class BacktestStatsUpdater:
    def __init__(self, db: BacktestDB, cutoff_date: dt.date):
        self.db = db
        self.cursor = db.cursor
        self.cutoff_date = cutoff_date

    def update_full(self):
        """İlk gün: 14 Mart dahil tüm geçmişten cache tablolarını sıfırdan kurar."""
        print(f"\n📊 FULL İSTATİSTİK CACHE -> cutoff: {self.cutoff_date}")
        self.clear_cache_tables()
        self.update_league_stats()
        self.update_team_form_cache()
        self.update_team_dna()
        self.update_referee_stats()

    def update_incremental(self, changed_date: dt.date):
        """
        Sonraki günler: sadece changed_date tarihinde bitmiş maçlardan etkilenen lig/takım/hakemleri yeniden hesaplar.
        Örnek: 16 Mart tahmini öncesi cutoff=15 Mart ise sadece 15 Mart maçlarının takımları/ligleri/hakemleri güncellenir.
        Bu yine sızıntısızdır; her hesap self.cutoff_date <= şartıyla yapılır.
        """
        print(f"\n📊 INCREMENTAL İSTATİSTİK CACHE -> changed_date: {changed_date} | cutoff: {self.cutoff_date}")
        categories, teams, referees = self.get_affected_scope(changed_date)
        if not categories and not teams and not referees:
            print("  güncellenecek bitmiş maç bulunamadı; cache aynı kaldı")
            return
        self.update_league_stats(category_ids=categories)
        self.update_team_form_cache(team_scope=teams)
        self.update_team_dna(team_scope=teams)
        self.update_referee_stats(referees=referees)
        print(f"  incremental özet: {len(categories)} lig | {len(teams)} takım | {len(referees)} hakem")

    def update_all(self):
        # Geriye uyumluluk için full rebuild olarak bırakıldı.
        self.update_full()

    def clear_cache_tables(self):
        for table in ("league_stats", "team_form_cache", "team_dna", "referee_stats"):
            self.cursor.execute(f"DELETE FROM {table}")
        print("  cache temizlendi: league_stats, team_form_cache, team_dna, referee_stats")

    def get_affected_scope(self, changed_date: dt.date):
        """Belirli günün bitmiş maçlarından etkilenen category_id, team_key ve referee listesini çıkarır."""
        self.cursor.execute("""
            SELECT DISTINCT category_id
            FROM results_football
            WHERE status IN ('finished','ended')
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
              AND category_id IS NOT NULL
              AND start_utc = %s
        """, (changed_date,))
        categories = sorted({int(r['category_id']) for r in self.cursor.fetchall() if r.get('category_id') is not None})

        self.cursor.execute("""
            SELECT DISTINCT home_team AS team_name, category_id
            FROM results_football
            WHERE status IN ('finished','ended')
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
              AND home_team IS NOT NULL AND category_id IS NOT NULL
              AND start_utc = %s
            UNION
            SELECT DISTINCT away_team AS team_name, category_id
            FROM results_football
            WHERE status IN ('finished','ended')
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
              AND away_team IS NOT NULL AND category_id IS NOT NULL
              AND start_utc = %s
        """, (changed_date, changed_date))
        teams = sorted({(r['team_name'], int(r['category_id'])) for r in self.cursor.fetchall() if r.get('team_name') and r.get('category_id') is not None})

        self.cursor.execute("""
            SELECT DISTINCT referee
            FROM results_football
            WHERE status IN ('finished','ended')
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
              AND referee IS NOT NULL AND referee <> ''
              AND start_utc = %s
        """, (changed_date,))
        referees = sorted({str(r['referee']) for r in self.cursor.fetchall() if r.get('referee')})
        return categories, teams, referees

    @staticmethod
    def _in_clause(values):
        return ','.join(['%s'] * len(values))

    def update_league_stats(self, category_ids: Optional[List[int]] = None):
        extra = ""
        params: List[Any] = [self.cutoff_date]
        if category_ids:
            extra = f" AND category_id IN ({self._in_clause(category_ids)})"
            params.extend(category_ids)
        self.cursor.execute(f"""
            SELECT
                category_id,
                COUNT(*) AS match_count,
                AVG(ft_home + ft_away) AS avg_goals,
                AVG(CASE WHEN shot_on_h IS NOT NULL AND shot_on_a IS NOT NULL THEN shot_on_h + shot_on_a END) AS avg_shot_on,
                AVG(CASE WHEN shot_h IS NOT NULL AND shot_a IS NOT NULL THEN (shot_h + shot_a) / 2.0 END) AS avg_total_shots,
                AVG(CASE WHEN corn_h IS NOT NULL AND corn_a IS NOT NULL THEN corn_h + corn_a END) AS avg_corners,
                SUM(CASE WHEN ft_home > 0 AND ft_away > 0 THEN 1 ELSE 0 END) / COUNT(*) AS btts_ratio,
                SUM(CASE WHEN (ft_home + ft_away) > 2.5 THEN 1 ELSE 0 END) / COUNT(*) AS over25_ratio,
                AVG(CASE WHEN ht_home IS NOT NULL AND ht_away IS NOT NULL THEN ht_home + ht_away END) AS avg_first_half_goals,
                SUM(CASE WHEN ht_home = 0 AND ht_away = 0 AND ft_home > 0 AND ft_away > 0 THEN 1 ELSE 0 END) /
                    NULLIF(SUM(CASE WHEN ht_home = 0 AND ht_away = 0 THEN 1 ELSE 0 END), 0) AS zero_zero_comeback_ratio
            FROM results_football
            WHERE status IN ('finished', 'ended')
              AND category_id IS NOT NULL
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
              AND start_utc <= %s
              {extra}
            GROUP BY category_id
        """, tuple(params))
        rows = self.cursor.fetchall()
        for row in rows:
            self.cursor.execute("""
                INSERT INTO league_stats
                (category_id, avg_goals, avg_shot_on, avg_total_shots, avg_corners,
                 btts_ratio, over25_ratio, avg_first_half_goals, zero_zero_comeback_ratio, match_count, last_updated)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                avg_goals=VALUES(avg_goals), avg_shot_on=VALUES(avg_shot_on),
                avg_total_shots=VALUES(avg_total_shots), avg_corners=VALUES(avg_corners),
                btts_ratio=VALUES(btts_ratio), over25_ratio=VALUES(over25_ratio),
                avg_first_half_goals=VALUES(avg_first_half_goals),
                zero_zero_comeback_ratio=VALUES(zero_zero_comeback_ratio),
                match_count=VALUES(match_count), last_updated=VALUES(last_updated)
            """, (
                row['category_id'],
                float(row['avg_goals'] or 0),
                float(row['avg_shot_on'] or 0),
                float(row['avg_total_shots'] or 0),
                float(row['avg_corners'] or 0),
                float(row['btts_ratio'] or 0),
                float(row['over25_ratio'] or 0),
                float(row['avg_first_half_goals'] or 0),
                float(row['zero_zero_comeback_ratio'] or 0),
                int(row['match_count'] or 0),
                self.cutoff_date,
            ))
        label = "etkilenen lig" if category_ids else "lig"
        print(f"  league_stats: {len(rows)} {label}")

    def update_team_form_cache(self, team_scope: Optional[List[Tuple[str, int]]] = None):
        if team_scope is None:
            self.cursor.execute("""
                SELECT DISTINCT home_team AS team_name, category_id
                FROM results_football
                WHERE home_team IS NOT NULL AND category_id IS NOT NULL AND start_utc <= %s
                UNION
                SELECT DISTINCT away_team AS team_name, category_id
                FROM results_football
                WHERE away_team IS NOT NULL AND category_id IS NOT NULL AND start_utc <= %s
            """, (self.cutoff_date, self.cutoff_date))
            teams = [(r['team_name'], int(r['category_id'])) for r in self.cursor.fetchall()]
        else:
            teams = list(team_scope)
        count = 0
        for team_name, cat_id in teams:
            team_key = f"{team_name}|{cat_id}"

            self.cursor.execute("""
                SELECT ft_home, ft_away, shot_on_h, shot_on_a, shot_h, shot_a, corn_h, corn_a,
                       poss_h, poss_a, ht_home, ht_away, home_team, away_team, start_utc, start_time_utc
                FROM results_football
                WHERE status IN ('finished', 'ended')
                  AND (home_team = %s OR away_team = %s)
                  AND category_id = %s
                  AND ft_home IS NOT NULL AND ft_away IS NOT NULL
                  AND start_utc <= %s
                ORDER BY start_utc DESC, start_time_utc DESC
                LIMIT 10
            """, (team_name, team_name, cat_id, self.cutoff_date))
            matches = self.cursor.fetchall()
            if len(matches) < 3:
                # Yeni takımın yeterli datası yoksa cache'e yazma. Varsa eski satırı temizle.
                self.cursor.execute("DELETE FROM team_form_cache WHERE team_key=%s", (team_key,))
                continue

            goals, shot_on, total_shots, corners, poss, first_half_goals = [], [], [], [], [], []
            btts_count = zero_zero_count = zero_zero_comeback = 0
            for m in matches:
                if m['home_team'] == team_name:
                    g = m['ft_home'] if m['ft_home'] is not None else 0
                    opp_g = m['ft_away'] if m['ft_away'] is not None else 0
                    so, ts, cor, ps, fhg, opp_fhg = m['shot_on_h'], m['shot_h'], m['corn_h'], m['poss_h'], m['ht_home'], m['ht_away']
                else:
                    g = m['ft_away'] if m['ft_away'] is not None else 0
                    opp_g = m['ft_home'] if m['ft_home'] is not None else 0
                    so, ts, cor, ps, fhg, opp_fhg = m['shot_on_a'], m['shot_a'], m['corn_a'], m['poss_a'], m['ht_away'], m['ht_home']

                goals.append(g)
                if so is not None: shot_on.append(so)
                if ts is not None: total_shots.append(ts)
                if cor is not None: corners.append(cor)
                if ps is not None: poss.append(ps)
                if fhg is not None: first_half_goals.append(fhg)
                if g > 0 and opp_g > 0: btts_count += 1
                if fhg is not None and opp_fhg is not None and fhg == 0 and opp_fhg == 0:
                    zero_zero_count += 1
                    if g > 0 and opp_g > 0:
                        zero_zero_comeback += 1

            avg_goals_10 = avg_clean(goals, 0.0)
            avg_shot_on_10 = avg_clean(shot_on, 4.0)
            avg_total_shots_10 = avg_clean(total_shots, 10.0)
            avg_corners_10 = avg_clean(corners, 4.0)
            avg_poss_10 = avg_clean(poss, 45.0)
            avg_fhg_10 = avg_clean(first_half_goals, 0.5)
            btts_ratio_10 = btts_count / len(matches)
            zz_ratio = (zero_zero_comeback / zero_zero_count) if zero_zero_count else 0.0

            self.cursor.execute("""
                SELECT ft_home
                FROM results_football
                WHERE status IN ('finished','ended') AND home_team=%s AND category_id=%s
                  AND ft_home IS NOT NULL AND start_utc <= %s
                ORDER BY start_utc DESC, start_time_utc DESC
                LIMIT 5
            """, (team_name, cat_id, self.cutoff_date))
            home_rows = self.cursor.fetchall()
            home_avg = avg_clean([r['ft_home'] for r in home_rows], 0.0)

            self.cursor.execute("""
                SELECT ft_away
                FROM results_football
                WHERE status IN ('finished','ended') AND away_team=%s AND category_id=%s
                  AND ft_away IS NOT NULL AND start_utc <= %s
                ORDER BY start_utc DESC, start_time_utc DESC
                LIMIT 5
            """, (team_name, cat_id, self.cutoff_date))
            away_rows = self.cursor.fetchall()
            away_avg = avg_clean([r['ft_away'] for r in away_rows], 0.0)

            last3 = matches[:3]
            l3_so, l3_ts, l3_cor, l3_poss = [], [], [], []
            last_match_shot_on = None
            for idx, m in enumerate(last3):
                if m['home_team'] == team_name:
                    so, ts, cor, ps = m['shot_on_h'], m['shot_h'], m['corn_h'], m['poss_h']
                else:
                    so, ts, cor, ps = m['shot_on_a'], m['shot_a'], m['corn_a'], m['poss_a']
                if so is not None: l3_so.append(so)
                if ts is not None: l3_ts.append(ts)
                if cor is not None: l3_cor.append(cor)
                if ps is not None: l3_poss.append(ps)
                if idx == 0 and so is not None:
                    last_match_shot_on = so

            avg_shot_on_3 = avg_clean(l3_so, avg_shot_on_10)
            avg_total_shots_3 = avg_clean(l3_ts, avg_total_shots_10)
            avg_corners_3 = avg_clean(l3_cor, avg_corners_10)
            avg_poss_3 = avg_clean(l3_poss, avg_poss_10)
            if last_match_shot_on is None:
                last_match_shot_on = int(round(avg_shot_on_3))

            self.cursor.execute("""
                INSERT INTO team_form_cache
                (team_key, last_10_avg_goals, last_10_avg_shot_on, last_10_avg_corners,
                 last_10_btts_ratio, last_10_avg_first_half_goals, zero_zero_comeback_ratio,
                 home_last_5_avg_goals, away_last_5_avg_goals,
                 last_3_avg_shot_on, last_3_avg_total_shots, last_3_avg_corners, last_3_avg_possession,
                 last_match_shot_on, last_updated)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                last_10_avg_goals=VALUES(last_10_avg_goals),
                last_10_avg_shot_on=VALUES(last_10_avg_shot_on),
                last_10_avg_corners=VALUES(last_10_avg_corners),
                last_10_btts_ratio=VALUES(last_10_btts_ratio),
                last_10_avg_first_half_goals=VALUES(last_10_avg_first_half_goals),
                zero_zero_comeback_ratio=VALUES(zero_zero_comeback_ratio),
                home_last_5_avg_goals=VALUES(home_last_5_avg_goals),
                away_last_5_avg_goals=VALUES(away_last_5_avg_goals),
                last_3_avg_shot_on=VALUES(last_3_avg_shot_on),
                last_3_avg_total_shots=VALUES(last_3_avg_total_shots),
                last_3_avg_corners=VALUES(last_3_avg_corners),
                last_3_avg_possession=VALUES(last_3_avg_possession),
                last_match_shot_on=VALUES(last_match_shot_on),
                last_updated=VALUES(last_updated)
            """, (
                team_key, avg_goals_10, avg_shot_on_10, avg_corners_10,
                btts_ratio_10, avg_fhg_10, zz_ratio,
                home_avg, away_avg,
                avg_shot_on_3, avg_total_shots_3, avg_corners_3, avg_poss_3,
                int(round(last_match_shot_on)), self.cutoff_date,
            ))
            count += 1
        label = "etkilenen takım" if team_scope is not None else "takım"
        print(f"  team_form_cache: {count} {label}")

    def update_team_dna(self, team_scope: Optional[List[Tuple[str, int]]] = None):
        if team_scope is None:
            self.cursor.execute("""
                SELECT team_key,
                       AVG(avg_goals) AS avg_goals,
                       AVG(avg_shot_on) AS avg_shot_on,
                       AVG(avg_corners) AS avg_corners,
                       SUM(total_matches) AS total_matches
                FROM (
                    SELECT CONCAT(home_team, '|', category_id) AS team_key,
                           AVG(ft_home) AS avg_goals,
                           AVG(shot_on_h) AS avg_shot_on,
                           AVG(corn_h) AS avg_corners,
                           COUNT(*) AS total_matches
                    FROM results_football
                    WHERE status IN ('finished','ended') AND start_utc <= %s
                      AND home_team IS NOT NULL AND category_id IS NOT NULL AND ft_home IS NOT NULL
                    GROUP BY home_team, category_id
                    UNION ALL
                    SELECT CONCAT(away_team, '|', category_id) AS team_key,
                           AVG(ft_away) AS avg_goals,
                           AVG(shot_on_a) AS avg_shot_on,
                           AVG(corn_a) AS avg_corners,
                           COUNT(*) AS total_matches
                    FROM results_football
                    WHERE status IN ('finished','ended') AND start_utc <= %s
                      AND away_team IS NOT NULL AND category_id IS NOT NULL AND ft_away IS NOT NULL
                    GROUP BY away_team, category_id
                ) X
                GROUP BY team_key
            """, (self.cutoff_date, self.cutoff_date))
            rows = self.cursor.fetchall()
        else:
            rows = []
            for team_name, cat_id in team_scope:
                team_key = f"{team_name}|{cat_id}"
                self.cursor.execute("""
                    SELECT AVG(avg_goals) AS avg_goals,
                           AVG(avg_shot_on) AS avg_shot_on,
                           AVG(avg_corners) AS avg_corners,
                           SUM(total_matches) AS total_matches
                    FROM (
                        SELECT AVG(ft_home) AS avg_goals, AVG(shot_on_h) AS avg_shot_on, AVG(corn_h) AS avg_corners, COUNT(*) AS total_matches
                        FROM results_football
                        WHERE status IN ('finished','ended') AND start_utc <= %s
                          AND home_team=%s AND category_id=%s AND ft_home IS NOT NULL
                        UNION ALL
                        SELECT AVG(ft_away) AS avg_goals, AVG(shot_on_a) AS avg_shot_on, AVG(corn_a) AS avg_corners, COUNT(*) AS total_matches
                        FROM results_football
                        WHERE status IN ('finished','ended') AND start_utc <= %s
                          AND away_team=%s AND category_id=%s AND ft_away IS NOT NULL
                    ) X
                """, (self.cutoff_date, team_name, cat_id, self.cutoff_date, team_name, cat_id))
                r = self.cursor.fetchone()
                if not r or not r.get('total_matches'):
                    self.cursor.execute("DELETE FROM team_dna WHERE team_key=%s", (team_key,))
                    continue
                r['team_key'] = team_key
                rows.append(r)
        for r in rows:
            self.cursor.execute("""
                INSERT INTO team_dna (team_key, avg_goals, avg_shot_on, avg_corners, total_matches, last_updated)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                avg_goals=VALUES(avg_goals), avg_shot_on=VALUES(avg_shot_on),
                avg_corners=VALUES(avg_corners), total_matches=VALUES(total_matches), last_updated=VALUES(last_updated)
            """, (r['team_key'], float(r['avg_goals'] or 0), float(r['avg_shot_on'] or 0), float(r['avg_corners'] or 0), int(r['total_matches'] or 0), self.cutoff_date))
        label = "etkilenen takım" if team_scope is not None else "takım"
        print(f"  team_dna: {len(rows)} {label}")

    def update_referee_stats(self, referees: Optional[List[str]] = None):
        extra = ""
        params: List[Any] = [self.cutoff_date]
        if referees:
            extra = f" AND referee IN ({self._in_clause(referees)})"
            params.extend(referees)
        self.cursor.execute(f"""
            SELECT referee, AVG(ft_home + ft_away) AS avg_goals, COUNT(*) AS match_count
            FROM results_football
            WHERE status IN ('finished','ended') AND referee IS NOT NULL
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
              AND start_utc <= %s
              {extra}
            GROUP BY referee
        """, tuple(params))
        rows = self.cursor.fetchall()
        for r in rows:
            self.cursor.execute("""
                INSERT INTO referee_stats (referee_name, avg_goals, match_count, last_updated)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE avg_goals=VALUES(avg_goals), match_count=VALUES(match_count), last_updated=VALUES(last_updated)
            """, (r['referee'], float(r['avg_goals'] or 0), int(r['match_count'] or 0), self.cutoff_date))
        label = "etkilenen hakem" if referees is not None else "hakem"
        print(f"  referee_stats: {len(rows)} {label}")

class BacktestPredictionEngine:
    def __init__(self, db: BacktestDB, target_date: dt.date):
        self.db = db
        self.cursor = db.cursor
        self.target_date = target_date
        self.cutoff_date = target_date - dt.timedelta(days=1)

    def get_league_stats(self, category_id):
        self.cursor.execute("SELECT * FROM league_stats WHERE category_id=%s", (category_id,))
        row = self.cursor.fetchone()
        if row:
            return {
                'avg_goals': float(row['avg_goals']) if row['avg_goals'] is not None else 2.5,
                'avg_shot_on': float(row['avg_shot_on']) if row['avg_shot_on'] is not None else 8.0,
                'avg_total_shots': float(row['avg_total_shots']) if row['avg_total_shots'] is not None else 11.0,
                'avg_corners': float(row['avg_corners']) if row['avg_corners'] is not None else 9.0,
                'btts_ratio': float(row['btts_ratio']) if row['btts_ratio'] is not None else 0.45,
                'over25_ratio': float(row['over25_ratio']) if row['over25_ratio'] is not None else 0.45,
                'avg_first_half_goals': float(row['avg_first_half_goals']) if row['avg_first_half_goals'] is not None else 1.1,
                'zero_zero_comeback_ratio': float(row['zero_zero_comeback_ratio']) if row['zero_zero_comeback_ratio'] is not None else 0.15,
            }
        return {'avg_goals': 2.5, 'avg_shot_on': 8.0, 'avg_total_shots': 11.0, 'avg_corners': 9.0,
                'btts_ratio': 0.45, 'over25_ratio': 0.45, 'avg_first_half_goals': 1.1, 'zero_zero_comeback_ratio': 0.15}

    def get_team_form_cache(self, team_key):
        self.cursor.execute("SELECT * FROM team_form_cache WHERE team_key=%s", (team_key,))
        row = self.cursor.fetchone()
        if row:
            return {
                'last_10_avg_goals': float(row['last_10_avg_goals']) if row['last_10_avg_goals'] is not None else 1.0,
                'last_10_avg_shot_on': float(row['last_10_avg_shot_on']) if row['last_10_avg_shot_on'] is not None else 4.0,
                'last_10_avg_corners': float(row['last_10_avg_corners']) if row['last_10_avg_corners'] is not None else 4.0,
                'last_10_btts_ratio': float(row['last_10_btts_ratio']) if row['last_10_btts_ratio'] is not None else 0.4,
                'last_10_avg_first_half_goals': float(row['last_10_avg_first_half_goals']) if row['last_10_avg_first_half_goals'] is not None else 0.5,
                'zero_zero_comeback_ratio': float(row['zero_zero_comeback_ratio']) if row['zero_zero_comeback_ratio'] is not None else 0.15,
                'home_last_5_avg_goals': float(row['home_last_5_avg_goals']) if row['home_last_5_avg_goals'] is not None else 1.0,
                'away_last_5_avg_goals': float(row['away_last_5_avg_goals']) if row['away_last_5_avg_goals'] is not None else 1.0,
                'last_3_avg_shot_on': float(row['last_3_avg_shot_on']) if row['last_3_avg_shot_on'] is not None else 4.0,
                'last_3_avg_total_shots': float(row['last_3_avg_total_shots']) if row['last_3_avg_total_shots'] is not None else 10.0,
                'last_3_avg_corners': float(row['last_3_avg_corners']) if row['last_3_avg_corners'] is not None else 4.0,
                'last_3_avg_possession': float(row['last_3_avg_possession']) if row['last_3_avg_possession'] is not None else 45.0,
                'last_match_shot_on': int(row['last_match_shot_on']) if row['last_match_shot_on'] is not None else 4,
            }
        return {'last_10_avg_goals': 1.0, 'last_10_avg_shot_on': 4.0, 'last_10_avg_corners': 4.0,
                'last_10_btts_ratio': 0.4, 'last_10_avg_first_half_goals': 0.5, 'zero_zero_comeback_ratio': 0.15,
                'home_last_5_avg_goals': 1.0, 'away_last_5_avg_goals': 1.0, 'last_3_avg_shot_on': 4.0,
                'last_3_avg_total_shots': 10.0, 'last_3_avg_corners': 4.0, 'last_3_avg_possession': 45.0,
                'last_match_shot_on': 4}

    def get_referee_penalty(self, referee_name, league_avg_goals):
        if not referee_name:
            return 1.0
        self.cursor.execute("SELECT avg_goals FROM referee_stats WHERE referee_name=%s", (referee_name,))
        row = self.cursor.fetchone()
        if row and row['avg_goals'] is not None and float(row['avg_goals']) < league_avg_goals * 0.9:
            return 0.9
        return 1.0

    def get_h2h_stats(self, home_team, away_team, category_id, last_n=10):
        self.cursor.execute("""
            SELECT ft_home, ft_away
            FROM results_football
            WHERE status IN ('finished','ended')
              AND category_id=%s
              AND ((home_team=%s AND away_team=%s) OR (home_team=%s AND away_team=%s))
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
              AND start_utc <= %s
            ORDER BY start_utc DESC, start_time_utc DESC
            LIMIT %s
        """, (category_id, home_team, away_team, away_team, home_team, self.cutoff_date, last_n))
        rows = self.cursor.fetchall()
        if not rows:
            return None, None
        total = len(rows)
        btts = sum(1 for m in rows if (m['ft_home'] or 0) > 0 and (m['ft_away'] or 0) > 0) / total
        over25 = sum(1 for m in rows if (m['ft_home'] or 0) + (m['ft_away'] or 0) >= 3) / total
        return btts, over25

    def get_team_prediction_success(self, team_name, category_id, before_date, limit=40):
        if not team_name or not before_date:
            return None, 0

        def fetch_rows(kilit_only=True):
            extra = "AND P.second_bonus > 0 AND P.h2h_score >= 5" if kilit_only else ""
            category_filter = "AND P.category_id = %s" if category_id else ""
            params = [before_date, team_name, team_name, before_date]
            if category_id:
                params.append(category_id)
            params.append(limit)
            sql = f"""
                SELECT P.odds_over, R.ft_home, R.ft_away
                FROM predictions P
                JOIN results_football R ON P.event_id = R.event_id
                INNER JOIN (
                    SELECT event_id, MAX(prediction_date) AS latest_prediction_date
                    FROM predictions
                    WHERE prediction_date < %s
                    GROUP BY event_id
                ) LP ON LP.event_id = P.event_id AND LP.latest_prediction_date = P.prediction_date
                WHERE R.status IN ('finished','ended')
                  AND R.ft_home IS NOT NULL AND R.ft_away IS NOT NULL
                  AND P.odds_over IS NOT NULL AND P.odds_over > 0
                  AND (P.home_team = %s OR P.away_team = %s)
                  AND P.match_date < %s
                  {category_filter}
                  {extra}
                ORDER BY P.match_date DESC
                LIMIT %s
            """
            self.cursor.execute(sql, tuple(params))
            return self.cursor.fetchall()

        rows = fetch_rows(kilit_only=True)
        if len(rows) < 8:
            fallback = fetch_rows(kilit_only=False)
            if len(fallback) > len(rows):
                rows = fallback
        sample = len(rows)
        if sample == 0:
            return None, 0
        wins = sum(1 for r in rows if (r['ft_home'] or 0) + (r['ft_away'] or 0) >= 3)
        return (wins / sample) * 100.0, sample

    def build_team_success_guard(self, home_team, away_team, category_id, before_date):
        home_rate, home_sample = self.get_team_prediction_success(home_team, category_id, before_date)
        away_rate, away_sample = self.get_team_prediction_success(away_team, category_id, before_date)
        weighted = []
        if home_rate is not None and home_sample > 0: weighted.append((home_rate, home_sample))
        if away_rate is not None and away_sample > 0: weighted.append((away_rate, away_sample))
        if weighted:
            total_sample = sum(s for _, s in weighted)
            combined_rate = sum(rate * sample for rate, sample in weighted) / total_sample
        else:
            total_sample, combined_rate = 0, None
        warning = False
        note = None
        if combined_rate is not None and total_sample >= TEAM_SUCCESS_WARN_MIN_SAMPLE and combined_rate < TEAM_SUCCESS_WARN_RATE:
            warning = True
            note = f"Takım güveni düşük: geçmiş O25 başarı %{combined_rate:.1f} ({total_sample} maç)."
        elif home_rate is not None and home_sample >= 8 and home_rate < 55:
            warning = True
            note = f"Ev sahibi geçmiş O25 başarısı düşük: %{home_rate:.1f} ({home_sample} maç)."
        elif away_rate is not None and away_sample >= 8 and away_rate < 55:
            warning = True
            note = f"Deplasman geçmiş O25 başarısı düşük: %{away_rate:.1f} ({away_sample} maç)."
        elif total_sample > 0 and total_sample < TEAM_SUCCESS_WARN_MIN_SAMPLE:
            note = f"Takım güven verisi az: {total_sample} maç."
        return {
            'home_team_success_rate': home_rate,
            'home_team_success_sample': home_sample,
            'away_team_success_rate': away_rate,
            'away_team_success_sample': away_sample,
            'team_success_rate': combined_rate,
            'team_success_sample': total_sample,
            'team_success_warning': warning,
            'team_success_note': note,
        }

    @staticmethod
    def calculate_form_score(home_cache, away_cache, league_avg_goals):
        general_avg = (home_cache['last_10_avg_goals'] + away_cache['last_10_avg_goals']) / 2.0
        norm_general = min(general_avg / league_avg_goals, 1.5) if league_avg_goals > 0 else 1.0
        general_score = min(31.5, general_avg * 7 * norm_general)
        specific_avg = (home_cache['home_last_5_avg_goals'] + away_cache['away_last_5_avg_goals']) / 2.0
        norm_specific = min(specific_avg / league_avg_goals, 1.5) if league_avg_goals > 0 else 1.0
        specific_score = min(13.5, specific_avg * 3 * norm_specific)
        return general_score + specific_score

    @staticmethod
    def calculate_attack_pressure(home_cache, away_cache, league_avg_shot_on, league_avg_total_shots, league_avg_corners):
        def pressure(cache):
            norm_shot_on = min(cache['last_3_avg_shot_on'] / league_avg_shot_on, 1.5) if league_avg_shot_on > 0 else 1.0
            norm_total_shots = min(cache['last_3_avg_total_shots'] / league_avg_total_shots, 1.5) if league_avg_total_shots > 0 else 1.0
            norm_corners = min(cache['last_3_avg_corners'] / league_avg_corners, 1.5) if league_avg_corners > 0 else 1.0
            norm_poss = min(cache['last_3_avg_possession'] / 50.0, 1.5)
            raw = (cache['last_3_avg_shot_on'] * 3 * norm_shot_on) + \
                  (cache['last_3_avg_corners'] * 1.5 * norm_corners) + \
                  (cache['last_3_avg_total_shots'] * 1 * norm_total_shots) + \
                  (cache['last_3_avg_possession'] * 0.2 * norm_poss)
            if cache['last_match_shot_on'] <= 1:
                raw -= (cache['last_3_avg_possession'] * 0.2 * norm_poss)
            return raw / 3.0
        return min(40.0, (pressure(home_cache) + pressure(away_cache)) / 2.0)

    @staticmethod
    def calculate_h2h_score(h2h_btts, h2h_over25, league_btts, league_over25):
        if h2h_btts is None:
            return 0.0
        norm_btts = min(h2h_btts / league_btts, 1.5) if league_btts > 0 else 1.0
        norm_over = min(h2h_over25 / league_over25, 1.5) if league_over25 > 0 else 1.0
        return min(15.0, ((h2h_btts * norm_btts + h2h_over25 * norm_over) / 2.0) * 15)

    @staticmethod
    def calculate_early_bonus(home_cache, away_cache, league_avg_first_half_goals):
        combined_fh = home_cache['last_10_avg_first_half_goals'] + away_cache['last_10_avg_first_half_goals']
        if combined_fh > league_avg_first_half_goals * 1.2:
            return 5
        if combined_fh > league_avg_first_half_goals:
            return 3
        return 0

    @staticmethod
    def calculate_second_half_bonus(home_cache, away_cache, league_zero_zero_comeback):
        avg_zz = (home_cache.get('zero_zero_comeback_ratio', 0) + away_cache.get('zero_zero_comeback_ratio', 0)) / 2.0
        if avg_zz > 0:
            if avg_zz > league_zero_zero_comeback * 1.2:
                return 5
            if avg_zz > league_zero_zero_comeback:
                return 2
        avg_btts = (home_cache['last_10_btts_ratio'] + away_cache['last_10_btts_ratio']) / 2.0
        if avg_btts > 0.55:
            return 3
        return 0

    def predict_match(self, match):
        home = match['home_team']
        away = match['away_team']
        cat_id = match['category_id'] or 0
        league = self.get_league_stats(cat_id)
        home_cache = self.get_team_form_cache(f"{home}|{cat_id}")
        away_cache = self.get_team_form_cache(f"{away}|{cat_id}")

        form_score = self.calculate_form_score(home_cache, away_cache, league['avg_goals'])
        pressure_score = self.calculate_attack_pressure(home_cache, away_cache, league['avg_shot_on'], league['avg_total_shots'], league['avg_corners'])
        h2h_btts, h2h_over25 = self.get_h2h_stats(home, away, cat_id)
        h2h_score = self.calculate_h2h_score(h2h_btts, h2h_over25, league['btts_ratio'], league['over25_ratio'])
        early_bonus = self.calculate_early_bonus(home_cache, away_cache, league['avg_first_half_goals'])
        second_bonus = self.calculate_second_half_bonus(home_cache, away_cache, league['zero_zero_comeback_ratio'])
        referee_penalty = self.get_referee_penalty(match.get('referee'), league['avg_goals'])
        raw_total = form_score + pressure_score + h2h_score + early_bonus + second_bonus
        net_total = raw_total * referee_penalty
        model_over25_prob = min(99.0, (net_total / 110.0) * 100)
        model_btts_prob = model_over25_prob * 0.95
        model_over15_prob = min(99.0, model_over25_prob + (100 - model_over25_prob) * 0.3)

        over25_odds = match.get('odds_o25')
        over15_odds = match.get('odds_o15')
        btts_odds = match.get('odds_btts_yes')
        team_guard = self.build_team_success_guard(home, away, cat_id, self.target_date)
        result = {
            'home_team': home, 'away_team': away, 'date': match['start_utc'], 'category_id': cat_id,
            'model_over_prob': model_over25_prob, 'model_over15_prob': model_over15_prob, 'model_btts_prob': model_btts_prob,
            'over_odds': over25_odds, 'over15_odds': over15_odds, 'btts_odds': btts_odds,
            'form_score': form_score, 'pressure_score': pressure_score, 'h2h_score': h2h_score,
            'early_bonus': early_bonus, 'second_bonus': second_bonus, 'referee_penalty': referee_penalty,
            'net_total': net_total, **team_guard,
        }
        if over25_odds and over25_odds > 0:
            result['over_edge'] = model_over25_prob - (100 / over25_odds)
            result['over_play'] = result['over_edge'] >= VALUE_EDGE_THRESHOLD
        else:
            result['over_edge'] = None; result['over_play'] = False
        if over15_odds and over15_odds > 0:
            result['over15_edge'] = model_over15_prob - (100 / over15_odds)
            result['over15_play'] = result['over15_edge'] >= VALUE_EDGE_THRESHOLD
        else:
            result['over15_edge'] = None; result['over15_play'] = False
        if btts_odds and btts_odds > 0:
            result['btts_edge'] = model_btts_prob - (100 / btts_odds)
            result['btts_play'] = result['btts_edge'] >= VALUE_EDGE_THRESHOLD
        else:
            result['btts_edge'] = None; result['btts_play'] = False
        return result

    def save_prediction(self, pred, event_id):
        self.cursor.execute("""
            INSERT INTO predictions
            (event_id, prediction_date, match_date, home_team, away_team, category_id,
             model_over_prob, model_over15_prob, model_btts_prob,
             odds_over, odds_over15, odds_btts,
             edge_over, edge_over15, edge_btts,
             play_over, play_over15, play_btts,
             form_score, pressure_score, h2h_score, early_bonus,
             second_bonus, referee_penalty, net_total_score,
             home_team_success_rate, home_team_success_sample,
             away_team_success_rate, away_team_success_sample,
             team_success_rate, team_success_sample, team_success_warning, team_success_note,
             updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON DUPLICATE KEY UPDATE
            match_date=VALUES(match_date), home_team=VALUES(home_team), away_team=VALUES(away_team), category_id=VALUES(category_id),
            model_over_prob=VALUES(model_over_prob), model_over15_prob=VALUES(model_over15_prob), model_btts_prob=VALUES(model_btts_prob),
            odds_over=VALUES(odds_over), odds_over15=VALUES(odds_over15), odds_btts=VALUES(odds_btts),
            edge_over=VALUES(edge_over), edge_over15=VALUES(edge_over15), edge_btts=VALUES(edge_btts),
            play_over=VALUES(play_over), play_over15=VALUES(play_over15), play_btts=VALUES(play_btts),
            form_score=VALUES(form_score), pressure_score=VALUES(pressure_score), h2h_score=VALUES(h2h_score),
            early_bonus=VALUES(early_bonus), second_bonus=VALUES(second_bonus), referee_penalty=VALUES(referee_penalty),
            net_total_score=VALUES(net_total_score),
            home_team_success_rate=VALUES(home_team_success_rate), home_team_success_sample=VALUES(home_team_success_sample),
            away_team_success_rate=VALUES(away_team_success_rate), away_team_success_sample=VALUES(away_team_success_sample),
            team_success_rate=VALUES(team_success_rate), team_success_sample=VALUES(team_success_sample),
            team_success_warning=VALUES(team_success_warning), team_success_note=VALUES(team_success_note),
            updated_at=NOW()
        """, (
            event_id, self.target_date, pred['date'], pred['home_team'], pred['away_team'], pred['category_id'],
            pred['model_over_prob'], pred['model_over15_prob'], pred['model_btts_prob'],
            pred['over_odds'], pred['over15_odds'], pred['btts_odds'],
            pred['over_edge'], pred['over15_edge'], pred['btts_edge'],
            bool_to_int(pred['over_play']), bool_to_int(pred['over15_play']), bool_to_int(pred['btts_play']),
            pred['form_score'], pred['pressure_score'], pred['h2h_score'], pred['early_bonus'],
            pred['second_bonus'], pred['referee_penalty'], pred['net_total'],
            pred['home_team_success_rate'], pred['home_team_success_sample'],
            pred['away_team_success_rate'], pred['away_team_success_sample'],
            pred['team_success_rate'], pred['team_success_sample'], bool_to_int(pred['team_success_warning']), pred['team_success_note'],
        ))

    def run_for_day(self):
        ids_str = ','.join(str(x) for x in sorted(MAJOR_TOURNAMENT_IDS))
        self.cursor.execute(f"""
            SELECT event_id, start_utc, start_time_utc, home_team, away_team,
                   odds_o25, odds_o15, odds_btts_yes, category_id, tournament_id, referee
            FROM results_football
            WHERE start_utc = %s
              AND home_team IS NOT NULL AND away_team IS NOT NULL
              AND (tournament_id IN ({ids_str}) OR category_id IN ({ids_str}))
        """, (self.target_date,))
        matches = self.cursor.fetchall()
        saved = 0
        print(f"🔮 TAHMİN -> {self.target_date}: {len(matches)} maç bulundu")
        for match in matches:
            pred = self.predict_match(match)
            self.save_prediction(pred, match['event_id'])
            saved += 1
        print(f"  predictions: {saved} tahmin kaydedildi")
        return saved


def clear_backtest_predictions(db: BacktestDB, start_date: dt.date, days: int):
    end_date = start_date + dt.timedelta(days=days - 1)
    db.cursor.execute("DELETE FROM predictions WHERE prediction_date BETWEEN %s AND %s", (start_date, end_date))
    print(f"🧹 Eski backtest tahminleri temizlendi: {start_date} - {end_date}")


def main():
    db = BacktestDB()
    db.connect()
    try:
        if CLEAR_EXISTING_BACKTEST_PREDICTIONS:
            clear_backtest_predictions(db, START_PREDICT_DATE, BACKTEST_DAYS)

        total_predictions = 0
        for i in range(BACKTEST_DAYS):
            predict_date = START_PREDICT_DATE + dt.timedelta(days=i)
            cutoff_date = predict_date - dt.timedelta(days=1)
            print("\n" + "=" * 72)
            print(f"GÜN {i + 1}/{BACKTEST_DAYS} | cutoff={cutoff_date} | predict_date={predict_date}")
            updater = BacktestStatsUpdater(db, cutoff_date)
            if i == 0:
                # 15 Mart tahmini öncesi: 14 Mart dahil tüm geçmiş cache kurulur.
                updater.update_full()
            else:
                # 16 Mart ve sonrası: sadece bir önceki gün oynanan maçların takımları/ligleri/hakemleri güncellenir.
                updater.update_incremental(changed_date=cutoff_date)
            total_predictions += BacktestPredictionEngine(db, predict_date).run_for_day()

        print("\n" + "=" * 72)
        print(f"✅ Backtest tamamlandı. Toplam tahmin: {total_predictions}")
        print(f"Tarih aralığı: {START_PREDICT_DATE} - {START_PREDICT_DATE + dt.timedelta(days=BACKTEST_DAYS - 1)}")
        print("Not: Son cache hali son günün cutoff'una göre kalır. Bu normaldir.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
