#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
İstatistik tablolarını günceller (league_stats, team_dna, team_form_cache, referee_stats)
Master prompt'taki tüm kurallara uygun şekilde:
- Son 10 maç gol, isabetli şut, korner, ilk yarı gol, 0-0 dönüş oranı
- Son 5 iç saha / dış saha gol
- Son 3 maç hücum verileri (toplam şut, isabetli şut, korner, topla oynama)
- Kesici kural için son maç isabetli şut

TARİH KISITLAMASI: STATS_UNTIL_DATE değişkeni ile belirtilen tarihe kadar olan maçlar kullanılır.
"""

import os
import datetime as dt
import mysql.connector
from collections import defaultdict

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "charset": "utf8mb4",
    "use_unicode": True,
    "collation": "utf8mb4_unicode_ci"
}

# ========== KONFİGÜRASYON ==========
# İstatistiklerin hesaplanacağı son tarih (bu tarih dahil).
# Örnek: "2026-04-19" -> 19 Nisan 2026 ve öncesi maçlar kullanılır.
# None verilirse tüm maçlar kullanılır.
STATS_UNTIL_DATE = "2026-04-15"   # <-- İhtiyacına göre değiştir
# ===================================

class StatsUpdater:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.cutoff_date = None
        if STATS_UNTIL_DATE:
            self.cutoff_date = dt.datetime.strptime(STATS_UNTIL_DATE, "%Y-%m-%d").date()

    def connect(self):
        self.conn = mysql.connector.connect(**DB_CONFIG)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor(dictionary=True)
        self._create_tables()

    def _create_tables(self):
        """Tablolar yoksa oluştur (utf8mb4)"""
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
        print("[DB] Tablolar kontrol edildi/oluşturuldu (utf8mb4).")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def _apply_cutoff_filter(self, query, params):
        """Eğer cutoff_date varsa sorguya start_utc <= cutoff_date koşulunu ekler."""
        if self.cutoff_date:
            # WHERE varsa AND ekle, yoksa WHERE ekle
            if "WHERE" in query.upper():
                query += " AND start_utc <= %s"
            else:
                # Sorguda WHERE yoksa, FROM ... tablo kısmından sonra ekle
                # Basitçe tablo isminden sonra "WHERE start_utc <= %s" eklemek riskli.
                # Daha güvenli: sorgunun sonuna " AND start_utc <= %s" ekleyelim ama WHERE yoksa hata verir.
                # Burada tüm sorguların zaten WHERE içerdiğini varsayıyoruz.
                # Eğer bir sorguda WHERE yoksa manuel düzeltme gerekir. Şimdilik uyarı bas.
                print("[UYARI] Sorguda WHERE yok, cutoff uygulanamadı:", query[:100])
                return query, params
            params = list(params) + [self.cutoff_date]
        return query, params

    def update_league_stats(self):
        """Lig bazlı ortalamalar (tüm bitmiş maçlar, cutoff tarihine kadar)"""
        query = """
            SELECT 
                category_id,
                COUNT(*) as match_count,
                AVG(COALESCE(ft_home,0) + COALESCE(ft_away,0)) as avg_goals,
                AVG(COALESCE(shot_on_h,0) + COALESCE(shot_on_a,0)) as avg_shot_on,
                AVG(COALESCE(shot_h,0) + COALESCE(shot_a,0)) as avg_total_shots,
                AVG(COALESCE(corn_h,0) + COALESCE(corn_a,0)) as avg_corners,
                SUM(CASE WHEN COALESCE(ft_home,0) > 0 AND COALESCE(ft_away,0) > 0 THEN 1 ELSE 0 END) / COUNT(*) as btts_ratio,
                SUM(CASE WHEN (COALESCE(ft_home,0) + COALESCE(ft_away,0)) > 2.5 THEN 1 ELSE 0 END) / COUNT(*) as over25_ratio,
                AVG(COALESCE(ht_home,0) + COALESCE(ht_away,0)) as avg_first_half_goals,
                SUM(CASE WHEN COALESCE(ht_home,0) = 0 AND COALESCE(ht_away,0) = 0 
                         AND COALESCE(ft_home,0) > 0 AND COALESCE(ft_away,0) > 0 THEN 1 ELSE 0 END) / 
                    NULLIF(SUM(CASE WHEN COALESCE(ht_home,0) = 0 AND COALESCE(ht_away,0) = 0 THEN 1 ELSE 0 END), 0) as zero_zero_comeback_ratio
            FROM results_football
            WHERE status IN ('finished', 'ended')
                AND category_id IS NOT NULL
        """
        params = []
        if self.cutoff_date:
            query += " AND start_utc <= %s"
            params.append(self.cutoff_date)
        query += " GROUP BY category_id"

        self.cursor.execute(query, params)
        rows = self.cursor.fetchall()
        today = dt.date.today()
        for row in rows:
            insert_sql = """
                INSERT INTO league_stats 
                (category_id, avg_goals, avg_shot_on, avg_total_shots, avg_corners, 
                 btts_ratio, over25_ratio, avg_first_half_goals, zero_zero_comeback_ratio, match_count, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                avg_goals = VALUES(avg_goals),
                avg_shot_on = VALUES(avg_shot_on),
                avg_total_shots = VALUES(avg_total_shots),
                avg_corners = VALUES(avg_corners),
                btts_ratio = VALUES(btts_ratio),
                over25_ratio = VALUES(over25_ratio),
                avg_first_half_goals = VALUES(avg_first_half_goals),
                zero_zero_comeback_ratio = VALUES(zero_zero_comeback_ratio),
                match_count = VALUES(match_count),
                last_updated = VALUES(last_updated)
            """
            self.cursor.execute(insert_sql, (
                row['category_id'],
                float(row['avg_goals']) if row['avg_goals'] is not None else 0,
                float(row['avg_shot_on']) if row['avg_shot_on'] is not None else 0,
                float(row['avg_total_shots']) if row['avg_total_shots'] is not None else 0,
                float(row['avg_corners']) if row['avg_corners'] is not None else 0,
                float(row['btts_ratio']) if row['btts_ratio'] is not None else 0,
                float(row['over25_ratio']) if row['over25_ratio'] is not None else 0,
                float(row['avg_first_half_goals']) if row['avg_first_half_goals'] is not None else 0,
                float(row['zero_zero_comeback_ratio']) if row['zero_zero_comeback_ratio'] is not None else 0,
                row['match_count'], today
            ))
        print(f"[league_stats] {len(rows)} lig güncellendi (cutoff: {self.cutoff_date}).")

    def update_team_form_cache(self):
        """Her takım için master prompt'taki tüm form ve hücum verileri (cutoff tarihine kadar)"""
        # Tüm benzersiz (team_name, category_id) kombinasyonlarını al
        self.cursor.execute("""
            SELECT DISTINCT home_team as team_name, category_id FROM results_football WHERE home_team IS NOT NULL
            UNION
            SELECT DISTINCT away_team, category_id FROM results_football WHERE away_team IS NOT NULL
        """)
        teams = self.cursor.fetchall()
        today = dt.date.today()
        count = 0

        for row in teams:
            team_name = row['team_name']
            cat_id = row['category_id']
            team_key = f"{team_name}|{cat_id}"

            # Son 10 maç (ev+deplasman) - en güncel tarih sırası, cutoff'a dikkat
            query_10 = """
                SELECT ft_home, ft_away, shot_on_h, shot_on_a, shot_h, shot_a, corn_h, corn_a, 
                       poss_h, poss_a, ht_home, ht_away, home_team, start_utc
                FROM results_football
                WHERE status IN ('finished', 'ended')
                    AND (home_team = %s OR away_team = %s)
                    AND category_id = %s
            """
            params = [team_name, team_name, cat_id]
            if self.cutoff_date:
                query_10 += " AND start_utc <= %s"
                params.append(self.cutoff_date)
            query_10 += " ORDER BY start_utc DESC, start_time_utc DESC LIMIT 10"
            self.cursor.execute(query_10, params)
            matches = self.cursor.fetchall()
            if len(matches) < 3:
                continue  # yetersiz veri

            goals = []
            shot_on = []
            total_shots = []
            corners = []
            poss = []
            first_half_goals = []
            btts_count = 0
            zero_zero_count = 0
            zero_zero_comeback = 0

            for idx, m in enumerate(matches):
                if m['home_team'] == team_name:
                    g = m['ft_home'] or 0
                    so = m['shot_on_h'] or 0
                    ts = (m['shot_h'] or 0) + (m['shot_a'] or 0)  # takımın toplam şutu
                    cor = m['corn_h'] or 0
                    ps = m['poss_h'] or 0
                    fhg = m['ht_home'] or 0
                    opp_g = m['ft_away'] or 0
                    opp_fhg = m['ht_away'] or 0
                else:
                    g = m['ft_away'] or 0
                    so = m['shot_on_a'] or 0
                    ts = (m['shot_h'] or 0) + (m['shot_a'] or 0)
                    cor = m['corn_a'] or 0
                    ps = m['poss_a'] or 0
                    fhg = m['ht_away'] or 0
                    opp_g = m['ft_home'] or 0
                    opp_fhg = m['ht_home'] or 0

                goals.append(g)
                shot_on.append(so)
                total_shots.append(ts)
                corners.append(cor)
                poss.append(ps)
                first_half_goals.append(fhg)

                if g > 0 and opp_g > 0:
                    btts_count += 1
                if fhg == 0 and opp_fhg == 0:
                    zero_zero_count += 1
                    if g > 0 and opp_g > 0:
                        zero_zero_comeback += 1

            # Ortalamalar
            avg_goals_10 = sum(goals) / len(goals)
            avg_shot_on_10 = sum(shot_on) / len(shot_on)
            avg_total_shots_10 = sum(total_shots) / len(total_shots)
            avg_corners_10 = sum(corners) / len(corners)
            avg_poss_10 = sum(poss) / len(poss)
            avg_fhg_10 = sum(first_half_goals) / len(first_half_goals)
            btts_ratio_10 = btts_count / len(matches)
            zero_zero_comeback_ratio = (zero_zero_comeback / zero_zero_count) if zero_zero_count > 0 else 0.0

            # Son 5 iç saha (cutoff)
            query_home = """
                SELECT COALESCE(ft_home,0) as ft_home FROM results_football
                WHERE status IN ('finished', 'ended') AND home_team = %s AND category_id = %s
            """
            params_home = [team_name, cat_id]
            if self.cutoff_date:
                query_home += " AND start_utc <= %s"
                params_home.append(self.cutoff_date)
            query_home += " ORDER BY start_utc DESC LIMIT 5"
            self.cursor.execute(query_home, params_home)
            home_matches = self.cursor.fetchall()
            home_goals = [m['ft_home'] for m in home_matches]
            home_avg = sum(home_goals) / len(home_goals) if home_goals else 0

            # Son 5 dış saha (cutoff)
            query_away = """
                SELECT COALESCE(ft_away,0) as ft_away FROM results_football
                WHERE status IN ('finished', 'ended') AND away_team = %s AND category_id = %s
            """
            params_away = [team_name, cat_id]
            if self.cutoff_date:
                query_away += " AND start_utc <= %s"
                params_away.append(self.cutoff_date)
            query_away += " ORDER BY start_utc DESC LIMIT 5"
            self.cursor.execute(query_away, params_away)
            away_matches = self.cursor.fetchall()
            away_goals = [m['ft_away'] for m in away_matches]
            away_avg = sum(away_goals) / len(away_goals) if away_goals else 0

            # Son 3 maç (hücum baskısı için) - aynı matches listesinin ilk 3'ü (zaten cutoff uygulanmış)
            last3 = matches[:3]
            last3_shot_on = []
            last3_total_shots = []
            last3_corners = []
            last3_poss = []
            last_match_shot_on = None
            for idx, m in enumerate(last3):
                if m['home_team'] == team_name:
                    so = m['shot_on_h'] or 0
                    ts = (m['shot_h'] or 0) + (m['shot_a'] or 0)
                    cor = m['corn_h'] or 0
                    ps = m['poss_h'] or 0
                else:
                    so = m['shot_on_a'] or 0
                    ts = (m['shot_h'] or 0) + (m['shot_a'] or 0)
                    cor = m['corn_a'] or 0
                    ps = m['poss_a'] or 0
                last3_shot_on.append(so)
                last3_total_shots.append(ts)
                last3_corners.append(cor)
                last3_poss.append(ps)
                if idx == 0:
                    last_match_shot_on = so
            avg_shot_on_3 = sum(last3_shot_on) / len(last3_shot_on) if last3_shot_on else 0
            avg_total_shots_3 = sum(last3_total_shots) / len(last3_total_shots) if last3_total_shots else 0
            avg_corners_3 = sum(last3_corners) / len(last3_corners) if last3_corners else 0
            avg_poss_3 = sum(last3_poss) / len(last3_poss) if last3_poss else 0

            # Kaydet
            insert_sql = """
                INSERT INTO team_form_cache 
                (team_key, last_10_avg_goals, last_10_avg_shot_on, last_10_avg_corners,
                 last_10_btts_ratio, last_10_avg_first_half_goals, zero_zero_comeback_ratio,
                 home_last_5_avg_goals, away_last_5_avg_goals,
                 last_3_avg_shot_on, last_3_avg_total_shots, last_3_avg_corners, last_3_avg_possession,
                 last_match_shot_on, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                last_10_avg_goals = VALUES(last_10_avg_goals),
                last_10_avg_shot_on = VALUES(last_10_avg_shot_on),
                last_10_avg_corners = VALUES(last_10_avg_corners),
                last_10_btts_ratio = VALUES(last_10_btts_ratio),
                last_10_avg_first_half_goals = VALUES(last_10_avg_first_half_goals),
                zero_zero_comeback_ratio = VALUES(zero_zero_comeback_ratio),
                home_last_5_avg_goals = VALUES(home_last_5_avg_goals),
                away_last_5_avg_goals = VALUES(away_last_5_avg_goals),
                last_3_avg_shot_on = VALUES(last_3_avg_shot_on),
                last_3_avg_total_shots = VALUES(last_3_avg_total_shots),
                last_3_avg_corners = VALUES(last_3_avg_corners),
                last_3_avg_possession = VALUES(last_3_avg_possession),
                last_match_shot_on = VALUES(last_match_shot_on),
                last_updated = VALUES(last_updated)
            """
            self.cursor.execute(insert_sql, (
                team_key, avg_goals_10, avg_shot_on_10, avg_corners_10,
                btts_ratio_10, avg_fhg_10, zero_zero_comeback_ratio,
                home_avg, away_avg,
                avg_shot_on_3, avg_total_shots_3, avg_corners_3, avg_poss_3,
                last_match_shot_on, today
            ))
            count += 1

        print(f"[team_form_cache] {count} takım güncellendi (cutoff: {self.cutoff_date}).")

    def update_team_dna(self):
        """Takım DNA'sı (tüm geçmiş ortalamalar) – isteğe bağlı, cutoff tarihine kadar."""
        query_home = """
            SELECT 
                CONCAT(home_team, '|', category_id) as team_key,
                AVG(COALESCE(ft_home,0)) as avg_goals,
                AVG(COALESCE(shot_on_h,0)) as avg_shot_on,
                AVG(COALESCE(corn_h,0)) as avg_corners,
                COUNT(*) as total_matches
            FROM results_football
            WHERE status IN ('finished', 'ended')
        """
        params = []
        if self.cutoff_date:
            query_home += " AND start_utc <= %s"
            params.append(self.cutoff_date)
        query_home += " GROUP BY home_team, category_id"
        self.cursor.execute(query_home, params)
        home_stats = {row['team_key']: row for row in self.cursor.fetchall()}

        query_away = """
            SELECT 
                CONCAT(away_team, '|', category_id) as team_key,
                AVG(COALESCE(ft_away,0)) as avg_goals,
                AVG(COALESCE(shot_on_a,0)) as avg_shot_on,
                AVG(COALESCE(corn_a,0)) as avg_corners,
                COUNT(*) as total_matches
            FROM results_football
            WHERE status IN ('finished', 'ended')
        """
        params_away = []
        if self.cutoff_date:
            query_away += " AND start_utc <= %s"
            params_away.append(self.cutoff_date)
        query_away += " GROUP BY away_team, category_id"
        self.cursor.execute(query_away, params_away)
        for row in self.cursor.fetchall():
            key = row['team_key']
            if key in home_stats:
                home_stats[key]['avg_goals'] = (home_stats[key]['avg_goals'] + row['avg_goals']) / 2
                home_stats[key]['avg_shot_on'] = (home_stats[key]['avg_shot_on'] + row['avg_shot_on']) / 2
                home_stats[key]['avg_corners'] = (home_stats[key]['avg_corners'] + row['avg_corners']) / 2
                home_stats[key]['total_matches'] += row['total_matches']
            else:
                home_stats[key] = row
        today = dt.date.today()
        for key, stats in home_stats.items():
            self.cursor.execute("""
                INSERT INTO team_dna (team_key, avg_goals, avg_shot_on, avg_corners, total_matches, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                avg_goals = VALUES(avg_goals),
                avg_shot_on = VALUES(avg_shot_on),
                avg_corners = VALUES(avg_corners),
                total_matches = VALUES(total_matches),
                last_updated = VALUES(last_updated)
            """, (key, stats['avg_goals'], stats['avg_shot_on'], stats['avg_corners'], stats['total_matches'], today))
        print(f"[team_dna] {len(home_stats)} takım güncellendi (cutoff: {self.cutoff_date}).")

    def update_referee_stats(self):
        """Hakem bazlı ortalama gol (cutoff tarihine kadar)"""
        query = """
            SELECT referee, AVG(COALESCE(ft_home,0) + COALESCE(ft_away,0)) as avg_goals, COUNT(*) as match_count
            FROM results_football
            WHERE status IN ('finished', 'ended') AND referee IS NOT NULL
        """
        params = []
        if self.cutoff_date:
            query += " AND start_utc <= %s"
            params.append(self.cutoff_date)
        query += " GROUP BY referee"
        self.cursor.execute(query, params)
        rows = self.cursor.fetchall()
        today = dt.date.today()
        for row in rows:
            insert_sql = """
                INSERT INTO referee_stats (referee_name, avg_goals, match_count, last_updated)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                avg_goals = VALUES(avg_goals),
                match_count = VALUES(match_count),
                last_updated = VALUES(last_updated)
            """
            self.cursor.execute(insert_sql, (row['referee'], float(row['avg_goals']), row['match_count'], today))
        print(f"[referee_stats] {len(rows)} hakem güncellendi (cutoff: {self.cutoff_date}).")

    def run(self):
        self.connect()
        self.update_league_stats()
        self.update_team_form_cache()
        self.update_team_dna()      # İstersen kaldırabilirsin, kullanılmıyor şu an
        self.update_referee_stats()
        self.close()
        print("Tüm istatistik tabloları güncellendi.")


if __name__ == "__main__":
    updater = StatsUpdater()
    updater.run()
