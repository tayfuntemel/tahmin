#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
İstatistik tablolarını günceller (league_stats, team_dna, team_form_cache, referee_stats)
Her çalıştırmada mevcut satırları günceller, yeni satır eklemez.
NULL değerler 0 olarak ele alınır.
UTF-8 karakter desteği eklendi.
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

class StatsUpdater:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = mysql.connector.connect(**DB_CONFIG)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor(dictionary=True)
        self._create_tables()

    def _create_tables(self):
        """Tablolar yoksa oluştur (utf8mb4)"""
        # league_stats
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS league_stats (
                category_id INT PRIMARY KEY,
                avg_goals FLOAT,
                avg_shot_on FLOAT,
                avg_corners FLOAT,
                btts_ratio FLOAT,
                over25_ratio FLOAT,
                match_count INT,
                last_updated DATE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        # team_dna
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
        # team_form_cache (tek satır, tarihsiz)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS team_form_cache (
                team_key VARCHAR(255) PRIMARY KEY,
                last_10_avg_goals FLOAT,
                last_10_avg_shot_on FLOAT,
                last_10_avg_corners FLOAT,
                last_10_btts_ratio FLOAT,
                home_last_5_avg_goals FLOAT,
                away_last_5_avg_goals FLOAT,
                last_3_avg_shot_on FLOAT,
                last_3_avg_corners FLOAT,
                last_3_avg_possession FLOAT,
                last_updated DATE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        # referee_stats
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

    def update_league_stats(self):
        """Lig bazlı ortalamaları hesapla ve kaydet (NULL'lar 0 sayılır)"""
        query = """
            SELECT 
                category_id,
                COUNT(*) as match_count,
                AVG(COALESCE(ft_home,0) + COALESCE(ft_away,0)) as avg_goals,
                AVG(COALESCE(shot_on_h,0) + COALESCE(shot_on_a,0)) as avg_shot_on,
                AVG(COALESCE(corn_h,0) + COALESCE(corn_a,0)) as avg_corners,
                SUM(CASE WHEN COALESCE(ft_home,0) > 0 AND COALESCE(ft_away,0) > 0 THEN 1 ELSE 0 END) / COUNT(*) as btts_ratio,
                SUM(CASE WHEN (COALESCE(ft_home,0) + COALESCE(ft_away,0)) > 2.5 THEN 1 ELSE 0 END) / COUNT(*) as over25_ratio
            FROM results_football
            WHERE status IN ('finished', 'ended')
                AND category_id IS NOT NULL
            GROUP BY category_id
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        today = dt.date.today()
        for row in rows:
            insert_sql = """
                INSERT INTO league_stats 
                (category_id, avg_goals, avg_shot_on, avg_corners, btts_ratio, over25_ratio, match_count, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                avg_goals = VALUES(avg_goals),
                avg_shot_on = VALUES(avg_shot_on),
                avg_corners = VALUES(avg_corners),
                btts_ratio = VALUES(btts_ratio),
                over25_ratio = VALUES(over25_ratio),
                match_count = VALUES(match_count),
                last_updated = VALUES(last_updated)
            """
            self.cursor.execute(insert_sql, (
                row['category_id'], 
                float(row['avg_goals']) if row['avg_goals'] is not None else 0,
                float(row['avg_shot_on']) if row['avg_shot_on'] is not None else 0,
                float(row['avg_corners']) if row['avg_corners'] is not None else 0,
                float(row['btts_ratio']) if row['btts_ratio'] is not None else 0,
                float(row['over25_ratio']) if row['over25_ratio'] is not None else 0,
                row['match_count'], today
            ))
        print(f"[league_stats] {len(rows)} lig güncellendi.")

    def update_team_dna(self):
        """Her takım için tüm geçmiş ortalamalar (NULL'lar 0)"""
        # Ev sahibi ortalamaları
        query_home = """
            SELECT 
                home_team as team_name, category_id,
                AVG(COALESCE(ft_home,0)) as avg_goals,
                AVG(COALESCE(shot_on_h,0)) as avg_shot_on,
                AVG(COALESCE(corn_h,0)) as avg_corners,
                COUNT(*) as total_matches
            FROM results_football
            WHERE status IN ('finished', 'ended')
            GROUP BY home_team, category_id
        """
        self.cursor.execute(query_home)
        home_rows = self.cursor.fetchall()
        
        # Deplasman ortalamaları
        query_away = """
            SELECT 
                away_team as team_name, category_id,
                AVG(COALESCE(ft_away,0)) as avg_goals,
                AVG(COALESCE(shot_on_a,0)) as avg_shot_on,
                AVG(COALESCE(corn_a,0)) as avg_corners,
                COUNT(*) as total_matches
            FROM results_football
            WHERE status IN ('finished', 'ended')
            GROUP BY away_team, category_id
        """
        self.cursor.execute(query_away)
        away_rows = self.cursor.fetchall()
        
        # Birleştir (ev + deplasman)
        team_stats = defaultdict(lambda: {'goals': [], 'shot_on': [], 'corners': [], 'count': 0})
        
        for row in home_rows:
            key = f"{row['team_name']}|{row['category_id']}"
            team_stats[key]['goals'].append(row['avg_goals'] or 0)
            team_stats[key]['shot_on'].append(row['avg_shot_on'] or 0)
            team_stats[key]['corners'].append(row['avg_corners'] or 0)
            team_stats[key]['count'] += row['total_matches']
        
        for row in away_rows:
            key = f"{row['team_name']}|{row['category_id']}"
            team_stats[key]['goals'].append(row['avg_goals'] or 0)
            team_stats[key]['shot_on'].append(row['avg_shot_on'] or 0)
            team_stats[key]['corners'].append(row['avg_corners'] or 0)
            team_stats[key]['count'] += row['total_matches']
        
        today = dt.date.today()
        for key, stats in team_stats.items():
            avg_goals = sum(stats['goals']) / len(stats['goals']) if stats['goals'] else 0
            avg_shot_on = sum(stats['shot_on']) / len(stats['shot_on']) if stats['shot_on'] else 0
            avg_corners = sum(stats['corners']) / len(stats['corners']) if stats['corners'] else 0
            insert_sql = """
                INSERT INTO team_dna (team_key, avg_goals, avg_shot_on, avg_corners, total_matches, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                avg_goals = VALUES(avg_goals),
                avg_shot_on = VALUES(avg_shot_on),
                avg_corners = VALUES(avg_corners),
                total_matches = VALUES(total_matches),
                last_updated = VALUES(last_updated)
            """
            self.cursor.execute(insert_sql, (key, avg_goals, avg_shot_on, avg_corners, stats['count'], today))
        print(f"[team_dna] {len(team_stats)} takım güncellendi.")

    def update_team_form_cache(self):
        """Her takım için son 10 maç, son 5 iç/dış, son 3 hücum verileri (NULL'lar 0)"""
        # Benzersiz team_key'leri al
        self.cursor.execute("SELECT DISTINCT CONCAT(home_team, '|', category_id) as team_key FROM results_football WHERE home_team IS NOT NULL")
        team_keys = [row['team_key'] for row in self.cursor.fetchall()]
        
        today = dt.date.today()
        for team_key in team_keys:
            parts = team_key.rsplit('|', 1)
            if len(parts) != 2:
                continue
            team_name, cat_id = parts[0], int(parts[1])
            
            # Son 10 maç (ev+deplasman)
            query_10 = """
                SELECT ft_home, ft_away, shot_on_h, shot_on_a, corn_h, corn_a, poss_h, poss_a, home_team
                FROM results_football
                WHERE status IN ('finished', 'ended')
                    AND (home_team = %s OR away_team = %s)
                    AND category_id = %s
                ORDER BY start_utc DESC, start_time_utc DESC
                LIMIT 10
            """
            self.cursor.execute(query_10, (team_name, team_name, cat_id))
            matches = self.cursor.fetchall()
            if len(matches) < 3:
                continue  # yetersiz veri, bu takımı atla
                
            # Gol ortalaması son 10
            goals = []
            shot_on_10 = []
            corners_10 = []
            btts_count = 0
            for m in matches:
                if m['home_team'] == team_name:
                    g = m['ft_home'] or 0
                    so = m['shot_on_h'] or 0
                    cor = m['corn_h'] or 0
                    opp_g = m['ft_away'] or 0
                else:
                    g = m['ft_away'] or 0
                    so = m['shot_on_a'] or 0
                    cor = m['corn_a'] or 0
                    opp_g = m['ft_home'] or 0
                goals.append(g)
                shot_on_10.append(so)
                corners_10.append(cor)
                if g > 0 and opp_g > 0:
                    btts_count += 1
            avg_goals_10 = sum(goals) / len(goals) if goals else 0
            avg_shot_on_10 = sum(shot_on_10) / len(shot_on_10) if shot_on_10 else 0
            avg_corners_10 = sum(corners_10) / len(corners_10) if corners_10 else 0
            btts_ratio = btts_count / len(matches) if matches else 0
            
            # İç saha son 5 maç
            self.cursor.execute("""
                SELECT COALESCE(ft_home,0) as ft_home FROM results_football
                WHERE status IN ('finished', 'ended') AND home_team = %s AND category_id = %s
                ORDER BY start_utc DESC LIMIT 5
            """, (team_name, cat_id))
            home_matches = self.cursor.fetchall()
            home_goals = [m['ft_home'] for m in home_matches]
            home_avg = sum(home_goals) / len(home_goals) if home_goals else 0
            
            # Dış saha son 5 maç
            self.cursor.execute("""
                SELECT COALESCE(ft_away,0) as ft_away FROM results_football
                WHERE status IN ('finished', 'ended') AND away_team = %s AND category_id = %s
                ORDER BY start_utc DESC LIMIT 5
            """, (team_name, cat_id))
            away_matches = self.cursor.fetchall()
            away_goals = [m['ft_away'] for m in away_matches]
            away_avg = sum(away_goals) / len(away_goals) if away_goals else 0
            
            # Son 3 maç hücum verileri
            last3 = matches[:3]
            last3_shot_on = []
            last3_corners = []
            last3_poss = []
            for m in last3:
                if m['home_team'] == team_name:
                    last3_shot_on.append(m['shot_on_h'] or 0)
                    last3_corners.append(m['corn_h'] or 0)
                    last3_poss.append(m['poss_h'] or 0)
                else:
                    last3_shot_on.append(m['shot_on_a'] or 0)
                    last3_corners.append(m['corn_a'] or 0)
                    last3_poss.append(m['poss_a'] or 0)
            avg_shot_on_3 = sum(last3_shot_on) / len(last3_shot_on) if last3_shot_on else 0
            avg_corners_3 = sum(last3_corners) / len(last3_corners) if last3_corners else 0
            avg_poss_3 = sum(last3_poss) / len(last3_poss) if last3_poss else 0
            
            insert_sql = """
                INSERT INTO team_form_cache 
                (team_key, last_10_avg_goals, last_10_avg_shot_on, last_10_avg_corners,
                 last_10_btts_ratio, home_last_5_avg_goals, away_last_5_avg_goals,
                 last_3_avg_shot_on, last_3_avg_corners, last_3_avg_possession, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                last_10_avg_goals = VALUES(last_10_avg_goals),
                last_10_avg_shot_on = VALUES(last_10_avg_shot_on),
                last_10_avg_corners = VALUES(last_10_avg_corners),
                last_10_btts_ratio = VALUES(last_10_btts_ratio),
                home_last_5_avg_goals = VALUES(home_last_5_avg_goals),
                away_last_5_avg_goals = VALUES(away_last_5_avg_goals),
                last_3_avg_shot_on = VALUES(last_3_avg_shot_on),
                last_3_avg_corners = VALUES(last_3_avg_corners),
                last_3_avg_possession = VALUES(last_3_avg_possession),
                last_updated = VALUES(last_updated)
            """
            self.cursor.execute(insert_sql, (
                team_key, avg_goals_10, avg_shot_on_10, avg_corners_10,
                btts_ratio, home_avg, away_avg,
                avg_shot_on_3, avg_corners_3, avg_poss_3, today
            ))
        print(f"[team_form_cache] {len(team_keys)} takım formu güncellendi.")

    def update_referee_stats(self):
        """Hakem bazlı ortalama gol (NULL'lar 0)"""
        query = """
            SELECT referee, AVG(COALESCE(ft_home,0) + COALESCE(ft_away,0)) as avg_goals, COUNT(*) as match_count
            FROM results_football
            WHERE status IN ('finished', 'ended') AND referee IS NOT NULL
            GROUP BY referee
        """
        self.cursor.execute(query)
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
        print(f"[referee_stats] {len(rows)} hakem güncellendi.")

    def run(self):
        self.connect()
        self.update_league_stats()
        self.update_team_dna()
        self.update_team_form_cache()
        self.update_referee_stats()
        self.close()
        print("Tüm istatistik tabloları güncellendi.")

if __name__ == "__main__":
    updater = StatsUpdater()
    updater.run()
