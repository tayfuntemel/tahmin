#!/usr/bin/env python3
"""
İstatistik tablolarını günceller:
- league_stats
- team_dna
- team_form_cache
- referee_stats
Her çalıştırmada aynı satırları günceller (yeni satır eklemez).
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
    "port": int(os.getenv("DB_PORT", 3306))
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
            )
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
            )
        """)
        # team_form_cache
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
            )
        """)
        # referee_stats
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS referee_stats (
                referee_name VARCHAR(128) PRIMARY KEY,
                avg_goals FLOAT,
                match_count INT,
                last_updated DATE
            )
        """)

    def update_league_stats(self):
        query = """
            SELECT 
                category_id,
                COUNT(*) as match_count,
                AVG(ft_home + ft_away) as avg_goals,
                AVG(shot_on_h + shot_on_a) as avg_shot_on,
                AVG(corn_h + corn_a) as avg_corners,
                SUM(CASE WHEN ft_home > 0 AND ft_away > 0 THEN 1 ELSE 0 END) / COUNT(*) as btts_ratio,
                SUM(CASE WHEN (ft_home + ft_away) > 2.5 THEN 1 ELSE 0 END) / COUNT(*) as over25_ratio
            FROM results_football
            WHERE status IN ('finished', 'ended')
                AND ft_home IS NOT NULL AND ft_away IS NOT NULL
                AND category_id IS NOT NULL
            GROUP BY category_id
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        today = dt.date.today()
        for row in rows:
            self.cursor.execute("""
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
            """, (row['category_id'], row['avg_goals'], row['avg_shot_on'], row['avg_corners'],
                  row['btts_ratio'], row['over25_ratio'], row['match_count'], today))
        print(f"[league_stats] {len(rows)} lig güncellendi.")

    def update_team_dna(self):
        query = """
            SELECT home_team as team_name, category_id, AVG(ft_home) as avg_goals, AVG(shot_on_h) as avg_shot_on, AVG(corn_h) as avg_corners, COUNT(*) as cnt
            FROM results_football
            WHERE status IN ('finished', 'ended') AND ft_home IS NOT NULL
            GROUP BY home_team, category_id
            UNION ALL
            SELECT away_team, category_id, AVG(ft_away), AVG(shot_on_a), AVG(corn_a), COUNT(*)
            FROM results_football
            WHERE status IN ('finished', 'ended') AND ft_away IS NOT NULL
            GROUP BY away_team, category_id
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        stats = defaultdict(lambda: {'goals': [], 'shot_on': [], 'corners': [], 'total': 0})
        for row in rows:
            key = f"{row['team_name']}|{row['category_id']}"
            stats[key]['goals'].append(row['avg_goals'])
            stats[key]['shot_on'].append(row['avg_shot_on'])
            stats[key]['corners'].append(row['avg_corners'])
            stats[key]['total'] += row['cnt']
        today = dt.date.today()
        for key, val in stats.items():
            avg_goals = sum(val['goals']) / len(val['goals'])
            avg_shot_on = sum(val['shot_on']) / len(val['shot_on'])
            avg_corners = sum(val['corners']) / len(val['corners'])
            self.cursor.execute("""
                INSERT INTO team_dna (team_key, avg_goals, avg_shot_on, avg_corners, total_matches, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                avg_goals = VALUES(avg_goals), avg_shot_on = VALUES(avg_shot_on),
                avg_corners = VALUES(avg_corners), total_matches = VALUES(total_matches),
                last_updated = VALUES(last_updated)
            """, (key, avg_goals, avg_shot_on, avg_corners, val['total'], today))
        print(f"[team_dna] {len(stats)} takım güncellendi.")

    def update_team_form_cache(self):
        # Tüm benzersiz takım_key'leri al
        self.cursor.execute("SELECT DISTINCT CONCAT(home_team, '|', category_id) as team_key FROM results_football WHERE home_team IS NOT NULL")
        team_keys = [row['team_key'] for row in self.cursor.fetchall()]
        today = dt.date.today()
        for team_key in team_keys:
            parts = team_key.rsplit('|', 1)
            if len(parts) != 2:
                continue
            team_name, cat_id = parts[0], int(parts[1])
            # Son 10 maç (ev+deplasman)
            self.cursor.execute("""
                SELECT ft_home, ft_away, shot_on_h, shot_on_a, corn_h, corn_a, poss_h, poss_a, home_team
                FROM results_football
                WHERE status IN ('finished', 'ended')
                    AND (home_team = %s OR away_team = %s)
                    AND category_id = %s
                ORDER BY start_utc DESC, start_time_utc DESC
                LIMIT 10
            """, (team_name, team_name, cat_id))
            matches = self.cursor.fetchall()
            if len(matches) < 3:
                continue
            goals = [m['ft_home'] if m['home_team'] == team_name else m['ft_away'] for m in matches]
            avg_goals_10 = sum(goals) / len(goals)
            shot_on = [m['shot_on_h'] if m['home_team'] == team_name else m['shot_on_a'] for m in matches]
            avg_shot_on_10 = sum(shot_on) / len(shot_on)
            corners = [m['corn_h'] if m['home_team'] == team_name else m['corn_a'] for m in matches]
            avg_corners_10 = sum(corners) / len(corners)
            btts_count = sum(1 for m in matches if (m['ft_home'] or 0) > 0 and (m['ft_away'] or 0) > 0)
            btts_ratio = btts_count / len(matches)
            # İç saha son 5
            self.cursor.execute("""
                SELECT ft_home FROM results_football
                WHERE status IN ('finished', 'ended') AND home_team = %s AND category_id = %s
                ORDER BY start_utc DESC LIMIT 5
            """, (team_name, cat_id))
            home_matches = self.cursor.fetchall()
            home_avg = sum(m['ft_home'] or 0 for m in home_matches) / len(home_matches) if home_matches else 0
            # Dış saha son 5
            self.cursor.execute("""
                SELECT ft_away FROM results_football
                WHERE status IN ('finished', 'ended') AND away_team = %s AND category_id = %s
                ORDER BY start_utc DESC LIMIT 5
            """, (team_name, cat_id))
            away_matches = self.cursor.fetchall()
            away_avg = sum(m['ft_away'] or 0 for m in away_matches) / len(away_matches) if away_matches else 0
            # Son 3 maç hücum
            last3 = matches[:3]
            last3_shot_on = [m['shot_on_h'] if m['home_team'] == team_name else m['shot_on_a'] for m in last3]
            last3_corners = [m['corn_h'] if m['home_team'] == team_name else m['corn_a'] for m in last3]
            last3_poss = [m['poss_h'] if m['home_team'] == team_name else m['poss_a'] for m in last3]
            avg_shot_on_3 = sum(last3_shot_on) / len(last3_shot_on)
            avg_corners_3 = sum(last3_corners) / len(last3_corners)
            avg_poss_3 = sum(last3_poss) / len(last3_poss)
            self.cursor.execute("""
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
            """, (team_key, avg_goals_10, avg_shot_on_10, avg_corners_10,
                  btts_ratio, home_avg, away_avg,
                  avg_shot_on_3, avg_corners_3, avg_poss_3, today))
        print(f"[team_form_cache] {len(team_keys)} takım güncellendi.")

    def update_referee_stats(self):
        query = """
            SELECT referee, AVG(ft_home + ft_away) as avg_goals, COUNT(*) as match_count
            FROM results_football
            WHERE status IN ('finished', 'ended') AND referee IS NOT NULL
            GROUP BY referee
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        today = dt.date.today()
        for row in rows:
            self.cursor.execute("""
                INSERT INTO referee_stats (referee_name, avg_goals, match_count, last_updated)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                avg_goals = VALUES(avg_goals), match_count = VALUES(match_count), last_updated = VALUES(last_updated)
            """, (row['referee'], row['avg_goals'], row['match_count'], today))
        print(f"[referee_stats] {len(rows)} hakem güncellendi.")

    def run(self):
        self.connect()
        self.update_league_stats()
        self.update_team_dna()
        self.update_team_form_cache()
        self.update_referee_stats()
        self.conn.close()
        print("Tüm istatistik tabloları güncellendi.")

if __name__ == "__main__":
    updater = StatsUpdater()
    updater.run()
