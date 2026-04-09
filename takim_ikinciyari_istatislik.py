#!/usr/bin/env python3
import os
import mysql.connector

CONFIG = {
    "db": {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306))
    }
}

SCHEMA_CREATE_SH_TABLE = """
CREATE TABLE IF NOT EXISTS team_second_half_analytics (
  id                  INT AUTO_INCREMENT PRIMARY KEY,
  team_name           VARCHAR(128) NOT NULL,
  tournament_id       INT NOT NULL,
  venue_type          ENUM('Overall', 'Home', 'Away') NOT NULL,
  matches_played      INT DEFAULT 0,
  sh_wins             INT DEFAULT 0,
  sh_draws            INT DEFAULT 0,
  sh_losses           INT DEFAULT 0,
  sh_goals_for        INT DEFAULT 0,
  sh_goals_against    INT DEFAULT 0,
  sh_avg_goals_for    FLOAT DEFAULT 0,
  sh_avg_goals_against FLOAT DEFAULT 0,
  sh_over_05_pct      FLOAT DEFAULT 0,
  sh_over_15_pct      FLOAT DEFAULT 0,
  sh_btts_yes_pct     FLOAT DEFAULT 0,
  last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY idx_team_tour_venue_sh (team_name, tournament_id, venue_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class SecondHalfAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.stats = {}

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_SH_TABLE)

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _init_team_struct(self):
        return {
            "matches": 0, "sh_wins": 0, "sh_draws": 0, "sh_losses": 0,
            "sh_goals_for": 0, "sh_goals_against": 0,
            "sh_over_05": 0, "sh_over_15": 0, "sh_btts": 0
        }

    def _get_team_node(self, team_name, tournament_id, venue_type):
        key = (team_name, tournament_id, venue_type)
        if key not in self.stats:
            self.stats[key] = self._init_team_struct()
        return self.stats[key]

    def _process_match(self, team_name, is_home, match, venue_type):
        tour_id = match['tournament_id']
        node = self._get_team_node(team_name, tour_id, venue_type)
        if is_home:
            sh_gf = match['ft_home'] - match['ht_home']
            sh_ga = match['ft_away'] - match['ht_away']
        else:
            sh_gf = match['ft_away'] - match['ht_away']
            sh_ga = match['ft_home'] - match['ht_home']
        node["matches"] += 1
        node["sh_goals_for"] += sh_gf
        node["sh_goals_against"] += sh_ga
        if sh_gf > sh_ga: node["sh_wins"] += 1
        elif sh_gf == sh_ga: node["sh_draws"] += 1
        else: node["sh_losses"] += 1
        total = sh_gf + sh_ga
        if total > 0.5: node["sh_over_05"] += 1
        if total > 1.5: node["sh_over_15"] += 1
        if sh_gf > 0 and sh_ga > 0: node["sh_btts"] += 1

    def analyze_second_half(self):
        self.cur.execute("SELECT * FROM results_football WHERE status IN ('finished','ended') AND ht_home IS NOT NULL AND ht_away IS NOT NULL AND ft_home IS NOT NULL AND ft_away IS NOT NULL")
        matches = self.cur.fetchall()
        for match in matches:
            home, away = match['home_team'], match['away_team']
            self._process_match(home, True, match, "Home")
            self._process_match(home, True, match, "Overall")
            self._process_match(away, False, match, "Away")
            self._process_match(away, False, match, "Overall")

        insert_query = """
            INSERT INTO team_second_half_analytics 
            (team_name, tournament_id, venue_type, matches_played, 
             sh_wins, sh_draws, sh_losses, sh_goals_for, sh_goals_against, 
             sh_avg_goals_for, sh_avg_goals_against, 
             sh_over_05_pct, sh_over_15_pct, sh_btts_yes_pct)
            VALUES (%(team_name)s, %(tournament_id)s, %(venue_type)s, %(matches_played)s,
             %(sh_wins)s, %(sh_draws)s, %(sh_losses)s, %(sh_goals_for)s, %(sh_goals_against)s,
             %(sh_avg_goals_for)s, %(sh_avg_goals_against)s,
             %(sh_over_05_pct)s, %(sh_over_15_pct)s, %(sh_btts_yes_pct)s)
            ON DUPLICATE KEY UPDATE
             matches_played=VALUES(matches_played),
             sh_wins=VALUES(sh_wins), sh_draws=VALUES(sh_draws), sh_losses=VALUES(sh_losses),
             sh_goals_for=VALUES(sh_goals_for), sh_goals_against=VALUES(sh_goals_against),
             sh_avg_goals_for=VALUES(sh_avg_goals_for), sh_avg_goals_against=VALUES(sh_avg_goals_against),
             sh_over_05_pct=VALUES(sh_over_05_pct), sh_over_15_pct=VALUES(sh_over_15_pct), sh_btts_yes_pct=VALUES(sh_btts_yes_pct)
        """
        count = 0
        for key, data in self.stats.items():
            team_name, tour_id, venue_type = key
            mp = data["matches"]
            if mp == 0: continue
            row = {
                "team_name": team_name, "tournament_id": tour_id, "venue_type": venue_type, "matches_played": mp,
                "sh_wins": data["sh_wins"], "sh_draws": data["sh_draws"], "sh_losses": data["sh_losses"],
                "sh_goals_for": data["sh_goals_for"], "sh_goals_against": data["sh_goals_against"],
                "sh_avg_goals_for": round(data["sh_goals_for"] / mp, 2),
                "sh_avg_goals_against": round(data["sh_goals_against"] / mp, 2),
                "sh_over_05_pct": round((data["sh_over_05"] / mp) * 100, 2),
                "sh_over_15_pct": round((data["sh_over_15"] / mp) * 100, 2),
                "sh_btts_yes_pct": round((data["sh_btts"] / mp) * 100, 2)
            }
            self.cur.execute(insert_query, row)
            count += 1
        print(f"[İKİNCİ YARI ANALİZİ] {count} takım güncellendi.")

if __name__ == "__main__":
    a = SecondHalfAnalyzer(CONFIG["db"])
    try:
        a.connect()
        a.analyze_second_half()
    finally:
        a.close()
