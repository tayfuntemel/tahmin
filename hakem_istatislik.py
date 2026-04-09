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

SCHEMA_CREATE_REFEREE_ANALYTICS = """
CREATE TABLE IF NOT EXISTS referee_analytics (
  referee_name      VARCHAR(128) PRIMARY KEY,
  total_matches     INT DEFAULT 0,
  home_wins         INT DEFAULT 0,
  draws             INT DEFAULT 0,
  away_wins         INT DEFAULT 0,
  home_win_pct      FLOAT DEFAULT 0,
  draw_pct          FLOAT DEFAULT 0,
  away_win_pct      FLOAT DEFAULT 0,
  over_25_pct       FLOAT DEFAULT 0,
  btts_yes_pct      FLOAT DEFAULT 0,
  avg_goals_match   FLOAT DEFAULT 0,
  avg_fouls_match   FLOAT DEFAULT 0,
  last_updated      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class RefereeAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.referees = {}

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_REFEREE_ANALYTICS)

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _init_referee_struct(self):
        return {
            "matches": 0, "home_wins": 0, "draws": 0, "away_wins": 0,
            "total_goals": 0, "over_25_count": 0, "btts_yes_count": 0,
            "total_fouls": 0, "matches_with_foul_data": 0
        }

    def analyze_referees(self):
        self.cur.execute("SELECT * FROM results_football WHERE status IN ('finished','ended') AND referee IS NOT NULL AND referee != ''")
        matches = self.cur.fetchall()
        for match in matches:
            ref = match['referee'].strip()
            if ref not in self.referees:
                self.referees[ref] = self._init_referee_struct()
            r = self.referees[ref]
            r["matches"] += 1
            gh, ga = match.get('ft_home'), match.get('ft_away')
            if gh is not None and ga is not None:
                total = gh + ga
                r["total_goals"] += total
                if gh > ga: r["home_wins"] += 1
                elif gh == ga: r["draws"] += 1
                else: r["away_wins"] += 1
                if total > 2.5: r["over_25_count"] += 1
                if gh > 0 and ga > 0: r["btts_yes_count"] += 1
            fh, fa = match.get('fouls_h'), match.get('fouls_a')
            if fh is not None and fa is not None:
                r["total_fouls"] += (fh + fa)
                r["matches_with_foul_data"] += 1

        insert_query = """
            INSERT INTO referee_analytics 
            (referee_name, total_matches, home_wins, draws, away_wins, 
             home_win_pct, draw_pct, away_win_pct, over_25_pct, btts_yes_pct, 
             avg_goals_match, avg_fouls_match)
            VALUES (%(referee_name)s, %(total_matches)s, %(home_wins)s, %(draws)s, %(away_wins)s,
             %(home_win_pct)s, %(draw_pct)s, %(away_win_pct)s, %(over_25_pct)s, %(btts_yes_pct)s, 
             %(avg_goals_match)s, %(avg_fouls_match)s)
            ON DUPLICATE KEY UPDATE
             total_matches=VALUES(total_matches), home_wins=VALUES(home_wins), draws=VALUES(draws), away_wins=VALUES(away_wins),
             home_win_pct=VALUES(home_win_pct), draw_pct=VALUES(draw_pct), away_win_pct=VALUES(away_win_pct),
             over_25_pct=VALUES(over_25_pct), btts_yes_pct=VALUES(btts_yes_pct),
             avg_goals_match=VALUES(avg_goals_match), avg_fouls_match=VALUES(avg_fouls_match)
        """
        count = 0
        for ref_name, r in self.referees.items():
            tm = r["matches"]
            if tm == 0: continue
            foul_matches = r["matches_with_foul_data"]
            avg_fouls = round(r["total_fouls"] / foul_matches, 2) if foul_matches > 0 else 0
            row = {
                "referee_name": ref_name,
                "total_matches": tm,
                "home_wins": r["home_wins"],
                "draws": r["draws"],
                "away_wins": r["away_wins"],
                "home_win_pct": round((r["home_wins"] / tm) * 100, 2),
                "draw_pct": round((r["draws"] / tm) * 100, 2),
                "away_win_pct": round((r["away_wins"] / tm) * 100, 2),
                "over_25_pct": round((r["over_25_count"] / tm) * 100, 2),
                "btts_yes_pct": round((r["btts_yes_count"] / tm) * 100, 2),
                "avg_goals_match": round(r["total_goals"] / tm, 2),
                "avg_fouls_match": avg_fouls
            }
            self.cur.execute(insert_query, row)
            count += 1
        print(f"[HAKEM ANALİZİ] {count} hakem güncellendi.")

if __name__ == "__main__":
    a = RefereeAnalyzer(CONFIG["db"])
    try:
        a.connect()
        a.analyze_referees()
    finally:
        a.close()
