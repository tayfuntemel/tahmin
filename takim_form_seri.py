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

SCHEMA_CREATE_FORM_TABLE = """
CREATE TABLE IF NOT EXISTS team_form_analytics (
  id                  INT AUTO_INCREMENT PRIMARY KEY,
  team_name           VARCHAR(128) NOT NULL,
  tournament_id       INT NOT NULL,
  venue_type          ENUM('Overall', 'Home', 'Away') NOT NULL,
  form_last_5         VARCHAR(32) DEFAULT '',
  points_last_5       INT DEFAULT 0,
  current_win_streak        INT DEFAULT 0,
  current_unbeaten_streak   INT DEFAULT 0,
  current_losing_streak     INT DEFAULT 0,
  current_no_win_streak     INT DEFAULT 0,
  current_clean_sheet_streak INT DEFAULT 0,
  current_scoring_streak     INT DEFAULT 0,
  current_over_25_streak     INT DEFAULT 0,
  last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY idx_team_tour_venue_form (team_name, tournament_id, venue_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class FormAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.stats = {}

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_FORM_TABLE)

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _init_team_struct(self):
        return {
            "form_queue": [],
            "win_streak": 0, "unbeaten_streak": 0, "losing_streak": 0, "no_win_streak": 0,
            "clean_sheet_streak": 0, "scoring_streak": 0, "over_25_streak": 0
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
            gf, ga = match['ft_home'], match['ft_away']
        else:
            gf, ga = match['ft_away'], match['ft_home']
        result = 'B'
        if gf > ga: result = 'G'
        elif gf < ga: result = 'M'

        node["form_queue"].append(result)
        if len(node["form_queue"]) > 5: node["form_queue"].pop(0)

        if result == 'G':
            node["win_streak"] += 1
            node["unbeaten_streak"] += 1
            node["losing_streak"] = 0
            node["no_win_streak"] = 0
        elif result == 'B':
            node["win_streak"] = 0
            node["unbeaten_streak"] += 1
            node["losing_streak"] = 0
            node["no_win_streak"] += 1
        else:
            node["win_streak"] = 0
            node["unbeaten_streak"] = 0
            node["losing_streak"] += 1
            node["no_win_streak"] += 1

        if ga == 0: node["clean_sheet_streak"] += 1
        else: node["clean_sheet_streak"] = 0
        if gf > 0: node["scoring_streak"] += 1
        else: node["scoring_streak"] = 0
        if (gf+ga) > 2.5: node["over_25_streak"] += 1
        else: node["over_25_streak"] = 0

    def analyze_form(self):
        self.cur.execute("SELECT * FROM results_football WHERE status IN ('finished','ended') AND ft_home IS NOT NULL AND ft_away IS NOT NULL ORDER BY start_utc ASC, start_time_utc ASC")
        matches = self.cur.fetchall()
        for match in matches:
            home, away = match['home_team'], match['away_team']
            if home and away:
                self._process_match(home, True, match, "Home")
                self._process_match(home, True, match, "Overall")
                self._process_match(away, False, match, "Away")
                self._process_match(away, False, match, "Overall")

        insert_query = """
            INSERT INTO team_form_analytics 
            (team_name, tournament_id, venue_type, form_last_5, points_last_5, 
             current_win_streak, current_unbeaten_streak, current_losing_streak, current_no_win_streak,
             current_clean_sheet_streak, current_scoring_streak, current_over_25_streak)
            VALUES (%(team_name)s, %(tournament_id)s, %(venue_type)s, %(form_last_5)s, %(points_last_5)s,
             %(win_streak)s, %(unbeaten_streak)s, %(losing_streak)s, %(no_win_streak)s,
             %(clean_sheet_streak)s, %(scoring_streak)s, %(over_25_streak)s)
            ON DUPLICATE KEY UPDATE
             form_last_5=VALUES(form_last_5), points_last_5=VALUES(points_last_5),
             current_win_streak=VALUES(current_win_streak), current_unbeaten_streak=VALUES(current_unbeaten_streak),
             current_losing_streak=VALUES(current_losing_streak), current_no_win_streak=VALUES(current_no_win_streak),
             current_clean_sheet_streak=VALUES(current_clean_sheet_streak), current_scoring_streak=VALUES(current_scoring_streak),
             current_over_25_streak=VALUES(current_over_25_streak)
        """
        count = 0
        for key, data in self.stats.items():
            team_name, tour_id, venue_type = key
            form_str = ",".join(data["form_queue"])
            pts = sum(3 if c=='G' else 1 if c=='B' else 0 for c in data["form_queue"])
            row = {
                "team_name": team_name, "tournament_id": tour_id, "venue_type": venue_type,
                "form_last_5": form_str, "points_last_5": pts,
                "win_streak": data["win_streak"], "unbeaten_streak": data["unbeaten_streak"],
                "losing_streak": data["losing_streak"], "no_win_streak": data["no_win_streak"],
                "clean_sheet_streak": data["clean_sheet_streak"], "scoring_streak": data["scoring_streak"],
                "over_25_streak": data["over_25_streak"]
            }
            self.cur.execute(insert_query, row)
            count += 1
        print(f"[FORM ANALİZİ] {count} takım güncellendi.")

if __name__ == "__main__":
    a = FormAnalyzer(CONFIG["db"])
    try:
        a.connect()
        a.analyze_form()
    finally:
        a.close()
