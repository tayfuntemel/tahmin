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

SCHEMA_CREATE_EFFICIENCY_TABLE = """
CREATE TABLE IF NOT EXISTS team_efficiency_analytics (
  id                  INT AUTO_INCREMENT PRIMARY KEY,
  team_name           VARCHAR(128) NOT NULL,
  tournament_id       INT NOT NULL,
  venue_type          ENUM('Overall', 'Home', 'Away') NOT NULL,
  matches_with_stats  INT DEFAULT 0,
  total_goals_scored  INT DEFAULT 0,
  total_goals_conceded INT DEFAULT 0,
  total_shots         INT DEFAULT 0,
  total_shots_on      INT DEFAULT 0,
  total_saves         INT DEFAULT 0,
  total_opp_shots_on  INT DEFAULT 0,
  avg_possession      FLOAT DEFAULT 0,
  avg_corners         FLOAT DEFAULT 0,
  avg_shots           FLOAT DEFAULT 0,
  shot_accuracy_pct   FLOAT DEFAULT 0,
  conversion_rate_pct FLOAT DEFAULT 0,
  save_rate_pct       FLOAT DEFAULT 0,
  pressure_index      FLOAT DEFAULT 0,
  last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY idx_team_tour_venue_eff (team_name, tournament_id, venue_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class EfficiencyAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.stats = {}

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_EFFICIENCY_TABLE)

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _init_team_struct(self):
        return {
            "matches": 0,
            "goals_scored": 0, "goals_conceded": 0,
            "shots": 0, "shots_on": 0,
            "saves": 0, "opp_shots_on": 0,
            "possession_sum": 0, "corners_sum": 0
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
            shots, shots_on = match['shot_h'], match['shot_on_h']
            saves = match['saves_h']
            opp_shots_on = match['shot_on_a']
            poss = match['poss_h']
            corners = match['corn_h']
        else:
            gf, ga = match['ft_away'], match['ft_home']
            shots, shots_on = match['shot_a'], match['shot_on_a']
            saves = match['saves_a']
            opp_shots_on = match['shot_on_h']
            poss = match['poss_a']
            corners = match['corn_a']
        if shots is None or poss is None:
            return
        node["matches"] += 1
        node["goals_scored"] += gf if gf else 0
        node["goals_conceded"] += ga if ga else 0
        node["shots"] += shots
        node["shots_on"] += shots_on if shots_on else 0
        node["saves"] += saves if saves else 0
        node["opp_shots_on"] += opp_shots_on if opp_shots_on else 0
        node["possession_sum"] += poss
        node["corners_sum"] += corners if corners else 0

    def analyze_efficiency(self):
        self.cur.execute("SELECT * FROM results_football WHERE status IN ('finished','ended')")
        matches = self.cur.fetchall()
        for match in matches:
            home, away = match['home_team'], match['away_team']
            if home and away:
                self._process_match(home, True, match, "Home")
                self._process_match(home, True, match, "Overall")
                self._process_match(away, False, match, "Away")
                self._process_match(away, False, match, "Overall")

        insert_query = """
            INSERT INTO team_efficiency_analytics 
            (team_name, tournament_id, venue_type, matches_with_stats, 
             total_goals_scored, total_goals_conceded, total_shots, total_shots_on, total_saves, total_opp_shots_on,
             avg_possession, avg_corners, avg_shots,
             shot_accuracy_pct, conversion_rate_pct, save_rate_pct, pressure_index)
            VALUES (%(team_name)s, %(tournament_id)s, %(venue_type)s, %(matches_with_stats)s,
             %(goals_scored)s, %(goals_conceded)s, %(shots)s, %(shots_on)s, %(saves)s, %(opp_shots_on)s,
             %(avg_possession)s, %(avg_corners)s, %(avg_shots)s,
             %(shot_accuracy_pct)s, %(conversion_rate_pct)s, %(save_rate_pct)s, %(pressure_index)s)
            ON DUPLICATE KEY UPDATE
             matches_with_stats=VALUES(matches_with_stats),
             total_goals_scored=VALUES(total_goals_scored), total_goals_conceded=VALUES(total_goals_conceded),
             total_shots=VALUES(total_shots), total_shots_on=VALUES(total_shots_on), 
             total_saves=VALUES(total_saves), total_opp_shots_on=VALUES(total_opp_shots_on),
             avg_possession=VALUES(avg_possession), avg_corners=VALUES(avg_corners), avg_shots=VALUES(avg_shots),
             shot_accuracy_pct=VALUES(shot_accuracy_pct), conversion_rate_pct=VALUES(conversion_rate_pct), 
             save_rate_pct=VALUES(save_rate_pct), pressure_index=VALUES(pressure_index)
        """
        count = 0
        for key, data in self.stats.items():
            team_name, tour_id, venue_type = key
            mp = data["matches"]
            if mp == 0: continue
            shot_acc = (data["shots_on"] / data["shots"] * 100) if data["shots"] > 0 else 0
            conversion = (data["goals_scored"] / data["shots_on"] * 100) if data["shots_on"] > 0 else 0
            conversion = min(conversion, 100.0)
            save_rate = (data["saves"] / data["opp_shots_on"] * 100) if data["opp_shots_on"] > 0 else 0
            save_rate = min(save_rate, 100.0)
            avg_poss = data["possession_sum"] / mp
            avg_shots = data["shots"] / mp
            avg_corners = data["corners_sum"] / mp
            pressure_idx = avg_poss + (avg_shots * 2) + (avg_corners * 3)
            row = {
                "team_name": team_name, "tournament_id": tour_id, "venue_type": venue_type, "matches_with_stats": mp,
                "goals_scored": data["goals_scored"], "goals_conceded": data["goals_conceded"],
                "shots": data["shots"], "shots_on": data["shots_on"], "saves": data["saves"], "opp_shots_on": data["opp_shots_on"],
                "avg_possession": round(avg_poss, 2), "avg_corners": round(avg_corners, 2), "avg_shots": round(avg_shots, 2),
                "shot_accuracy_pct": round(shot_acc, 2), "conversion_rate_pct": round(conversion, 2),
                "save_rate_pct": round(save_rate, 2), "pressure_index": round(pressure_idx, 2)
            }
            self.cur.execute(insert_query, row)
            count += 1
        print(f"[VERİMLİLİK ANALİZİ] {count} takım güncellendi.")

if __name__ == "__main__":
    a = EfficiencyAnalyzer(CONFIG["db"])
    try:
        a.connect()
        a.analyze_efficiency()
    finally:
        a.close()
