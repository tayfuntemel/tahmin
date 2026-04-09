#!/usr/bin/env python3
import os, json
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

SCHEMA_CREATE_ANALYTICS_TABLE = """
CREATE TABLE IF NOT EXISTS team_analytics (
  id              INT AUTO_INCREMENT PRIMARY KEY,
  team_name       VARCHAR(128) NOT NULL,
  tournament_id   INT NOT NULL,
  category_id     INT NULL,
  venue_type      ENUM('Overall', 'Home', 'Away') NOT NULL,
  matches_played  INT DEFAULT 0,
  wins            INT DEFAULT 0,
  draws           INT DEFAULT 0,
  losses          INT DEFAULT 0,
  goals_for       INT DEFAULT 0,
  goals_against   INT DEFAULT 0,
  avg_possession  FLOAT DEFAULT 0,
  avg_shots       FLOAT DEFAULT 0,
  avg_shots_on    FLOAT DEFAULT 0,
  avg_corners     FLOAT DEFAULT 0,
  avg_fouls       FLOAT DEFAULT 0,
  referee_stats   JSON NULL,
  formation_stats JSON NULL,
  odds_stats      JSON NULL,
  last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY idx_team_tour_venue (team_name, tournament_id, venue_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class Analyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.stats = {}

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_ANALYTICS_TABLE)

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _init_team_struct(self):
        return {
            "matches_played": 0, "wins": 0, "draws": 0, "losses": 0,
            "goals_for": 0, "goals_against": 0,
            "possession": 0, "shots": 0, "shots_on": 0, "corners": 0, "fouls": 0,
            "referees": {}, "formations": {}, "odds_o25_ranges": {}
        }

    def _get_team_node(self, team_name, tournament_id, category_id, venue_type):
        key = (team_name, tournament_id, category_id, venue_type)
        if key not in self.stats:
            self.stats[key] = self._init_team_struct()
        return self.stats[key]

    def _process_match_for_team(self, team_name, is_home, match, venue_type):
        tour_id = match['tournament_id']
        cat_id = match['category_id']
        node = self._get_team_node(team_name, tour_id, cat_id, venue_type)
        if is_home:
            gf, ga = match['ft_home'], match['ft_away']
            poss, shots = match['poss_h'], match['shot_h']
            shots_on, corners = match['shot_on_h'], match['corn_h']
            fouls = match['fouls_h']
            formation = match['formation_h']
        else:
            gf, ga = match['ft_away'], match['ft_home']
            poss, shots = match['poss_a'], match['shot_a']
            shots_on, corners = match['shot_on_a'], match['corn_a']
            fouls = match['fouls_a']
            formation = match['formation_a']

        node["matches_played"] += 1
        if gf is not None and ga is not None:
            node["goals_for"] += gf
            node["goals_against"] += ga
            if gf > ga: node["wins"] += 1
            elif gf == ga: node["draws"] += 1
            else: node["losses"] += 1
        if poss: node["possession"] += poss
        if shots: node["shots"] += shots
        if shots_on: node["shots_on"] += shots_on
        if corners: node["corners"] += corners
        if fouls: node["fouls"] += fouls

        referee = match.get('referee')
        if referee:
            if referee not in node["referees"]:
                node["referees"][referee] = {"matches":0, "wins":0, "draws":0, "losses":0, "gf":0, "ga":0}
            rn = node["referees"][referee]
            rn["matches"] += 1
            if gf is not None and ga is not None:
                rn["gf"] += gf
                rn["ga"] += ga
                if gf > ga: rn["wins"] += 1
                elif gf == ga: rn["draws"] += 1
                else: rn["losses"] += 1

        if formation:
            if formation not in node["formations"]:
                node["formations"][formation] = {"matches":0, "wins":0, "gf":0, "ga":0}
            fn = node["formations"][formation]
            fn["matches"] += 1
            if gf is not None and ga is not None:
                fn["gf"] += gf
                fn["ga"] += ga
                if gf > ga: fn["wins"] += 1

        o25 = match.get('odds_o25')
        if o25 and gf is not None and ga is not None:
            if o25 <= 1.50: rng = "<= 1.50"
            elif o25 <= 1.80: rng = "1.51 - 1.80"
            elif o25 <= 2.10: rng = "1.81 - 2.10"
            else: rng = "> 2.10"
            if rng not in node["odds_o25_ranges"]:
                node["odds_o25_ranges"][rng] = {"matches":0, "total_match_goals":0}
            node["odds_o25_ranges"][rng]["matches"] += 1
            node["odds_o25_ranges"][rng]["total_match_goals"] += (gf+ga)

    def analyze_and_save(self):
        self.cur.execute("SELECT * FROM results_football WHERE status IN ('finished','ended') AND home_team IS NOT NULL AND away_team IS NOT NULL AND tournament_id IS NOT NULL")
        matches = self.cur.fetchall()
        for match in matches:
            home, away = match['home_team'], match['away_team']
            self._process_match_for_team(home, True, match, "Home")
            self._process_match_for_team(home, True, match, "Overall")
            self._process_match_for_team(away, False, match, "Away")
            self._process_match_for_team(away, False, match, "Overall")

        insert_query = """
            INSERT INTO team_analytics 
            (team_name, tournament_id, category_id, venue_type, matches_played, wins, draws, losses, goals_for, goals_against,
             avg_possession, avg_shots, avg_shots_on, avg_corners, avg_fouls,
             referee_stats, formation_stats, odds_stats)
            VALUES (%(team_name)s, %(tournament_id)s, %(category_id)s, %(venue_type)s, %(matches_played)s, %(wins)s, %(draws)s, %(losses)s, %(goals_for)s, %(goals_against)s,
             %(avg_possession)s, %(avg_shots)s, %(avg_shots_on)s, %(avg_corners)s, %(avg_fouls)s,
             %(referee_stats)s, %(formation_stats)s, %(odds_stats)s)
            ON DUPLICATE KEY UPDATE
            matches_played=VALUES(matches_played), wins=VALUES(wins), draws=VALUES(draws), losses=VALUES(losses),
            goals_for=VALUES(goals_for), goals_against=VALUES(goals_against),
            avg_possession=VALUES(avg_possession), avg_shots=VALUES(avg_shots), avg_shots_on=VALUES(avg_shots_on),
            avg_corners=VALUES(avg_corners), avg_fouls=VALUES(avg_fouls),
            referee_stats=VALUES(referee_stats), formation_stats=VALUES(formation_stats), odds_stats=VALUES(odds_stats)
        """
        count = 0
        for key, data in self.stats.items():
            team_name, tour_id, cat_id, venue_type = key
            mp = data["matches_played"]
            if mp == 0: continue
            row = {
                "team_name": team_name, "tournament_id": tour_id, "category_id": cat_id, "venue_type": venue_type,
                "matches_played": mp, "wins": data["wins"], "draws": data["draws"], "losses": data["losses"],
                "goals_for": data["goals_for"], "goals_against": data["goals_against"],
                "avg_possession": round(data["possession"] / mp, 2),
                "avg_shots": round(data["shots"] / mp, 2),
                "avg_shots_on": round(data["shots_on"] / mp, 2),
                "avg_corners": round(data["corners"] / mp, 2),
                "avg_fouls": round(data["fouls"] / mp, 2),
                "referee_stats": json.dumps(data["referees"], ensure_ascii=False),
                "formation_stats": json.dumps(data["formations"], ensure_ascii=False),
                "odds_stats": json.dumps(data["odds_o25_ranges"], ensure_ascii=False)
            }
            self.cur.execute(insert_query, row)
            count += 1
        print(f"[TAKIM ANALİZİ] {count} kayıt güncellendi.")

if __name__ == "__main__":
    a = Analyzer(CONFIG["db"])
    try:
        a.connect()
        a.analyze_and_save()
    finally:
        a.close()
