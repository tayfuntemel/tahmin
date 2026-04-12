#!/usr/bin/env python3
import os
import json
import mysql.connector

# ==========================================
# 1. VERİTABANI KONFİGÜRASYONU
# ==========================================
CONFIG = {
    "db": {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306))
    }
}

# ==========================================
# 2. VERİTABANI TABLO ŞEMALARI (SCHEMAS)
# ==========================================
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

SCHEMA_CREATE_HT_TABLE = """
CREATE TABLE IF NOT EXISTS team_half_time_analytics (
  id                  INT AUTO_INCREMENT PRIMARY KEY,
  team_name           VARCHAR(128) NOT NULL,
  tournament_id       INT NOT NULL,
  venue_type          ENUM('Overall', 'Home', 'Away') NOT NULL,
  matches_played      INT DEFAULT 0,
  ht_wins             INT DEFAULT 0,
  ht_draws            INT DEFAULT 0,
  ht_losses           INT DEFAULT 0,
  ht_goals_for        INT DEFAULT 0,
  ht_goals_against    INT DEFAULT 0,
  ht_avg_goals_for    FLOAT DEFAULT 0,
  ht_avg_goals_against FLOAT DEFAULT 0,
  ht_over_05_pct      FLOAT DEFAULT 0,
  ht_over_15_pct      FLOAT DEFAULT 0,
  ht_btts_yes_pct     FLOAT DEFAULT 0,
  ht_win_ft_win       INT DEFAULT 0,
  ht_win_ft_not_win   INT DEFAULT 0,
  ht_lose_ft_win      INT DEFAULT 0,
  ht_lose_ft_draw     INT DEFAULT 0,
  last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY idx_team_tour_venue (team_name, tournament_id, venue_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

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

SCHEMA_CREATE_LEAGUE_ANALYTICS = """
CREATE TABLE IF NOT EXISTS league_analytics (
  tournament_id     INT PRIMARY KEY,
  tournament_name   VARCHAR(128) NULL,
  category_name     VARCHAR(128) NULL,
  country           VARCHAR(64) NULL,
  total_matches     INT DEFAULT 0,
  home_wins         INT DEFAULT 0,
  draws             INT DEFAULT 0,
  away_wins         INT DEFAULT 0,
  home_win_pct      FLOAT DEFAULT 0,
  draw_pct          FLOAT DEFAULT 0,
  away_win_pct      FLOAT DEFAULT 0,
  over_25_pct       FLOAT DEFAULT 0,
  under_25_pct      FLOAT DEFAULT 0,
  btts_yes_pct      FLOAT DEFAULT 0,
  avg_goals_match   FLOAT DEFAULT 0,
  avg_goals_home    FLOAT DEFAULT 0,
  avg_goals_away    FLOAT DEFAULT 0,
  avg_odds_1        FLOAT DEFAULT 0,
  avg_odds_x        FLOAT DEFAULT 0,
  avg_odds_2        FLOAT DEFAULT 0,
  avg_odds_o25      FLOAT DEFAULT 0,
  avg_odds_u25      FLOAT DEFAULT 0,
  avg_odds_btts_yes FLOAT DEFAULT 0,
  last_updated      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

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

# ==========================================
# 3. ANALİZ SINIFLARI (CLASSES)
# ==========================================

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
            "matches": 0, "goals_scored": 0, "goals_conceded": 0,
            "shots": 0, "shots_on": 0, "saves": 0, "opp_shots_on": 0,
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

    def analyze(self):
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


class TeamGeneralAnalyzer:
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

    def analyze(self):
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
        print(f"[GENEL TAKIM ANALİZİ] {count} kayıt güncellendi.")


class HalfTimeAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.stats = {}

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_HT_TABLE)

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _init_team_struct(self):
        return {
            "matches": 0, "ht_wins": 0, "ht_draws": 0, "ht_losses": 0,
            "ht_goals_for": 0, "ht_goals_against": 0,
            "ht_over_05": 0, "ht_over_15": 0, "ht_btts": 0,
            "ht_win_ft_win": 0, "ht_win_ft_not_win": 0,
            "ht_lose_ft_win": 0, "ht_lose_ft_draw": 0
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
            ht_gf, ht_ga = match['ht_home'], match['ht_away']
            ft_gf, ft_ga = match['ft_home'], match['ft_away']
        else:
            ht_gf, ht_ga = match['ht_away'], match['ht_home']
            ft_gf, ft_ga = match['ft_away'], match['ft_home']
        node["matches"] += 1
        node["ht_goals_for"] += ht_gf
        node["ht_goals_against"] += ht_ga
        if ht_gf > ht_ga:
            node["ht_wins"] += 1
            if ft_gf > ft_ga: node["ht_win_ft_win"] += 1
            else: node["ht_win_ft_not_win"] += 1
        elif ht_gf == ht_ga:
            node["ht_draws"] += 1
        else:
            node["ht_losses"] += 1
            if ft_gf > ft_ga: node["ht_lose_ft_win"] += 1
            elif ft_gf == ft_ga: node["ht_lose_ft_draw"] += 1
        total_ht = ht_gf + ht_ga
        if total_ht > 0.5: node["ht_over_05"] += 1
        if total_ht > 1.5: node["ht_over_15"] += 1
        if ht_gf > 0 and ht_ga > 0: node["ht_btts"] += 1

    def analyze(self):
        self.cur.execute("SELECT * FROM results_football WHERE status IN ('finished','ended') AND ht_home IS NOT NULL AND ht_away IS NOT NULL AND ft_home IS NOT NULL AND ft_away IS NOT NULL")
        matches = self.cur.fetchall()
        for match in matches:
            home, away = match['home_team'], match['away_team']
            self._process_match(home, True, match, "Home")
            self._process_match(home, True, match, "Overall")
            self._process_match(away, False, match, "Away")
            self._process_match(away, False, match, "Overall")

        insert_query = """
            INSERT INTO team_half_time_analytics 
            (team_name, tournament_id, venue_type, matches_played, 
             ht_wins, ht_draws, ht_losses, ht_goals_for, ht_goals_against, 
             ht_avg_goals_for, ht_avg_goals_against, 
             ht_over_05_pct, ht_over_15_pct, ht_btts_yes_pct, 
             ht_win_ft_win, ht_win_ft_not_win, ht_lose_ft_win, ht_lose_ft_draw)
            VALUES (%(team_name)s, %(tournament_id)s, %(venue_type)s, %(matches_played)s,
             %(ht_wins)s, %(ht_draws)s, %(ht_losses)s, %(ht_goals_for)s, %(ht_goals_against)s,
             %(ht_avg_goals_for)s, %(ht_avg_goals_against)s,
             %(ht_over_05_pct)s, %(ht_over_15_pct)s, %(ht_btts_yes_pct)s,
             %(ht_win_ft_win)s, %(ht_win_ft_not_win)s, %(ht_lose_ft_win)s, %(ht_lose_ft_draw)s)
            ON DUPLICATE KEY UPDATE
             matches_played=VALUES(matches_played),
             ht_wins=VALUES(ht_wins), ht_draws=VALUES(ht_draws), ht_losses=VALUES(ht_losses),
             ht_goals_for=VALUES(ht_goals_for), ht_goals_against=VALUES(ht_goals_against),
             ht_avg_goals_for=VALUES(ht_avg_goals_for), ht_avg_goals_against=VALUES(ht_avg_goals_against),
             ht_over_05_pct=VALUES(ht_over_05_pct), ht_over_15_pct=VALUES(ht_over_15_pct), ht_btts_yes_pct=VALUES(ht_btts_yes_pct),
             ht_win_ft_win=VALUES(ht_win_ft_win), ht_win_ft_not_win=VALUES(ht_win_ft_not_win), 
             ht_lose_ft_win=VALUES(ht_lose_ft_win), ht_lose_ft_draw=VALUES(ht_lose_ft_draw)
        """
        count = 0
        for key, data in self.stats.items():
            team_name, tour_id, venue_type = key
            mp = data["matches"]
            if mp == 0: continue
            row = {
                "team_name": team_name, "tournament_id": tour_id, "venue_type": venue_type, "matches_played": mp,
                "ht_wins": data["ht_wins"], "ht_draws": data["ht_draws"], "ht_losses": data["ht_losses"],
                "ht_goals_for": data["ht_goals_for"], "ht_goals_against": data["ht_goals_against"],
                "ht_avg_goals_for": round(data["ht_goals_for"] / mp, 2),
                "ht_avg_goals_against": round(data["ht_goals_against"] / mp, 2),
                "ht_over_05_pct": round((data["ht_over_05"] / mp) * 100, 2),
                "ht_over_15_pct": round((data["ht_over_15"] / mp) * 100, 2),
                "ht_btts_yes_pct": round((data["ht_btts"] / mp) * 100, 2),
                "ht_win_ft_win": data["ht_win_ft_win"], "ht_win_ft_not_win": data["ht_win_ft_not_win"],
                "ht_lose_ft_win": data["ht_lose_ft_win"], "ht_lose_ft_draw": data["ht_lose_ft_draw"]
            }
            self.cur.execute(insert_query, row)
            count += 1
        print(f"[İLK YARI ANALİZİ] {count} takım güncellendi.")


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

    def analyze(self):
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

    def analyze(self):
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


class LeagueAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.leagues = {}

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_LEAGUE_ANALYTICS)

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _init_league_struct(self, match):
        return {
            "tournament_name": match.get("tournament_name", "Bilinmiyor"),
            "category_name": match.get("category_name", "Bilinmiyor"),
            "country": match.get("country", "Bilinmiyor"),
            "matches": 0, "home_wins": 0, "draws": 0, "away_wins": 0,
            "goals_home": 0, "goals_away": 0, "total_goals": 0,
            "over_25_count": 0, "under_25_count": 0, "btts_yes_count": 0,
            "sum_odds_1": 0, "count_odds_1": 0,
            "sum_odds_x": 0, "count_odds_x": 0,
            "sum_odds_2": 0, "count_odds_2": 0,
            "sum_odds_o25": 0, "count_odds_o25": 0,
            "sum_odds_u25": 0, "count_odds_u25": 0,
            "sum_odds_btts_yes": 0, "count_odds_btts_yes": 0
        }

    def analyze(self):
        self.cur.execute("SELECT * FROM results_football WHERE status IN ('finished','ended') AND tournament_id IS NOT NULL")
        matches = self.cur.fetchall()
        for match in matches:
            t_id = match['tournament_id']
            if t_id not in self.leagues:
                self.leagues[t_id] = self._init_league_struct(match)
            lg = self.leagues[t_id]
            lg["matches"] += 1
            gh, ga = match.get('ft_home'), match.get('ft_away')
            if gh is not None and ga is not None:
                lg["goals_home"] += gh
                lg["goals_away"] += ga
                total = gh + ga
                lg["total_goals"] += total
                if gh > ga: lg["home_wins"] += 1
                elif gh == ga: lg["draws"] += 1
                else: lg["away_wins"] += 1
                if total > 2.5: lg["over_25_count"] += 1
                else: lg["under_25_count"] += 1
                if gh > 0 and ga > 0: lg["btts_yes_count"] += 1
            def add_odds(field, sum_key, cnt_key):
                val = match.get(field)
                if val:
                    lg[sum_key] += val
                    lg[cnt_key] += 1
            add_odds('odds_1', 'sum_odds_1', 'count_odds_1')
            add_odds('odds_x', 'sum_odds_x', 'count_odds_x')
            add_odds('odds_2', 'sum_odds_2', 'count_odds_2')
            add_odds('odds_o25', 'sum_odds_o25', 'count_odds_o25')
            add_odds('odds_u25', 'sum_odds_u25', 'count_odds_u25')
            add_odds('odds_btts_yes', 'sum_odds_btts_yes', 'count_odds_btts_yes')

        insert_query = """
            INSERT INTO league_analytics 
            (tournament_id, tournament_name, category_name, country, total_matches, 
             home_wins, draws, away_wins, home_win_pct, draw_pct, away_win_pct, 
             over_25_pct, under_25_pct, btts_yes_pct, avg_goals_match, avg_goals_home, avg_goals_away, 
             avg_odds_1, avg_odds_x, avg_odds_2, avg_odds_o25, avg_odds_u25, avg_odds_btts_yes)
            VALUES (%(tournament_id)s, %(tournament_name)s, %(category_name)s, %(country)s, %(total_matches)s,
             %(home_wins)s, %(draws)s, %(away_wins)s, %(home_win_pct)s, %(draw_pct)s, %(away_win_pct)s,
             %(over_25_pct)s, %(under_25_pct)s, %(btts_yes_pct)s, %(avg_goals_match)s, %(avg_goals_home)s, %(avg_goals_away)s,
             %(avg_odds_1)s, %(avg_odds_x)s, %(avg_odds_2)s, %(avg_odds_o25)s, %(avg_odds_u25)s, %(avg_odds_btts_yes)s)
            ON DUPLICATE KEY UPDATE
             total_matches=VALUES(total_matches), home_wins=VALUES(home_wins), draws=VALUES(draws), away_wins=VALUES(away_wins),
             home_win_pct=VALUES(home_win_pct), draw_pct=VALUES(draw_pct), away_win_pct=VALUES(away_win_pct),
             over_25_pct=VALUES(over_25_pct), under_25_pct=VALUES(under_25_pct), btts_yes_pct=VALUES(btts_yes_pct),
             avg_goals_match=VALUES(avg_goals_match), avg_goals_home=VALUES(avg_goals_home), avg_goals_away=VALUES(avg_goals_away),
             avg_odds_1=VALUES(avg_odds_1), avg_odds_x=VALUES(avg_odds_x), avg_odds_2=VALUES(avg_odds_2),
             avg_odds_o25=VALUES(avg_odds_o25), avg_odds_u25=VALUES(avg_odds_u25), avg_odds_btts_yes=VALUES(avg_odds_btts_yes)
        """
        count = 0
        for t_id, lg in self.leagues.items():
            tm = lg["matches"]
            if tm == 0: continue
            row = {
                "tournament_id": t_id,
                "tournament_name": lg["tournament_name"],
                "category_name": lg["category_name"],
                "country": lg["country"],
                "total_matches": tm,
                "home_wins": lg["home_wins"],
                "draws": lg["draws"],
                "away_wins": lg["away_wins"],
                "home_win_pct": round((lg["home_wins"] / tm) * 100, 2),
                "draw_pct": round((lg["draws"] / tm) * 100, 2),
                "away_win_pct": round((lg["away_wins"] / tm) * 100, 2),
                "over_25_pct": round((lg["over_25_count"] / tm) * 100, 2),
                "under_25_pct": round((lg["under_25_count"] / tm) * 100, 2),
                "btts_yes_pct": round((lg["btts_yes_count"] / tm) * 100, 2),
                "avg_goals_match": round(lg["total_goals"] / tm, 2),
                "avg_goals_home": round(lg["goals_home"] / tm, 2),
                "avg_goals_away": round(lg["goals_away"] / tm, 2),
                "avg_odds_1": round(lg["sum_odds_1"] / lg["count_odds_1"], 2) if lg["count_odds_1"] > 0 else 0,
                "avg_odds_x": round(lg["sum_odds_x"] / lg["count_odds_x"], 2) if lg["count_odds_x"] > 0 else 0,
                "avg_odds_2": round(lg["sum_odds_2"] / lg["count_odds_2"], 2) if lg["count_odds_2"] > 0 else 0,
                "avg_odds_o25": round(lg["sum_odds_o25"] / lg["count_odds_o25"], 2) if lg["count_odds_o25"] > 0 else 0,
                "avg_odds_u25": round(lg["sum_odds_u25"] / lg["count_odds_u25"], 2) if lg["count_odds_u25"] > 0 else 0,
                "avg_odds_btts_yes": round(lg["sum_odds_btts_yes"] / lg["count_odds_btts_yes"], 2) if lg["count_odds_btts_yes"] > 0 else 0
            }
            self.cur.execute(insert_query, row)
            count += 1
        print(f"[LİG ANALİZİ] {count} lig güncellendi.")


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

    def analyze(self):
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

# ==========================================
# 4. UYGULAMAYI ÇALIŞTIRMA (MAIN RUNNER)
# ==========================================

if __name__ == "__main__":
    # Tüm sınıfları bir listede topluyoruz.
    analyzers_to_run = [
        (EfficiencyAnalyzer(CONFIG["db"]), "Verimlilik Analizi"),
        (TeamGeneralAnalyzer(CONFIG["db"]), "Genel Takım Analizi"),
        (HalfTimeAnalyzer(CONFIG["db"]), "İlk Yarı Analizi"),
        (SecondHalfAnalyzer(CONFIG["db"]), "İkinci Yarı Analizi"),
        (FormAnalyzer(CONFIG["db"]), "Form ve Seri Analizi"),
        (LeagueAnalyzer(CONFIG["db"]), "Lig Analizi"),
        (RefereeAnalyzer(CONFIG["db"]), "Hakem Analizi")
    ]

    print("İstatistik analizi başlıyor... Lütfen bekleyin.")
    
    for analyzer, name in analyzers_to_run:
        try:
            analyzer.connect()
            analyzer.analyze()
        except Exception as e:
            print(f"HATA OLUŞTU [{name}]: {e}")
        finally:
            analyzer.close()

    print("\n[BAŞARILI] Tüm istatistik analizleri eksiksiz olarak tamamlandı!")
