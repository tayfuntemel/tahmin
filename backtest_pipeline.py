#!/usr/bin/env python3
"""
backtest_pipeline.py - Tam ve hatasız backtest pipeline'ı.
Veri sızıntısı önlenir: Her gün için istatistikler T-1 tarihine kadar hesaplanır.
MySQL NULL/0 hatası (Numpy Float Type Error) giderilmiştir.
"""

import os
import sys
import json
import argparse
import mysql.connector
import pandas as pd
import numpy as np
import joblib
import time
from datetime import datetime, timedelta, timezone
from sklearn.preprocessing import StandardScaler
from sklearn.calibration import CalibratedClassifierCV
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
import xgboost as xgb
from scipy.stats import uniform, randint
import warnings
warnings.filterwarnings("ignore")

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

MODEL_DIR = "models"
os.makedirs(MODEL_DIR, exist_ok=True)

# Eşik değerleri
THRESHOLD_O15 = 0.62
THRESHOLD_O25 = 0.52
THRESHOLD_O35 = 0.42

USE_HYPEROPT = True

# ==========================================
# 2. TABLO ŞEMALARI
# ==========================================
SCHEMAS = {
    "team_efficiency_analytics": """
    CREATE TABLE IF NOT EXISTS team_efficiency_analytics (
      id INT AUTO_INCREMENT PRIMARY KEY,
      team_name VARCHAR(128) NOT NULL,
      tournament_id INT NOT NULL,
      venue_type ENUM('Overall', 'Home', 'Away') NOT NULL,
      matches_with_stats INT DEFAULT 0,
      total_goals_scored INT DEFAULT 0,
      total_goals_conceded INT DEFAULT 0,
      total_shots INT DEFAULT 0,
      total_shots_on INT DEFAULT 0,
      total_saves INT DEFAULT 0,
      total_opp_shots_on INT DEFAULT 0,
      avg_possession FLOAT DEFAULT 0,
      avg_corners FLOAT DEFAULT 0,
      avg_shots FLOAT DEFAULT 0,
      shot_accuracy_pct FLOAT DEFAULT 0,
      conversion_rate_pct FLOAT DEFAULT 0,
      save_rate_pct FLOAT DEFAULT 0,
      pressure_index FLOAT DEFAULT 0,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY idx_team_tour_venue_eff (team_name, tournament_id, venue_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "team_analytics": """
    CREATE TABLE IF NOT EXISTS team_analytics (
      id INT AUTO_INCREMENT PRIMARY KEY,
      team_name VARCHAR(128) NOT NULL,
      tournament_id INT NOT NULL,
      category_id INT NULL,
      venue_type ENUM('Overall', 'Home', 'Away') NOT NULL,
      matches_played INT DEFAULT 0,
      wins INT DEFAULT 0,
      draws INT DEFAULT 0,
      losses INT DEFAULT 0,
      goals_for INT DEFAULT 0,
      goals_against INT DEFAULT 0,
      avg_possession FLOAT DEFAULT 0,
      avg_shots FLOAT DEFAULT 0,
      avg_shots_on FLOAT DEFAULT 0,
      avg_corners FLOAT DEFAULT 0,
      avg_fouls FLOAT DEFAULT 0,
      referee_stats JSON NULL,
      formation_stats JSON NULL,
      odds_stats JSON NULL,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY idx_team_tour_venue (team_name, tournament_id, venue_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "team_half_time_analytics": """
    CREATE TABLE IF NOT EXISTS team_half_time_analytics (
      id INT AUTO_INCREMENT PRIMARY KEY,
      team_name VARCHAR(128) NOT NULL,
      tournament_id INT NOT NULL,
      venue_type ENUM('Overall', 'Home', 'Away') NOT NULL,
      matches_played INT DEFAULT 0,
      ht_wins INT DEFAULT 0,
      ht_draws INT DEFAULT 0,
      ht_losses INT DEFAULT 0,
      ht_goals_for INT DEFAULT 0,
      ht_goals_against INT DEFAULT 0,
      ht_avg_goals_for FLOAT DEFAULT 0,
      ht_avg_goals_against FLOAT DEFAULT 0,
      ht_over_05_pct FLOAT DEFAULT 0,
      ht_over_15_pct FLOAT DEFAULT 0,
      ht_btts_yes_pct FLOAT DEFAULT 0,
      ht_win_ft_win INT DEFAULT 0,
      ht_win_ft_not_win INT DEFAULT 0,
      ht_lose_ft_win INT DEFAULT 0,
      ht_lose_ft_draw INT DEFAULT 0,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY idx_team_tour_venue (team_name, tournament_id, venue_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "team_second_half_analytics": """
    CREATE TABLE IF NOT EXISTS team_second_half_analytics (
      id INT AUTO_INCREMENT PRIMARY KEY,
      team_name VARCHAR(128) NOT NULL,
      tournament_id INT NOT NULL,
      venue_type ENUM('Overall', 'Home', 'Away') NOT NULL,
      matches_played INT DEFAULT 0,
      sh_wins INT DEFAULT 0,
      sh_draws INT DEFAULT 0,
      sh_losses INT DEFAULT 0,
      sh_goals_for INT DEFAULT 0,
      sh_goals_against INT DEFAULT 0,
      sh_avg_goals_for FLOAT DEFAULT 0,
      sh_avg_goals_against FLOAT DEFAULT 0,
      sh_over_05_pct FLOAT DEFAULT 0,
      sh_over_15_pct FLOAT DEFAULT 0,
      sh_btts_yes_pct FLOAT DEFAULT 0,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY idx_team_tour_venue_sh (team_name, tournament_id, venue_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "team_form_analytics": """
    CREATE TABLE IF NOT EXISTS team_form_analytics (
      id INT AUTO_INCREMENT PRIMARY KEY,
      team_name VARCHAR(128) NOT NULL,
      tournament_id INT NOT NULL,
      venue_type ENUM('Overall', 'Home', 'Away') NOT NULL,
      form_last_5 VARCHAR(32) DEFAULT '',
      points_last_5 INT DEFAULT 0,
      current_win_streak INT DEFAULT 0,
      current_unbeaten_streak INT DEFAULT 0,
      current_losing_streak INT DEFAULT 0,
      current_no_win_streak INT DEFAULT 0,
      current_clean_sheet_streak INT DEFAULT 0,
      current_scoring_streak INT DEFAULT 0,
      current_over_25_streak INT DEFAULT 0,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY idx_team_tour_venue_form (team_name, tournament_id, venue_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "league_analytics": """
    CREATE TABLE IF NOT EXISTS league_analytics (
      tournament_id INT PRIMARY KEY,
      tournament_name VARCHAR(128) NULL,
      category_name VARCHAR(128) NULL,
      country VARCHAR(64) NULL,
      total_matches INT DEFAULT 0,
      home_wins INT DEFAULT 0,
      draws INT DEFAULT 0,
      away_wins INT DEFAULT 0,
      home_win_pct FLOAT DEFAULT 0,
      draw_pct FLOAT DEFAULT 0,
      away_win_pct FLOAT DEFAULT 0,
      over_25_pct FLOAT DEFAULT 0,
      under_25_pct FLOAT DEFAULT 0,
      btts_yes_pct FLOAT DEFAULT 0,
      avg_goals_match FLOAT DEFAULT 0,
      avg_goals_home FLOAT DEFAULT 0,
      avg_goals_away FLOAT DEFAULT 0,
      avg_odds_1 FLOAT DEFAULT 0,
      avg_odds_x FLOAT DEFAULT 0,
      avg_odds_2 FLOAT DEFAULT 0,
      avg_odds_o25 FLOAT DEFAULT 0,
      avg_odds_u25 FLOAT DEFAULT 0,
      avg_odds_btts_yes FLOAT DEFAULT 0,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "referee_analytics": """
    CREATE TABLE IF NOT EXISTS referee_analytics (
      referee_name VARCHAR(128) PRIMARY KEY,
      total_matches INT DEFAULT 0,
      home_wins INT DEFAULT 0,
      draws INT DEFAULT 0,
      away_wins INT DEFAULT 0,
      home_win_pct FLOAT DEFAULT 0,
      draw_pct FLOAT DEFAULT 0,
      away_win_pct FLOAT DEFAULT 0,
      over_25_pct FLOAT DEFAULT 0,
      btts_yes_pct FLOAT DEFAULT 0,
      avg_goals_match FLOAT DEFAULT 0,
      avg_fouls_match FLOAT DEFAULT 0,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "match_predictions": """
    CREATE TABLE IF NOT EXISTS `match_predictions` (
      `id` INT AUTO_INCREMENT PRIMARY KEY,
      `event_id` BIGINT UNSIGNED NOT NULL,
      `predicted_market` VARCHAR(10) NULL,
      `probability` FLOAT NULL,
      `prob_o15` FLOAT NULL,
      `prob_o25` FLOAT NULL,
      `prob_o35` FLOAT NULL,
      `actual_result` VARCHAR(10) NULL,
      `is_correct` BOOLEAN NULL,
      `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY `unique_event` (`event_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
}

class Database:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None

    def connect(self):
        try:
            if self.conn:
                self.close()
            self.conn = mysql.connector.connect(**self.cfg)
            self.conn.autocommit = True
            self.cur = self.conn.cursor(dictionary=True)
        except mysql.connector.Error as err:
            print(f"    [HATA] Veritabanı bağlantı hatası: {err}")
            time.sleep(5) 
            self.connect()

    def check_connection(self):
        try:
            if self.conn and self.conn.is_connected():
                self.conn.ping(reconnect=True, attempts=3, delay=2)
                self.cur = self.conn.cursor(dictionary=True)
            else:
                self.connect()
        except Exception as e:
            print(f"    [UYARI] Bağlantı yenilenirken hata: {e}. Yeniden bağlanılıyor...")
            self.connect()

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def create_all_tables(self):
        self.check_connection()
        for table, schema in SCHEMAS.items():
            self.cur.execute(schema)

    def truncate_analytics_tables(self):
        self.check_connection()
        tables = [
            "team_efficiency_analytics", "team_analytics", "team_half_time_analytics",
            "team_second_half_analytics", "team_form_analytics", "league_analytics", "referee_analytics"
        ]
        for t in tables:
            self.cur.execute(f"TRUNCATE TABLE {t}")

    def get_matches_finished(self, max_date=None):
        self.check_connection()
        query = """
            SELECT * FROM results_football
            WHERE status IN ('finished','ended')
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
        """
        if max_date:
            query += f" AND start_utc < '{max_date}'"
        query += " ORDER BY start_utc ASC, start_time_utc ASC"
        self.cur.execute(query)
        return self.cur.fetchall()

# ==========================================
# 3. TAM ANALİZ SINIFLARI 
# ==========================================
class EfficiencyAnalyzer:
    def __init__(self, db):
        self.db = db
        self.stats = {}

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

    def analyze(self, matches):
        for match in matches:
            home, away = match['home_team'], match['away_team']
            if not home or not away:
                continue
            for is_home, t_name in [(True, home), (False, away)]:
                if is_home:
                    gf = match['ft_home']
                    ga = match['ft_away']
                    shots = match['shot_h']
                    shots_on = match['shot_on_h']
                    saves = match['saves_h']
                    opp_shots_on = match['shot_on_a']
                    poss = match['poss_h']
                    corners = match['corn_h']
                else:
                    gf = match['ft_away']
                    ga = match['ft_home']
                    shots = match['shot_a']
                    shots_on = match['shot_on_a']
                    saves = match['saves_a']
                    opp_shots_on = match['shot_on_h']
                    poss = match['poss_a']
                    corners = match['corn_a']
                if shots is None or poss is None:
                    continue
                for venue in ["Overall", "Home" if is_home else "Away"]:
                    node = self._get_team_node(t_name, match['tournament_id'], venue)
                    node["matches"] += 1
                    node["goals_scored"] += gf if gf else 0
                    node["goals_conceded"] += ga if ga else 0
                    node["shots"] += shots if shots else 0
                    node["shots_on"] += shots_on if shots_on else 0
                    node["saves"] += saves if saves else 0
                    node["opp_shots_on"] += opp_shots_on if opp_shots_on else 0
                    node["possession_sum"] += poss if poss else 0
                    node["corners_sum"] += corners if corners else 0

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
        
        self.db.check_connection()
        for key, data in self.stats.items():
            mp = data["matches"]
            if mp == 0:
                continue
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
                "team_name": key[0], "tournament_id": key[1], "venue_type": key[2], "matches_with_stats": mp,
                "goals_scored": data["goals_scored"], "goals_conceded": data["goals_conceded"],
                "shots": data["shots"], "shots_on": data["shots_on"], "saves": data["saves"], "opp_shots_on": data["opp_shots_on"],
                "avg_possession": round(avg_poss, 2), "avg_corners": round(avg_corners, 2), "avg_shots": round(avg_shots, 2),
                "shot_accuracy_pct": round(shot_acc, 2), "conversion_rate_pct": round(conversion, 2),
                "save_rate_pct": round(save_rate, 2), "pressure_index": round(pressure_idx, 2)
            }
            self.db.cur.execute(insert_query, row)

class TeamGeneralAnalyzer:
    def __init__(self, db):
        self.db = db
        self.stats = {}

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

    def analyze(self, matches):
        for match in matches:
            home, away = match['home_team'], match['away_team']
            if not home or not away:
                continue
            for is_home, t_name in [(True, home), (False, away)]:
                if is_home:
                    gf = match['ft_home']
                    ga = match['ft_away']
                    poss = match['poss_h']
                    shots = match['shot_h']
                    shots_on = match['shot_on_h']
                    corners = match['corn_h']
                    fouls = match['fouls_h']
                else:
                    gf = match['ft_away']
                    ga = match['ft_home']
                    poss = match['poss_a']
                    shots = match['shot_a']
                    shots_on = match['shot_on_a']
                    corners = match['corn_a']
                    fouls = match['fouls_a']
                for venue in ["Overall", "Home" if is_home else "Away"]:
                    node = self._get_team_node(t_name, match['tournament_id'], match['category_id'], venue)
                    node["matches_played"] += 1
                    if gf is not None and ga is not None:
                        node["goals_for"] += gf
                        node["goals_against"] += ga
                        if gf > ga:
                            node["wins"] += 1
                        elif gf == ga:
                            node["draws"] += 1
                        else:
                            node["losses"] += 1
                    if poss: node["possession"] += poss
                    if shots: node["shots"] += shots
                    if shots_on: node["shots_on"] += shots_on
                    if corners: node["corners"] += corners
                    if fouls: node["fouls"] += fouls

        insert_query = """
            INSERT INTO team_analytics 
            (team_name, tournament_id, category_id, venue_type, matches_played, wins, draws, losses, goals_for, goals_against,
             avg_possession, avg_shots, avg_shots_on, avg_corners, avg_fouls)
            VALUES (%(team_name)s, %(tournament_id)s, %(category_id)s, %(venue_type)s, %(matches_played)s, %(wins)s, %(draws)s, %(losses)s, %(goals_for)s, %(goals_against)s,
             %(avg_possession)s, %(avg_shots)s, %(avg_shots_on)s, %(avg_corners)s, %(avg_fouls)s)
            ON DUPLICATE KEY UPDATE
             matches_played=VALUES(matches_played), wins=VALUES(wins), draws=VALUES(draws), losses=VALUES(losses),
             goals_for=VALUES(goals_for), goals_against=VALUES(goals_against),
             avg_possession=VALUES(avg_possession), avg_shots=VALUES(avg_shots), avg_shots_on=VALUES(avg_shots_on),
             avg_corners=VALUES(avg_corners), avg_fouls=VALUES(avg_fouls)
        """
        
        self.db.check_connection()
        for key, data in self.stats.items():
            mp = data["matches_played"]
            if mp == 0:
                continue
            row = {
                "team_name": key[0], "tournament_id": key[1], "category_id": key[2], "venue_type": key[3],
                "matches_played": mp, "wins": data["wins"], "draws": data["draws"], "losses": data["losses"],
                "goals_for": data["goals_for"], "goals_against": data["goals_against"],
                "avg_possession": round(data["possession"] / mp, 2) if mp else 0,
                "avg_shots": round(data["shots"] / mp, 2) if mp else 0,
                "avg_shots_on": round(data["shots_on"] / mp, 2) if mp else 0,
                "avg_corners": round(data["corners"] / mp, 2) if mp else 0,
                "avg_fouls": round(data["fouls"] / mp, 2) if mp else 0,
            }
            self.db.cur.execute(insert_query, row)

class HalfTimeAnalyzer:
    def __init__(self, db):
        self.db = db
        self.stats = {}

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

    def analyze(self, matches):
        for match in matches:
            if match['ht_home'] is None or match['ht_away'] is None:
                continue
            home, away = match['home_team'], match['away_team']
            for is_home, t_name in [(True, home), (False, away)]:
                if is_home:
                    ht_gf = match['ht_home']
                    ht_ga = match['ht_away']
                    ft_gf = match['ft_home']
                    ft_ga = match['ft_away']
                else:
                    ht_gf = match['ht_away']
                    ht_ga = match['ht_home']
                    ft_gf = match['ft_away']
                    ft_ga = match['ft_home']
                for venue in ["Overall", "Home" if is_home else "Away"]:
                    node = self._get_team_node(t_name, match['tournament_id'], venue)
                    node["matches"] += 1
                    node["ht_goals_for"] += ht_gf
                    node["ht_goals_against"] += ht_ga
                    if ht_gf > ht_ga:
                        node["ht_wins"] += 1
                        if ft_gf > ft_ga:
                            node["ht_win_ft_win"] += 1
                        else:
                            node["ht_win_ft_not_win"] += 1
                    elif ht_gf == ht_ga:
                        node["ht_draws"] += 1
                    else:
                        node["ht_losses"] += 1
                        if ft_gf > ft_ga:
                            node["ht_lose_ft_win"] += 1
                        elif ft_gf == ft_ga:
                            node["ht_lose_ft_draw"] += 1
                    total_ht = ht_gf + ht_ga
                    if total_ht > 0.5:
                        node["ht_over_05"] += 1
                    if total_ht > 1.5:
                        node["ht_over_15"] += 1
                    if ht_gf > 0 and ht_ga > 0:
                        node["ht_btts"] += 1

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
        
        self.db.check_connection()
        for key, data in self.stats.items():
            mp = data["matches"]
            if mp == 0:
                continue
            row = {
                "team_name": key[0], "tournament_id": key[1], "venue_type": key[2], "matches_played": mp,
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
            self.db.cur.execute(insert_query, row)

class SecondHalfAnalyzer:
    def __init__(self, db):
        self.db = db
        self.stats = {}

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

    def analyze(self, matches):
        for match in matches:
            if match['ht_home'] is None or match['ht_away'] is None:
                continue
            home, away = match['home_team'], match['away_team']
            for is_home, t_name in [(True, home), (False, away)]:
                if is_home:
                    sh_gf = match['ft_home'] - match['ht_home']
                    sh_ga = match['ft_away'] - match['ht_away']
                else:
                    sh_gf = match['ft_away'] - match['ht_away']
                    sh_ga = match['ft_home'] - match['ht_home']
                for venue in ["Overall", "Home" if is_home else "Away"]:
                    node = self._get_team_node(t_name, match['tournament_id'], venue)
                    node["matches"] += 1
                    node["sh_goals_for"] += sh_gf
                    node["sh_goals_against"] += sh_ga
                    if sh_gf > sh_ga:
                        node["sh_wins"] += 1
                    elif sh_gf == sh_ga:
                        node["sh_draws"] += 1
                    else:
                        node["sh_losses"] += 1
                    total = sh_gf + sh_ga
                    if total > 0.5:
                        node["sh_over_05"] += 1
                    if total > 1.5:
                        node["sh_over_15"] += 1
                    if sh_gf > 0 and sh_ga > 0:
                        node["sh_btts"] += 1

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
        
        self.db.check_connection()
        for key, data in self.stats.items():
            mp = data["matches"]
            if mp == 0:
                continue
            row = {
                "team_name": key[0], "tournament_id": key[1], "venue_type": key[2], "matches_played": mp,
                "sh_wins": data["sh_wins"], "sh_draws": data["sh_draws"], "sh_losses": data["sh_losses"],
                "sh_goals_for": data["sh_goals_for"], "sh_goals_against": data["sh_goals_against"],
                "sh_avg_goals_for": round(data["sh_goals_for"] / mp, 2),
                "sh_avg_goals_against": round(data["sh_goals_against"] / mp, 2),
                "sh_over_05_pct": round((data["sh_over_05"] / mp) * 100, 2),
                "sh_over_15_pct": round((data["sh_over_15"] / mp) * 100, 2),
                "sh_btts_yes_pct": round((data["sh_btts"] / mp) * 100, 2)
            }
            self.db.cur.execute(insert_query, row)

class FormAnalyzer:
    def __init__(self, db):
        self.db = db
        self.stats = {}

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

    def analyze(self, matches):
        for match in matches:
            home, away = match['home_team'], match['away_team']
            for is_home, t_name in [(True, home), (False, away)]:
                if is_home:
                    gf = match['ft_home']
                    ga = match['ft_away']
                else:
                    gf = match['ft_away']
                    ga = match['ft_home']
                result = 'G' if gf > ga else ('B' if gf == ga else 'M')
                for venue in ["Overall", "Home" if is_home else "Away"]:
                    node = self._get_team_node(t_name, match['tournament_id'], venue)
                    node["form_queue"].append(result)
                    if len(node["form_queue"]) > 5:
                        node["form_queue"].pop(0)
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
                    if ga == 0:
                        node["clean_sheet_streak"] += 1
                    else:
                        node["clean_sheet_streak"] = 0
                    if gf > 0:
                        node["scoring_streak"] += 1
                    else:
                        node["scoring_streak"] = 0
                    if (gf + ga) > 2.5:
                        node["over_25_streak"] += 1
                    else:
                        node["over_25_streak"] = 0

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
        
        self.db.check_connection()
        for key, data in self.stats.items():
            form_str = ",".join(data["form_queue"])
            pts = sum(3 if c=='G' else 1 if c=='B' else 0 for c in data["form_queue"])
            row = {
                "team_name": key[0], "tournament_id": key[1], "venue_type": key[2],
                "form_last_5": form_str, "points_last_5": pts,
                "win_streak": data["win_streak"], "unbeaten_streak": data["unbeaten_streak"],
                "losing_streak": data["losing_streak"], "no_win_streak": data["no_win_streak"],
                "clean_sheet_streak": data["clean_sheet_streak"], "scoring_streak": data["scoring_streak"],
                "over_25_streak": data["over_25_streak"]
            }
            self.db.cur.execute(insert_query, row)

class LeagueAnalyzer:
    def __init__(self, db):
        self.db = db
        self.leagues = {}

    def _init_league_struct(self, match):
        return {
            "tournament_name": match.get("tournament_name", "Bilinmiyor"),
            "category_name": match.get("category_name", "Bilinmiyor"),
            "country": match.get("country", "Bilinmiyor"),
            "matches": 0, "home_wins": 0, "draws": 0, "away_wins": 0,
            "goals_home": 0, "goals_away": 0, "total_goals": 0,
            "over_25_count": 0, "under_25_count": 0, "btts_yes_count": 0
        }

    def analyze(self, matches):
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
                if gh > ga:
                    lg["home_wins"] += 1
                elif gh == ga:
                    lg["draws"] += 1
                else:
                    lg["away_wins"] += 1
                if total > 2.5:
                    lg["over_25_count"] += 1
                else:
                    lg["under_25_count"] += 1
                if gh > 0 and ga > 0:
                    lg["btts_yes_count"] += 1

        insert_query = """
            INSERT INTO league_analytics 
            (tournament_id, tournament_name, category_name, country, total_matches, 
             home_wins, draws, away_wins, home_win_pct, draw_pct, away_win_pct, 
             over_25_pct, under_25_pct, btts_yes_pct, avg_goals_match, avg_goals_home, avg_goals_away)
            VALUES (%(tournament_id)s, %(tournament_name)s, %(category_name)s, %(country)s, %(total_matches)s,
             %(home_wins)s, %(draws)s, %(away_wins)s, %(home_win_pct)s, %(draw_pct)s, %(away_win_pct)s,
             %(over_25_pct)s, %(under_25_pct)s, %(btts_yes_pct)s, %(avg_goals_match)s, %(avg_goals_home)s, %(avg_goals_away)s)
            ON DUPLICATE KEY UPDATE
             total_matches=VALUES(total_matches), home_wins=VALUES(home_wins), draws=VALUES(draws), away_wins=VALUES(away_wins),
             home_win_pct=VALUES(home_win_pct), draw_pct=VALUES(draw_pct), away_win_pct=VALUES(away_win_pct),
             over_25_pct=VALUES(over_25_pct), under_25_pct=VALUES(under_25_pct), btts_yes_pct=VALUES(btts_yes_pct),
             avg_goals_match=VALUES(avg_goals_match), avg_goals_home=VALUES(avg_goals_home), avg_goals_away=VALUES(avg_goals_away)
        """
        
        self.db.check_connection()
        for t_id, lg in self.leagues.items():
            tm = lg["matches"]
            if tm == 0:
                continue
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
            }
            self.db.cur.execute(insert_query, row)

class RefereeAnalyzer:
    def __init__(self, db):
        self.db = db
        self.referees = {}

    def _init_referee_struct(self):
        return {
            "matches": 0, "home_wins": 0, "draws": 0, "away_wins": 0,
            "total_goals": 0, "over_25_count": 0, "btts_yes_count": 0
        }

    def analyze(self, matches):
        for match in matches:
            ref = match.get('referee')
            if not ref:
                continue
            ref = ref.strip()
            if ref not in self.referees:
                self.referees[ref] = self._init_referee_struct()
            r = self.referees[ref]
            r["matches"] += 1
            gh, ga = match.get('ft_home'), match.get('ft_away')
            if gh is not None and ga is not None:
                total = gh + ga
                r["total_goals"] += total
                if gh > ga:
                    r["home_wins"] += 1
                elif gh == ga:
                    r["draws"] += 1
                else:
                    r["away_wins"] += 1
                if total > 2.5:
                    r["over_25_count"] += 1
                if gh > 0 and ga > 0:
                    r["btts_yes_count"] += 1

        insert_query = """
            INSERT INTO referee_analytics 
            (referee_name, total_matches, home_wins, draws, away_wins, 
             home_win_pct, draw_pct, away_win_pct, over_25_pct, btts_yes_pct, 
             avg_goals_match)
            VALUES (%(referee_name)s, %(total_matches)s, %(home_wins)s, %(draws)s, %(away_wins)s,
             %(home_win_pct)s, %(draw_pct)s, %(away_win_pct)s, %(over_25_pct)s, %(btts_yes_pct)s, 
             %(avg_goals_match)s)
            ON DUPLICATE KEY UPDATE
             total_matches=VALUES(total_matches), home_wins=VALUES(home_wins), draws=VALUES(draws), away_wins=VALUES(away_wins),
             home_win_pct=VALUES(home_win_pct), draw_pct=VALUES(draw_pct), away_win_pct=VALUES(away_win_pct),
             over_25_pct=VALUES(over_25_pct), btts_yes_pct=VALUES(btts_yes_pct),
             avg_goals_match=VALUES(avg_goals_match)
        """
        
        self.db.check_connection()
        for ref_name, r in self.referees.items():
            tm = r["matches"]
            if tm == 0:
                continue
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
                "avg_goals_match": round(r["total_goals"] / tm, 2)
            }
            self.db.cur.execute(insert_query, row)

def run_all_analyzers(db, max_date=None):
    db.truncate_analytics_tables()
    matches = db.get_matches_finished(max_date)
    if not matches:
        return
    analyzers = [
        EfficiencyAnalyzer(db),
        TeamGeneralAnalyzer(db),
        HalfTimeAnalyzer(db),
        SecondHalfAnalyzer(db),
        FormAnalyzer(db),
        LeagueAnalyzer(db),
        RefereeAnalyzer(db)
    ]
    for analyzer in analyzers:
        analyzer.analyze(matches)

# ==========================================
# 4. MODEL EĞİTİMİ
# ==========================================
def get_ml_features_query():
    return """
    SELECT 
        r.event_id, r.start_utc, r.ft_home, r.ft_away,
        COALESCE(ha.matches_played, ho.matches_played) as home_matches,
        COALESCE(ha.goals_for, ho.goals_for) as home_gf,
        COALESCE(ha.goals_against, ho.goals_against) as home_ga,
        COALESCE(ha.wins, ho.wins) as home_wins,
        COALESCE(ha.draws, ho.draws) as home_draws,
        COALESCE(ha.losses, ho.losses) as home_losses,
        COALESCE(ha.avg_possession, ho.avg_possession) as home_poss,
        COALESCE(ha.avg_shots, ho.avg_shots) as home_shots,
        COALESCE(ha.avg_shots_on, ho.avg_shots_on) as home_sot,
        COALESCE(aa.matches_played, ao.matches_played) as away_matches,
        COALESCE(aa.goals_for, ao.goals_for) as away_gf,
        COALESCE(aa.goals_against, ao.goals_against) as away_ga,
        COALESCE(aa.wins, ao.wins) as away_wins,
        COALESCE(aa.draws, ao.draws) as away_draws,
        COALESCE(aa.losses, ao.losses) as away_losses,
        COALESCE(aa.avg_possession, ao.avg_possession) as away_poss,
        COALESCE(aa.avg_shots, ao.avg_shots) as away_shots,
        COALESCE(aa.avg_shots_on, ao.avg_shots_on) as away_sot,
        hf.points_last_5 as home_form_pts,
        af.points_last_5 as away_form_pts,
        he.conversion_rate_pct as home_conv,
        ae.conversion_rate_pct as away_conv,
        he.save_rate_pct as home_save,
        ae.save_rate_pct as away_save,
        hht.ht_btts_yes_pct as home_ht_btts,
        aht.ht_btts_yes_pct as away_ht_btts,
        la.avg_goals_match as league_avg_goals,
        la.home_win_pct as league_home_win_pct,
        la.draw_pct as league_draw_pct,
        ra.avg_goals_match as ref_avg_goals
    FROM results_football r
    LEFT JOIN team_analytics ha ON r.home_team = ha.team_name AND r.tournament_id = ha.tournament_id AND r.category_id = ha.category_id AND ha.venue_type = 'Home'
    LEFT JOIN team_analytics ho ON r.home_team = ho.team_name AND r.tournament_id = ho.tournament_id AND r.category_id = ho.category_id AND ho.venue_type = 'Overall'
    LEFT JOIN team_analytics aa ON r.away_team = aa.team_name AND r.tournament_id = aa.tournament_id AND r.category_id = aa.category_id AND aa.venue_type = 'Away'
    LEFT JOIN team_analytics ao ON r.away_team = ao.team_name AND r.tournament_id = ao.tournament_id AND r.category_id = ao.category_id AND ao.venue_type = 'Overall'
    LEFT JOIN team_form_analytics hf ON r.home_team = hf.team_name AND r.tournament_id = hf.tournament_id AND hf.venue_type = 'Home'
    LEFT JOIN team_form_analytics af ON r.away_team = af.team_name AND r.tournament_id = af.tournament_id AND af.venue_type = 'Away'
    LEFT JOIN team_efficiency_analytics he ON r.home_team = he.team_name AND r.tournament_id = he.tournament_id AND he.venue_type = 'Home'
    LEFT JOIN team_efficiency_analytics ae ON r.away_team = ae.team_name AND r.tournament_id = ae.tournament_id AND ae.venue_type = 'Away'
    LEFT JOIN team_half_time_analytics hht ON r.home_team = hht.team_name AND r.tournament_id = hht.tournament_id AND hht.venue_type = 'Home'
    LEFT JOIN team_half_time_analytics aht ON r.away_team = aht.team_name AND r.tournament_id = aht.tournament_id AND aht.venue_type = 'Away'
    LEFT JOIN league_analytics la ON r.tournament_id = la.tournament_id
    LEFT JOIN referee_analytics ra ON r.referee = ra.referee_name
    """

def load_training_data_up_to_date(db, max_date=None):
    db.check_connection()
    query = get_ml_features_query() + " WHERE r.status IN ('finished','ended') AND r.ft_home IS NOT NULL AND r.ft_away IS NOT NULL"
    if max_date:
        query += f" AND r.start_utc < '{max_date}'"
    query += " ORDER BY r.start_utc"
    db.cur.execute(query)
    df = pd.DataFrame(db.cur.fetchall())
    return df

def prepare_features(df):
    df['total_goals'] = df['ft_home'] + df['ft_away']
    df['o15'] = (df['total_goals'] > 1.5).astype(int)
    df['o25'] = (df['total_goals'] > 2.5).astype(int)
    df['o35'] = (df['total_goals'] > 3.5).astype(int)

    feature_cols = [
        'home_matches', 'home_gf', 'home_ga', 'home_wins', 'home_draws', 'home_losses',
        'home_poss', 'home_shots', 'home_sot',
        'away_matches', 'away_gf', 'away_ga', 'away_wins', 'away_draws', 'away_losses',
        'away_poss', 'away_shots', 'away_sot',
        'home_form_pts', 'away_form_pts',
        'home_conv', 'away_conv', 'home_save', 'away_save',
        'home_ht_btts', 'away_ht_btts',
        'league_avg_goals', 'league_home_win_pct', 'league_draw_pct',
        'ref_avg_goals'
    ]
    X = df[feature_cols].copy()
    X = X.dropna()
    y_o15 = df.loc[X.index, 'o15']
    y_o25 = df.loc[X.index, 'o25']
    y_o35 = df.loc[X.index, 'o35']
    dates = df.loc[X.index, 'start_utc']
    return X, y_o15, y_o25, y_o35, dates

def train_xgb_model(X, y, model_name):
    if USE_HYPEROPT:
        param_dist = {
            'n_estimators': randint(100, 500),
            'max_depth': randint(3, 10),
            'learning_rate': uniform(0.01, 0.3),
            'subsample': uniform(0.6, 0.4),
            'colsample_bytree': uniform(0.6, 0.4),
            'gamma': uniform(0, 0.5),
            'reg_alpha': uniform(0, 2),
            'reg_lambda': uniform(0, 2)
        }
        tscv = TimeSeriesSplit(n_splits=3)
        base_model = xgb.XGBClassifier(objective='binary:logistic', random_state=42, eval_metric='logloss')
        random_search = RandomizedSearchCV(base_model, param_distributions=param_dist, n_iter=20, cv=tscv, scoring='roc_auc', n_jobs=-1, random_state=42)
        random_search.fit(X, y)
        best_model = random_search.best_estimator_
        calibrated = CalibratedClassifierCV(best_model, method='sigmoid', cv=3)
        calibrated.fit(X, y)
        return calibrated
    else:
        base_model = xgb.XGBClassifier(n_estimators=150, max_depth=5, learning_rate=0.1, objective='binary:logistic', random_state=42, eval_metric='logloss')
        calibrated = CalibratedClassifierCV(base_model, method='sigmoid', cv=3)
        calibrated.fit(X, y)
        return calibrated

def train_models(db, max_date=None):
    df = load_training_data_up_to_date(db, max_date)
    if df.empty or len(df) < 50:
        print(f"    Yetersiz eğitim verisi ({len(df)} maç), model eğitilemedi.")
        return False
    X, y_o15, y_o25, y_o35, _ = prepare_features(df)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    model_o15 = train_xgb_model(X_scaled, y_o15, "O15")
    model_o25 = train_xgb_model(X_scaled, y_o25, "O25")
    model_o35 = train_xgb_model(X_scaled, y_o35, "O35")
    
    joblib.dump(scaler, f"{MODEL_DIR}/scaler.pkl")
    joblib.dump(model_o15, f"{MODEL_DIR}/o15_model.pkl")
    joblib.dump(model_o25, f"{MODEL_DIR}/o25_model.pkl")
    joblib.dump(model_o35, f"{MODEL_DIR}/o35_model.pkl")
    return True

# ==========================================
# 5. TAHMİN (FLOAT / INT DÖNÜŞÜMLERİ EKLENDİ)
# ==========================================
def predict_matches(db, target_date=None, is_backtest=False):
    try:
        scaler = joblib.load(f"{MODEL_DIR}/scaler.pkl")
        model_o15 = joblib.load(f"{MODEL_DIR}/o15_model.pkl")
        model_o25 = joblib.load(f"{MODEL_DIR}/o25_model.pkl")
        model_o35 = joblib.load(f"{MODEL_DIR}/o35_model.pkl")
    except Exception as e:
        print(f"    Model yüklenemedi: {e}")
        return [], 0, 0

    db.check_connection()
    query = get_ml_features_query()
    
    if is_backtest and target_date:
        query += f" WHERE DATE(r.start_utc) = '{target_date}' AND r.status IN ('finished','ended')"
    elif target_date:
        query += f" WHERE DATE(r.start_utc) = '{target_date}'"
    else:
        tz_tr = timezone(timedelta(hours=3))
        today = datetime.now(tz_tr).date()
        end_date = today + timedelta(days=2)
        query += f" WHERE DATE(r.start_utc) BETWEEN '{today}' AND '{end_date}' AND r.status IN ('notstarted', 'scheduled')"
    
    db.cur.execute(query)
    df = pd.DataFrame(db.cur.fetchall())
    if df.empty:
        return [], 0, 0

    feature_cols = [
        'home_matches', 'home_gf', 'home_ga', 'home_wins', 'home_draws', 'home_losses',
        'home_poss', 'home_shots', 'home_sot',
        'away_matches', 'away_gf', 'away_ga', 'away_wins', 'away_draws', 'away_losses',
        'away_poss', 'away_shots', 'away_sot',
        'home_form_pts', 'away_form_pts',
        'home_conv', 'away_conv', 'home_save', 'away_save',
        'home_ht_btts', 'away_ht_btts',
        'league_avg_goals', 'league_home_win_pct', 'league_draw_pct',
        'ref_avg_goals'
    ]
    
    df[feature_cols] = df[feature_cols].fillna(0)

    results = []
    correct = 0
    total = 0
    for idx, row in df.iterrows():
        X = np.array([row[col] for col in feature_cols]).reshape(1, -1)
        X_scaled = scaler.transform(X)
        
        # NUMPY SAYILARINI SAF PYTHON FLOAT FORMATINA ÇEVİRİYORUZ (MySQL NULL hatasını engelleyen kodlar)
        prob_o15 = float(model_o15.predict_proba(X_scaled)[0][1])
        prob_o25 = float(model_o25.predict_proba(X_scaled)[0][1])
        prob_o35 = float(model_o35.predict_proba(X_scaled)[0][1])
        
        market, prob = decide_market(prob_o15, prob_o25, prob_o35)
        
        # DEĞERLERİ VERİTABANI İÇİN TEMİZLİYORUZ
        prob_clean = float(prob)
        event_id_clean = int(row['event_id'])
        
        is_correct = None
        actual_result = None
        
        if is_backtest and market != 'NO_BET':
            total_goals = int(row['ft_home'] + row['ft_away'])
            if market == 'O1.5':
                is_correct = bool(total_goals > 1.5)
            elif market == 'O2.5':
                is_correct = bool(total_goals > 2.5)
            else:
                is_correct = bool(total_goals > 3.5)
            actual_result = f"{total_goals}"
            if is_correct:
                correct += 1
            total += 1
            
        db.check_connection() 
        db.cur.execute("""
            INSERT INTO `match_predictions` 
            (event_id, predicted_market, probability, prob_o15, prob_o25, prob_o35, actual_result, is_correct, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
            predicted_market=VALUES(predicted_market), probability=VALUES(probability),
            prob_o15=VALUES(prob_o15), prob_o25=VALUES(prob_o25), prob_o35=VALUES(prob_o35),
            actual_result=VALUES(actual_result), is_correct=VALUES(is_correct)
        """, (event_id_clean, market, prob_clean, prob_o15, prob_o25, prob_o35, actual_result, is_correct))
        
        results.append((market, prob_clean, is_correct))
    
    return results, total, correct

def decide_market(prob_o15, prob_o25, prob_o35):
    if prob_o35 >= THRESHOLD_O35:
        return 'O3.5', prob_o35
    elif prob_o25 >= THRESHOLD_O25:
        return 'O2.5', prob_o25
    elif prob_o15 >= THRESHOLD_O15:
        return 'O1.5', prob_o15
    else:
        return 'NO_BET', max(prob_o15, prob_o25, prob_o35)

# ==========================================
# 6. BACKTEST DÖNGÜSÜ
# ==========================================
def run_backtest(db, start_date_str=None, end_date_str=None, step_days=1):
    tz_tr = timezone(timedelta(hours=3))
    today = datetime.now(tz_tr).date()
    
    if not start_date_str:
        start_date = today - timedelta(days=40)
    else:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    if not end_date_str:
        end_date = today - timedelta(days=1)
    else:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    current = start_date
    total_bets = 0
    total_won = 0
    daily_results = []
    
    while current <= end_date:
        run_all_analyzers(db, max_date=current)
        success = train_models(db, max_date=current)
        if not success:
            current += timedelta(days=step_days)
            continue
        _, valid, won = predict_matches(db, target_date=current, is_backtest=True)
        if valid > 0:
            acc = (won/valid)*100
            daily_results.append((current, valid, won, acc))
            total_bets += valid
            total_won += won
        current += timedelta(days=step_days)

# ==========================================
# 7. ANA FONKSİYON (TARİH BURADA SABİTLENDİ)
# ==========================================
def main():
    parser = argparse.ArgumentParser(description="Football Prediction Pipeline with Backtest")
    parser.add_argument("--mode", choices=["stats", "train", "predict", "full", "backtest"], default="predict", help="Çalışma modu")
    parser.add_argument("--start", help="Backtest başlangıç tarihi (YYYY-MM-DD)")
    parser.add_argument("--end", help="Backtest bitiş tarihi (YYYY-MM-DD)")
    parser.add_argument("--step", type=int, default=1, help="Backtest adım gün sayısı")
    parser.add_argument("--date", help="Belirli bir tarih için çalıştır (YYYY-MM-DD)")
    args = parser.parse_args()
    
    # ---------------------------------------------------------
    # KANKA TARİHİ BURAYA YAZIYORSUN: (GG.AA.YYYY formatında)
    manuel_tarih = "03.04.2026" 
    
    # Tarihi Python'un anlayacağı formata çeviriyoruz
    target_date_obj = datetime.strptime(manuel_tarih, "%d.%m.%Y").date()
    print(f"ÇALIŞTIRILAN TARİH: {target_date_obj} (Sisteme Sabitlendi)")
    # ---------------------------------------------------------

    db = Database(CONFIG["db"])
    db.connect()
    db.create_all_tables()
    
    if args.mode == "stats":
        run_all_analyzers(db, max_date=target_date_obj)
    elif args.mode == "train":
        train_models(db, max_date=target_date_obj)
    elif args.mode == "predict":
        predict_matches(db, target_date=target_date_obj)
    elif args.mode == "full":
        run_all_analyzers(db, max_date=target_date_obj)
        train_models(db, max_date=target_date_obj)
        predict_matches(db, target_date=target_date_obj)
    elif args.mode == "backtest":
        run_backtest(db, args.start, args.end, args.step)
    
    db.close()

if __name__ == "__main__":
    main()
