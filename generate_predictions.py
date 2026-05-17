#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
generate_predictions.py

Oran kullanmayan özel futbol tahmin motoru.

Ürettiği marketler:
- 1. yarı karşılıklı gol
- 2. yarı karşılıklı gol
- ev sahibi iki yarıda da gol
- deplasman iki yarıda da gol
- ev sahibi iki yarıyı da kazanır
- deplasman iki yarıyı da kazanır
- toplam gol aralığı: 0-1, 2-3, 4-5, 6+
- iki yarıda 1.5 üst
- iki yarıda 1.5 alt

Kullanmaz:
- bahis oranları
- odds kolonları
- stake market isimleri
- şut/faul/kart marketleri

Bu sürümde korner marketleri tamamen kapalıdır.
Maç başına tahmin üretmek için global eşik yanında market bazlı alt sınır kullanılır.
Amaç: her maça tahmin basmak değil, sinyal güçlü olduğunda az ve seçici tahmin üretmek.
"""

import os
import sys
from typing import Any, Dict, List, Optional

import mysql.connector


# ============================================================
# CONFIG
# ============================================================

MODEL_VERSION = os.getenv("MODEL_VERSION", "v6_no_corners_selective_market_floor_2026_05_15")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "550"))
TARGET_DAYS_AHEAD = int(os.getenv("TARGET_DAYS_AHEAD", "1"))

MIN_TEAM_MATCHES = int(os.getenv("MIN_TEAM_MATCHES", "5"))
MIN_LEAGUE_MATCHES = int(os.getenv("MIN_LEAGUE_MATCHES", "10"))
MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "0.66"))

MAX_TEAM_PROFILE_MATCHES = int(os.getenv("MAX_TEAM_PROFILE_MATCHES", "20"))


# ============================================================
# MARKET MAP
# ============================================================

MARKET_MAP = {
    "first_half_btts_yes": {
        "market_type": "half_btts",
        "selection": "first_half_btts_yes",
        "market_key": "first_half_both_teams_to_score",
        "market_name": "1. Yarı Karşılıklı Gol",
        "selection_key": "yes",
        "selection_name": "Evet",
    },
    "second_half_btts_yes": {
        "market_type": "half_btts",
        "selection": "first_half_over_1_5",
        "market_key": "first_half_total_goals",
        "market_name": "İlk Yarı 1.5 Üst",
        "selection_key": "over_1_5",
        "selection_name": "Evet",
    },
    "home_scores_both_halves": {
        "market_type": "team_halves_goal",
        "selection": "home_scores_both_halves",
        "market_key": "home_scores_both_halves",
        "market_name": "Ev Sahibi İki Yarıda da Gol Atar",
        "selection_key": "yes",
        "selection_name": "Evet",
    },
    "away_scores_both_halves": {
        "market_type": "team_halves_goal",
        "selection": "away_scores_both_halves",
        "market_key": "away_scores_both_halves",
        "market_name": "Deplasman İki Yarıda da Gol Atar",
        "selection_key": "yes",
        "selection_name": "Evet",
    },
    "home_wins_both_halves": {
        "market_type": "team_halves_result",
        "selection": "home_wins_both_halves",
        "market_key": "home_wins_both_halves",
        "market_name": "Ev Sahibi İki Yarıyı da Kazanır",
        "selection_key": "yes",
        "selection_name": "Evet",
    },
    "away_wins_both_halves": {
        "market_type": "team_halves_result",
        "selection": "away_wins_both_halves",
        "market_key": "away_wins_both_halves",
        "market_name": "Deplasman İki Yarıyı da Kazanır",
        "selection_key": "yes",
        "selection_name": "Evet",
    },
    "total_goals_0_1": {
        "market_type": "goal_range",
        "selection": "total_goals_0_1",
        "market_key": "total_goals_range",
        "market_name": "Toplam Gol Aralığı",
        "selection_key": "0_1",
        "selection_name": "0-1",
    },
    "total_goals_2_3": {
        "market_type": "goal_range",
        "selection": "total_goals_2_3",
        "market_key": "total_goals_range",
        "market_name": "Toplam Gol Aralığı",
        "selection_key": "2_3",
        "selection_name": "2-3",
    },
    "total_goals_4_5": {
        "market_type": "goal_range",
        "selection": "total_goals_4_5",
        "market_key": "total_goals_range",
        "market_name": "Toplam Gol Aralığı",
        "selection_key": "4_5",
        "selection_name": "4-5",
    },
    "total_goals_6_plus": {
        "market_type": "goal_range",
        "selection": "total_goals_6_plus",
        "market_key": "total_goals_range",
        "market_name": "Toplam Gol Aralığı",
        "selection_key": "6_plus",
        "selection_name": "6+",
    },
    "both_halves_over_1_5": {
        "market_type": "both_halves_goals",
        "selection": "both_halves_over_1_5",
        "market_key": "both_halves_total_goals",
        "market_name": "İki Yarıda da 1.5 Üst",
        "selection_key": "over_1_5",
        "selection_name": "Evet",
    },
    "both_halves_under_1_5": {
        "market_type": "both_halves_goals",
        "selection": "both_halves_under_1_5",
        "market_key": "both_halves_total_goals",
        "market_name": "İki Yarıda da 1.5 Alt",
        "selection_key": "under_1_5",
        "selection_name": "Evet",
    },
}


# Pozitif: seçilme şansı artar.
# Negatif: seçilme şansı azalır.
# İyi giden marketler: 2. yarı KG, toplam gol 2-3, deplasman iki yarıda da gol.
MARKET_PRIORITY_BONUS = {
    "first_half_btts_yes": 0.010,
    "second_half_btts_yes": 0.060,

    "home_scores_both_halves": 0.010,
    "away_scores_both_halves": 0.065,

    "home_wins_both_halves": -0.020,
    "away_wins_both_halves": -0.020,

    "total_goals_0_1": 0.000,
    "total_goals_2_3": 0.065,
    "total_goals_4_5": 0.005,
    "total_goals_6_plus": -0.030,

    "both_halves_over_1_5": 0.000,
    "both_halves_under_1_5": -0.005,
}


MARKET_TYPE_ADJUSTMENT = {
    "half_btts": 0.025,
    "team_halves_goal": 0.030,
    "team_halves_result": 0.040,
    "goal_range": 0.025,
    "both_halves_goals": 0.030,
}


# Market bazlı alt sınır. Global MIN_CONFIDENCE bunun altında kalsa bile bu sınır geçerli olur.
# Ana üçlü daha ulaşılabilir, diğerleri daha seçici tutulur.
MARKET_MIN_CONFIDENCE = {
    "second_half_btts_yes": float(os.getenv("MIN_SECOND_HALF_BTTS", "0.66")),
    "first_half_over_1_5": float(os.getenv("MIN_SECOND_HALF_BTTS", "0.66")),
    "total_goals_2_3": float(os.getenv("MIN_TOTAL_GOALS_2_3", "0.66")),
    "away_scores_both_halves": float(os.getenv("MIN_AWAY_SCORES_BOTH_HALVES", "0.67")),

    "first_half_btts_yes": float(os.getenv("MIN_FIRST_HALF_BTTS", "0.70")),
    "home_scores_both_halves": float(os.getenv("MIN_HOME_SCORES_BOTH_HALVES", "0.70")),
    "home_wins_both_halves": float(os.getenv("MIN_HOME_WINS_BOTH_HALVES", "0.76")),
    "away_wins_both_halves": float(os.getenv("MIN_AWAY_WINS_BOTH_HALVES", "0.76")),
    "total_goals_0_1": float(os.getenv("MIN_TOTAL_GOALS_0_1", "0.70")),
    "total_goals_4_5": float(os.getenv("MIN_TOTAL_GOALS_4_5", "0.72")),
    "total_goals_6_plus": float(os.getenv("MIN_TOTAL_GOALS_6_PLUS", "0.78")),
    "both_halves_over_1_5": float(os.getenv("MIN_BOTH_HALVES_OVER_1_5", "0.73")),
    "both_halves_under_1_5": float(os.getenv("MIN_BOTH_HALVES_UNDER_1_5", "0.72")),
}

# Bu motor korner üretmez.
DISABLED_MARKET_TYPES = {"corners"}


# ============================================================
# SQL
# ============================================================

CREATE_PREDICTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS predictions_football (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

  event_id BIGINT UNSIGNED NOT NULL,
  start_utc DATE NULL,
  start_time_utc TIME NULL,

  home_team VARCHAR(128) NULL,
  away_team VARCHAR(128) NULL,

  tournament_id INT NULL,
  tournament_name VARCHAR(128) NULL,
  country VARCHAR(64) NULL,

  market_type VARCHAR(64) NOT NULL,
  selection VARCHAR(64) NOT NULL,

  market_key VARCHAR(64) NULL,
  market_name VARCHAR(128) NULL,
  selection_key VARCHAR(64) NULL,
  selection_name VARCHAR(128) NULL,

  confidence_score FLOAT NOT NULL,
  selection_score FLOAT NULL,
  confidence_label VARCHAR(32) NOT NULL,

  model_version VARCHAR(64) NOT NULL,
  reason_text TEXT NULL,

  input_sample_size INT NULL,
  home_sample_size INT NULL,
  away_sample_size INT NULL,
  league_sample_size INT NULL,

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_event_model (event_id, model_version),
  KEY idx_event_id (event_id),
  KEY idx_start_utc (start_utc),
  KEY idx_market (market_type, selection),
  KEY idx_market_key (market_key, selection_key),
  KEY idx_confidence (confidence_score),
  KEY idx_selection_score (selection_score),
  KEY idx_model_version (model_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# ============================================================
# HELPERS
# ============================================================

def safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def avg(values: List[Optional[float]]) -> Optional[float]:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def rate(values: List[bool]) -> Optional[float]:
    if not values:
        return None
    return sum(1 for v in values if v) / len(values)


def val(value: Optional[float], default: float) -> float:
    if value is None:
        return default
    return float(value)


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def confidence_label(score: float) -> str:
    if score >= 0.74:
        return "high"
    if score >= 0.58:
        return "medium"
    return "low"


# ============================================================
# DB
# ============================================================

class DB:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.conn = None
        self.cur = None

    def connect(self) -> None:
        self.close(silent=True)

        missing = [k for k, v in self.cfg.items() if k != "port" and not v]
        if missing:
            raise RuntimeError(f"Eksik DB ortam değişkenleri: {missing}")

        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)

        self.cur.execute(CREATE_PREDICTIONS_TABLE)
        self.ensure_columns()

        print("[DB] Bağlantı başarılı.")
        print("[DB] predictions_football tablosu hazır.")

    def ensure_columns(self) -> None:
        columns = {
            "market_key": "VARCHAR(64) NULL",
            "market_name": "VARCHAR(128) NULL",
            "selection_key": "VARCHAR(64) NULL",
            "selection_name": "VARCHAR(128) NULL",
            "confidence_score": "FLOAT NOT NULL DEFAULT 0",
            "selection_score": "FLOAT NULL",
            "confidence_label": "VARCHAR(32) NOT NULL DEFAULT 'low'",
            "model_version": "VARCHAR(64) NOT NULL DEFAULT 'unknown'",
            "reason_text": "TEXT NULL",
            "input_sample_size": "INT NULL",
            "home_sample_size": "INT NULL",
            "away_sample_size": "INT NULL",
            "league_sample_size": "INT NULL",
        }

        for col, definition in columns.items():
            try:
                self.cur.execute(
                    f"ALTER TABLE predictions_football ADD COLUMN {col} {definition};"
                )
            except mysql.connector.Error:
                pass

    def ping(self) -> None:
        try:
            self.conn.ping(reconnect=True, attempts=3, delay=1)
        except mysql.connector.Error:
            print("[DB] Bağlantı koptu, yeniden bağlanılıyor...")
            self.connect()

    def get_target_fixtures(self, days_ahead: int) -> List[Dict[str, Any]]:
        self.ping()

        query = """
        SELECT
            event_id,
            start_utc,
            start_time_utc,
            home_team,
            away_team,
            tournament_id,
            tournament_name,
            country,
            status
        FROM results_football
        WHERE status IN ('notstarted', 'scheduled')
          AND start_utc >= CURDATE()
          AND start_utc <= DATE_ADD(CURDATE(), INTERVAL %s DAY)
          AND home_team IS NOT NULL
          AND away_team IS NOT NULL
        ORDER BY start_utc ASC, start_time_utc ASC
        """

        self.cur.execute(query, (days_ahead,))
        return list(self.cur.fetchall())

    def get_history(
        self,
        home_team: str,
        away_team: str,
        tournament_id: Optional[int],
        lookback_days: int,
    ) -> List[Dict[str, Any]]:
        self.ping()

        query = """
        SELECT
            event_id,
            start_utc,

            home_team,
            away_team,

            tournament_id,
            tournament_name,
            country,

            ht_home,
            ht_away,
            ft_home,
            ft_away,

            corn_h,
            corn_a,

            shot_h,
            shot_a,
            shot_on_h,
            shot_on_a,

            poss_h,
            poss_a
        FROM results_football
        WHERE status IN ('finished', 'ended')
          AND start_utc >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
          AND ft_home IS NOT NULL
          AND ft_away IS NOT NULL
          AND (
                home_team = %s
             OR away_team = %s
             OR home_team = %s
             OR away_team = %s
             OR tournament_id = %s
          )
        ORDER BY start_utc DESC
        """

        self.cur.execute(
            query,
            (
                lookback_days,
                home_team,
                home_team,
                away_team,
                away_team,
                tournament_id,
            ),
        )
        return list(self.cur.fetchall())

    def upsert_prediction(self, prediction: Dict[str, Any]) -> None:
        self.ping()

        query = """
        INSERT INTO predictions_football (
            event_id,
            start_utc,
            start_time_utc,
            home_team,
            away_team,
            tournament_id,
            tournament_name,
            country,

            market_type,
            selection,
            market_key,
            market_name,
            selection_key,
            selection_name,

            confidence_score,
            selection_score,
            confidence_label,
            model_version,
            reason_text,

            input_sample_size,
            home_sample_size,
            away_sample_size,
            league_sample_size
        )
        VALUES (
            %(event_id)s,
            %(start_utc)s,
            %(start_time_utc)s,
            %(home_team)s,
            %(away_team)s,
            %(tournament_id)s,
            %(tournament_name)s,
            %(country)s,

            %(market_type)s,
            %(selection)s,
            %(market_key)s,
            %(market_name)s,
            %(selection_key)s,
            %(selection_name)s,

            %(confidence_score)s,
            %(selection_score)s,
            %(confidence_label)s,
            %(model_version)s,
            %(reason_text)s,

            %(input_sample_size)s,
            %(home_sample_size)s,
            %(away_sample_size)s,
            %(league_sample_size)s
        )
        ON DUPLICATE KEY UPDATE
            start_utc = VALUES(start_utc),
            start_time_utc = VALUES(start_time_utc),
            home_team = VALUES(home_team),
            away_team = VALUES(away_team),
            tournament_id = VALUES(tournament_id),
            tournament_name = VALUES(tournament_name),
            country = VALUES(country),

            market_type = VALUES(market_type),
            selection = VALUES(selection),
            market_key = VALUES(market_key),
            market_name = VALUES(market_name),
            selection_key = VALUES(selection_key),
            selection_name = VALUES(selection_name),

            confidence_score = VALUES(confidence_score),
            selection_score = VALUES(selection_score),
            confidence_label = VALUES(confidence_label),
            reason_text = VALUES(reason_text),

            input_sample_size = VALUES(input_sample_size),
            home_sample_size = VALUES(home_sample_size),
            away_sample_size = VALUES(away_sample_size),
            league_sample_size = VALUES(league_sample_size),
            updated_at = CURRENT_TIMESTAMP
        """

        self.cur.execute(query, prediction)

    def close(self, silent: bool = False) -> None:
        try:
            if self.cur:
                self.cur.close()
        except Exception as e:
            if not silent:
                print(f"[DB UYARI] Cursor kapatılamadı: {e}")

        try:
            if self.conn:
                self.conn.close()
        except Exception as e:
            if not silent:
                print(f"[DB UYARI] Bağlantı kapatılamadı: {e}")

        self.cur = None
        self.conn = None


# ============================================================
# FEATURE BUILDER
# ============================================================

def build_match_view(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ft_home = safe_float(row.get("ft_home"), None)
    ft_away = safe_float(row.get("ft_away"), None)
    ht_home = safe_float(row.get("ht_home"), None)
    ht_away = safe_float(row.get("ht_away"), None)

    if ft_home is None or ft_away is None:
        return None

    total_goals = ft_home + ft_away

    has_half = ht_home is not None and ht_away is not None
    if has_half:
        second_half_home = max(ft_home - ht_home, 0)
        second_half_away = max(ft_away - ht_away, 0)
        first_half_goals = ht_home + ht_away
        second_half_goals = second_half_home + second_half_away
    else:
        second_half_home = None
        second_half_away = None
        first_half_goals = None
        second_half_goals = None

    corn_h = safe_float(row.get("corn_h"), None)
    corn_a = safe_float(row.get("corn_a"), None)

    has_corners = corn_h is not None and corn_a is not None
    total_corners = corn_h + corn_a if has_corners else None

    return {
        "ft_home": ft_home,
        "ft_away": ft_away,
        "ht_home": ht_home,
        "ht_away": ht_away,
        "second_half_home": second_half_home,
        "second_half_away": second_half_away,

        "total_goals": total_goals,
        "first_half_goals": first_half_goals,
        "second_half_goals": second_half_goals,

        "has_half": has_half,
        "has_corners": has_corners,
        "total_corners": total_corners,

        "first_half_btts": has_half and ht_home >= 1 and ht_away >= 1,
        "second_half_btts": has_half and second_half_home >= 1 and second_half_away >= 1,

        "home_scores_both_halves": has_half and ht_home >= 1 and second_half_home >= 1,
        "away_scores_both_halves": has_half and ht_away >= 1 and second_half_away >= 1,

        "home_wins_both_halves": has_half and ht_home > ht_away and second_half_home > second_half_away,
        "away_wins_both_halves": has_half and ht_away > ht_home and second_half_away > second_half_home,

        "goal_range_0_1": total_goals <= 1,
        "goal_range_2_3": 2 <= total_goals <= 3,
        "goal_range_4_5": 4 <= total_goals <= 5,
        "goal_range_6_plus": total_goals >= 6,

        "both_halves_over_1_5": has_half and first_half_goals >= 2 and second_half_goals >= 2,
        "both_halves_under_1_5": has_half and first_half_goals <= 1 and second_half_goals <= 1,

        "corners_over_7_5": has_corners and total_corners >= 8,
        "corners_over_8_5": has_corners and total_corners >= 9,
        "corners_over_9_5": has_corners and total_corners >= 10,
        "corners_under_10_5": has_corners and total_corners <= 10,
        "corners_under_11_5": has_corners and total_corners <= 11,
    }


def build_team_view(row: Dict[str, Any], team: str) -> Optional[Dict[str, Any]]:
    match = build_match_view(row)
    if not match:
        return None

    is_home = row.get("home_team") == team
    is_away = row.get("away_team") == team

    if not is_home and not is_away:
        return None

    ft_home = match["ft_home"]
    ft_away = match["ft_away"]
    ht_home = match["ht_home"]
    ht_away = match["ht_away"]
    has_half = match["has_half"]

    if is_home:
        goals_for = ft_home
        goals_against = ft_away

        if has_half:
            ht_for = ht_home
            ht_against = ht_away
            sh_for = match["second_half_home"]
            sh_against = match["second_half_away"]
            wins_first_half = ht_home > ht_away
            wins_second_half = match["second_half_home"] > match["second_half_away"]
        else:
            ht_for = None
            ht_against = None
            sh_for = None
            sh_against = None
            wins_first_half = False
            wins_second_half = False

        corners_for = safe_float(row.get("corn_h"), None)
        corners_against = safe_float(row.get("corn_a"), None)
        shots_for = safe_float(row.get("shot_h"), None)
        shots_against = safe_float(row.get("shot_a"), None)
        shots_on_for = safe_float(row.get("shot_on_h"), None)
        possession = safe_float(row.get("poss_h"), None)

    else:
        goals_for = ft_away
        goals_against = ft_home

        if has_half:
            ht_for = ht_away
            ht_against = ht_home
            sh_for = match["second_half_away"]
            sh_against = match["second_half_home"]
            wins_first_half = ht_away > ht_home
            wins_second_half = match["second_half_away"] > match["second_half_home"]
        else:
            ht_for = None
            ht_against = None
            sh_for = None
            sh_against = None
            wins_first_half = False
            wins_second_half = False

        corners_for = safe_float(row.get("corn_a"), None)
        corners_against = safe_float(row.get("corn_h"), None)
        shots_for = safe_float(row.get("shot_a"), None)
        shots_against = safe_float(row.get("shot_h"), None)
        shots_on_for = safe_float(row.get("shot_on_a"), None)
        possession = safe_float(row.get("poss_a"), None)

    return {
        **match,

        "goals_for": goals_for,
        "goals_against": goals_against,

        "ht_for": ht_for,
        "ht_against": ht_against,
        "sh_for": sh_for,
        "sh_against": sh_against,

        "scores_first_half": has_half and ht_for >= 1,
        "scores_second_half": has_half and sh_for >= 1,
        "scores_both_halves": has_half and ht_for >= 1 and sh_for >= 1,

        "wins_first_half": has_half and wins_first_half,
        "wins_second_half": has_half and wins_second_half,
        "wins_both_halves": has_half and wins_first_half and wins_second_half,

        "concedes_first_half": has_half and ht_against >= 1,
        "concedes_second_half": has_half and sh_against >= 1,
        "concedes_both_halves": has_half and ht_against >= 1 and sh_against >= 1,

        "corners_for": corners_for,
        "corners_against": corners_against,
        "shots_for": shots_for,
        "shots_against": shots_against,
        "shots_on_for": shots_on_for,
        "possession": possession,
    }


def profile_from_match_views(views: List[Dict[str, Any]]) -> Dict[str, Any]:
    half_views = [v for v in views if v["has_half"]]
    corner_views = [v for v in views if v["has_corners"]]

    return {
        "sample_size": len(views),
        "half_sample_size": len(half_views),
        "corner_sample_size": len(corner_views),

        "goals_avg": avg([v["total_goals"] for v in views]),
        "first_half_goals_avg": avg([v["first_half_goals"] for v in half_views]),
        "second_half_goals_avg": avg([v["second_half_goals"] for v in half_views]),

        "first_half_btts_rate": rate([v["first_half_btts"] for v in half_views]),
        "second_half_btts_rate": rate([v["second_half_btts"] for v in half_views]),

        "home_scores_both_halves_rate": rate([v["home_scores_both_halves"] for v in half_views]),
        "away_scores_both_halves_rate": rate([v["away_scores_both_halves"] for v in half_views]),

        "home_wins_both_halves_rate": rate([v["home_wins_both_halves"] for v in half_views]),
        "away_wins_both_halves_rate": rate([v["away_wins_both_halves"] for v in half_views]),

        "goal_range_0_1_rate": rate([v["goal_range_0_1"] for v in views]),
        "goal_range_2_3_rate": rate([v["goal_range_2_3"] for v in views]),
        "goal_range_4_5_rate": rate([v["goal_range_4_5"] for v in views]),
        "goal_range_6_plus_rate": rate([v["goal_range_6_plus"] for v in views]),

        "both_halves_over_1_5_rate": rate([v["both_halves_over_1_5"] for v in half_views]),
        "both_halves_under_1_5_rate": rate([v["both_halves_under_1_5"] for v in half_views]),

        "total_corners_avg": avg([v["total_corners"] for v in corner_views]),
        "corners_over_7_5_rate": rate([v["corners_over_7_5"] for v in corner_views]),
        "corners_over_8_5_rate": rate([v["corners_over_8_5"] for v in corner_views]),
        "corners_over_9_5_rate": rate([v["corners_over_9_5"] for v in corner_views]),
        "corners_under_10_5_rate": rate([v["corners_under_10_5"] for v in corner_views]),
        "corners_under_11_5_rate": rate([v["corners_under_11_5"] for v in corner_views]),
    }


def profile_from_team_views(views: List[Dict[str, Any]]) -> Dict[str, Any]:
    half_views = [v for v in views if v["has_half"]]
    corner_views = [v for v in views if v["has_corners"]]

    return {
        "sample_size": len(views),
        "half_sample_size": len(half_views),
        "corner_sample_size": len(corner_views),

        "goals_for_avg": avg([v["goals_for"] for v in views]),
        "goals_against_avg": avg([v["goals_against"] for v in views]),

        "ht_for_avg": avg([v["ht_for"] for v in half_views]),
        "ht_against_avg": avg([v["ht_against"] for v in half_views]),
        "sh_for_avg": avg([v["sh_for"] for v in half_views]),
        "sh_against_avg": avg([v["sh_against"] for v in half_views]),

        "scores_first_half_rate": rate([v["scores_first_half"] for v in half_views]),
        "scores_second_half_rate": rate([v["scores_second_half"] for v in half_views]),
        "scores_both_halves_rate": rate([v["scores_both_halves"] for v in half_views]),

        "wins_first_half_rate": rate([v["wins_first_half"] for v in half_views]),
        "wins_second_half_rate": rate([v["wins_second_half"] for v in half_views]),
        "wins_both_halves_rate": rate([v["wins_both_halves"] for v in half_views]),

        "concedes_first_half_rate": rate([v["concedes_first_half"] for v in half_views]),
        "concedes_second_half_rate": rate([v["concedes_second_half"] for v in half_views]),
        "concedes_both_halves_rate": rate([v["concedes_both_halves"] for v in half_views]),

        "goal_range_0_1_rate": rate([v["goal_range_0_1"] for v in views]),
        "goal_range_2_3_rate": rate([v["goal_range_2_3"] for v in views]),
        "goal_range_4_5_rate": rate([v["goal_range_4_5"] for v in views]),
        "goal_range_6_plus_rate": rate([v["goal_range_6_plus"] for v in views]),

        "both_halves_over_1_5_rate": rate([v["both_halves_over_1_5"] for v in half_views]),
        "both_halves_under_1_5_rate": rate([v["both_halves_under_1_5"] for v in half_views]),

        "corners_for_avg": avg([v["corners_for"] for v in corner_views]),
        "corners_against_avg": avg([v["corners_against"] for v in corner_views]),
        "total_corners_avg": avg([v["total_corners"] for v in corner_views]),

        "corners_over_7_5_rate": rate([v["corners_over_7_5"] for v in corner_views]),
        "corners_over_8_5_rate": rate([v["corners_over_8_5"] for v in corner_views]),
        "corners_over_9_5_rate": rate([v["corners_over_9_5"] for v in corner_views]),
        "corners_under_10_5_rate": rate([v["corners_under_10_5"] for v in corner_views]),
        "corners_under_11_5_rate": rate([v["corners_under_11_5"] for v in corner_views]),

        "shots_for_avg": avg([v["shots_for"] for v in views]),
        "shots_against_avg": avg([v["shots_against"] for v in views]),
        "shots_on_for_avg": avg([v["shots_on_for"] for v in views]),
        "possession_avg": avg([v["possession"] for v in views]),
    }


def build_features(fixture: Dict[str, Any], history: List[Dict[str, Any]]) -> Dict[str, Any]:
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]
    tournament_id = fixture.get("tournament_id")

    home_views = []
    away_views = []
    league_views = []
    all_relevant_match_views = []

    for row in history:
        mv = build_match_view(row)
        if mv:
            all_relevant_match_views.append(mv)

        hv = build_team_view(row, home_team)
        if hv:
            home_views.append(hv)

        av = build_team_view(row, away_team)
        if av:
            away_views.append(av)

        if tournament_id is not None and row.get("tournament_id") == tournament_id and mv:
            league_views.append(mv)

    home_views = home_views[:MAX_TEAM_PROFILE_MATCHES]
    away_views = away_views[:MAX_TEAM_PROFILE_MATCHES]

    return {
        "fixture": fixture,
        "home": profile_from_team_views(home_views),
        "away": profile_from_team_views(away_views),
        "league": profile_from_match_views(league_views),
        "all": profile_from_match_views(all_relevant_match_views),
        "input_sample_size": len(history),
    }


# ============================================================
# MARKET ENGINE
# ============================================================

def sample_quality(features: Dict[str, Any], market_family: str) -> float:
    home_n = int(features["home"]["sample_size"])
    away_n = int(features["away"]["sample_size"])
    league_n = int(features["league"]["sample_size"])

    home_half_n = int(features["home"]["half_sample_size"])
    away_half_n = int(features["away"]["half_sample_size"])
    league_half_n = int(features["league"]["half_sample_size"])

    home_corner_n = int(features["home"]["corner_sample_size"])
    away_corner_n = int(features["away"]["corner_sample_size"])
    league_corner_n = int(features["league"]["corner_sample_size"])

    if market_family == "corners":
        team_part = min(home_corner_n, away_corner_n) / 14.0
        league_part = league_corner_n / 70.0
    elif market_family in {"half_btts", "team_halves_goal", "team_halves_result", "both_halves_goals"}:
        team_part = min(home_half_n, away_half_n) / 8.0
        league_part = league_half_n / 30.0
    else:
        team_part = min(home_n, away_n) / 8.0
        league_part = league_n / 35.0

    return clamp((team_part * 0.70) + (league_part * 0.30))


def make_candidate(
    features: Dict[str, Any],
    code: str,
    raw_score: float,
    reason: str,
) -> Dict[str, Any]:
    fixture = features["fixture"]
    meta = MARKET_MAP[code]

    quality = sample_quality(features, meta["market_type"])

    final_score = clamp((raw_score * 0.84) + (quality * 0.16))

    selection_score = clamp(
        final_score
        + MARKET_PRIORITY_BONUS.get(code, 0.0)
        + MARKET_TYPE_ADJUSTMENT.get(meta["market_type"], 0.0)
    )

    return {
        "event_id": fixture["event_id"],
        "start_utc": fixture.get("start_utc"),
        "start_time_utc": fixture.get("start_time_utc"),

        "home_team": fixture.get("home_team"),
        "away_team": fixture.get("away_team"),

        "tournament_id": fixture.get("tournament_id"),
        "tournament_name": fixture.get("tournament_name"),
        "country": fixture.get("country"),

        "market_type": meta["market_type"],
        "selection": meta["selection"],
        "market_key": meta["market_key"],
        "market_name": meta["market_name"],
        "selection_key": meta["selection_key"],
        "selection_name": meta["selection_name"],

        "confidence_score": round(final_score, 4),
        "selection_score": round(selection_score, 4),
        "confidence_label": confidence_label(final_score),
        "model_version": MODEL_VERSION,
        "reason_text": reason,

        "input_sample_size": int(features["input_sample_size"]),
        "home_sample_size": int(features["home"]["sample_size"]),
        "away_sample_size": int(features["away"]["sample_size"]),
        "league_sample_size": int(features["league"]["sample_size"]),
    }


def generate_candidates(features: Dict[str, Any]) -> List[Dict[str, Any]]:
    h = features["home"]
    a = features["away"]
    l = features["league"]
    allp = features["all"]

    candidates = []

    # 1. yarı KG
    raw = (
        val(h["scores_first_half_rate"], 0.35) * 0.22
        + val(a["scores_first_half_rate"], 0.30) * 0.22
        + val(h["concedes_first_half_rate"], 0.35) * 0.16
        + val(a["concedes_first_half_rate"], 0.35) * 0.16
        + val(l["first_half_btts_rate"], 0.22) * 0.10
        + min((val(h["ht_for_avg"], 0.45) + val(a["ht_for_avg"], 0.40)) / 1.55, 1.0) * 0.14
    )
    candidates.append(make_candidate(
        features,
        "first_half_btts_yes",
        raw,
        "İki takımın ilk yarı gol bulma ve ilk yarı gol yeme profili 1. yarı karşılıklı golü destekliyor.",
    ))

    # 2. yarı KG
    raw = (
        val(h["scores_second_half_rate"], 0.40) * 0.22
        + val(a["scores_second_half_rate"], 0.35) * 0.22
        + val(h["concedes_second_half_rate"], 0.40) * 0.16
        + val(a["concedes_second_half_rate"], 0.40) * 0.16
        + val(l["second_half_btts_rate"], 0.25) * 0.10
        + min((val(h["sh_for_avg"], 0.55) + val(a["sh_for_avg"], 0.50)) / 1.70, 1.0) * 0.14
    )
    candidates.append(make_candidate(
        features,
        "second_half_btts_yes",
        raw,
        "İki takımın ikinci yarı gol bulma/yeme profili 2. yarı karşılıklı golü destekliyor.",
    ))

    # Ev sahibi iki yarıda da gol
    raw = (
        val(h["scores_both_halves_rate"], 0.22) * 0.36
        + val(a["concedes_both_halves_rate"], 0.18) * 0.24
        + min(val(h["goals_for_avg"], 1.35) / 2.50, 1.0) * 0.16
        + min(val(a["goals_against_avg"], 1.25) / 2.35, 1.0) * 0.14
        + min((val(h["ht_for_avg"], 0.45) + val(h["sh_for_avg"], 0.55)) / 1.55, 1.0) * 0.10
    )
    candidates.append(make_candidate(
        features,
        "home_scores_both_halves",
        raw,
        "Ev sahibinin iki yarıda da gol bulma ve rakibin iki yarıda gol yeme profili destekliyor.",
    ))

    # Deplasman iki yarıda da gol
    raw = (
        val(a["scores_both_halves_rate"], 0.18) * 0.36
        + val(h["concedes_both_halves_rate"], 0.18) * 0.24
        + min(val(a["goals_for_avg"], 1.20) / 2.50, 1.0) * 0.16
        + min(val(h["goals_against_avg"], 1.20) / 2.35, 1.0) * 0.14
        + min((val(a["ht_for_avg"], 0.40) + val(a["sh_for_avg"], 0.50)) / 1.55, 1.0) * 0.10
    )
    candidates.append(make_candidate(
        features,
        "away_scores_both_halves",
        raw,
        "Deplasmanın iki yarıda da gol bulma ve ev sahibinin iki yarıda gol yeme profili destekliyor.",
    ))

    # Ev sahibi iki yarıyı da kazanır
    raw = (
        val(h["wins_both_halves_rate"], 0.10) * 0.42
        + min(val(h["goals_for_avg"], 1.45) / 2.80, 1.0) * 0.18
        + min(val(a["goals_against_avg"], 1.35) / 2.60, 1.0) * 0.16
        + val(h["wins_first_half_rate"], 0.25) * 0.12
        + val(h["wins_second_half_rate"], 0.28) * 0.12
    )
    candidates.append(make_candidate(
        features,
        "home_wins_both_halves",
        raw,
        "Ev sahibinin iki yarıda üstünlük kurma, gol üretme ve rakibin zayıf savunma profili destekliyor.",
    ))

    # Deplasman iki yarıyı da kazanır
    raw = (
        val(a["wins_both_halves_rate"], 0.08) * 0.42
        + min(val(a["goals_for_avg"], 1.30) / 2.80, 1.0) * 0.18
        + min(val(h["goals_against_avg"], 1.30) / 2.60, 1.0) * 0.16
        + val(a["wins_first_half_rate"], 0.22) * 0.12
        + val(a["wins_second_half_rate"], 0.25) * 0.12
    )
    candidates.append(make_candidate(
        features,
        "away_wins_both_halves",
        raw,
        "Deplasmanın iki yarıda üstünlük kurma ve ev sahibinin gol yeme profili destekliyor.",
    ))

    # Gol aralığı 0-1
    expected_goals = val(h["goals_for_avg"], 1.2) + val(a["goals_for_avg"], 1.1)
    conceded_mix = val(h["goals_against_avg"], 1.1) + val(a["goals_against_avg"], 1.1)

    low_goal_signal = 1.0 - min((expected_goals + conceded_mix * 0.35) / 3.40, 1.0)
    raw = (
        val(h["goal_range_0_1_rate"], 0.18) * 0.26
        + val(a["goal_range_0_1_rate"], 0.18) * 0.26
        + val(l["goal_range_0_1_rate"], 0.18) * 0.16
        + low_goal_signal * 0.24
        + val(allp["goal_range_0_1_rate"], 0.18) * 0.08
    )
    candidates.append(make_candidate(
        features,
        "total_goals_0_1",
        raw,
        "Düşük gol temposu ve 0-1 gol aralığı geçmişi bu aralığı destekliyor.",
    ))

    # Gol aralığı 2-3
    balanced_goal_signal = 1.0 - min(abs(expected_goals - 2.45) / 2.45, 1.0)
    raw = (
        val(h["goal_range_2_3_rate"], 0.42) * 0.28
        + val(a["goal_range_2_3_rate"], 0.42) * 0.28
        + val(l["goal_range_2_3_rate"], 0.42) * 0.16
        + balanced_goal_signal * 0.20
        + val(allp["goal_range_2_3_rate"], 0.42) * 0.08
    )
    candidates.append(make_candidate(
        features,
        "total_goals_2_3",
        raw,
        "Maçın gol beklentisi 2-3 gol bandına yakın görünüyor.",
    ))

    # Gol aralığı 4-5
    high_goal_signal = min((expected_goals + conceded_mix * 0.35) / 3.80, 1.0)
    raw = (
        val(h["goal_range_4_5_rate"], 0.20) * 0.26
        + val(a["goal_range_4_5_rate"], 0.20) * 0.26
        + val(l["goal_range_4_5_rate"], 0.20) * 0.14
        + high_goal_signal * 0.24
        + min(conceded_mix / 3.40, 1.0) * 0.10
    )
    candidates.append(make_candidate(
        features,
        "total_goals_4_5",
        raw,
        "Yüksek gol üretimi ve savunma açıklığı 4-5 gol aralığını destekliyor.",
    ))

    # Gol aralığı 6+
    very_high_goal_signal = min((expected_goals + conceded_mix * 0.50) / 4.70, 1.0)
    raw = (
        val(h["goal_range_6_plus_rate"], 0.05) * 0.34
        + val(a["goal_range_6_plus_rate"], 0.05) * 0.34
        + val(l["goal_range_6_plus_rate"], 0.05) * 0.10
        + very_high_goal_signal * 0.16
        + min(conceded_mix / 4.20, 1.0) * 0.06
    )
    candidates.append(make_candidate(
        features,
        "total_goals_6_plus",
        raw,
        "Çok yüksek gol profili ve açık savunma sinyali 6+ gol aralığını destekliyor.",
    ))

    # İki yarıda 1.5 üst
    fh_attack = val(h["ht_for_avg"], 0.45) + val(a["ht_for_avg"], 0.40)
    sh_attack = val(h["sh_for_avg"], 0.55) + val(a["sh_for_avg"], 0.50)

    raw = (
        val(h["both_halves_over_1_5_rate"], 0.12) * 0.30
        + val(a["both_halves_over_1_5_rate"], 0.12) * 0.30
        + val(l["both_halves_over_1_5_rate"], 0.12) * 0.12
        + min(fh_attack / 1.45, 1.0) * 0.14
        + min(sh_attack / 1.60, 1.0) * 0.14
    )
    candidates.append(make_candidate(
        features,
        "both_halves_over_1_5",
        raw,
        "İki yarının da gollü geçme profili iki yarıda 1.5 üstü destekliyor.",
    ))

    # İki yarıda 1.5 alt
    raw = (
        val(h["both_halves_under_1_5_rate"], 0.58) * 0.28
        + val(a["both_halves_under_1_5_rate"], 0.58) * 0.28
        + val(l["both_halves_under_1_5_rate"], 0.58) * 0.16
        + (1.0 - min(fh_attack / 1.90, 1.0)) * 0.14
        + (1.0 - min(sh_attack / 2.05, 1.0)) * 0.14
    )
    candidates.append(make_candidate(
        features,
        "both_halves_under_1_5",
        raw,
        "İki yarıda da düşük gol temposu iki yarıda 1.5 altı destekliyor.",
    ))

    # Korner marketleri bilinçli olarak kapalı.

    return candidates


def select_best_prediction(features: Dict[str, Any], min_confidence: float) -> Optional[Dict[str, Any]]:
    candidates = generate_candidates(features)

    valid = []
    for c in candidates:
        if c["market_type"] in DISABLED_MARKET_TYPES:
            continue
        market_floor = max(min_confidence, MARKET_MIN_CONFIDENCE.get(c["selection"], min_confidence))
        if c["confidence_score"] >= market_floor and c["selection_score"] >= market_floor:
            c["market_floor"] = round(market_floor, 4)
            valid.append(c)

    if not valid:
        return None

    valid.sort(key=lambda x: x["selection_score"], reverse=True)
    best = valid[0]

    best["reason_text"] = (
        best["reason_text"]
        + f" Confidence: {best['confidence_score']}. Seçim skoru: {best['selection_score']}. Market alt sınırı: {best.get('market_floor', min_confidence)}."
    )

    return best


# ============================================================
# MAIN
# ============================================================

def print_config() -> None:
    print("[AYARLAR]")
    print(f"MODEL_VERSION       : {MODEL_VERSION}")
    print(f"LOOKBACK_DAYS       : {LOOKBACK_DAYS}")
    print(f"TARGET_DAYS_AHEAD   : {TARGET_DAYS_AHEAD}")
    print(f"MIN_TEAM_MATCHES    : {MIN_TEAM_MATCHES}")
    print(f"MIN_LEAGUE_MATCHES  : {MIN_LEAGUE_MATCHES}")
    print(f"MIN_CONFIDENCE      : {MIN_CONFIDENCE}")
    print("ODDS_USAGE          : disabled")
    print("MARKETS             : halves, goal_range")
    print("CORNERS             : disabled")
    print(f"MARKET_FLOORS       : {MARKET_MIN_CONFIDENCE}")


def process_fixture(db: DB, fixture: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    event_id = fixture["event_id"]
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]
    tournament_id = fixture.get("tournament_id")

    print(f"\n[MAÇ] {home_team} - {away_team} | event_id={event_id}")

    history = db.get_history(
        home_team=home_team,
        away_team=away_team,
        tournament_id=tournament_id,
        lookback_days=LOOKBACK_DAYS,
    )

    if not history:
        print("[ATLANDI] Geçmiş veri yok.")
        return None

    features = build_features(fixture, history)

    home_n = features["home"]["sample_size"]
    away_n = features["away"]["sample_size"]
    league_n = features["league"]["sample_size"]

    home_half_n = features["home"]["half_sample_size"]
    away_half_n = features["away"]["half_sample_size"]
    home_corner_n = features["home"]["corner_sample_size"]
    away_corner_n = features["away"]["corner_sample_size"]

    print(
        f"[ÖRNEKLEM] input={features['input_sample_size']} "
        f"| home={home_n} | away={away_n} | league={league_n} "
        f"| half={home_half_n}/{away_half_n} | corner={home_corner_n}/{away_corner_n}"
    )

    if home_n < MIN_TEAM_MATCHES:
        print(f"[ATLANDI] Ev sahibi örneklem düşük: {home_n}")
        return None

    if away_n < MIN_TEAM_MATCHES:
        print(f"[ATLANDI] Deplasman örneklem düşük: {away_n}")
        return None

    if league_n < MIN_LEAGUE_MATCHES:
        print(f"[UYARI] Lig örneklemi düşük: {league_n}. Takım verisiyle devam ediliyor.")

    prediction = select_best_prediction(features, MIN_CONFIDENCE)

    if prediction is None:
        print("[ATLANDI] Yeterli confidence veren özel market yok.")
        return None

    db.upsert_prediction(prediction)

    print(
        "[YAZILDI] "
        f"{prediction['market_name']} / {prediction['selection_name']} "
        f"| confidence={prediction['confidence_score']} "
        f"| selection_score={prediction['selection_score']} "
        f"| label={prediction['confidence_label']}"
    )
    print(f"[NEDEN] {prediction['reason_text']}")

    return prediction


def main() -> None:
    print_config()

    db = DB(DB_CONFIG)

    total_fixtures = 0
    total_predictions = 0
    total_skipped = 0

    market_counter: Dict[str, int] = {}

    try:
        db.connect()

        fixtures = db.get_target_fixtures(days_ahead=TARGET_DAYS_AHEAD)
        total_fixtures = len(fixtures)

        print(f"\n[TAHMİN] İşlenecek fikstür sayısı: {total_fixtures}")

        if total_fixtures == 0:
            print("[SONUÇ] Bugün/yarın için başlamamış fikstür bulunamadı.")
            return

        for fixture in fixtures:
            prediction = process_fixture(db, fixture)

            if prediction:
                total_predictions += 1
                market_type = prediction.get("market_type", "unknown")
                market_counter[market_type] = market_counter.get(market_type, 0) + 1
            else:
                total_skipped += 1

        print("\n[SONUÇ]")
        print(f"Toplam fikstür     : {total_fixtures}")
        print(f"Üretilen tahmin    : {total_predictions}")
        print(f"Atlanan maç        : {total_skipped}")

        if market_counter:
            print("\n[MARKET DAĞILIMI]")
            for market_type, count in sorted(market_counter.items(), key=lambda x: x[1], reverse=True):
                print(f"{market_type:<22}: {count}")

        print("\n[BİTTİ] Dengelenmiş özel market tahmin üretimi tamamlandı.")

    except mysql.connector.Error as e:
        print(f"[MYSQL HATA] {e}")
        sys.exit(1)

    except Exception as e:
        print(f"[KRİTİK HATA] Tahmin üretimi durdu: {e}")
        sys.exit(1)

    finally:
        db.close()


if __name__ == "__main__":
    main()
