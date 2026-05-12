#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
generate_predictions.py

Stake uyumlu, oran kullanmayan futbol tahmin motoru.

Kullanılan tablo:
- results_football

Oluşturulan/yazılan tablo:
- predictions_football

Üretilen marketler:
- Full Time Result
- Double Chance
- Total Goals
- Both Teams To Score
- Home Team Total Goals
- Away Team Total Goals
- 1st Half Total Goals

Kullanılmayanlar:
- odds kolonları
- şut marketi
- korner marketi
- faul marketi
- kart proxy
"""

import os
import sys
from typing import Any, Dict, List, Optional

import mysql.connector


MODEL_VERSION = os.getenv("MODEL_VERSION", "v3_stake_playable_markets_no_odds")

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
MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "0.68"))
MAX_TEAM_PROFILE_MATCHES = int(os.getenv("MAX_TEAM_PROFILE_MATCHES", "20"))


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

  bookmaker VARCHAR(32) NULL,
  sport VARCHAR(32) NULL,

  market_type VARCHAR(64) NOT NULL,
  selection VARCHAR(64) NOT NULL,

  market_key VARCHAR(64) NULL,
  stake_market_name VARCHAR(128) NULL,
  selection_key VARCHAR(64) NULL,
  stake_selection_name VARCHAR(128) NULL,

  confidence_score FLOAT NOT NULL,
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
  KEY idx_stake_market (market_key, selection_key),
  KEY idx_confidence (confidence_score),
  KEY idx_model_version (model_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


STAKE_MARKET_MAP = {
    "home_win": ["result", "home_win", "full_time_result", "Full Time Result", "home", "Home"],
    "draw": ["result", "draw", "full_time_result", "Full Time Result", "draw", "Draw"],
    "away_win": ["result", "away_win", "full_time_result", "Full Time Result", "away", "Away"],

    "home_or_draw": ["double_chance", "home_or_draw", "double_chance", "Double Chance", "home_or_draw", "1X"],
    "home_or_away": ["double_chance", "home_or_away", "double_chance", "Double Chance", "home_or_away", "12"],
    "draw_or_away": ["double_chance", "draw_or_away", "double_chance", "Double Chance", "draw_or_away", "X2"],

    "over_0_5": ["goals", "over_0_5", "total_goals", "Total Goals", "over_0_5", "Over 0.5"],
    "over_1_5": ["goals", "over_1_5", "total_goals", "Total Goals", "over_1_5", "Over 1.5"],
    "over_2_5": ["goals", "over_2_5", "total_goals", "Total Goals", "over_2_5", "Over 2.5"],
    "over_3_5": ["goals", "over_3_5", "total_goals", "Total Goals", "over_3_5", "Over 3.5"],
    "under_2_5": ["goals", "under_2_5", "total_goals", "Total Goals", "under_2_5", "Under 2.5"],
    "under_3_5": ["goals", "under_3_5", "total_goals", "Total Goals", "under_3_5", "Under 3.5"],
    "under_4_5": ["goals", "under_4_5", "total_goals", "Total Goals", "under_4_5", "Under 4.5"],

    "btts_yes": ["btts", "btts_yes", "both_teams_to_score", "Both Teams To Score", "yes", "Yes"],
    "btts_no": ["btts", "btts_no", "both_teams_to_score", "Both Teams To Score", "no", "No"],

    "home_over_0_5": ["team_goals", "home_over_0_5", "home_team_total_goals", "Home Team Total Goals", "home_over_0_5", "Over 0.5"],
    "home_over_1_5": ["team_goals", "home_over_1_5", "home_team_total_goals", "Home Team Total Goals", "home_over_1_5", "Over 1.5"],
    "home_under_1_5": ["team_goals", "home_under_1_5", "home_team_total_goals", "Home Team Total Goals", "home_under_1_5", "Under 1.5"],

    "away_over_0_5": ["team_goals", "away_over_0_5", "away_team_total_goals", "Away Team Total Goals", "away_over_0_5", "Over 0.5"],
    "away_over_1_5": ["team_goals", "away_over_1_5", "away_team_total_goals", "Away Team Total Goals", "away_over_1_5", "Over 1.5"],
    "away_under_1_5": ["team_goals", "away_under_1_5", "away_team_total_goals", "Away Team Total Goals", "away_under_1_5", "Under 1.5"],

    "first_half_over_0_5": ["first_half_goals", "first_half_over_0_5", "first_half_total_goals", "1st Half Total Goals", "first_half_over_0_5", "Over 0.5"],
    "first_half_under_1_5": ["first_half_goals", "first_half_under_1_5", "first_half_total_goals", "1st Half Total Goals", "first_half_under_1_5", "Under 1.5"],
}


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
    if score >= 0.78:
        return "high"
    if score >= 0.68:
        return "medium"
    return "low"


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
            "bookmaker": "VARCHAR(32) NULL",
            "sport": "VARCHAR(32) NULL",
            "market_key": "VARCHAR(64) NULL",
            "stake_market_name": "VARCHAR(128) NULL",
            "selection_key": "VARCHAR(64) NULL",
            "stake_selection_name": "VARCHAR(128) NULL",
            "input_sample_size": "INT NULL",
            "home_sample_size": "INT NULL",
            "away_sample_size": "INT NULL",
            "league_sample_size": "INT NULL",
            "reason_text": "TEXT NULL",
            "model_version": "VARCHAR(64) NOT NULL DEFAULT 'unknown'",
            "confidence_label": "VARCHAR(32) NOT NULL DEFAULT 'low'",
        }

        for col, definition in columns.items():
            try:
                self.cur.execute(f"ALTER TABLE predictions_football ADD COLUMN {col} {definition};")
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
            poss_h,
            poss_a,
            shot_h,
            shot_a,
            shot_on_h,
            shot_on_a
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
            bookmaker,
            sport,
            market_type,
            selection,
            market_key,
            stake_market_name,
            selection_key,
            stake_selection_name,
            confidence_score,
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
            %(bookmaker)s,
            %(sport)s,
            %(market_type)s,
            %(selection)s,
            %(market_key)s,
            %(stake_market_name)s,
            %(selection_key)s,
            %(stake_selection_name)s,
            %(confidence_score)s,
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
            bookmaker = VALUES(bookmaker),
            sport = VALUES(sport),
            market_type = VALUES(market_type),
            selection = VALUES(selection),
            market_key = VALUES(market_key),
            stake_market_name = VALUES(stake_market_name),
            selection_key = VALUES(selection_key),
            stake_selection_name = VALUES(stake_selection_name),
            confidence_score = VALUES(confidence_score),
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


def team_match_view(row: Dict[str, Any], team: str) -> Optional[Dict[str, Any]]:
    is_home = row.get("home_team") == team
    is_away = row.get("away_team") == team

    if not is_home and not is_away:
        return None

    ft_home = safe_float(row.get("ft_home"), 0.0)
    ft_away = safe_float(row.get("ft_away"), 0.0)

    ht_home = safe_float(row.get("ht_home"), None)
    ht_away = safe_float(row.get("ht_away"), None)

    if ft_home is None or ft_away is None:
        return None

    if is_home:
        goals_for = ft_home
        goals_against = ft_away
        ht_goals_for = ht_home
        ht_goals_against = ht_away
        shots_for = safe_float(row.get("shot_h"), None)
        shots_against = safe_float(row.get("shot_a"), None)
        shots_on_for = safe_float(row.get("shot_on_h"), None)
        possession = safe_float(row.get("poss_h"), None)
        win = ft_home > ft_away
        draw = ft_home == ft_away
        loss = ft_home < ft_away
    else:
        goals_for = ft_away
        goals_against = ft_home
        ht_goals_for = ht_away
        ht_goals_against = ht_home
        shots_for = safe_float(row.get("shot_a"), None)
        shots_against = safe_float(row.get("shot_h"), None)
        shots_on_for = safe_float(row.get("shot_on_a"), None)
        possession = safe_float(row.get("poss_a"), None)
        win = ft_away > ft_home
        draw = ft_away == ft_home
        loss = ft_away < ft_home

    total_goals = ft_home + ft_away
    first_half_goals = None

    if ht_home is not None and ht_away is not None:
        first_half_goals = ht_home + ht_away

    return {
        "goals_for": goals_for,
        "goals_against": goals_against,
        "total_goals": total_goals,
        "ht_goals_for": ht_goals_for,
        "ht_goals_against": ht_goals_against,
        "first_half_goals": first_half_goals,
        "shots_for": shots_for,
        "shots_against": shots_against,
        "shots_on_for": shots_on_for,
        "possession": possession,
        "win": win,
        "draw": draw,
        "loss": loss,
        "not_lose": win or draw,
        "not_draw": win or loss,
        "scored": goals_for > 0,
        "scored_2plus": goals_for >= 2,
        "conceded": goals_against > 0,
        "conceded_2plus": goals_against >= 2,
        "btts": goals_for > 0 and goals_against > 0,
        "over_05": total_goals >= 1,
        "over_15": total_goals >= 2,
        "over_25": total_goals >= 3,
        "over_35": total_goals >= 4,
        "under_25": total_goals <= 2,
        "under_35": total_goals <= 3,
        "under_45": total_goals <= 4,
        "first_half_over_05": first_half_goals is not None and first_half_goals >= 1,
        "first_half_under_15": first_half_goals is not None and first_half_goals <= 1,
    }


def build_team_profile(history: List[Dict[str, Any]], team: str) -> Dict[str, Any]:
    views = []

    for row in history:
        view = team_match_view(row, team)
        if view:
            views.append(view)

    views = views[:MAX_TEAM_PROFILE_MATCHES]

    return {
        "sample_size": len(views),
        "goals_for_avg": avg([v["goals_for"] for v in views]),
        "goals_against_avg": avg([v["goals_against"] for v in views]),
        "total_goals_avg": avg([v["total_goals"] for v in views]),
        "ht_goals_for_avg": avg([v["ht_goals_for"] for v in views]),
        "ht_goals_against_avg": avg([v["ht_goals_against"] for v in views]),
        "first_half_goals_avg": avg([v["first_half_goals"] for v in views]),
        "shots_for_avg": avg([v["shots_for"] for v in views]),
        "shots_against_avg": avg([v["shots_against"] for v in views]),
        "shots_on_for_avg": avg([v["shots_on_for"] for v in views]),
        "possession_avg": avg([v["possession"] for v in views]),
        "win_rate": rate([v["win"] for v in views]),
        "draw_rate": rate([v["draw"] for v in views]),
        "loss_rate": rate([v["loss"] for v in views]),
        "not_lose_rate": rate([v["not_lose"] for v in views]),
        "not_draw_rate": rate([v["not_draw"] for v in views]),
        "scored_rate": rate([v["scored"] for v in views]),
        "scored_2plus_rate": rate([v["scored_2plus"] for v in views]),
        "conceded_rate": rate([v["conceded"] for v in views]),
        "conceded_2plus_rate": rate([v["conceded_2plus"] for v in views]),
        "btts_rate": rate([v["btts"] for v in views]),
        "over_05_rate": rate([v["over_05"] for v in views]),
        "over_15_rate": rate([v["over_15"] for v in views]),
        "over_25_rate": rate([v["over_25"] for v in views]),
        "over_35_rate": rate([v["over_35"] for v in views]),
        "under_25_rate": rate([v["under_25"] for v in views]),
        "under_35_rate": rate([v["under_35"] for v in views]),
        "under_45_rate": rate([v["under_45"] for v in views]),
        "first_half_over_05_rate": rate([
            v["first_half_over_05"] for v in views if v["first_half_goals"] is not None
        ]),
        "first_half_under_15_rate": rate([
            v["first_half_under_15"] for v in views if v["first_half_goals"] is not None
        ]),
    }


def build_league_profile(history: List[Dict[str, Any]], tournament_id: Optional[int]) -> Dict[str, Any]:
    rows = [
        row for row in history
        if tournament_id is not None and row.get("tournament_id") == tournament_id
    ]

    goals = []
    first_half_goals = []
    home_win_flags = []
    draw_flags = []
    away_win_flags = []
    btts_flags = []
    over_05_flags = []
    over_15_flags = []
    over_25_flags = []
    over_35_flags = []
    under_25_flags = []
    under_35_flags = []
    under_45_flags = []
    first_half_over_05_flags = []
    first_half_under_15_flags = []

    for row in rows:
        ft_home = safe_float(row.get("ft_home"), 0.0)
        ft_away = safe_float(row.get("ft_away"), 0.0)

        if ft_home is None or ft_away is None:
            continue

        total_goals = ft_home + ft_away
        goals.append(total_goals)

        ht_home = safe_float(row.get("ht_home"), None)
        ht_away = safe_float(row.get("ht_away"), None)

        if ht_home is not None and ht_away is not None:
            fh_goals = ht_home + ht_away
            first_half_goals.append(fh_goals)
            first_half_over_05_flags.append(fh_goals >= 1)
            first_half_under_15_flags.append(fh_goals <= 1)

        home_win_flags.append(ft_home > ft_away)
        draw_flags.append(ft_home == ft_away)
        away_win_flags.append(ft_away > ft_home)
        btts_flags.append(ft_home > 0 and ft_away > 0)
        over_05_flags.append(total_goals >= 1)
        over_15_flags.append(total_goals >= 2)
        over_25_flags.append(total_goals >= 3)
        over_35_flags.append(total_goals >= 4)
        under_25_flags.append(total_goals <= 2)
        under_35_flags.append(total_goals <= 3)
        under_45_flags.append(total_goals <= 4)

    return {
        "sample_size": len(rows),
        "goals_avg": avg(goals),
        "first_half_goals_avg": avg(first_half_goals),
        "home_win_rate": rate(home_win_flags),
        "draw_rate": rate(draw_flags),
        "away_win_rate": rate(away_win_flags),
        "btts_rate": rate(btts_flags),
        "over_05_rate": rate(over_05_flags),
        "over_15_rate": rate(over_15_flags),
        "over_25_rate": rate(over_25_flags),
        "over_35_rate": rate(over_35_flags),
        "under_25_rate": rate(under_25_flags),
        "under_35_rate": rate(under_35_flags),
        "under_45_rate": rate(under_45_flags),
        "first_half_over_05_rate": rate(first_half_over_05_flags),
        "first_half_under_15_rate": rate(first_half_under_15_flags),
    }


def build_features(fixture: Dict[str, Any], history: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "fixture": fixture,
        "home": build_team_profile(history, fixture["home_team"]),
        "away": build_team_profile(history, fixture["away_team"]),
        "league": build_league_profile(history, fixture.get("tournament_id")),
        "input_sample_size": len(history),
    }


def sample_quality(home_n: int, away_n: int, league_n: int) -> float:
    team_part = min(home_n, away_n) / 10.0
    league_part = league_n / 50.0
    return clamp((team_part * 0.70) + (league_part * 0.30))


def make_candidate(
    features: Dict[str, Any],
    code: str,
    raw_score: float,
    reason: str,
) -> Dict[str, Any]:
    fixture = features["fixture"]
    market_type, selection, market_key, stake_market_name, selection_key, stake_selection_name = STAKE_MARKET_MAP[code]

    home_n = int(features["home"]["sample_size"])
    away_n = int(features["away"]["sample_size"])
    league_n = int(features["league"]["sample_size"])

    quality = sample_quality(home_n, away_n, league_n)
    final_score = clamp((raw_score * 0.85) + (quality * 0.15))

    return {
        "event_id": fixture["event_id"],
        "start_utc": fixture.get("start_utc"),
        "start_time_utc": fixture.get("start_time_utc"),
        "home_team": fixture.get("home_team"),
        "away_team": fixture.get("away_team"),
        "tournament_id": fixture.get("tournament_id"),
        "tournament_name": fixture.get("tournament_name"),
        "country": fixture.get("country"),
        "bookmaker": "stake",
        "sport": "football",
        "market_type": market_type,
        "selection": selection,
        "market_key": market_key,
        "stake_market_name": stake_market_name,
        "selection_key": selection_key,
        "stake_selection_name": stake_selection_name,
        "confidence_score": round(final_score, 4),
        "confidence_label": confidence_label(final_score),
        "model_version": MODEL_VERSION,
        "reason_text": reason,
        "input_sample_size": int(features["input_sample_size"]),
        "home_sample_size": home_n,
        "away_sample_size": away_n,
        "league_sample_size": league_n,
    }


def generate_candidates(features: Dict[str, Any]) -> List[Dict[str, Any]]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    candidates = []

    raw_home_win = (
        val(h["win_rate"], 0.42) * 0.28
        + val(a["loss_rate"], 0.35) * 0.22
        + val(l["home_win_rate"], 0.43) * 0.12
        + min(val(h["goals_for_avg"], 1.2) / 2.2, 1.0) * 0.18
        + min(val(a["goals_against_avg"], 1.2) / 2.2, 1.0) * 0.20
    )
    candidates.append(make_candidate(features, "home_win", raw_home_win, "Ev sahibinin kazanma ve gol üretme profili Home tarafını destekliyor."))

    raw_draw = (
        val(h["draw_rate"], 0.25) * 0.22
        + val(a["draw_rate"], 0.25) * 0.22
        + val(l["draw_rate"], 0.25) * 0.16
        + val(h["under_25_rate"], 0.45) * 0.20
        + val(a["under_25_rate"], 0.45) * 0.20
    )
    candidates.append(make_candidate(features, "draw", raw_draw, "Beraberlik oranları ve düşük skor sinyali Draw ihtimalini destekliyor."))

    raw_away_win = (
        val(a["win_rate"], 0.32) * 0.28
        + val(h["loss_rate"], 0.30) * 0.22
        + val(l["away_win_rate"], 0.30) * 0.12
        + min(val(a["goals_for_avg"], 1.1) / 2.2, 1.0) * 0.18
        + min(val(h["goals_against_avg"], 1.2) / 2.2, 1.0) * 0.20
    )
    candidates.append(make_candidate(features, "away_win", raw_away_win, "Deplasmanın kazanma ve gol üretme profili Away tarafını destekliyor."))

    raw_1x = (
        val(h["not_lose_rate"], 0.65) * 0.38
        + (1.0 - val(a["win_rate"], 0.30)) * 0.26
        + min(val(h["goals_for_avg"], 1.2) / 2.0, 1.0) * 0.18
        + min(val(a["goals_against_avg"], 1.2) / 2.0, 1.0) * 0.18
    )
    candidates.append(make_candidate(features, "home_or_draw", raw_1x, "Ev sahibinin kaybetmeme profili 1X tarafını destekliyor."))

    raw_12 = (
        val(h["not_draw_rate"], 0.70) * 0.30
        + val(a["not_draw_rate"], 0.70) * 0.30
        + (1.0 - val(l["draw_rate"], 0.25)) * 0.20
        + val(h["over_15_rate"], 0.65) * 0.10
        + val(a["over_15_rate"], 0.65) * 0.10
    )
    candidates.append(make_candidate(features, "home_or_away", raw_12, "Beraberlik dışı sonuç üretme sinyali 12 tarafını destekliyor."))

    raw_x2 = (
        val(a["not_lose_rate"], 0.60) * 0.38
        + (1.0 - val(h["win_rate"], 0.40)) * 0.26
        + min(val(a["goals_for_avg"], 1.1) / 2.0, 1.0) * 0.18
        + min(val(h["goals_against_avg"], 1.2) / 2.0, 1.0) * 0.18
    )
    candidates.append(make_candidate(features, "draw_or_away", raw_x2, "Deplasmanın kaybetmeme profili X2 tarafını destekliyor."))

    raw_o05 = (
        val(h["over_05_rate"], 0.88) * 0.28
        + val(a["over_05_rate"], 0.88) * 0.28
        + val(l["over_05_rate"], 0.88) * 0.16
        + val(h["scored_rate"], 0.70) * 0.14
        + val(a["scored_rate"], 0.65) * 0.14
    )
    candidates.append(make_candidate(features, "over_0_5", raw_o05, "Maçlarda gol çıkma oranı Over 0.5 tarafını destekliyor."))

    raw_o15 = (
        val(h["over_15_rate"], 0.65) * 0.25
        + val(a["over_15_rate"], 0.65) * 0.25
        + val(l["over_15_rate"], 0.65) * 0.16
        + min((val(h["goals_for_avg"], 1.2) + val(a["goals_for_avg"], 1.1)) / 3.0, 1.0) * 0.18
        + min((val(h["goals_against_avg"], 1.1) + val(a["goals_against_avg"], 1.1)) / 3.0, 1.0) * 0.16
    )
    candidates.append(make_candidate(features, "over_1_5", raw_o15, "Gol ortalamaları ve 1.5 üst geçmişi Over 1.5 tarafını destekliyor."))

    raw_o25 = (
        val(h["over_25_rate"], 0.50) * 0.25
        + val(a["over_25_rate"], 0.50) * 0.25
        + val(l["over_25_rate"], 0.50) * 0.15
        + min((val(h["goals_for_avg"], 1.1) + val(a["goals_for_avg"], 1.1)) / 3.2, 1.0) * 0.18
        + min((val(h["goals_against_avg"], 1.1) + val(a["goals_against_avg"], 1.1)) / 3.2, 1.0) * 0.17
    )
    candidates.append(make_candidate(features, "over_2_5", raw_o25, "Gol üretimi ve 2.5 üst geçmişi Over 2.5 tarafını destekliyor."))

    raw_o35 = (
        val(h["over_35_rate"], 0.30) * 0.28
        + val(a["over_35_rate"], 0.30) * 0.28
        + val(l["over_35_rate"], 0.30) * 0.14
        + min((val(h["goals_for_avg"], 1.3) + val(a["goals_for_avg"], 1.2)) / 4.0, 1.0) * 0.15
        + min((val(h["goals_against_avg"], 1.2) + val(a["goals_against_avg"], 1.2)) / 4.0, 1.0) * 0.15
    )
    candidates.append(make_candidate(features, "over_3_5", raw_o35, "Yüksek gol temposu Over 3.5 tarafını destekliyor."))

    raw_u25 = (
        val(h["under_25_rate"], 0.45) * 0.30
        + val(a["under_25_rate"], 0.45) * 0.30
        + val(l["under_25_rate"], 0.45) * 0.18
        + (1.0 - min((val(h["goals_for_avg"], 1.1) + val(a["goals_for_avg"], 1.1)) / 3.8, 1.0)) * 0.22
    )
    candidates.append(make_candidate(features, "under_2_5", raw_u25, "Düşük gol üretimi Under 2.5 tarafını destekliyor."))

    raw_u35 = (
        val(h["under_35_rate"], 0.70) * 0.30
        + val(a["under_35_rate"], 0.70) * 0.30
        + val(l["under_35_rate"], 0.70) * 0.18
        + (1.0 - min((val(h["goals_for_avg"], 1.2) + val(a["goals_for_avg"], 1.2)) / 4.2, 1.0)) * 0.22
    )
    candidates.append(make_candidate(features, "under_3_5", raw_u35, "Gol temposu 3.5 altında kalma tarafını destekliyor."))

    raw_u45 = (
        val(h["under_45_rate"], 0.82) * 0.34
        + val(a["under_45_rate"], 0.82) * 0.34
        + val(l["under_45_rate"], 0.82) * 0.22
        + (1.0 - val(h["over_35_rate"], 0.25)) * 0.05
        + (1.0 - val(a["over_35_rate"], 0.25)) * 0.05
    )
    candidates.append(make_candidate(features, "under_4_5", raw_u45, "Çok yüksek skor sinyali düşük; Under 4.5 tarafı destekleniyor."))

    raw_btts_yes = (
        val(h["btts_rate"], 0.50) * 0.20
        + val(a["btts_rate"], 0.50) * 0.20
        + val(l["btts_rate"], 0.50) * 0.12
        + val(h["scored_rate"], 0.68) * 0.14
        + val(a["scored_rate"], 0.62) * 0.14
        + val(h["conceded_rate"], 0.60) * 0.10
        + val(a["conceded_rate"], 0.60) * 0.10
    )
    candidates.append(make_candidate(features, "btts_yes", raw_btts_yes, "İki tarafın gol bulma ve gol yeme profili BTTS Yes tarafını destekliyor."))

    raw_btts_no = (
        (1.0 - val(h["btts_rate"], 0.50)) * 0.25
        + (1.0 - val(a["btts_rate"], 0.50)) * 0.25
        + (1.0 - val(l["btts_rate"], 0.50)) * 0.16
        + max(1.0 - val(h["scored_rate"], 0.68), 1.0 - val(a["scored_rate"], 0.62)) * 0.22
        + val(h["under_35_rate"], 0.70) * 0.06
        + val(a["under_35_rate"], 0.70) * 0.06
    )
    candidates.append(make_candidate(features, "btts_no", raw_btts_no, "KG oranları düşük; BTTS No tarafı destekleniyor."))

    raw_home_o05 = (
        val(h["scored_rate"], 0.70) * 0.34
        + val(a["conceded_rate"], 0.62) * 0.26
        + min(val(h["goals_for_avg"], 1.2) / 2.0, 1.0) * 0.22
        + min(val(a["goals_against_avg"], 1.2) / 2.0, 1.0) * 0.18
    )
    candidates.append(make_candidate(features, "home_over_0_5", raw_home_o05, "Ev sahibinin gol bulma profili Home Team Over 0.5 tarafını destekliyor."))

    raw_home_o15 = (
        val(h["scored_2plus_rate"], 0.35) * 0.34
        + val(a["conceded_2plus_rate"], 0.30) * 0.26
        + min(val(h["goals_for_avg"], 1.3) / 2.6, 1.0) * 0.24
        + min(val(a["goals_against_avg"], 1.3) / 2.6, 1.0) * 0.16
    )
    candidates.append(make_candidate(features, "home_over_1_5", raw_home_o15, "Ev sahibinin 2+ gol üretme profili Home Team Over 1.5 tarafını destekliyor."))

    raw_home_u15 = (
        (1.0 - val(h["scored_2plus_rate"], 0.35)) * 0.36
        + (1.0 - val(a["conceded_2plus_rate"], 0.30)) * 0.26
        + (1.0 - min(val(h["goals_for_avg"], 1.2) / 2.5, 1.0)) * 0.24
        + val(l["under_35_rate"], 0.70) * 0.14
    )
    candidates.append(make_candidate(features, "home_under_1_5", raw_home_u15, "Ev sahibinin 2+ gol sinyali sınırlı; Home Team Under 1.5 destekleniyor."))

    raw_away_o05 = (
        val(a["scored_rate"], 0.62) * 0.34
        + val(h["conceded_rate"], 0.62) * 0.26
        + min(val(a["goals_for_avg"], 1.1) / 2.0, 1.0) * 0.22
        + min(val(h["goals_against_avg"], 1.2) / 2.0, 1.0) * 0.18
    )
    candidates.append(make_candidate(features, "away_over_0_5", raw_away_o05, "Deplasmanın gol bulma profili Away Team Over 0.5 tarafını destekliyor."))

    raw_away_o15 = (
        val(a["scored_2plus_rate"], 0.28) * 0.34
        + val(h["conceded_2plus_rate"], 0.30) * 0.26
        + min(val(a["goals_for_avg"], 1.2) / 2.6, 1.0) * 0.24
        + min(val(h["goals_against_avg"], 1.3) / 2.6, 1.0) * 0.16
    )
    candidates.append(make_candidate(features, "away_over_1_5", raw_away_o15, "Deplasmanın 2+ gol profili Away Team Over 1.5 tarafını destekliyor."))

    raw_away_u15 = (
        (1.0 - val(a["scored_2plus_rate"], 0.28)) * 0.36
        + (1.0 - val(h["conceded_2plus_rate"], 0.30)) * 0.26
        + (1.0 - min(val(a["goals_for_avg"], 1.1) / 2.5, 1.0)) * 0.24
        + val(l["under_35_rate"], 0.70) * 0.14
    )
    candidates.append(make_candidate(features, "away_under_1_5", raw_away_u15, "Deplasmanın 2+ gol sinyali sınırlı; Away Team Under 1.5 destekleniyor."))

    raw_fh_o05 = (
        val(h["first_half_over_05_rate"], 0.60) * 0.30
        + val(a["first_half_over_05_rate"], 0.60) * 0.30
        + val(l["first_half_over_05_rate"], 0.60) * 0.18
        + min((val(h["ht_goals_for_avg"], 0.45) + val(a["ht_goals_for_avg"], 0.40)) / 1.5, 1.0) * 0.22
    )
    candidates.append(make_candidate(features, "first_half_over_0_5", raw_fh_o05, "İlk yarı gol sinyali 1st Half Over 0.5 tarafını destekliyor."))

    raw_fh_u15 = (
        val(h["first_half_under_15_rate"], 0.72) * 0.32
        + val(a["first_half_under_15_rate"], 0.72) * 0.32
        + val(l["first_half_under_15_rate"], 0.72) * 0.18
        + (1.0 - min((val(h["ht_goals_for_avg"], 0.45) + val(a["ht_goals_for_avg"], 0.40)) / 2.0, 1.0)) * 0.18
    )
    candidates.append(make_candidate(features, "first_half_under_1_5", raw_fh_u15, "İlk yarı düşük skor profili 1st Half Under 1.5 tarafını destekliyor."))

    return candidates


def select_best_prediction(features: Dict[str, Any], min_confidence: float) -> Optional[Dict[str, Any]]:
    candidates = generate_candidates(features)
    valid = [c for c in candidates if c["confidence_score"] >= min_confidence]

    if not valid:
        return None

    valid.sort(key=lambda x: x["confidence_score"], reverse=True)
    return valid[0]


def print_config() -> None:
    print("[AYARLAR]")
    print(f"MODEL_VERSION       : {MODEL_VERSION}")
    print(f"LOOKBACK_DAYS       : {LOOKBACK_DAYS}")
    print(f"TARGET_DAYS_AHEAD   : {TARGET_DAYS_AHEAD}")
    print(f"MIN_TEAM_MATCHES    : {MIN_TEAM_MATCHES}")
    print(f"MIN_LEAGUE_MATCHES  : {MIN_LEAGUE_MATCHES}")
    print(f"MIN_CONFIDENCE      : {MIN_CONFIDENCE}")
    print("BOOKMAKER           : stake")
    print("ODDS_USAGE          : disabled")


def process_fixture(db: DB, fixture: Dict[str, Any]) -> bool:
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
        return False

    features = build_features(fixture, history)

    home_n = features["home"]["sample_size"]
    away_n = features["away"]["sample_size"]
    league_n = features["league"]["sample_size"]

    print(
        f"[ÖRNEKLEM] input={features['input_sample_size']} "
        f"| home={home_n} | away={away_n} | league={league_n}"
    )

    if home_n < MIN_TEAM_MATCHES:
        print(f"[ATLANDI] Ev sahibi örneklem düşük: {home_n}")
        return False

    if away_n < MIN_TEAM_MATCHES:
        print(f"[ATLANDI] Deplasman örneklem düşük: {away_n}")
        return False

    if league_n < MIN_LEAGUE_MATCHES:
        print(f"[UYARI] Lig örneklemi düşük: {league_n}. Takım verisiyle devam ediliyor.")

    prediction = select_best_prediction(features, MIN_CONFIDENCE)

    if prediction is None:
        print("[ATLANDI] Yeterli confidence veren Stake marketi yok.")
        return False

    db.upsert_prediction(prediction)

    print(
        "[YAZILDI] "
        f"{prediction['stake_market_name']} / {prediction['stake_selection_name']} "
        f"| confidence={prediction['confidence_score']} "
        f"| label={prediction['confidence_label']}"
    )
    print(f"[NEDEN] {prediction['reason_text']}")

    return True


def main() -> None:
    print_config()

    db = DB(DB_CONFIG)

    total_fixtures = 0
    total_predictions = 0
    total_skipped = 0

    try:
        db.connect()

        fixtures = db.get_target_fixtures(days_ahead=TARGET_DAYS_AHEAD)
        total_fixtures = len(fixtures)

        print(f"\n[TAHMİN] İşlenecek fikstür sayısı: {total_fixtures}")

        if total_fixtures == 0:
            print("[SONUÇ] Bugün/yarın için başlamamış fikstür bulunamadı.")
            return

        for fixture in fixtures:
            ok = process_fixture(db, fixture)

            if ok:
                total_predictions += 1
            else:
                total_skipped += 1

        print("\n[SONUÇ]")
        print(f"Toplam fikstür     : {total_fixtures}")
        print(f"Üretilen tahmin    : {total_predictions}")
        print(f"Atlanan maç        : {total_skipped}")
        print("[BİTTİ] Stake uyumlu tahmin üretimi tamamlandı.")

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
