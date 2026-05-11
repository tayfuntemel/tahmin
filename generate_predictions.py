#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
generate_predictions.py

Tek dosyalık oran kullanmayan futbol tahmin motoru.

Ne yapar?
- MySQL veritabanına bağlanır.
- predictions_football tablosunu oluşturur.
- results_football tablosundan bugün/yarın oynanacak başlamamış maçları alır.
- Son 550 günlük bitmiş maç verisinden takım/lig profili çıkarır.
- Gol, KG, korner, şut, isabetli şut, faul gibi marketleri skorlar.
- En yüksek güven veren marketi seçer.
- Tahmini predictions_football tablosuna yazar.

Oran kullanmaz:
- odds_1
- odds_x
- odds_2
- odds_btts
- odds_over_under
hiçbir şekilde SELECT edilmez ve modele girmez.
"""

import os
import sys
import datetime as dt
from typing import Dict, Any, List, Optional

import mysql.connector


# ============================================================
# CONFIG
# ============================================================

MODEL_VERSION = os.getenv("MODEL_VERSION", "v1_stat_signal_no_odds_single_file")

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
  KEY idx_confidence (confidence_score),
  KEY idx_model_version (model_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# ============================================================
# BASIC HELPERS
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
    if score >= 0.78:
        return "high"
    if score >= 0.68:
        return "medium"
    return "low"


def today_tr_date() -> dt.date:
    tz_tr = dt.timezone(dt.timedelta(hours=3))
    return dt.datetime.now(tz_tr).date()


# ============================================================
# DB LAYER
# ============================================================

class DB:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.conn = None
        self.cur = None

    def connect(self) -> None:
        self.close(silent=True)

        missing = [
            key for key, value in self.cfg.items()
            if key != "port" and not value
        ]

        if missing:
            raise RuntimeError(
                f"Eksik DB ortam değişkenleri: {missing}. "
                "DB_HOST, DB_USER, DB_PASSWORD, DB_NAME ayarlı olmalı."
            )

        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)

        self.cur.execute(CREATE_PREDICTIONS_TABLE)
        self.ensure_prediction_columns()

        print("[DB] Bağlantı başarılı.")
        print("[DB] predictions_football tablosu hazır.")

    def ensure_prediction_columns(self) -> None:
        """
        Eski tablo varsa eksik kolonları sessizce ekler.
        """
        columns = {
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
        """
        Bugün ve yarın oynanacak başlamamış maçları alır.
        Oran kolonları alınmaz.
        """
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
        """
        Son 550 günlük geçmiş.
        Oran kolonları alınmaz.
        """
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

            corn_h,
            corn_a,

            shot_h,
            shot_a,

            shot_on_h,
            shot_on_a,

            fouls_h,
            fouls_a,

            offsides_h,
            offsides_a,

            saves_h,
            saves_a,

            passes_h,
            passes_a,

            tackles_h,
            tackles_a,

            referee,
            formation_h,
            formation_a
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
            %(market_type)s,
            %(selection)s,
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
            market_type = VALUES(market_type),
            selection = VALUES(selection),
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


# ============================================================
# FEATURE BUILDER
# ============================================================

def team_match_view(row: Dict[str, Any], team: str) -> Optional[Dict[str, Any]]:
    """
    Bir maçı seçilen takım perspektifine çevirir.
    """
    is_home = row.get("home_team") == team
    is_away = row.get("away_team") == team

    if not is_home and not is_away:
        return None

    ft_home = safe_float(row.get("ft_home"), 0.0)
    ft_away = safe_float(row.get("ft_away"), 0.0)

    if ft_home is None or ft_away is None:
        return None

    if is_home:
        goals_for = ft_home
        goals_against = ft_away

        corners_for = safe_float(row.get("corn_h"), None)
        corners_against = safe_float(row.get("corn_a"), None)

        shots_for = safe_float(row.get("shot_h"), None)
        shots_against = safe_float(row.get("shot_a"), None)

        shots_on_for = safe_float(row.get("shot_on_h"), None)
        shots_on_against = safe_float(row.get("shot_on_a"), None)

        fouls_for = safe_float(row.get("fouls_h"), None)
        fouls_against = safe_float(row.get("fouls_a"), None)

        possession = safe_float(row.get("poss_h"), None)

        passes_for = safe_float(row.get("passes_h"), None)
        tackles_for = safe_float(row.get("tackles_h"), None)

    else:
        goals_for = ft_away
        goals_against = ft_home

        corners_for = safe_float(row.get("corn_a"), None)
        corners_against = safe_float(row.get("corn_h"), None)

        shots_for = safe_float(row.get("shot_a"), None)
        shots_against = safe_float(row.get("shot_h"), None)

        shots_on_for = safe_float(row.get("shot_on_a"), None)
        shots_on_against = safe_float(row.get("shot_on_h"), None)

        fouls_for = safe_float(row.get("fouls_a"), None)
        fouls_against = safe_float(row.get("fouls_h"), None)

        possession = safe_float(row.get("poss_a"), None)

        passes_for = safe_float(row.get("passes_a"), None)
        tackles_for = safe_float(row.get("tackles_a"), None)

    total_goals = ft_home + ft_away

    total_corners_raw = safe_float(row.get("corn_h"), 0.0) + safe_float(row.get("corn_a"), 0.0)
    total_shots_raw = safe_float(row.get("shot_h"), 0.0) + safe_float(row.get("shot_a"), 0.0)
    total_shots_on_raw = safe_float(row.get("shot_on_h"), 0.0) + safe_float(row.get("shot_on_a"), 0.0)
    total_fouls_raw = safe_float(row.get("fouls_h"), 0.0) + safe_float(row.get("fouls_a"), 0.0)

    total_corners = total_corners_raw if total_corners_raw > 0 else None
    total_shots = total_shots_raw if total_shots_raw > 0 else None
    total_shots_on = total_shots_on_raw if total_shots_on_raw > 0 else None
    total_fouls = total_fouls_raw if total_fouls_raw > 0 else None

    return {
        "goals_for": goals_for,
        "goals_against": goals_against,
        "total_goals": total_goals,

        "corners_for": corners_for,
        "corners_against": corners_against,
        "total_corners": total_corners,

        "shots_for": shots_for,
        "shots_against": shots_against,
        "total_shots": total_shots,

        "shots_on_for": shots_on_for,
        "shots_on_against": shots_on_against,
        "total_shots_on": total_shots_on,

        "fouls_for": fouls_for,
        "fouls_against": fouls_against,
        "total_fouls": total_fouls,

        "possession": possession,
        "passes_for": passes_for,
        "tackles_for": tackles_for,

        "scored": goals_for > 0,
        "conceded": goals_against > 0,
        "btts": goals_for > 0 and goals_against > 0,

        "over_05": total_goals > 0.5,
        "over_15": total_goals > 1.5,
        "over_25": total_goals > 2.5,
        "over_35": total_goals > 3.5,

        "under_25": total_goals < 2.5,
        "under_35": total_goals < 3.5,
        "under_45": total_goals < 4.5,
    }


def build_team_profile(
    history: List[Dict[str, Any]],
    team: str,
    limit: int = MAX_TEAM_PROFILE_MATCHES,
) -> Dict[str, Any]:
    views = []

    for row in history:
        view = team_match_view(row, team)
        if view:
            views.append(view)

    views = views[:limit]

    return {
        "sample_size": len(views),

        "goals_for_avg": avg([v["goals_for"] for v in views]),
        "goals_against_avg": avg([v["goals_against"] for v in views]),
        "total_goals_avg": avg([v["total_goals"] for v in views]),

        "corners_for_avg": avg([v["corners_for"] for v in views]),
        "corners_against_avg": avg([v["corners_against"] for v in views]),
        "total_corners_avg": avg([v["total_corners"] for v in views]),

        "shots_for_avg": avg([v["shots_for"] for v in views]),
        "shots_against_avg": avg([v["shots_against"] for v in views]),
        "total_shots_avg": avg([v["total_shots"] for v in views]),

        "shots_on_for_avg": avg([v["shots_on_for"] for v in views]),
        "shots_on_against_avg": avg([v["shots_on_against"] for v in views]),
        "total_shots_on_avg": avg([v["total_shots_on"] for v in views]),

        "fouls_for_avg": avg([v["fouls_for"] for v in views]),
        "fouls_against_avg": avg([v["fouls_against"] for v in views]),
        "total_fouls_avg": avg([v["total_fouls"] for v in views]),

        "possession_avg": avg([v["possession"] for v in views]),
        "passes_avg": avg([v["passes_for"] for v in views]),
        "tackles_avg": avg([v["tackles_for"] for v in views]),

        "scored_rate": rate([v["scored"] for v in views]),
        "conceded_rate": rate([v["conceded"] for v in views]),
        "btts_rate": rate([v["btts"] for v in views]),

        "over_05_rate": rate([v["over_05"] for v in views]),
        "over_15_rate": rate([v["over_15"] for v in views]),
        "over_25_rate": rate([v["over_25"] for v in views]),
        "over_35_rate": rate([v["over_35"] for v in views]),

        "under_25_rate": rate([v["under_25"] for v in views]),
        "under_35_rate": rate([v["under_35"] for v in views]),
        "under_45_rate": rate([v["under_45"] for v in views]),
    }


def build_league_profile(
    history: List[Dict[str, Any]],
    tournament_id: Optional[int],
) -> Dict[str, Any]:
    league_rows = [
        row for row in history
        if tournament_id is not None and row.get("tournament_id") == tournament_id
    ]

    goals = []
    corners = []
    shots = []
    shots_on = []
    fouls = []

    btts_flags = []
    over_05_flags = []
    over_15_flags = []
    over_25_flags = []
    over_35_flags = []
    under_35_flags = []

    home_win_flags = []

    for row in league_rows:
        ft_home = safe_float(row.get("ft_home"), 0.0)
        ft_away = safe_float(row.get("ft_away"), 0.0)

        if ft_home is None or ft_away is None:
            continue

        total_goals = ft_home + ft_away
        goals.append(total_goals)

        total_corners = safe_float(row.get("corn_h"), 0.0) + safe_float(row.get("corn_a"), 0.0)
        total_shots = safe_float(row.get("shot_h"), 0.0) + safe_float(row.get("shot_a"), 0.0)
        total_shots_on = safe_float(row.get("shot_on_h"), 0.0) + safe_float(row.get("shot_on_a"), 0.0)
        total_fouls = safe_float(row.get("fouls_h"), 0.0) + safe_float(row.get("fouls_a"), 0.0)

        if total_corners > 0:
            corners.append(total_corners)
        if total_shots > 0:
            shots.append(total_shots)
        if total_shots_on > 0:
            shots_on.append(total_shots_on)
        if total_fouls > 0:
            fouls.append(total_fouls)

        btts_flags.append(ft_home > 0 and ft_away > 0)
        over_05_flags.append(total_goals > 0.5)
        over_15_flags.append(total_goals > 1.5)
        over_25_flags.append(total_goals > 2.5)
        over_35_flags.append(total_goals > 3.5)
        under_35_flags.append(total_goals < 3.5)

        home_win_flags.append(ft_home > ft_away)

    return {
        "sample_size": len(league_rows),

        "goals_avg": avg(goals),
        "corners_avg": avg(corners),
        "shots_avg": avg(shots),
        "shots_on_avg": avg(shots_on),
        "fouls_avg": avg(fouls),

        "btts_rate": rate(btts_flags),
        "over_05_rate": rate(over_05_flags),
        "over_15_rate": rate(over_15_flags),
        "over_25_rate": rate(over_25_flags),
        "over_35_rate": rate(over_35_flags),
        "under_35_rate": rate(under_35_flags),

        "home_win_rate": rate(home_win_flags),
    }


def build_features(
    fixture: Dict[str, Any],
    history: List[Dict[str, Any]],
) -> Dict[str, Any]:
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]
    tournament_id = fixture.get("tournament_id")

    home_profile = build_team_profile(history, home_team)
    away_profile = build_team_profile(history, away_team)
    league_profile = build_league_profile(history, tournament_id)

    return {
        "fixture": fixture,
        "home": home_profile,
        "away": away_profile,
        "league": league_profile,
        "input_sample_size": len(history),
    }


# ============================================================
# MARKET ENGINE
# ============================================================

def sample_quality(home_n: int, away_n: int, league_n: int) -> float:
    team_part = min(home_n, away_n) / 10.0
    league_part = league_n / 50.0

    return clamp((team_part * 0.70) + (league_part * 0.30))


def make_candidate(
    market_type: str,
    selection: str,
    raw_score: float,
    reason: str,
    features: Dict[str, Any],
) -> Dict[str, Any]:
    fixture = features["fixture"]

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

        "market_type": market_type,
        "selection": selection,

        "confidence_score": round(final_score, 4),
        "confidence_label": confidence_label(final_score),

        "model_version": MODEL_VERSION,
        "reason_text": reason,

        "input_sample_size": int(features["input_sample_size"]),
        "home_sample_size": home_n,
        "away_sample_size": away_n,
        "league_sample_size": league_n,
    }


def score_goals_over_15(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    attack_signal = min((val(h["goals_for_avg"], 1.2) + val(a["goals_for_avg"], 1.1)) / 3.0, 1.0)
    concede_signal = min((val(h["goals_against_avg"], 1.1) + val(a["goals_against_avg"], 1.1)) / 3.0, 1.0)
    shots_signal = min((val(h["shots_for_avg"], 10.0) + val(a["shots_for_avg"], 10.0)) / 26.0, 1.0)

    raw_score = (
        val(h["over_15_rate"], 0.65) * 0.23
        + val(a["over_15_rate"], 0.65) * 0.23
        + val(l["over_15_rate"], 0.65) * 0.16
        + attack_signal * 0.18
        + concede_signal * 0.12
        + shots_signal * 0.08
    )

    return make_candidate(
        market_type="goals",
        selection="over_1_5",
        raw_score=raw_score,
        reason="İki takımın gol ortalaması, 1.5 üst oranı ve şut hacmi 1.5 üst tarafını destekliyor.",
        features=features,
    )


def score_goals_over_25(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    attack_signal = min((val(h["goals_for_avg"], 1.1) + val(a["goals_for_avg"], 1.1)) / 3.2, 1.0)
    concede_signal = min((val(h["goals_against_avg"], 1.1) + val(a["goals_against_avg"], 1.1)) / 3.2, 1.0)
    shot_signal = min((val(h["shots_for_avg"], 10.0) + val(a["shots_for_avg"], 10.0)) / 28.0, 1.0)
    sot_signal = min((val(h["shots_on_for_avg"], 3.5) + val(a["shots_on_for_avg"], 3.5)) / 9.0, 1.0)

    raw_score = (
        val(h["over_25_rate"], 0.50) * 0.21
        + val(a["over_25_rate"], 0.50) * 0.21
        + val(l["over_25_rate"], 0.50) * 0.14
        + attack_signal * 0.17
        + concede_signal * 0.12
        + shot_signal * 0.09
        + sot_signal * 0.06
    )

    return make_candidate(
        market_type="goals",
        selection="over_2_5",
        raw_score=raw_score,
        reason="Gol üretimi, yenen gol ortalaması, toplam şut ve isabetli şut hacmi 2.5 üst yönünde sinyal veriyor.",
        features=features,
    )


def score_goals_under_35(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    home_under = val(h["under_35_rate"], 0.70)
    away_under = val(a["under_35_rate"], 0.70)
    league_under = val(l["under_35_rate"], 0.70)

    low_attack_signal = 1.0 - min((val(h["goals_for_avg"], 1.2) + val(a["goals_for_avg"], 1.2)) / 4.0, 1.0)
    low_shot_signal = 1.0 - min((val(h["shots_for_avg"], 10.0) + val(a["shots_for_avg"], 10.0)) / 35.0, 1.0)

    raw_score = (
        home_under * 0.27
        + away_under * 0.27
        + league_under * 0.20
        + low_attack_signal * 0.16
        + low_shot_signal * 0.10
    )

    return make_candidate(
        market_type="goals",
        selection="under_3_5",
        raw_score=raw_score,
        reason="Takımların gol temposu ve lig profili 3.5 alt tarafını destekliyor.",
        features=features,
    )


def score_btts_yes(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    home_scores = val(h["scored_rate"], 0.65)
    away_scores = val(a["scored_rate"], 0.60)
    home_concedes = val(h["conceded_rate"], 0.60)
    away_concedes = val(a["conceded_rate"], 0.60)

    raw_score = (
        val(h["btts_rate"], 0.50) * 0.19
        + val(a["btts_rate"], 0.50) * 0.19
        + val(l["btts_rate"], 0.50) * 0.13
        + home_scores * 0.15
        + away_scores * 0.15
        + home_concedes * 0.095
        + away_concedes * 0.095
    )

    return make_candidate(
        market_type="btts",
        selection="btts_yes",
        raw_score=raw_score,
        reason="İki tarafın gol bulma ve gol yeme oranları KG var tarafını destekliyor.",
        features=features,
    )


def score_btts_no(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    home_btts_no = 1.0 - val(h["btts_rate"], 0.50)
    away_btts_no = 1.0 - val(a["btts_rate"], 0.50)
    league_btts_no = 1.0 - val(l["btts_rate"], 0.50)

    one_side_low_scoring = max(
        1.0 - val(h["scored_rate"], 0.65),
        1.0 - val(a["scored_rate"], 0.60),
    )

    raw_score = (
        home_btts_no * 0.26
        + away_btts_no * 0.26
        + league_btts_no * 0.18
        + one_side_low_scoring * 0.20
        + val(h["under_35_rate"], 0.70) * 0.05
        + val(a["under_35_rate"], 0.70) * 0.05
    )

    return make_candidate(
        market_type="btts",
        selection="btts_no",
        raw_score=raw_score,
        reason="KG oranları düşük, taraflardan en az birinin gol bulma sinyali zayıf görünüyor.",
        features=features,
    )


def score_corners_over_85(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    expected_corners = (
        val(h["corners_for_avg"], 4.5)
        + val(a["corners_for_avg"], 4.0)
        + val(h["corners_against_avg"], 4.0) * 0.35
        + val(a["corners_against_avg"], 4.0) * 0.35
    )

    corner_signal = min(expected_corners / 11.5, 1.0)
    league_signal = min(val(l["corners_avg"], 9.0) / 10.5, 1.0)
    shot_signal = min((val(h["shots_for_avg"], 10.0) + val(a["shots_for_avg"], 10.0)) / 28.0, 1.0)
    possession_spread_signal = min(
        abs(val(h["possession_avg"], 50.0) - val(a["possession_avg"], 50.0)) / 20.0,
        1.0,
    )

    raw_score = (
        corner_signal * 0.52
        + league_signal * 0.20
        + shot_signal * 0.22
        + possession_spread_signal * 0.06
    )

    return make_candidate(
        market_type="corners",
        selection="corners_over_8_5",
        raw_score=raw_score,
        reason="Korner üretimi, rakibe verilen korner, şut hacmi ve lig korner ortalaması 8.5 üst korneri destekliyor.",
        features=features,
    )


def score_corners_over_95(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    expected_corners = (
        val(h["corners_for_avg"], 4.5)
        + val(a["corners_for_avg"], 4.0)
        + val(h["corners_against_avg"], 4.0) * 0.35
        + val(a["corners_against_avg"], 4.0) * 0.35
    )

    corner_signal = min(expected_corners / 12.8, 1.0)
    league_signal = min(val(l["corners_avg"], 9.0) / 11.0, 1.0)
    shot_signal = min((val(h["shots_for_avg"], 10.0) + val(a["shots_for_avg"], 10.0)) / 30.0, 1.0)

    raw_score = (
        corner_signal * 0.57
        + league_signal * 0.18
        + shot_signal * 0.25
    )

    return make_candidate(
        market_type="corners",
        selection="corners_over_9_5",
        raw_score=raw_score,
        reason="Toplam korner beklentisi ve maç temposu 9.5 üst korner tarafına yakın sinyal veriyor.",
        features=features,
    )


def score_corners_under_115(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    expected_corners = (
        val(h["corners_for_avg"], 4.5)
        + val(a["corners_for_avg"], 4.0)
        + val(h["corners_against_avg"], 4.0) * 0.30
        + val(a["corners_against_avg"], 4.0) * 0.30
    )

    low_corner_signal = 1.0 - min(expected_corners / 14.0, 1.0)
    league_low_signal = 1.0 - min(val(l["corners_avg"], 9.0) / 13.0, 1.0)
    low_shot_signal = 1.0 - min((val(h["shots_for_avg"], 10.0) + val(a["shots_for_avg"], 10.0)) / 34.0, 1.0)

    raw_score = (
        low_corner_signal * 0.55
        + league_low_signal * 0.20
        + low_shot_signal * 0.25
    )

    return make_candidate(
        market_type="corners",
        selection="corners_under_11_5",
        raw_score=raw_score,
        reason="Korner ve şut hacmi yüksek çizgi için sınırlı kaldığından 11.5 alt korner tarafı destekleniyor.",
        features=features,
    )


def score_shots_over_205(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    expected_shots = (
        val(h["shots_for_avg"], 10.0)
        + val(a["shots_for_avg"], 10.0)
        + val(h["shots_against_avg"], 10.0) * 0.30
        + val(a["shots_against_avg"], 10.0) * 0.30
    )

    shot_signal = min(expected_shots / 28.0, 1.0)
    league_signal = min(val(l["shots_avg"], 21.0) / 25.0, 1.0)
    goal_signal = min((val(h["goals_for_avg"], 1.1) + val(a["goals_for_avg"], 1.1)) / 3.0, 1.0)

    raw_score = (
        shot_signal * 0.60
        + league_signal * 0.25
        + goal_signal * 0.15
    )

    return make_candidate(
        market_type="shots",
        selection="shots_over_20_5",
        raw_score=raw_score,
        reason="İki takımın şut üretimi, rakibe verilen şut ve lig temposu 20.5 üst şutu destekliyor.",
        features=features,
    )


def score_shots_over_225(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    expected_shots = (
        val(h["shots_for_avg"], 10.0)
        + val(a["shots_for_avg"], 10.0)
        + val(h["shots_against_avg"], 10.0) * 0.25
        + val(a["shots_against_avg"], 10.0) * 0.25
    )

    shot_signal = min(expected_shots / 31.0, 1.0)
    league_signal = min(val(l["shots_avg"], 21.0) / 27.0, 1.0)
    possession_signal = min(
        (val(h["possession_avg"], 50.0) + val(a["possession_avg"], 50.0)) / 105.0,
        1.0,
    )

    raw_score = (
        shot_signal * 0.62
        + league_signal * 0.25
        + possession_signal * 0.13
    )

    return make_candidate(
        market_type="shots",
        selection="shots_over_22_5",
        raw_score=raw_score,
        reason="Şut profili yüksek tempolu maç ihtimalini destekliyor; 22.5 üst şut adayı güçlü.",
        features=features,
    )


def score_shots_on_target_over_65(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    expected_sot = (
        val(h["shots_on_for_avg"], 3.8)
        + val(a["shots_on_for_avg"], 3.5)
        + val(h["shots_on_against_avg"], 3.5) * 0.25
        + val(a["shots_on_against_avg"], 3.5) * 0.25
    )

    sot_signal = min(expected_sot / 9.0, 1.0)
    league_signal = min(val(l["shots_on_avg"], 7.0) / 8.0, 1.0)
    goal_signal = min((val(h["goals_for_avg"], 1.1) + val(a["goals_for_avg"], 1.1)) / 3.0, 1.0)

    raw_score = (
        sot_signal * 0.65
        + league_signal * 0.25
        + goal_signal * 0.10
    )

    return make_candidate(
        market_type="shots",
        selection="shots_on_target_over_6_5",
        raw_score=raw_score,
        reason="İsabetli şut üretimi ve rakiplerin verdiği isabetli şut hacmi 6.5 üst isabetli şutu destekliyor.",
        features=features,
    )


def score_fouls_over_245(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    expected_fouls = (
        val(h["fouls_for_avg"], 11.0)
        + val(a["fouls_for_avg"], 11.0)
        + val(h["fouls_against_avg"], 11.0) * 0.20
        + val(a["fouls_against_avg"], 11.0) * 0.20
    )

    foul_signal = min(expected_fouls / 30.0, 1.0)
    league_signal = min(val(l["fouls_avg"], 23.0) / 27.0, 1.0)
    tackle_signal = min((val(h["tackles_avg"], 15.0) + val(a["tackles_avg"], 15.0)) / 40.0, 1.0)

    raw_score = (
        foul_signal * 0.65
        + league_signal * 0.25
        + tackle_signal * 0.10
    )

    return make_candidate(
        market_type="fouls",
        selection="fouls_over_24_5",
        raw_score=raw_score,
        reason="Takımların faul ortalaması, lig sertliği ve mücadele seviyesi 24.5 üst faul tarafını destekliyor.",
        features=features,
    )


def score_fouls_under_285(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    expected_fouls = (
        val(h["fouls_for_avg"], 11.0)
        + val(a["fouls_for_avg"], 11.0)
        + val(h["fouls_against_avg"], 11.0) * 0.15
        + val(a["fouls_against_avg"], 11.0) * 0.15
    )

    low_foul_signal = 1.0 - min(expected_fouls / 34.0, 1.0)
    league_low_signal = 1.0 - min(val(l["fouls_avg"], 23.0) / 31.0, 1.0)

    raw_score = (
        low_foul_signal * 0.70
        + league_low_signal * 0.30
    )

    return make_candidate(
        market_type="fouls",
        selection="fouls_under_28_5",
        raw_score=raw_score,
        reason="Faul temposu yüksek çizgi için sınırlı göründüğünden 28.5 alt faul tarafı destekleniyor.",
        features=features,
    )


def score_home_score_over_05(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    home_score_signal = val(h["scored_rate"], 0.70)
    away_concede_signal = val(a["conceded_rate"], 0.65)
    home_attack = min(val(h["goals_for_avg"], 1.2) / 2.0, 1.0)
    away_defense_leak = min(val(a["goals_against_avg"], 1.2) / 2.0, 1.0)
    league_goal_signal = min(val(l["goals_avg"], 2.5) / 3.2, 1.0)

    raw_score = (
        home_score_signal * 0.30
        + away_concede_signal * 0.25
        + home_attack * 0.20
        + away_defense_leak * 0.15
        + league_goal_signal * 0.10
    )

    return make_candidate(
        market_type="team_goals",
        selection="home_over_0_5",
        raw_score=raw_score,
        reason="Ev sahibinin gol bulma oranı ve deplasmanın gol yeme profili ev sahibi 0.5 üst golü destekliyor.",
        features=features,
    )


def score_away_score_over_05(features: Dict[str, Any]) -> Dict[str, Any]:
    h = features["home"]
    a = features["away"]
    l = features["league"]

    away_score_signal = val(a["scored_rate"], 0.62)
    home_concede_signal = val(h["conceded_rate"], 0.62)
    away_attack = min(val(a["goals_for_avg"], 1.1) / 2.0, 1.0)
    home_defense_leak = min(val(h["goals_against_avg"], 1.1) / 2.0, 1.0)
    league_goal_signal = min(val(l["goals_avg"], 2.5) / 3.2, 1.0)

    raw_score = (
        away_score_signal * 0.30
        + home_concede_signal * 0.25
        + away_attack * 0.20
        + home_defense_leak * 0.15
        + league_goal_signal * 0.10
    )

    return make_candidate(
        market_type="team_goals",
        selection="away_over_0_5",
        raw_score=raw_score,
        reason="Deplasmanın gol bulma oranı ve ev sahibinin gol yeme profili deplasman 0.5 üst golü destekliyor.",
        features=features,
    )


def generate_candidates(features: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Tüm market adaylarını üretir.
    Burada oran yok. Tamamen istatistik sinyalidir.
    """
    candidates = [
        score_goals_over_15(features),
        score_goals_over_25(features),
        score_goals_under_35(features),

        score_btts_yes(features),
        score_btts_no(features),

        score_corners_over_85(features),
        score_corners_over_95(features),
        score_corners_under_115(features),

        score_shots_over_205(features),
        score_shots_over_225(features),
        score_shots_on_target_over_65(features),

        score_fouls_over_245(features),
        score_fouls_under_285(features),

        score_home_score_over_05(features),
        score_away_score_over_05(features),
    ]

    return candidates


def select_best_prediction(
    features: Dict[str, Any],
    min_confidence: float,
) -> Optional[Dict[str, Any]]:
    candidates = generate_candidates(features)

    valid_candidates = [
        c for c in candidates
        if c["confidence_score"] >= min_confidence
    ]

    if not valid_candidates:
        return None

    valid_candidates.sort(
        key=lambda x: x["confidence_score"],
        reverse=True,
    )

    return valid_candidates[0]


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
        print(f"[UYARI] Lig örneklemi düşük: {league_n}. Tahmin yine de takım verisiyle değerlendirilecek.")

    prediction = select_best_prediction(
        features=features,
        min_confidence=MIN_CONFIDENCE,
    )

    if prediction is None:
        print("[ATLANDI] Yeterli confidence veren market yok.")
        return False

    db.upsert_prediction(prediction)

    print(
        "[YAZILDI] "
        f"{prediction['market_type']} / {prediction['selection']} "
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
        print("[BİTTİ] Tahmin üretimi tamamlandı.")

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
