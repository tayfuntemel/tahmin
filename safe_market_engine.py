#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SAFE MARKET ENGINE - SIFIRDAN TAHMIN + MARKET SECIM MOTORU

Amaç:
- Çok maç değil, düşük riskli az maç.
- Önce marketleri değerlendirir, sonra maç başına en güvenli tek marketi seçer.
- Güven düşükse NO_BET üretir.
- Tahmin günü veya sonrası veriyi kullanmaz. Cutoff = predict_date - 1 gün.

Kullanım:
    python3 safe_market_engine.py tomorrow
    python3 safe_market_engine.py 2026-05-06
    python3 safe_market_engine.py backtest 2026-02-04 2026-05-04

Önemli ENV ayarları:
    DB_HOST DB_USER DB_PASSWORD DB_NAME DB_PORT
    MIN_FINAL_CONFIDENCE=86
    MIN_MODEL_PROB=80
    MIN_EDGE=2
    MAX_DAILY_PICKS=4
    MAX_ODDS=1.85
    MIN_SAMPLE=35
"""

from __future__ import annotations

import os
import sys
import math
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import mysql.connector

TR_TZ = ZoneInfo("Europe/Istanbul")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "charset": "utf8mb4",
    "use_unicode": True,
    "collation": "utf8mb4_unicode_ci",
}

SETTINGS = {
    "min_final_confidence": float(os.getenv("MIN_FINAL_CONFIDENCE", "86")),
    "min_model_prob": float(os.getenv("MIN_MODEL_PROB", "80")),
    "min_edge": float(os.getenv("MIN_EDGE", "2")),
    "min_sample": int(os.getenv("MIN_SAMPLE", "35")),
    "max_odds": float(os.getenv("MAX_ODDS", "1.85")),
    "min_odds": float(os.getenv("MIN_ODDS", "1.08")),
    "max_daily_picks": int(os.getenv("MAX_DAILY_PICKS", "4")),
    "max_risk_score": float(os.getenv("MAX_RISK_SCORE", "22")),
    "lookback_days": int(os.getenv("LOOKBACK_DAYS", "500")),
    "team_lookback_matches": int(os.getenv("TEAM_LOOKBACK_MATCHES", "50")),
    "h2h_lookback_matches": int(os.getenv("H2H_LOOKBACK_MATCHES", "12")),
    "clear_existing": os.getenv("CLEAR_EXISTING_SAFE", "1").strip().lower() not in {"0", "false", "no", "hayir", "hayır"},
}

# 1X2 sürprizleri yüksek oynayan adamı yakar. Bu yüzden 1X2 yok; çift şans bile ceza yer.
MARKETS = {
    "O05": {"label": "0.5 ÜST", "odds_col": "odds_o05", "family": "total", "base_risk": 2.0},
    "O15": {"label": "1.5 ÜST", "odds_col": "odds_o15", "family": "total", "base_risk": 6.0},
    "O25": {"label": "2.5 ÜST", "odds_col": "odds_o25", "family": "total", "base_risk": 16.0},
    "U35": {"label": "3.5 ALT", "odds_col": "odds_u35", "family": "total", "base_risk": 9.0},
    "U45": {"label": "4.5 ALT", "odds_col": "odds_u45", "family": "total", "base_risk": 4.0},
    "U55": {"label": "5.5 ALT", "odds_col": "odds_u55", "family": "total", "base_risk": 3.0},
    "BTTS_NO": {"label": "KG YOK", "odds_col": "odds_btts_no", "family": "btts", "base_risk": 13.0},
    "BTTS_YES": {"label": "KG VAR", "odds_col": "odds_btts_yes", "family": "btts", "base_risk": 18.0},
    "DC_1X": {"label": "1X", "odds_col": "odds_1x", "family": "dc", "base_risk": 10.0},
    "DC_X2": {"label": "X2", "odds_col": "odds_x2", "family": "dc", "base_risk": 12.0},
    "DC_12": {"label": "12", "odds_col": "odds_12", "family": "dc", "base_risk": 14.0},
}

RESULTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS results_football (
  event_id BIGINT UNSIGNED NOT NULL,
  start_utc DATE NULL,
  start_time_utc TIME NULL,
  match_year INT NULL,
  match_week INT NULL,
  status VARCHAR(32) NULL,
  home_team VARCHAR(128) NULL,
  away_team VARCHAR(128) NULL,
  ht_home INT NULL,
  ht_away INT NULL,
  ft_home INT NULL,
  ft_away INT NULL,
  poss_h INT NULL,
  poss_a INT NULL,
  corn_h INT NULL,
  corn_a INT NULL,
  shot_h INT NULL,
  shot_a INT NULL,
  shot_on_h INT NULL,
  shot_on_a INT NULL,
  fouls_h INT NULL,
  fouls_a INT NULL,
  offsides_h INT NULL,
  offsides_a INT NULL,
  saves_h INT NULL,
  saves_a INT NULL,
  passes_h INT NULL,
  passes_a INT NULL,
  tackles_h INT NULL,
  tackles_a INT NULL,
  referee VARCHAR(128) NULL,
  formation_h VARCHAR(32) NULL,
  formation_a VARCHAR(32) NULL,
  odds_1 FLOAT NULL,
  odds_x FLOAT NULL,
  odds_2 FLOAT NULL,
  odds_1x FLOAT NULL,
  odds_12 FLOAT NULL,
  odds_x2 FLOAT NULL,
  odds_btts_yes FLOAT NULL,
  odds_btts_no FLOAT NULL,
  odds_o05 FLOAT NULL,
  odds_u05 FLOAT NULL,
  odds_o15 FLOAT NULL,
  odds_u15 FLOAT NULL,
  odds_o25 FLOAT NULL,
  odds_u25 FLOAT NULL,
  odds_o35 FLOAT NULL,
  odds_u35 FLOAT NULL,
  odds_o45 FLOAT NULL,
  odds_u45 FLOAT NULL,
  odds_o55 FLOAT NULL,
  odds_u55 FLOAT NULL,
  odds_o65 FLOAT NULL,
  odds_u65 FLOAT NULL,
  odds_o75 FLOAT NULL,
  odds_u75 FLOAT NULL,
  tournament_id INT NULL,
  tournament_name VARCHAR(128) NULL,
  category_id INT NULL,
  category_name VARCHAR(128) NULL,
  country VARCHAR(64) NULL,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (event_id),
  KEY idx_date (start_utc),
  KEY idx_status (status),
  KEY idx_tournament (tournament_id),
  KEY idx_category_date (category_id, start_utc),
  KEY idx_teams_date (home_team, away_team, start_utc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

SAFE_SCHEMA = """
CREATE TABLE IF NOT EXISTS safe_market_predictions (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  event_id BIGINT UNSIGNED NOT NULL,
  prediction_date DATE NOT NULL,
  match_date DATE NOT NULL,
  start_time_utc TIME NULL,
  home_team VARCHAR(128) NOT NULL,
  away_team VARCHAR(128) NOT NULL,
  tournament_id INT NULL,
  tournament_name VARCHAR(128) NULL,
  category_id INT NULL,
  category_name VARCHAR(128) NULL,
  country VARCHAR(64) NULL,
  selected_market VARCHAR(32) NOT NULL,
  selected_label VARCHAR(64) NOT NULL,
  selected_odds FLOAT NULL,
  model_prob FLOAT NULL,
  implied_prob FLOAT NULL,
  edge FLOAT NULL,
  confidence FLOAT NULL,
  risk_score FLOAT NULL,
  sample_score FLOAT NULL,
  league_sample INT NOT NULL DEFAULT 0,
  team_sample INT NOT NULL DEFAULT 0,
  h2h_sample INT NOT NULL DEFAULT 0,
  decision VARCHAR(16) NOT NULL DEFAULT 'NO_BET',
  reason VARCHAR(255) NULL,
  actual_result TINYINT NULL,
  ft_home INT NULL,
  ft_away INT NULL,
  profit_unit FLOAT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_event_prediction (event_id, prediction_date),
  KEY idx_prediction_date (prediction_date),
  KEY idx_decision (decision),
  KEY idx_confidence (confidence),
  KEY idx_market (selected_market)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

@dataclass
class Candidate:
    market: str
    label: str
    odds: float
    model_prob: float
    implied_prob: float
    edge: float
    confidence: float
    risk_score: float
    sample_score: float
    league_sample: int
    team_sample: int
    h2h_sample: int
    decision: str
    reason: str


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def safe_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except Exception:
        return None


def market_hit(market: str, row: Dict[str, Any]) -> Optional[int]:
    fh, fa = row.get("ft_home"), row.get("ft_away")
    if fh is None or fa is None:
        return None
    try:
        h, a = int(fh), int(fa)
    except Exception:
        return None
    total = h + a
    if market == "O05": return int(total >= 1)
    if market == "O15": return int(total >= 2)
    if market == "O25": return int(total >= 3)
    if market == "U35": return int(total <= 3)
    if market == "U45": return int(total <= 4)
    if market == "U55": return int(total <= 5)
    if market == "BTTS_YES": return int(h > 0 and a > 0)
    if market == "BTTS_NO": return int(not (h > 0 and a > 0))
    if market == "DC_1X": return int(h >= a)
    if market == "DC_X2": return int(a >= h)
    if market == "DC_12": return int(h != a)
    return None


def rate_for_market(market: str, rows: List[Dict[str, Any]]) -> Tuple[Optional[float], int]:
    hits = []
    for r in rows:
        hit = market_hit(market, r)
        if hit is not None:
            hits.append(hit)
    if not hits:
        return None, 0
    return (sum(hits) / len(hits)) * 100.0, len(hits)


class DB:
    def __init__(self) -> None:
        self.conn = None
        self.cur = None

    def connect(self) -> None:
        missing = [k for k, v in DB_CONFIG.items() if k in {"host", "user", "database"} and not v]
        if missing:
            raise RuntimeError(f"Eksik DB ortam değişkeni: {', '.join(missing)}")
        self.conn = mysql.connector.connect(**DB_CONFIG)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(RESULTS_SCHEMA)
        self.cur.execute(SAFE_SCHEMA)
        self.ensure_safe_columns()

    def ensure_safe_columns(self) -> None:
        columns = {
            "profit_unit": "FLOAT NULL",
            "actual_result": "TINYINT NULL",
            "ft_home": "INT NULL",
            "ft_away": "INT NULL",
            "reason": "VARCHAR(255) NULL",
        }
        for col, typ in columns.items():
            try:
                self.cur.execute(f"ALTER TABLE safe_market_predictions ADD COLUMN {col} {typ}")
            except mysql.connector.Error as e:
                if e.errno != 1060:
                    print(f"[DB UYARI] {col}: {e}")

    def close(self) -> None:
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def finished_rows(self, cutoff: dt.date, lookback_days: int) -> List[Dict[str, Any]]:
        start = cutoff - dt.timedelta(days=lookback_days)
        self.cur.execute(
            """
            SELECT * FROM results_football
            WHERE status IN ('finished','ended')
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
              AND start_utc BETWEEN %s AND %s
            ORDER BY start_utc DESC, start_time_utc DESC
            """,
            (start, cutoff),
        )
        return list(self.cur.fetchall())

    def matches_for_date(self, target: dt.date) -> List[Dict[str, Any]]:
        self.cur.execute(
            """
            SELECT * FROM results_football
            WHERE start_utc = %s
              AND home_team IS NOT NULL AND away_team IS NOT NULL
            ORDER BY start_time_utc ASC, event_id ASC
            """,
            (target,),
        )
        return list(self.cur.fetchall())

    def clear_predictions(self, target: dt.date) -> None:
        self.cur.execute("DELETE FROM safe_market_predictions WHERE prediction_date=%s", (target,))

    def save(self, match: Dict[str, Any], pred_date: dt.date, c: Candidate, actual_result: Optional[int] = None) -> None:
        profit = None
        if actual_result is not None and c.decision == "BET" and c.odds:
            profit = (c.odds - 1.0) if actual_result == 1 else -1.0
        self.cur.execute(
            """
            INSERT INTO safe_market_predictions
            (event_id, prediction_date, match_date, start_time_utc, home_team, away_team,
             tournament_id, tournament_name, category_id, category_name, country,
             selected_market, selected_label, selected_odds, model_prob, implied_prob, edge,
             confidence, risk_score, sample_score, league_sample, team_sample, h2h_sample,
             decision, reason, actual_result, ft_home, ft_away, profit_unit, updated_at)
            VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON DUPLICATE KEY UPDATE
             match_date=VALUES(match_date), start_time_utc=VALUES(start_time_utc), home_team=VALUES(home_team), away_team=VALUES(away_team),
             tournament_id=VALUES(tournament_id), tournament_name=VALUES(tournament_name), category_id=VALUES(category_id), category_name=VALUES(category_name), country=VALUES(country),
             selected_market=VALUES(selected_market), selected_label=VALUES(selected_label), selected_odds=VALUES(selected_odds),
             model_prob=VALUES(model_prob), implied_prob=VALUES(implied_prob), edge=VALUES(edge), confidence=VALUES(confidence), risk_score=VALUES(risk_score), sample_score=VALUES(sample_score),
             league_sample=VALUES(league_sample), team_sample=VALUES(team_sample), h2h_sample=VALUES(h2h_sample), decision=VALUES(decision), reason=VALUES(reason),
             actual_result=VALUES(actual_result), ft_home=VALUES(ft_home), ft_away=VALUES(ft_away), profit_unit=VALUES(profit_unit), updated_at=NOW()
            """,
            (
                match.get("event_id"), pred_date, match.get("start_utc"), match.get("start_time_utc"), match.get("home_team"), match.get("away_team"),
                match.get("tournament_id"), match.get("tournament_name"), match.get("category_id"), match.get("category_name"), match.get("country"),
                c.market, c.label, c.odds, c.model_prob, c.implied_prob, c.edge, c.confidence, c.risk_score, c.sample_score,
                c.league_sample, c.team_sample, c.h2h_sample, c.decision, c.reason, actual_result, match.get("ft_home"), match.get("ft_away"), profit,
            ),
        )


class SafeMarketEngine:
    def __init__(self, db: DB, target_date: dt.date) -> None:
        self.db = db
        self.target_date = target_date
        self.cutoff = target_date - dt.timedelta(days=1)
        self.history = db.finished_rows(self.cutoff, SETTINGS["lookback_days"])
        self.by_category: Dict[Any, List[Dict[str, Any]]] = {}
        self.by_team: Dict[Tuple[str, Any], List[Dict[str, Any]]] = {}
        for r in self.history:
            cat = r.get("category_id")
            if cat is not None:
                self.by_category.setdefault(cat, []).append(r)
            for side in ("home_team", "away_team"):
                t = r.get(side)
                if t and cat is not None:
                    self.by_team.setdefault((str(t), cat), []).append(r)

    def get_team_rows(self, home: str, away: str, cat: Any) -> List[Dict[str, Any]]:
        rows = []
        rows.extend(self.by_team.get((home, cat), [])[: SETTINGS["team_lookback_matches"]])
        rows.extend(self.by_team.get((away, cat), [])[: SETTINGS["team_lookback_matches"]])
        # event_id dedupe
        seen = set()
        out = []
        for r in rows:
            eid = r.get("event_id")
            if eid in seen:
                continue
            seen.add(eid)
            out.append(r)
        return out

    def get_h2h_rows(self, home: str, away: str, cat: Any) -> List[Dict[str, Any]]:
        out = []
        for r in self.history:
            if r.get("category_id") != cat:
                continue
            h, a = r.get("home_team"), r.get("away_team")
            if (h == home and a == away) or (h == away and a == home):
                out.append(r)
                if len(out) >= SETTINGS["h2h_lookback_matches"]:
                    break
        return out

    def candidate_for_market(self, match: Dict[str, Any], market: str) -> Optional[Candidate]:
        meta = MARKETS[market]
        odds = safe_float(match.get(meta["odds_col"]))
        if odds is None:
            return None
        if odds < SETTINGS["min_odds"] or odds > SETTINGS["max_odds"]:
            return Candidate(market, meta["label"], odds, 0, 100 / odds, -99, 0, 99, 0, 0, 0, 0, "NO_BET", "oran bandı dışı")

        home, away, cat = str(match.get("home_team")), str(match.get("away_team")), match.get("category_id")
        league_rows = self.by_category.get(cat, [])
        team_rows = self.get_team_rows(home, away, cat)
        h2h_rows = self.get_h2h_rows(home, away, cat)
        league_rate, league_n = rate_for_market(market, league_rows)
        team_rate, team_n = rate_for_market(market, team_rows)
        h2h_rate, h2h_n = rate_for_market(market, h2h_rows)

        # Eksik veriye güvenme. H2H yoksa ceza var ama sistem çalışır.
        rates = []
        weights = []
        if league_rate is not None:
            rates.append(league_rate); weights.append(0.42)
        if team_rate is not None:
            rates.append(team_rate); weights.append(0.43)
        if h2h_rate is not None and h2h_n >= 3:
            rates.append(h2h_rate); weights.append(0.15)
        if not rates:
            return Candidate(market, meta["label"], odds, 0, 100 / odds, -99, 0, 99, 0, 0, 0, 0, "NO_BET", "veri yok")
        wsum = sum(weights)
        model = sum(r * w for r, w in zip(rates, weights)) / wsum

        implied = 100.0 / odds
        edge = model - implied
        sample_score = clamp((min(league_n, 250) / 250) * 45 + (min(team_n, 80) / 80) * 45 + (min(h2h_n, 8) / 8) * 10, 0, 100)
        disagreement = 0.0
        if league_rate is not None and team_rate is not None:
            disagreement += abs(league_rate - team_rate) * 0.28
        if h2h_rate is not None and h2h_n >= 3:
            disagreement += abs(model - h2h_rate) * 0.12
        odds_risk = max(0.0, (odds - 1.35) * 16.0)
        sample_penalty = max(0.0, SETTINGS["min_sample"] - min(league_n, team_n)) * 0.35
        risk_score = clamp(float(meta["base_risk"]) + odds_risk + disagreement + sample_penalty, 0, 99)
        confidence = clamp(model + edge * 0.35 + sample_score * 0.10 - risk_score * 0.62, 0, 99)

        reasons = []
        if model < SETTINGS["min_model_prob"]:
            reasons.append("model düşük")
        if edge < SETTINGS["min_edge"]:
            reasons.append("edge düşük")
        if confidence < SETTINGS["min_final_confidence"]:
            reasons.append("güven düşük")
        if risk_score > SETTINGS["max_risk_score"]:
            reasons.append("risk yüksek")
        if min(league_n, team_n) < SETTINGS["min_sample"]:
            reasons.append("örnek az")
        decision = "NO_BET" if reasons else "BET"
        reason = ", ".join(reasons) if reasons else "güvenli bantta"
        return Candidate(market, meta["label"], odds, model, implied, edge, confidence, risk_score, sample_score, league_n, team_n, h2h_n, decision, reason)

    def select_for_match(self, match: Dict[str, Any]) -> Candidate:
        candidates = [self.candidate_for_market(match, m) for m in MARKETS]
        candidates = [c for c in candidates if c is not None]
        if not candidates:
            return Candidate("NO_MARKET", "market yok", 0, 0, 0, 0, 0, 99, 0, 0, 0, 0, "NO_BET", "oran yok")
        # BET olanlarda önce confidence, sonra düşük risk, sonra yüksek model.
        bettable = [c for c in candidates if c.decision == "BET"]
        pool = bettable if bettable else candidates
        return sorted(pool, key=lambda c: (c.decision == "BET", c.confidence, -c.risk_score, c.model_prob, c.edge), reverse=True)[0]

    def run_day(self) -> Dict[str, Any]:
        if SETTINGS["clear_existing"]:
            self.db.clear_predictions(self.target_date)
        matches = self.db.matches_for_date(self.target_date)
        selected: List[Tuple[Dict[str, Any], Candidate, Optional[int]]] = []
        for m in matches:
            c = self.select_for_match(m)
            actual = market_hit(c.market, m) if c.market in MARKETS else None
            selected.append((m, c, actual))
        # Günlük limit: sadece en iyi N BET kalır, kalan BET'ler pasife alınır.
        bets = sorted([(m, c, a) for m, c, a in selected if c.decision == "BET"], key=lambda x: x[1].confidence, reverse=True)
        allowed_ids = {m.get("event_id") for m, _, _ in bets[: SETTINGS["max_daily_picks"]]}
        saved = bet_count = 0
        for m, c, actual in selected:
            if c.decision == "BET" and m.get("event_id") not in allowed_ids:
                c = Candidate(c.market, c.label, c.odds, c.model_prob, c.implied_prob, c.edge, c.confidence, c.risk_score, c.sample_score, c.league_sample, c.team_sample, c.h2h_sample, "NO_BET", "günlük limit dışında")
            if c.decision == "BET":
                bet_count += 1
            self.db.save(m, self.target_date, c, actual)
            saved += 1
        return {"date": str(self.target_date), "matches": len(matches), "saved": saved, "bets": bet_count}


def parse_date_arg(s: str) -> dt.date:
    if s == "today":
        return dt.datetime.now(TR_TZ).date()
    if s == "tomorrow":
        return dt.datetime.now(TR_TZ).date() + dt.timedelta(days=1)
    if s == "yesterday":
        return dt.datetime.now(TR_TZ).date() - dt.timedelta(days=1)
    return dt.date.fromisoformat(s)


def run_single(target: dt.date) -> None:
    db = DB(); db.connect()
    try:
        print("=" * 72)
        print(f"SAFE MARKET ENGINE | predict_date={target} | cutoff={target - dt.timedelta(days=1)}")
        print("Ayarlar:", SETTINGS)
        result = SafeMarketEngine(db, target).run_day()
        print(f"✅ tamamlandı: {result}")
    finally:
        db.close()


def run_backtest(start: dt.date, end: dt.date) -> None:
    db = DB(); db.connect()
    try:
        d = start
        total_saved = total_bets = 0
        while d <= end:
            print("\n" + "=" * 72)
            result = SafeMarketEngine(db, d).run_day()
            print(f"{d}: {result['bets']} bahis / {result['matches']} maç")
            total_saved += result["saved"]
            total_bets += result["bets"]
            d += dt.timedelta(days=1)
        db.cur.execute(
            """
            SELECT COUNT(*) bet_count,
                   SUM(CASE WHEN actual_result=1 THEN 1 ELSE 0 END) win_count,
                   SUM(CASE WHEN actual_result=0 THEN 1 ELSE 0 END) lose_count,
                   SUM(profit_unit) profit_unit,
                   AVG(selected_odds) avg_odds,
                   AVG(confidence) avg_confidence
            FROM safe_market_predictions
            WHERE prediction_date BETWEEN %s AND %s AND decision='BET' AND actual_result IS NOT NULL
            """,
            (start, end),
        )
        row = db.cur.fetchone() or {}
        print("\n" + "=" * 72)
        print("✅ BACKTEST ÖZET")
        print(f"Tarih: {start} - {end}")
        print(f"Toplam kayıt: {total_saved} | BET: {total_bets}")
        print(f"Sonuçlanan BET: {row.get('bet_count') or 0} | Kazanan: {row.get('win_count') or 0} | Kaybeden: {row.get('lose_count') or 0}")
        print(f"Profit unit: {float(row.get('profit_unit') or 0):.2f} | Avg odds: {float(row.get('avg_odds') or 0):.2f} | Avg confidence: {float(row.get('avg_confidence') or 0):.1f}")
    finally:
        db.close()


def main() -> None:
    if len(sys.argv) >= 2 and sys.argv[1] == "backtest":
        if len(sys.argv) < 4:
            raise SystemExit("Kullanım: python3 safe_market_engine.py backtest YYYY-MM-DD YYYY-MM-DD")
        run_backtest(dt.date.fromisoformat(sys.argv[2]), dt.date.fromisoformat(sys.argv[3]))
        return
    target = parse_date_arg(sys.argv[1]) if len(sys.argv) >= 2 else parse_date_arg("tomorrow")
    run_single(target)


if __name__ == "__main__":
    main()
