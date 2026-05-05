#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SAFE MARKET BACKTEST V1

Sıfırdan kurulum için tek amaç:
- results_football tablosundaki bitmiş maçları okur.
- Her maç için sadece maçtan ÖNCEKİ verileri kullanır. Sızıntı yoktur.
- Birden fazla marketi değerlendirir.
- En güvenli marketi seçer.
- Eşik geçmeyen maçı NO_BET yapar.
- Sonuçları safe_market_backtest_results tablosuna yazar.

Kullanım:
    pip3 install pyyaml mysql-connector-python
    python3 safe_market_backtest_v1.py --config safe_market_backtest_v1.yml

Tarih ezmek için:
    python3 safe_market_backtest_v1.py --config safe_market_backtest_v1.yml --start 2026-02-04 --end 2026-05-04
"""

import argparse
import dataclasses
import datetime as dt
import math
import os
import statistics
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import yaml
except ImportError:
    print("[HATA] PyYAML yok. Kurulum: pip3 install pyyaml")
    sys.exit(1)

try:
    import mysql.connector
except ImportError:
    print("[HATA] mysql-connector-python yok. Kurulum: pip3 install mysql-connector-python")
    sys.exit(1)


# ==========================================================
# GENEL YARDIMCILAR
# ==========================================================

def env_value(value: Any) -> Any:
    """YML içinde ${ENV_NAME} yazılırsa ortam değişkeninden okur."""
    if not isinstance(value, str):
        return value
    v = value.strip()
    if v.startswith("${") and v.endswith("}"):
        return os.getenv(v[2:-1], "")
    return value


def parse_date(value: Any, field_name: str) -> dt.date:
    try:
        return dt.date.fromisoformat(str(value).strip())
    except Exception as exc:
        raise ValueError(f"{field_name} geçerli tarih değil: {value}") from exc


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except Exception:
        return default


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def pct(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return round(float(x) * 100.0, 4)


def mean(values: Sequence[float], default: float = 0.0) -> float:
    clean = [float(v) for v in values if v is not None]
    return sum(clean) / len(clean) if clean else default


def stdev_or_zero(values: Sequence[float]) -> float:
    clean = [float(v) for v in values if v is not None]
    if len(clean) < 2:
        return 0.0
    return statistics.pstdev(clean)


# ==========================================================
# MARKET TANIMLARI
# ==========================================================

@dataclasses.dataclass(frozen=True)
class MarketDef:
    code: str
    label: str
    odds_column: str
    risk_weight: float
    min_odds: float
    max_odds: float


DEFAULT_MARKETS: Dict[str, MarketDef] = {
    "O05": MarketDef("O05", "0.5 ÜST", "odds_o05", 2.0, 1.01, 1.25),
    "O15": MarketDef("O15", "1.5 ÜST", "odds_o15", 7.0, 1.12, 1.55),
    "U35": MarketDef("U35", "3.5 ALT", "odds_u35", 9.0, 1.12, 1.65),
    "U45": MarketDef("U45", "4.5 ALT", "odds_u45", 5.0, 1.05, 1.38),
    "U55": MarketDef("U55", "5.5 ALT", "odds_u55", 3.0, 1.01, 1.22),
    "DC_1X": MarketDef("DC_1X", "1X", "odds_1x", 8.0, 1.08, 1.55),
    "DC_X2": MarketDef("DC_X2", "X2", "odds_x2", 8.0, 1.08, 1.55),
    "DC_12": MarketDef("DC_12", "12", "odds_12", 10.0, 1.08, 1.65),
    "BTTS_NO": MarketDef("BTTS_NO", "KG YOK", "odds_btts_no", 13.0, 1.25, 1.95),
    "BTTS_YES": MarketDef("BTTS_YES", "KG VAR", "odds_btts_yes", 16.0, 1.35, 2.05),
    "O25": MarketDef("O25", "2.5 ÜST", "odds_o25", 18.0, 1.35, 2.10),
    "U25": MarketDef("U25", "2.5 ALT", "odds_u25", 15.0, 1.30, 2.05),
}


def market_hit(code: str, row: Dict[str, Any]) -> Optional[int]:
    """Bir geçmiş maçta market tuttu mu? 1/0 döner. Veri yoksa None."""
    fh = row.get("ft_home")
    fa = row.get("ft_away")
    if fh is None or fa is None:
        return None
    try:
        h = int(fh)
        a = int(fa)
    except Exception:
        return None
    total = h + a

    if code == "O05":
        return int(total >= 1)
    if code == "O15":
        return int(total >= 2)
    if code == "O25":
        return int(total >= 3)
    if code == "U25":
        return int(total <= 2)
    if code == "U35":
        return int(total <= 3)
    if code == "U45":
        return int(total <= 4)
    if code == "U55":
        return int(total <= 5)
    if code == "BTTS_YES":
        return int(h > 0 and a > 0)
    if code == "BTTS_NO":
        return int(h == 0 or a == 0)
    if code == "DC_1X":
        return int(h >= a)
    if code == "DC_X2":
        return int(a >= h)
    if code == "DC_12":
        return int(h != a)
    return None


def match_goals(row: Dict[str, Any]) -> Optional[int]:
    if row.get("ft_home") is None or row.get("ft_away") is None:
        return None
    return int(row["ft_home"]) + int(row["ft_away"])


# ==========================================================
# VERİ MODELİ
# ==========================================================

@dataclasses.dataclass
class MarketEvaluation:
    market_code: str
    market_label: str
    odds: float
    implied_prob: float
    model_prob: float
    edge: float
    confidence: float
    risk_score: float
    sample_score: float
    league_sample: int
    home_sample: int
    away_sample: int
    h2h_sample: int
    decision_reason: str


@dataclasses.dataclass
class BacktestPick:
    event_id: int
    match_date: dt.date
    home_team: str
    away_team: str
    category_id: Optional[int]
    tournament_id: Optional[int]
    market_code: str
    market_label: str
    odds: Optional[float]
    implied_prob: Optional[float]
    model_prob: Optional[float]
    edge: Optional[float]
    confidence: Optional[float]
    risk_score: Optional[float]
    sample_score: Optional[float]
    league_sample: int
    home_sample: int
    away_sample: int
    h2h_sample: int
    actual_hit: Optional[int]
    profit_unit: float
    decision: str
    decision_reason: str


# ==========================================================
# DB KATMANI
# ==========================================================

class DB:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.conn = None
        self.cur = None

    def connect(self) -> None:
        db_cfg = self.config["database"]
        conn_args = {
            "host": env_value(db_cfg.get("host", "127.0.0.1")),
            "user": env_value(db_cfg.get("user", "")),
            "password": env_value(db_cfg.get("password", "")),
            "database": env_value(db_cfg.get("name", "")),
            "port": int(env_value(db_cfg.get("port", 3306)) or 3306),
            "charset": "utf8mb4",
            "use_unicode": True,
            "collation": "utf8mb4_unicode_ci",
        }
        self.conn = mysql.connector.connect(**conn_args)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.create_result_table()

    def close(self) -> None:
        try:
            if self.cur:
                self.cur.close()
        finally:
            if self.conn:
                self.conn.close()

    def create_result_table(self) -> None:
        result_table = self.config["database"].get("result_table", "safe_market_backtest_results")
        self.cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {result_table} (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                event_id BIGINT UNSIGNED NOT NULL,
                match_date DATE NOT NULL,
                home_team VARCHAR(128) NOT NULL,
                away_team VARCHAR(128) NOT NULL,
                category_id INT NULL,
                tournament_id INT NULL,
                market_code VARCHAR(32) NOT NULL,
                market_label VARCHAR(64) NOT NULL,
                odds FLOAT NULL,
                implied_prob FLOAT NULL,
                model_prob FLOAT NULL,
                edge_value FLOAT NULL,
                confidence FLOAT NULL,
                risk_score FLOAT NULL,
                sample_score FLOAT NULL,
                league_sample INT NOT NULL DEFAULT 0,
                home_sample INT NOT NULL DEFAULT 0,
                away_sample INT NOT NULL DEFAULT 0,
                h2h_sample INT NOT NULL DEFAULT 0,
                actual_hit TINYINT NULL,
                profit_unit FLOAT NOT NULL DEFAULT 0,
                decision VARCHAR(16) NOT NULL,
                decision_reason VARCHAR(255) NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_event_market_backtest (event_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

    def clear_results(self, start: dt.date, end: dt.date) -> None:
        result_table = self.config["database"].get("result_table", "safe_market_backtest_results")
        self.cur.execute(f"DELETE FROM {result_table} WHERE match_date BETWEEN %s AND %s", (start, end))

    def fetch_backtest_matches(self, start: dt.date, end: dt.date) -> List[Dict[str, Any]]:
        source_table = self.config["database"].get("source_table", "results_football")
        self.cur.execute(f"""
            SELECT *
            FROM {source_table}
            WHERE start_utc BETWEEN %s AND %s
              AND status IN ('finished','ended')
              AND ft_home IS NOT NULL
              AND ft_away IS NOT NULL
              AND home_team IS NOT NULL
              AND away_team IS NOT NULL
            ORDER BY start_utc ASC, start_time_utc ASC, event_id ASC
        """, (start, end))
        return self.cur.fetchall()

    def fetch_history_for_match(self, match: Dict[str, Any], lookback_days: int) -> Dict[str, List[Dict[str, Any]]]:
        source_table = self.config["database"].get("source_table", "results_football")
        match_date = match["start_utc"]
        if not isinstance(match_date, dt.date):
            match_date = parse_date(match_date, "match_date")
        cutoff = match_date - dt.timedelta(days=1)
        since = cutoff - dt.timedelta(days=int(lookback_days))

        category_id = match.get("category_id")
        home = match.get("home_team")
        away = match.get("away_team")

        # Lig geçmişi
        self.cur.execute(f"""
            SELECT *
            FROM {source_table}
            WHERE status IN ('finished','ended')
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
              AND start_utc BETWEEN %s AND %s
              AND category_id <=> %s
            ORDER BY start_utc DESC, start_time_utc DESC
        """, (since, cutoff, category_id))
        league_rows = self.cur.fetchall()

        # Ev sahibi takım geçmişi aynı lig
        self.cur.execute(f"""
            SELECT *
            FROM {source_table}
            WHERE status IN ('finished','ended')
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
              AND start_utc BETWEEN %s AND %s
              AND category_id <=> %s
              AND (home_team=%s OR away_team=%s)
            ORDER BY start_utc DESC, start_time_utc DESC
            LIMIT 80
        """, (since, cutoff, category_id, home, home))
        home_rows = self.cur.fetchall()

        # Deplasman takım geçmişi aynı lig
        self.cur.execute(f"""
            SELECT *
            FROM {source_table}
            WHERE status IN ('finished','ended')
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
              AND start_utc BETWEEN %s AND %s
              AND category_id <=> %s
              AND (home_team=%s OR away_team=%s)
            ORDER BY start_utc DESC, start_time_utc DESC
            LIMIT 80
        """, (since, cutoff, category_id, away, away))
        away_rows = self.cur.fetchall()

        # H2H geçmişi
        self.cur.execute(f"""
            SELECT *
            FROM {source_table}
            WHERE status IN ('finished','ended')
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
              AND start_utc BETWEEN %s AND %s
              AND category_id <=> %s
              AND (
                    (home_team=%s AND away_team=%s)
                 OR (home_team=%s AND away_team=%s)
              )
            ORDER BY start_utc DESC, start_time_utc DESC
            LIMIT 20
        """, (since, cutoff, category_id, home, away, away, home))
        h2h_rows = self.cur.fetchall()

        return {
            "league": league_rows,
            "home": home_rows,
            "away": away_rows,
            "h2h": h2h_rows,
        }

    def save_pick(self, pick: BacktestPick) -> None:
        result_table = self.config["database"].get("result_table", "safe_market_backtest_results")
        self.cur.execute(f"""
            INSERT INTO {result_table}
            (event_id, match_date, home_team, away_team, category_id, tournament_id,
             market_code, market_label, odds, implied_prob, model_prob, edge_value,
             confidence, risk_score, sample_score, league_sample, home_sample, away_sample,
             h2h_sample, actual_hit, profit_unit, decision, decision_reason, updated_at)
            VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON DUPLICATE KEY UPDATE
             match_date=VALUES(match_date), home_team=VALUES(home_team), away_team=VALUES(away_team),
             category_id=VALUES(category_id), tournament_id=VALUES(tournament_id),
             market_code=VALUES(market_code), market_label=VALUES(market_label), odds=VALUES(odds),
             implied_prob=VALUES(implied_prob), model_prob=VALUES(model_prob), edge_value=VALUES(edge_value),
             confidence=VALUES(confidence), risk_score=VALUES(risk_score), sample_score=VALUES(sample_score),
             league_sample=VALUES(league_sample), home_sample=VALUES(home_sample), away_sample=VALUES(away_sample),
             h2h_sample=VALUES(h2h_sample), actual_hit=VALUES(actual_hit), profit_unit=VALUES(profit_unit),
             decision=VALUES(decision), decision_reason=VALUES(decision_reason), updated_at=NOW()
        """, (
            pick.event_id, pick.match_date, pick.home_team, pick.away_team, pick.category_id, pick.tournament_id,
            pick.market_code, pick.market_label, pick.odds, pick.implied_prob, pick.model_prob, pick.edge,
            pick.confidence, pick.risk_score, pick.sample_score, pick.league_sample, pick.home_sample,
            pick.away_sample, pick.h2h_sample, pick.actual_hit, pick.profit_unit, pick.decision, pick.decision_reason,
        ))


# ==========================================================
# MOTOR
# ==========================================================

class SafeMarketBacktester:
    def __init__(self, db: DB, config: Dict[str, Any]):
        self.db = db
        self.config = config
        self.market_order = config["markets"].get("enabled", list(DEFAULT_MARKETS.keys()))
        self.market_defs = self.build_market_defs()

    def build_market_defs(self) -> Dict[str, MarketDef]:
        defs = dict(DEFAULT_MARKETS)
        custom = self.config.get("market_limits", {}) or {}
        for code, override in custom.items():
            if code not in defs or not isinstance(override, dict):
                continue
            base = defs[code]
            defs[code] = MarketDef(
                code=base.code,
                label=str(override.get("label", base.label)),
                odds_column=str(override.get("odds_column", base.odds_column)),
                risk_weight=float(override.get("risk_weight", base.risk_weight)),
                min_odds=float(override.get("min_odds", base.min_odds)),
                max_odds=float(override.get("max_odds", base.max_odds)),
            )
        return defs

    def sample_rate(self, code: str, rows: Sequence[Dict[str, Any]]) -> Tuple[Optional[float], int, float]:
        hits: List[int] = []
        goals: List[int] = []
        for r in rows:
            hit = market_hit(code, r)
            if hit is not None:
                hits.append(hit)
            g = match_goals(r)
            if g is not None:
                goals.append(g)
        if not hits:
            return None, 0, 0.0
        rate = sum(hits) / len(hits)
        volatility = stdev_or_zero(goals)
        return rate, len(hits), volatility

    def weighted_probability(self, code: str, history: Dict[str, List[Dict[str, Any]]]) -> Tuple[float, Dict[str, Any]]:
        cfg = self.config["scoring"]
        global_prior = float(cfg.get("global_prior", 0.62))
        min_sample = int(cfg.get("min_sample_for_full_weight", 40))

        components = []
        detail: Dict[str, Any] = {}
        weights = {
            "league": float(cfg.get("league_weight", 0.42)),
            "home": float(cfg.get("home_weight", 0.22)),
            "away": float(cfg.get("away_weight", 0.22)),
            "h2h": float(cfg.get("h2h_weight", 0.14)),
        }

        for key in ("league", "home", "away", "h2h"):
            rate, sample, volatility = self.sample_rate(code, history.get(key, []))
            detail[f"{key}_rate"] = rate
            detail[f"{key}_sample"] = sample
            detail[f"{key}_volatility"] = volatility
            if rate is None or sample == 0:
                # Veri yoksa global prior düşük ağırlıkla girer.
                adjusted_weight = weights[key] * 0.15
                components.append((global_prior, adjusted_weight))
            else:
                sample_factor = clamp(sample / min_sample, 0.15, 1.0)
                adjusted_weight = weights[key] * sample_factor
                # Sample azsa prior'a yaklaştır.
                shrink = clamp(sample / (sample + 18.0), 0.0, 1.0)
                shrunk_rate = (rate * shrink) + (global_prior * (1.0 - shrink))
                components.append((shrunk_rate, adjusted_weight))

        total_weight = sum(w for _, w in components)
        if total_weight <= 0:
            return global_prior, detail
        prob = sum(p * w for p, w in components) / total_weight
        return clamp(prob, 0.01, 0.99), detail

    def calculate_sample_score(self, detail: Dict[str, Any]) -> float:
        cfg = self.config["scoring"]
        min_total = int(cfg.get("min_total_sample", 60))
        total_sample = (
            int(detail.get("league_sample") or 0)
            + int(detail.get("home_sample") or 0)
            + int(detail.get("away_sample") or 0)
            + int(detail.get("h2h_sample") or 0)
        )
        return clamp((total_sample / max(min_total, 1)) * 100.0, 0.0, 100.0)

    def calculate_risk_score(self, market: MarketDef, odds: float, detail: Dict[str, Any]) -> float:
        cfg = self.config["risk"]
        risk = float(market.risk_weight)

        # Oran yükseldikçe varyans artar.
        risk += max(0.0, odds - 1.30) * float(cfg.get("odds_risk_multiplier", 18.0))

        # Lig gol oynaklığı yüksekse alt/üst marketlere risk ekle.
        league_vol = float(detail.get("league_volatility") or 0.0)
        if market.code.startswith("O") or market.code.startswith("U") or market.code.startswith("BTTS"):
            risk += max(0.0, league_vol - 1.35) * float(cfg.get("volatility_risk_multiplier", 7.0))

        # H2H hiç yoksa küçük ceza, takım datası azsa büyük ceza.
        if int(detail.get("h2h_sample") or 0) == 0:
            risk += float(cfg.get("no_h2h_penalty", 3.0))
        if int(detail.get("home_sample") or 0) < int(cfg.get("low_team_sample_limit", 8)):
            risk += float(cfg.get("low_team_sample_penalty", 5.0))
        if int(detail.get("away_sample") or 0) < int(cfg.get("low_team_sample_limit", 8)):
            risk += float(cfg.get("low_team_sample_penalty", 5.0))

        return round(clamp(risk, 0.0, 100.0), 4)

    def evaluate_market(self, match: Dict[str, Any], market: MarketDef, history: Dict[str, List[Dict[str, Any]]]) -> Optional[MarketEvaluation]:
        odds = safe_float(match.get(market.odds_column))
        if odds is None or odds <= 1.0:
            return None
        if odds < market.min_odds or odds > market.max_odds:
            return None

        model_prob, detail = self.weighted_probability(market.code, history)
        sample_score = self.calculate_sample_score(detail)
        implied_prob = 1.0 / odds
        edge = model_prob - implied_prob
        risk_score = self.calculate_risk_score(market, odds, detail)

        # Güven: model + edge + sample, eksi risk.
        confidence = (
            model_prob * 100.0
            + edge * float(self.config["scoring"].get("edge_confidence_multiplier", 85.0))
            + sample_score * float(self.config["scoring"].get("sample_confidence_multiplier", 0.08))
            - risk_score
        )
        confidence = round(clamp(confidence, 0.0, 100.0), 4)

        return MarketEvaluation(
            market_code=market.code,
            market_label=market.label,
            odds=round(odds, 4),
            implied_prob=round(implied_prob * 100.0, 4),
            model_prob=round(model_prob * 100.0, 4),
            edge=round(edge * 100.0, 4),
            confidence=confidence,
            risk_score=risk_score,
            sample_score=round(sample_score, 4),
            league_sample=int(detail.get("league_sample") or 0),
            home_sample=int(detail.get("home_sample") or 0),
            away_sample=int(detail.get("away_sample") or 0),
            h2h_sample=int(detail.get("h2h_sample") or 0),
            decision_reason="OK",
        )

    def passes_thresholds(self, ev: MarketEvaluation) -> Tuple[bool, str]:
        th = self.config["thresholds"]
        if ev.model_prob < float(th.get("min_model_prob", 80.0)):
            return False, f"model düşük: {ev.model_prob:.1f}"
        if ev.edge < float(th.get("min_edge", 2.5)):
            return False, f"edge düşük: {ev.edge:.1f}"
        if ev.confidence < float(th.get("min_confidence", 82.0)):
            return False, f"güven düşük: {ev.confidence:.1f}"
        if ev.risk_score > float(th.get("max_risk_score", 28.0)):
            return False, f"risk yüksek: {ev.risk_score:.1f}"
        if ev.league_sample < int(th.get("min_league_sample", 30)):
            return False, f"lig örneği az: {ev.league_sample}"
        if (ev.home_sample + ev.away_sample) < int(th.get("min_team_total_sample", 16)):
            return False, f"takım örneği az: {ev.home_sample + ev.away_sample}"
        return True, "PLAY"

    def choose_pick_for_match(self, match: Dict[str, Any], history: Dict[str, List[Dict[str, Any]]]) -> BacktestPick:
        match_date = match["start_utc"] if isinstance(match["start_utc"], dt.date) else parse_date(match["start_utc"], "match_date")
        event_id = int(match["event_id"])
        home = str(match["home_team"])
        away = str(match["away_team"])
        category_id = match.get("category_id")
        tournament_id = match.get("tournament_id")

        evaluations: List[MarketEvaluation] = []
        for code in self.market_order:
            market = self.market_defs.get(code)
            if not market:
                continue
            ev = self.evaluate_market(match, market, history)
            if ev is not None:
                evaluations.append(ev)

        if not evaluations:
            return BacktestPick(
                event_id=event_id, match_date=match_date, home_team=home, away_team=away,
                category_id=category_id, tournament_id=tournament_id,
                market_code="NO_BET", market_label="NO_BET", odds=None, implied_prob=None,
                model_prob=None, edge=None, confidence=None, risk_score=None, sample_score=None,
                league_sample=0, home_sample=0, away_sample=0, h2h_sample=0,
                actual_hit=None, profit_unit=0.0, decision="NO_BET", decision_reason="uygun oran/market yok",
            )

        # Önce eşik geçenleri al, sonra confidence yüksek + risk düşük + edge yüksek seç.
        playable: List[Tuple[MarketEvaluation, str]] = []
        rejected_reasons: List[str] = []
        for ev in evaluations:
            ok, reason = self.passes_thresholds(ev)
            if ok:
                playable.append((ev, reason))
            else:
                rejected_reasons.append(f"{ev.market_code}: {reason}")

        if not playable:
            best = sorted(evaluations, key=lambda x: (x.confidence, x.edge, -x.risk_score), reverse=True)[0]
            return BacktestPick(
                event_id=event_id, match_date=match_date, home_team=home, away_team=away,
                category_id=category_id, tournament_id=tournament_id,
                market_code="NO_BET", market_label="NO_BET", odds=best.odds,
                implied_prob=best.implied_prob, model_prob=best.model_prob, edge=best.edge,
                confidence=best.confidence, risk_score=best.risk_score, sample_score=best.sample_score,
                league_sample=best.league_sample, home_sample=best.home_sample,
                away_sample=best.away_sample, h2h_sample=best.h2h_sample,
                actual_hit=None, profit_unit=0.0, decision="NO_BET",
                decision_reason="; ".join(rejected_reasons[:3])[:250],
            )

        chosen = sorted(
            (p[0] for p in playable),
            key=lambda x: (x.confidence, x.edge, -x.risk_score, x.model_prob),
            reverse=True,
        )[0]
        actual = market_hit(chosen.market_code, match)
        profit = 0.0
        if actual is not None:
            profit = round((chosen.odds - 1.0) if actual == 1 else -1.0, 4)

        return BacktestPick(
            event_id=event_id, match_date=match_date, home_team=home, away_team=away,
            category_id=category_id, tournament_id=tournament_id,
            market_code=chosen.market_code, market_label=chosen.market_label,
            odds=chosen.odds, implied_prob=chosen.implied_prob, model_prob=chosen.model_prob,
            edge=chosen.edge, confidence=chosen.confidence, risk_score=chosen.risk_score,
            sample_score=chosen.sample_score, league_sample=chosen.league_sample,
            home_sample=chosen.home_sample, away_sample=chosen.away_sample,
            h2h_sample=chosen.h2h_sample, actual_hit=actual, profit_unit=profit,
            decision="PLAY", decision_reason="PLAY",
        )

    def run(self, start: dt.date, end: dt.date) -> None:
        cfg = self.config
        lookback_days = int(cfg["backtest"].get("lookback_days", 500))
        clear_before_run = bool(cfg["backtest"].get("clear_before_run", True))
        max_daily_picks = int(cfg["thresholds"].get("max_daily_picks", 3))

        if clear_before_run:
            self.db.clear_results(start, end)
            print(f"[DB] Eski sonuçlar temizlendi: {start} - {end}")

        matches = self.db.fetch_backtest_matches(start, end)
        print(f"[BACKTEST] Tarih: {start} - {end} | maç: {len(matches)} | lookback: {lookback_days} gün")

        picks_by_date: Dict[dt.date, List[BacktestPick]] = {}
        no_bet_count = 0

        for idx, match in enumerate(matches, start=1):
            history = self.db.fetch_history_for_match(match, lookback_days)
            pick = self.choose_pick_for_match(match, history)
            picks_by_date.setdefault(pick.match_date, []).append(pick)
            if idx % 100 == 0:
                print(f"  işlenen maç: {idx}/{len(matches)}")

        final_picks: List[BacktestPick] = []
        for day, day_picks in sorted(picks_by_date.items()):
            play = [p for p in day_picks if p.decision == "PLAY"]
            no_bets = [p for p in day_picks if p.decision != "PLAY"]

            play_sorted = sorted(
                play,
                key=lambda p: (p.confidence or 0, p.edge or 0, -(p.risk_score or 999)),
                reverse=True,
            )
            allowed = play_sorted[:max_daily_picks]
            blocked = play_sorted[max_daily_picks:]

            final_picks.extend(allowed)
            final_picks.extend(no_bets)

            for p in blocked:
                p.decision = "NO_BET"
                p.actual_hit = None
                p.profit_unit = 0.0
                p.decision_reason = f"günlük limit dışı: max_daily_picks={max_daily_picks}"
                final_picks.append(p)

        total_play = 0
        total_win = 0
        total_profit = 0.0
        market_stats: Dict[str, Dict[str, float]] = {}

        for p in final_picks:
            self.db.save_pick(p)
            if p.decision == "PLAY":
                total_play += 1
                total_win += 1 if p.actual_hit == 1 else 0
                total_profit += p.profit_unit
                ms = market_stats.setdefault(p.market_code, {"play": 0, "win": 0, "profit": 0.0})
                ms["play"] += 1
                ms["win"] += 1 if p.actual_hit == 1 else 0
                ms["profit"] += p.profit_unit
            else:
                no_bet_count += 1

        hit_rate = (total_win / total_play * 100.0) if total_play else 0.0
        roi = (total_profit / total_play * 100.0) if total_play else 0.0

        print("\n" + "=" * 72)
        print("BACKTEST ÖZET")
        print("=" * 72)
        print(f"Toplam maç       : {len(matches)}")
        print(f"PLAY             : {total_play}")
        print(f"NO_BET           : {no_bet_count}")
        print(f"Kazanan          : {total_win}")
        print(f"Başarı           : %{hit_rate:.2f}")
        print(f"Birim kar        : {total_profit:.2f}")
        print(f"ROI              : %{roi:.2f}")

        if market_stats:
            print("\nMARKET ÖZET")
            for code, stat in sorted(market_stats.items(), key=lambda kv: kv[1]["profit"], reverse=True):
                play = int(stat["play"])
                win = int(stat["win"])
                profit = float(stat["profit"])
                rate = (win / play * 100.0) if play else 0.0
                roi_m = (profit / play * 100.0) if play else 0.0
                label = self.market_defs.get(code).label if code in self.market_defs else code
                print(f"  {code:8s} {label:10s} | maç={play:4d} | başarı=%{rate:6.2f} | kar={profit:8.2f} | ROI=%{roi_m:7.2f}")

        print(f"\n[DB] Sonuç tablosu: {self.config['database'].get('result_table', 'safe_market_backtest_results')}")


# ==========================================================
# CONFIG
# ==========================================================

def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    required = ["database", "backtest", "markets", "thresholds", "scoring", "risk"]
    missing = [k for k in required if k not in cfg]
    if missing:
        raise ValueError(f"YML eksik bölümler: {', '.join(missing)}")
    return cfg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sızıntısız güvenli market backtest motoru")
    parser.add_argument("--config", default="safe_market_backtest_v1.yml", help="YML config dosyası")
    parser.add_argument("--start", default=None, help="Başlangıç tarihi YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="Bitiş tarihi YYYY-MM-DD")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)

    start = parse_date(args.start or cfg["backtest"].get("start_date"), "start_date")
    end = parse_date(args.end or cfg["backtest"].get("end_date"), "end_date")
    if end < start:
        raise ValueError(f"Bitiş tarihi başlangıçtan küçük olamaz: {end} < {start}")

    db = DB(cfg)
    db.connect()
    try:
        SafeMarketBacktester(db, cfg).run(start, end)
    finally:
        db.close()


if __name__ == "__main__":
    main()
