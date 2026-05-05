#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SAFE MARKET BACKTEST MOTORU

Amaç:
- results_football tablosundaki 500 günlük geçmişten sızıntısız backtest yapmak.
- Her maç için önce marketleri test eder, sonra sadece en güvenli marketi seçer.
- Eşiklerden geçemeyen maça NO_BET der.
- Fikstur_cek.py ve maclari_guncelle.py dosyalarına dokunmadan çalışır.

Kullanım:
    python3 safe_market_backtest.py --config safe_market_backtest.yml
    python3 safe_market_backtest.py --config safe_market_backtest.yml --start 2026-02-04 --end 2026-05-04

Gerekli tablo:
    results_football

Oluşturduğu tablo:
    safe_market_backtest_results
"""

import argparse
import datetime as dt
import math
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------


def _env_replace(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    pattern = re.compile(r"\$\{([A-Z0-9_]+)(?::([^}]*))?\}")

    def repl(match: re.Match) -> str:
        key = match.group(1)
        default = match.group(2) if match.group(2) is not None else ""
        return os.getenv(key, default)

    return pattern.sub(repl, value)


def _walk_env(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _walk_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk_env(v) for v in obj]
    return _env_replace(obj)


def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config bulunamadı: {path}")
    if yaml is None:
        raise RuntimeError("PyYAML kurulu değil. Kurulum: pip3 install pyyaml")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return _walk_env(data)


def as_date(value: Any) -> dt.date:
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value).strip())


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


# ------------------------------------------------------------
# MARKET MODEL
# ------------------------------------------------------------


@dataclass
class Market:
    code: str
    label: str
    odds_column: str
    enabled: bool
    min_odds: float
    max_odds: float
    min_probability: float
    min_edge: float
    risk_weight: float


def market_won(code: str, ft_home: int, ft_away: int) -> bool:
    total = ft_home + ft_away
    if code == "O05":
        return total >= 1
    if code == "O15":
        return total >= 2
    if code == "O25":
        return total >= 3
    if code == "O35":
        return total >= 4
    if code == "U25":
        return total <= 2
    if code == "U35":
        return total <= 3
    if code == "U45":
        return total <= 4
    if code == "U55":
        return total <= 5
    if code == "BTTS_YES":
        return ft_home > 0 and ft_away > 0
    if code == "BTTS_NO":
        return ft_home == 0 or ft_away == 0
    if code == "DC_1X":
        return ft_home >= ft_away
    if code == "DC_X2":
        return ft_away >= ft_home
    if code == "DC_12":
        return ft_home != ft_away
    if code == "HOME_WIN":
        return ft_home > ft_away
    if code == "AWAY_WIN":
        return ft_away > ft_home
    return False


def market_sql_condition(code: str) -> str:
    if code == "O05":
        return "(ft_home + ft_away) >= 1"
    if code == "O15":
        return "(ft_home + ft_away) >= 2"
    if code == "O25":
        return "(ft_home + ft_away) >= 3"
    if code == "O35":
        return "(ft_home + ft_away) >= 4"
    if code == "U25":
        return "(ft_home + ft_away) <= 2"
    if code == "U35":
        return "(ft_home + ft_away) <= 3"
    if code == "U45":
        return "(ft_home + ft_away) <= 4"
    if code == "U55":
        return "(ft_home + ft_away) <= 5"
    if code == "BTTS_YES":
        return "(ft_home > 0 AND ft_away > 0)"
    if code == "BTTS_NO":
        return "(ft_home = 0 OR ft_away = 0)"
    if code == "DC_1X":
        return "(ft_home >= ft_away)"
    if code == "DC_X2":
        return "(ft_away >= ft_home)"
    if code == "DC_12":
        return "(ft_home <> ft_away)"
    if code == "HOME_WIN":
        return "(ft_home > ft_away)"
    if code == "AWAY_WIN":
        return "(ft_away > ft_home)"
    return "0"


# ------------------------------------------------------------
# DB
# ------------------------------------------------------------


class DB:
    def __init__(self, cfg: Dict[str, Any]):
        db_cfg = cfg.get("database", {})
        self.conn = mysql.connector.connect(
            host=db_cfg.get("host"),
            user=db_cfg.get("user"),
            password=db_cfg.get("password"),
            database=db_cfg.get("name") or db_cfg.get("database"),
            port=to_int(db_cfg.get("port"), 3306),
            charset="utf8mb4",
            use_unicode=True,
            collation="utf8mb4_unicode_ci",
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)

    def close(self) -> None:
        try:
            self.cur.close()
        finally:
            self.conn.close()

    def ensure_output_table(self, table_name: str) -> None:
        self.cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(64) NOT NULL,
                event_id BIGINT UNSIGNED NOT NULL,
                match_date DATE NOT NULL,
                start_time_utc TIME NULL,
                home_team VARCHAR(128) NOT NULL,
                away_team VARCHAR(128) NOT NULL,
                country VARCHAR(64) NULL,
                tournament_id INT NULL,
                tournament_name VARCHAR(128) NULL,
                category_id INT NULL,
                selected_market VARCHAR(32) NULL,
                selected_label VARCHAR(64) NULL,
                selected_odds FLOAT NULL,
                model_probability FLOAT NULL,
                implied_probability FLOAT NULL,
                edge_value FLOAT NULL,
                confidence_score FLOAT NULL,
                risk_score FLOAT NULL,
                sample_score FLOAT NULL,
                league_sample INT NOT NULL DEFAULT 0,
                home_sample INT NOT NULL DEFAULT 0,
                away_sample INT NOT NULL DEFAULT 0,
                h2h_sample INT NOT NULL DEFAULT 0,
                decision VARCHAR(16) NOT NULL,
                reject_reason VARCHAR(255) NULL,
                ft_home INT NULL,
                ft_away INT NULL,
                won BOOLEAN NULL,
                profit_unit FLOAT NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_run_event (run_id, event_id),
                KEY idx_run_date (run_id, match_date),
                KEY idx_decision (decision),
                KEY idx_market (selected_market)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

    def clear_run(self, table_name: str, run_id: str) -> None:
        self.cur.execute(f"DELETE FROM {table_name} WHERE run_id=%s", (run_id,))

    def fetch_finished_matches(self, start: dt.date, end: dt.date, only_major_ids: List[int]) -> List[Dict[str, Any]]:
        ids_filter = ""
        params: List[Any] = [start, end]
        if only_major_ids:
            placeholders = ",".join(["%s"] * len(only_major_ids))
            ids_filter = f"AND (tournament_id IN ({placeholders}) OR category_id IN ({placeholders}))"
            params.extend(only_major_ids)
            params.extend(only_major_ids)

        self.cur.execute(f"""
            SELECT *
            FROM results_football
            WHERE status IN ('finished','ended')
              AND start_utc BETWEEN %s AND %s
              AND ft_home IS NOT NULL AND ft_away IS NOT NULL
              AND home_team IS NOT NULL AND away_team IS NOT NULL
              {ids_filter}
            ORDER BY start_utc ASC, start_time_utc ASC, event_id ASC
        """, tuple(params))
        return self.cur.fetchall()

    def historical_rate(
        self,
        code: str,
        cutoff: dt.date,
        lookback_days: int,
        category_id: Optional[int] = None,
        team_name: Optional[str] = None,
        opponent_name: Optional[str] = None,
        min_date: Optional[dt.date] = None,
    ) -> Tuple[Optional[float], int]:
        cond = market_sql_condition(code)
        params: List[Any] = []
        where = [
            "status IN ('finished','ended')",
            "ft_home IS NOT NULL AND ft_away IS NOT NULL",
            "start_utc <= %s",
        ]
        params.append(cutoff)

        lower = min_date or (cutoff - dt.timedelta(days=lookback_days))
        where.append("start_utc >= %s")
        params.append(lower)

        if category_id is not None:
            where.append("category_id = %s")
            params.append(category_id)

        if team_name and opponent_name:
            where.append("((home_team=%s AND away_team=%s) OR (home_team=%s AND away_team=%s))")
            params.extend([team_name, opponent_name, opponent_name, team_name])
        elif team_name:
            where.append("(home_team=%s OR away_team=%s)")
            params.extend([team_name, team_name])

        self.cur.execute(f"""
            SELECT COUNT(*) AS sample,
                   SUM(CASE WHEN {cond} THEN 1 ELSE 0 END) AS wins
            FROM results_football
            WHERE {' AND '.join(where)}
        """, tuple(params))
        row = self.cur.fetchone() or {}
        sample = int(row.get("sample") or 0)
        wins = int(row.get("wins") or 0)
        if sample <= 0:
            return None, 0
        return wins / sample, sample

    def insert_result(self, table_name: str, row: Dict[str, Any]) -> None:
        cols = list(row.keys())
        placeholders = ",".join(["%s"] * len(cols))
        updates = ",".join([f"{c}=VALUES({c})" for c in cols if c not in {"run_id", "event_id"}])
        sql = f"""
            INSERT INTO {table_name} ({','.join(cols)})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {updates}
        """
        self.cur.execute(sql, tuple(row[c] for c in cols))


# ------------------------------------------------------------
# ENGINE
# ------------------------------------------------------------


class SafeMarketBacktester:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.db = DB(cfg)
        self.output_table = cfg.get("output", {}).get("table", "safe_market_backtest_results")
        self.markets = self._load_markets(cfg)
        self.weights = cfg.get("model", {}).get("weights", {})
        self.risk = cfg.get("risk", {})
        self.bt = cfg.get("backtest", {})
        self.db.ensure_output_table(self.output_table)

    def close(self) -> None:
        self.db.close()

    @staticmethod
    def _load_markets(cfg: Dict[str, Any]) -> List[Market]:
        markets: List[Market] = []
        for item in cfg.get("markets", []):
            markets.append(Market(
                code=str(item.get("code")),
                label=str(item.get("label") or item.get("code")),
                odds_column=str(item.get("odds_column")),
                enabled=bool(item.get("enabled", True)),
                min_odds=to_float(item.get("min_odds"), 1.01),
                max_odds=to_float(item.get("max_odds"), 10.0),
                min_probability=to_float(item.get("min_probability"), 0.70),
                min_edge=to_float(item.get("min_edge"), 0.0),
                risk_weight=to_float(item.get("risk_weight"), 1.0),
            ))
        return [m for m in markets if m.enabled]

    def run(self, start: dt.date, end: dt.date, run_id: Optional[str] = None) -> None:
        run_id = run_id or f"safe_{start}_{end}"
        if bool(self.bt.get("clear_existing_run", True)):
            self.db.clear_run(self.output_table, run_id)

        major_ids = [int(x) for x in self.cfg.get("league_filter", {}).get("major_tournament_ids", [])]
        matches = self.db.fetch_finished_matches(start, end, major_ids)
        print(f"[BACKTEST] {start} - {end} | maç: {len(matches)} | run_id={run_id}")

        daily_limit = to_int(self.risk.get("max_daily_picks"), 3)
        picked_by_day: Dict[dt.date, int] = {}
        total_bet = total_win = 0
        total_profit = 0.0

        for i, match in enumerate(matches, start=1):
            match_date = as_date(match["start_utc"])
            current_daily = picked_by_day.get(match_date, 0)
            result = self.evaluate_match(match, run_id)

            if result["decision"] == "PLAY" and current_daily >= daily_limit:
                result["decision"] = "NO_BET"
                result["reject_reason"] = f"Günlük limit doldu: {daily_limit}"
                result["won"] = None
                result["profit_unit"] = 0.0

            if result["decision"] == "PLAY":
                picked_by_day[match_date] = current_daily + 1
                total_bet += 1
                if result["won"]:
                    total_win += 1
                total_profit += float(result["profit_unit"] or 0)

            self.db.insert_result(self.output_table, result)

            if i % 250 == 0:
                print(f"  işlenen: {i}/{len(matches)} | bet={total_bet} | profit={total_profit:.2f}")

        roi = (total_profit / total_bet * 100.0) if total_bet else 0.0
        hit = (total_win / total_bet * 100.0) if total_bet else 0.0
        print("\n" + "=" * 72)
        print(f"Bitti: {start} - {end}")
        print(f"PLAY: {total_bet} | WIN: {total_win} | HIT: %{hit:.2f} | PROFIT(unit): {total_profit:.2f} | ROI: %{roi:.2f}")
        print(f"Sonuç tablosu: {self.output_table} | run_id={run_id}")
        print("=" * 72)

    def evaluate_match(self, match: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        cutoff_days = to_int(self.bt.get("cutoff_days_before_match"), 1)
        lookback_days = to_int(self.bt.get("lookback_days"), 500)
        match_date = as_date(match["start_utc"])
        cutoff = match_date - dt.timedelta(days=cutoff_days)

        best: Optional[Dict[str, Any]] = None
        rejects: List[str] = []

        for market in self.markets:
            odds = to_float(match.get(market.odds_column), 0.0)
            if odds <= 0:
                rejects.append(f"{market.code}: oran yok")
                continue
            if odds < market.min_odds or odds > market.max_odds:
                rejects.append(f"{market.code}: oran bandı dışı {odds}")
                continue

            score = self.score_market(match, market, odds, cutoff, lookback_days)
            if score["reject_reason"]:
                rejects.append(f"{market.code}: {score['reject_reason']}")
                continue

            if best is None or score["confidence_score"] > best["confidence_score"]:
                best = score

        base = {
            "run_id": run_id,
            "event_id": int(match["event_id"]),
            "match_date": match_date,
            "start_time_utc": match.get("start_time_utc"),
            "home_team": match.get("home_team"),
            "away_team": match.get("away_team"),
            "country": match.get("country"),
            "tournament_id": match.get("tournament_id"),
            "tournament_name": match.get("tournament_name"),
            "category_id": match.get("category_id"),
            "ft_home": match.get("ft_home"),
            "ft_away": match.get("ft_away"),
        }

        if best is None:
            base.update({
                "selected_market": None,
                "selected_label": None,
                "selected_odds": None,
                "model_probability": None,
                "implied_probability": None,
                "edge_value": None,
                "confidence_score": None,
                "risk_score": None,
                "sample_score": None,
                "league_sample": 0,
                "home_sample": 0,
                "away_sample": 0,
                "h2h_sample": 0,
                "decision": "NO_BET",
                "reject_reason": "; ".join(rejects[:4]) if rejects else "Uygun market yok",
                "won": None,
                "profit_unit": 0.0,
            })
            return base

        won = market_won(best["selected_market"], int(match["ft_home"]), int(match["ft_away"]))
        profit = (float(best["selected_odds"]) - 1.0) if won else -1.0
        base.update(best)
        base.update({
            "decision": "PLAY",
            "reject_reason": None,
            "won": 1 if won else 0,
            "profit_unit": round(profit, 4),
        })
        return base

    def score_market(self, match: Dict[str, Any], market: Market, odds: float, cutoff: dt.date, lookback_days: int) -> Dict[str, Any]:
        cat_id = match.get("category_id")
        home = match.get("home_team")
        away = match.get("away_team")

        league_rate, league_sample = self.db.historical_rate(market.code, cutoff, lookback_days, category_id=cat_id)
        home_rate, home_sample = self.db.historical_rate(market.code, cutoff, lookback_days, category_id=cat_id, team_name=home)
        away_rate, away_sample = self.db.historical_rate(market.code, cutoff, lookback_days, category_id=cat_id, team_name=away)
        h2h_rate, h2h_sample = self.db.historical_rate(market.code, cutoff, lookback_days, category_id=cat_id, team_name=home, opponent_name=away)

        min_league_sample = to_int(self.risk.get("min_league_sample"), 40)
        min_team_sample = to_int(self.risk.get("min_team_sample"), 8)
        min_total_sample = to_int(self.risk.get("min_total_sample"), 60)

        total_sample = league_sample + home_sample + away_sample + h2h_sample
        if league_sample < min_league_sample:
            return self.reject(market, odds, f"lig örneği az {league_sample}/{min_league_sample}", league_sample, home_sample, away_sample, h2h_sample)
        if home_sample < min_team_sample or away_sample < min_team_sample:
            return self.reject(market, odds, f"takım örneği az H:{home_sample} A:{away_sample}", league_sample, home_sample, away_sample, h2h_sample)
        if total_sample < min_total_sample:
            return self.reject(market, odds, f"toplam örnek az {total_sample}/{min_total_sample}", league_sample, home_sample, away_sample, h2h_sample)

        rates: List[Tuple[float, float]] = []
        if league_rate is not None:
            rates.append((league_rate, to_float(self.weights.get("league"), 0.40)))
        if home_rate is not None:
            rates.append((home_rate, to_float(self.weights.get("home_team"), 0.22)))
        if away_rate is not None:
            rates.append((away_rate, to_float(self.weights.get("away_team"), 0.22)))
        if h2h_rate is not None and h2h_sample >= to_int(self.risk.get("min_h2h_sample_for_weight"), 3):
            rates.append((h2h_rate, to_float(self.weights.get("h2h"), 0.16)))

        if not rates:
            return self.reject(market, odds, "oran hesaplanamadı", league_sample, home_sample, away_sample, h2h_sample)

        weight_sum = sum(w for _, w in rates)
        probability = sum(rate * weight for rate, weight in rates) / weight_sum

        # Aşırı iyimserliği kırp: düşük örneklerde olasılığı piyasa implied değerine yaklaştır.
        implied = 1.0 / odds
        shrink_min_sample = to_int(self.risk.get("shrink_until_sample"), 180)
        shrink_strength = max(0.0, min(1.0, (shrink_min_sample - total_sample) / max(1, shrink_min_sample)))
        probability = (probability * (1.0 - shrink_strength * 0.35)) + (implied * shrink_strength * 0.35)

        # Rate disagreement risk.
        raw_rates = [r for r, _ in rates]
        mean = sum(raw_rates) / len(raw_rates)
        variance = sum((r - mean) ** 2 for r in raw_rates) / len(raw_rates)
        disagreement = math.sqrt(variance)

        probability_pct = probability * 100.0
        implied_pct = implied * 100.0
        edge = probability_pct - implied_pct

        sample_score = min(12.0, math.log(max(total_sample, 1), 2) * 1.35)
        odds_penalty = max(0.0, (odds - 1.45) * 10.0)
        disagreement_penalty = disagreement * 100.0 * to_float(self.risk.get("disagreement_penalty_multiplier"), 0.55)
        market_penalty = market.risk_weight * to_float(self.risk.get("market_risk_multiplier"), 4.0)
        risk_score = odds_penalty + disagreement_penalty + market_penalty

        confidence = probability_pct + (edge * to_float(self.risk.get("edge_confidence_multiplier"), 0.20)) + sample_score - risk_score

        min_conf = to_float(self.risk.get("min_confidence"), 78.0)
        min_prob = max(market.min_probability * 100.0, to_float(self.risk.get("min_model_probability"), 72.0))
        min_edge = max(market.min_edge, to_float(self.risk.get("min_edge"), 1.0))
        max_risk = to_float(self.risk.get("max_risk_score"), 24.0)

        if probability_pct < min_prob:
            return self.reject(market, odds, f"model düşük %{probability_pct:.1f}/{min_prob:.1f}", league_sample, home_sample, away_sample, h2h_sample)
        if edge < min_edge:
            return self.reject(market, odds, f"edge düşük {edge:.2f}/{min_edge:.2f}", league_sample, home_sample, away_sample, h2h_sample)
        if risk_score > max_risk:
            return self.reject(market, odds, f"risk yüksek {risk_score:.1f}/{max_risk:.1f}", league_sample, home_sample, away_sample, h2h_sample)
        if confidence < min_conf:
            return self.reject(market, odds, f"confidence düşük {confidence:.1f}/{min_conf:.1f}", league_sample, home_sample, away_sample, h2h_sample)

        return {
            "selected_market": market.code,
            "selected_label": market.label,
            "selected_odds": round(odds, 3),
            "model_probability": round(probability_pct, 3),
            "implied_probability": round(implied_pct, 3),
            "edge_value": round(edge, 3),
            "confidence_score": round(confidence, 3),
            "risk_score": round(risk_score, 3),
            "sample_score": round(sample_score, 3),
            "league_sample": league_sample,
            "home_sample": home_sample,
            "away_sample": away_sample,
            "h2h_sample": h2h_sample,
            "reject_reason": None,
        }

    @staticmethod
    def reject(market: Market, odds: float, reason: str, league_sample: int, home_sample: int, away_sample: int, h2h_sample: int) -> Dict[str, Any]:
        return {
            "selected_market": market.code,
            "selected_label": market.label,
            "selected_odds": round(odds, 3) if odds else None,
            "model_probability": None,
            "implied_probability": round((1.0 / odds) * 100.0, 3) if odds else None,
            "edge_value": None,
            "confidence_score": None,
            "risk_score": None,
            "sample_score": None,
            "league_sample": league_sample,
            "home_sample": home_sample,
            "away_sample": away_sample,
            "h2h_sample": h2h_sample,
            "reject_reason": reason,
        }


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sızıntısız güvenli market backtest motoru")
    p.add_argument("--config", default="safe_market_backtest.yml", help="YML config yolu")
    p.add_argument("--start", default=None, help="Backtest başlangıç tarihi YYYY-MM-DD")
    p.add_argument("--end", default=None, help="Backtest bitiş tarihi YYYY-MM-DD")
    p.add_argument("--run-id", default=None, help="Sonuç tablosunda kullanılacak run_id")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)

    start = as_date(args.start or cfg.get("backtest", {}).get("start_date"))
    end = as_date(args.end or cfg.get("backtest", {}).get("end_date"))
    if end < start:
        raise ValueError("end_date start_date'den küçük olamaz")

    engine = SafeMarketBacktester(cfg)
    try:
        engine.run(start=start, end=end, run_id=args.run_id or cfg.get("backtest", {}).get("run_id"))
    finally:
        engine.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nKullanıcı durdurdu.")
        sys.exit(130)
    except Exception as exc:
        print(f"[KRİTİK HATA] {exc}")
        sys.exit(1)
