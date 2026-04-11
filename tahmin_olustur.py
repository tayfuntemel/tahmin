#!/usr/bin/env python3
import os
import mysql.connector
import math
from datetime import datetime, timedelta, timezone

CONFIG = {
    "db": {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306))
    }
}

SCHEMA_PREDICTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS match_predictions (
    event_id BIGINT UNSIGNED NOT NULL,
    home_team VARCHAR(128) NULL,
    away_team VARCHAR(128) NULL,
    start_utc DATE NULL,
    start_time_utc TIME NULL,
    exp_goals_home FLOAT NULL,
    exp_goals_away FLOAT NULL,
    prob_ms1 FLOAT NULL,
    prob_ms0 FLOAT NULL,
    prob_ms2 FLOAT NULL,
    prob_o15 FLOAT NULL,
    prob_o35 FLOAT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

def poisson_prob(lambda_: float, k: int) -> float:
    if lambda_ <= 0:
        return 1.0 if k == 0 else 0.0
    return (math.exp(-lambda_) * (lambda_ ** k)) / math.factorial(k)

def safe_div(num, denom, default=0.0):
    return num / denom if denom != 0 else default

def get_team_stat(cursor, team_name, tournament_id, venue, table, fields):
    query = f"SELECT {', '.join(fields)} FROM {table} WHERE team_name = %s AND tournament_id = %s AND venue_type = %s"
    cursor.execute(query, (team_name, tournament_id, venue))
    return cursor.fetchone()

class Predictor:
    def __init__(self, db_config):
        self.conn = mysql.connector.connect(**db_config)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self._init_db()
        self.home_bias = 0.0
        self.away_bias = 0.0
        self._load_calibration_biases()

    def _init_db(self):
        self.cur.execute(SCHEMA_PREDICTIONS_TABLE)

    def _load_calibration_biases(self):
        try:
            self.cur.execute("SELECT param_name, param_value FROM model_calibration")
            for row in self.cur.fetchall():
                if row['param_name'] == 'home_xg_bias':
                    self.home_bias = float(row['param_value'])
                elif row['param_name'] == 'away_xg_bias':
                    self.away_bias = float(row['param_value'])
            print(f"Kalibrasyon: Ev {self.home_bias:.3f}, Deplasman {self.away_bias:.3f}")
        except:
            print("Kalibrasyon yok, varsayılan 0.0")

    def close(self):
        self.cur.close()
        self.conn.close()

    def get_upcoming_matches(self, days_ahead=1):
        tz_tr = timezone(timedelta(hours=3))
        today = datetime.now(tz_tr).date()
        end_date = today + timedelta(days=days_ahead)
        query = """
            SELECT rf.* FROM results_football rf
            LEFT JOIN match_predictions mp ON rf.event_id = mp.event_id
            WHERE rf.start_utc BETWEEN %s AND %s
              AND rf.status IN ('notstarted', 'scheduled')
              AND mp.event_id IS NULL
            ORDER BY rf.start_utc, rf.start_time_utc
        """
        self.cur.execute(query, (today, end_date))
        return self.cur.fetchall()

    def get_league_stats(self, tournament_id):
        self.cur.execute("SELECT avg_goals_home, avg_goals_away FROM league_analytics WHERE tournament_id = %s", (tournament_id,))
        return self.cur.fetchone()

    def compute_expected_goals(self, home_stats, away_stats, league_stats, home_form, away_form,
                               home_eff, away_eff, home_ht, away_ht, home_sh, away_sh, ref_stats):
        league_home = league_stats["avg_goals_home"]
        league_away = league_stats["avg_goals_away"]

        home_avg_for = home_stats.get("goals_for_home", 0) / max(1, home_stats["matches_played_home"])
        home_avg_against = home_stats.get("goals_against_home", 0) / max(1, home_stats["matches_played_home"])
        away_avg_for = away_stats.get("goals_for_away", 0) / max(1, away_stats["matches_played_away"])
        away_avg_against = away_stats.get("goals_against_away", 0) / max(1, away_stats["matches_played_away"])

        home_attack = safe_div(home_avg_for, league_home, 1.0)
        away_defense = safe_div(away_avg_against, league_away, 1.0)
        away_attack = safe_div(away_avg_for, league_away, 1.0)
        home_defense = safe_div(home_avg_against, league_home, 1.0)

        expected_home = home_attack * away_defense * league_home
        expected_away = away_attack * home_defense * league_away

        # Form etkileri
        expected_home *= (1 + 0.05 * home_form.get("current_scoring_streak", 0))
        expected_away *= (1 + 0.05 * away_form.get("current_scoring_streak", 0))
        expected_home *= (1 - 0.05 * away_form.get("current_clean_sheet_streak", 0))
        expected_away *= (1 - 0.05 * home_form.get("current_clean_sheet_streak", 0))

        # Verimlilik
        home_conv = home_eff.get("conversion_rate_pct", 10) / 100
        away_conv = away_eff.get("conversion_rate_pct", 10) / 100
        expected_home *= (1 + 0.2 * (home_conv - 0.1))
        expected_away *= (1 + 0.2 * (away_conv - 0.1))

        home_save = home_eff.get("save_rate_pct", 70) / 100
        away_save = away_eff.get("save_rate_pct", 70) / 100
        expected_home *= (1 - 0.2 * (away_save - 0.7))
        expected_away *= (1 - 0.2 * (home_save - 0.7))

        # Baskı endeksi
        p_h = home_eff.get("pressure_index", 50)
        p_a = away_eff.get("pressure_index", 50)
        if p_h > 85:
            expected_home *= (1 + min(0.10, (p_h - 85) * 0.002))
        if p_a > 85:
            expected_away *= (1 + min(0.10, (p_a - 85) * 0.002))

        # İlk/ikinci yarı BTTS
        if (home_ht.get("ht_btts_yes_pct", 0) + home_sh.get("sh_btts_yes_pct", 0)) > 60 and \
           (away_ht.get("ht_btts_yes_pct", 0) + away_sh.get("sh_btts_yes_pct", 0)) > 60:
            expected_home *= 1.05
            expected_away *= 1.05

        # Hakem
        if ref_stats:
            avg_ref_goals = ref_stats.get("avg_goals_match", 2.5)
            league_total = league_home + league_away
            if league_total > 0:
                ratio = avg_ref_goals / league_total
                expected_home *= (1 + (ratio - 1) * 0.10)
                expected_away *= (1 + (ratio - 1) * 0.10)

        expected_home += self.home_bias
        expected_away += self.away_bias
        return max(0.1, expected_home), max(0.1, expected_away)

    def predict_match(self, match):
        league_stats = self.get_league_stats(match["tournament_id"])
        if not league_stats:
            return None

        home_stats = get_team_stat(self.cur, match["home_team"], match["tournament_id"], "Home", "team_analytics",
                                   ["goals_for as goals_for_home", "goals_against as goals_against_home", "matches_played as matches_played_home"])
        if not home_stats:
            home_stats = get_team_stat(self.cur, match["home_team"], match["tournament_id"], "Overall", "team_analytics",
                                       ["goals_for as goals_for_home", "goals_against as goals_against_home", "matches_played as matches_played_home"])
        away_stats = get_team_stat(self.cur, match["away_team"], match["tournament_id"], "Away", "team_analytics",
                                   ["goals_for as goals_for_away", "goals_against as goals_against_away", "matches_played as matches_played_away"])
        if not away_stats:
            away_stats = get_team_stat(self.cur, match["away_team"], match["tournament_id"], "Overall", "team_analytics",
                                       ["goals_for as goals_for_away", "goals_against as goals_against_away", "matches_played as matches_played_away"])
        if not home_stats or not away_stats:
            return None

        # Form, verimlilik, yarı istatistikleri (kısaltılmış)
        home_form = get_team_stat(self.cur, match["home_team"], match["tournament_id"], "Home", "team_form_analytics", ["current_scoring_streak", "current_clean_sheet_streak"]) or \
                    get_team_stat(self.cur, match["home_team"], match["tournament_id"], "Overall", "team_form_analytics", ["current_scoring_streak", "current_clean_sheet_streak"])
        away_form = get_team_stat(self.cur, match["away_team"], match["tournament_id"], "Away", "team_form_analytics", ["current_scoring_streak", "current_clean_sheet_streak"]) or \
                    get_team_stat(self.cur, match["away_team"], match["tournament_id"], "Overall", "team_form_analytics", ["current_scoring_streak", "current_clean_sheet_streak"])
        home_eff = get_team_stat(self.cur, match["home_team"], match["tournament_id"], "Home", "team_efficiency_analytics", ["conversion_rate_pct", "save_rate_pct", "pressure_index"]) or \
                   get_team_stat(self.cur, match["home_team"], match["tournament_id"], "Overall", "team_efficiency_analytics", ["conversion_rate_pct", "save_rate_pct", "pressure_index"])
        away_eff = get_team_stat(self.cur, match["away_team"], match["tournament_id"], "Away", "team_efficiency_analytics", ["conversion_rate_pct", "save_rate_pct", "pressure_index"]) or \
                   get_team_stat(self.cur, match["away_team"], match["tournament_id"], "Overall", "team_efficiency_analytics", ["conversion_rate_pct", "save_rate_pct", "pressure_index"])
        home_ht = get_team_stat(self.cur, match["home_team"], match["tournament_id"], "Home", "team_half_time_analytics", ["ht_btts_yes_pct"]) or \
                  get_team_stat(self.cur, match["home_team"], match["tournament_id"], "Overall", "team_half_time_analytics", ["ht_btts_yes_pct"])
        away_ht = get_team_stat(self.cur, match["away_team"], match["tournament_id"], "Away", "team_half_time_analytics", ["ht_btts_yes_pct"]) or \
                  get_team_stat(self.cur, match["away_team"], match["tournament_id"], "Overall", "team_half_time_analytics", ["ht_btts_yes_pct"])
        home_sh = get_team_stat(self.cur, match["home_team"], match["tournament_id"], "Home", "team_second_half_analytics", ["sh_btts_yes_pct"]) or \
                  get_team_stat(self.cur, match["home_team"], match["tournament_id"], "Overall", "team_second_half_analytics", ["sh_btts_yes_pct"])
        away_sh = get_team_stat(self.cur, match["away_team"], match["tournament_id"], "Away", "team_second_half_analytics", ["sh_btts_yes_pct"]) or \
                  get_team_stat(self.cur, match["away_team"], match["tournament_id"], "Overall", "team_second_half_analytics", ["sh_btts_yes_pct"])

        if not all([home_form, away_form, home_eff, away_eff, home_ht, away_ht, home_sh, away_sh]):
            return None

        ref_stats = {}
        if match.get("referee"):
            self.cur.execute("SELECT avg_goals_match FROM referee_analytics WHERE referee_name = %s", (match["referee"],))
            ref_stats = self.cur.fetchone() or {}

        lambda_home, lambda_away = self.compute_expected_goals(
            home_stats, away_stats, league_stats, home_form, away_form,
            home_eff, away_eff, home_ht, away_ht, home_sh, away_sh, ref_stats
        )

        max_goals = 10
        score_probs = {}
        total = 0.0
        for i in range(max_goals+1):
            for j in range(max_goals+1):
                p = poisson_prob(lambda_home, i) * poisson_prob(lambda_away, j)
                score_probs[(i,j)] = p
                total += p
        if total > 0:
            for k in score_probs:
                score_probs[k] /= total

        prob_ms1 = sum(p for (i,j),p in score_probs.items() if i > j)
        prob_ms0 = sum(p for (i,j),p in score_probs.items() if i == j)
        prob_ms2 = sum(p for (i,j),p in score_probs.items() if i < j)
        prob_o15 = sum(p for (i,j),p in score_probs.items() if i+j > 1.5)
        prob_o35 = sum(p for (i,j),p in score_probs.items() if i+j > 3.5)

        return {
            "event_id": match["event_id"],
            "home_team": match["home_team"],
            "away_team": match["away_team"],
            "start_utc": match["start_utc"],
            "start_time_utc": match["start_time_utc"],
            "exp_goals_home": round(lambda_home, 2),
            "exp_goals_away": round(lambda_away, 2),
            "prob_ms1": round(prob_ms1*100, 1),
            "prob_ms0": round(prob_ms0*100, 1),
            "prob_ms2": round(prob_ms2*100, 1),
            "prob_o15": round(prob_o15*100, 1),
            "prob_o35": round(prob_o35*100, 1)
        }

    def save_prediction(self, pred):
        self.cur.execute("""
            INSERT INTO match_predictions
            (event_id, home_team, away_team, start_utc, start_time_utc,
             exp_goals_home, exp_goals_away,
             prob_ms1, prob_ms0, prob_ms2, prob_o15, prob_o35)
            VALUES (%(event_id)s, %(home_team)s, %(away_team)s, %(start_utc)s, %(start_time_utc)s,
                    %(exp_goals_home)s, %(exp_goals_away)s,
                    %(prob_ms1)s, %(prob_ms0)s, %(prob_ms2)s, %(prob_o15)s, %(prob_o35)s)
        """, pred)

    def run(self, days_ahead=1):
        matches = self.get_upcoming_matches(days_ahead)
        print(f"{len(matches)} yeni maç tahmin edilecek.")
        for match in matches:
            try:
                pred = self.predict_match(match)
                if pred:
                    self.save_prediction(pred)
                    print(f"Kaydedildi: {match['home_team']} vs {match['away_team']}")
                else:
                    print(f"Atlandı (yetersiz veri): {match['home_team']} vs {match['away_team']}")
            except Exception as e:
                print(f"Hata: {e}")

if __name__ == "__main__":
    p = Predictor(CONFIG["db"])
    try:
        p.run(days_ahead=1)
    finally:
        p.close()
