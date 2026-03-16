#!/usr/bin/env python3
"""
ultimate_predictor.py - Tüm analitik tabloları kullanarak maç tahmini yapan gelişmiş algoritma.
Güncelleme: Yeni Marketler (1.5 Üst, 3.5 Alt, 1X, X2, KG Var) - Sadece Bugün ve Yarın
Eklenen Özellik: Cron için satır sayısı kontrolü ve başarısız durumlarda otomatik tekrar mekanizması.
"""

import mysql.connector
import math
import time
from datetime import datetime, date, timedelta
from typing import Dict, Any, Tuple, Optional

# ======================== YAPILANDIRMA ========================
CONFIG = {
    "db": {
        "host": "netscout.fun",
        "user": "netscout_veri",
        "password": "i.34temel1",
        "database": "netscout_veri",
        "port": 3306
    }
}

# ======================== SQL ŞEMASI ========================
SCHEMA_PREDICTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS match_predictions (
    event_id BIGINT UNSIGNED NOT NULL,
    home_team VARCHAR(128) NULL,
    away_team VARCHAR(128) NULL,
    start_utc DATE NULL,
    start_time_utc TIME NULL,
    
    exp_goals_home FLOAT NULL,
    exp_goals_away FLOAT NULL,
    
    prob_1x FLOAT NULL,
    prob_x2 FLOAT NULL,
    prob_o15 FLOAT NULL,
    prob_u35 FLOAT NULL,
    prob_btts FLOAT NULL,
    
    value_1x FLOAT NULL,
    value_x2 FLOAT NULL,
    value_o15 FLOAT NULL,
    value_u35 FLOAT NULL,
    value_btts FLOAT NULL,
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# ======================== YARDIMCI FONKSİYONLAR ========================
def poisson_prob(lambda_: float, k: int) -> float:
    if lambda_ <= 0:
        return 1.0 if k == 0 else 0.0
    return (math.exp(-lambda_) * (lambda_ ** k)) / math.factorial(k)

def safe_div(num: float, denom: float, default: float = 0.0) -> float:
    return num / denom if denom != 0 else default

def get_team_stat(cursor, team_name: str, tournament_id: int, venue: str, table: str, fields: list) -> Dict[str, Any]:
    query = f"SELECT {', '.join(fields)} FROM {table} WHERE team_name = %s AND tournament_id = %s AND venue_type = %s"
    cursor.execute(query, (team_name, tournament_id, venue))
    row = cursor.fetchone()
    if row:
        return row
    default_data = {}
    for field in fields:
        if " as " in field.lower(): key = field.lower().split(" as ")[1].strip()
        elif " AS " in field: key = field.split(" AS ")[1].strip()
        else: key = field.strip()
        default_data[key] = 0
    return default_data

# ======================== ANA TAHMİN SINIFI ========================
class UltimatePredictor:
    def __init__(self, db_config):
        self.conn = mysql.connector.connect(**db_config)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self._init_db()

    def _init_db(self):
        self.cur.execute(SCHEMA_PREDICTIONS_TABLE)

    def close(self):
        self.cur.close()
        self.conn.close()

    def get_prediction_count(self) -> int:
        """Veritabanındaki toplam tahmin (satır) sayısını döndürür."""
        self.cur.execute("SELECT COUNT(*) as count FROM match_predictions")
        row = self.cur.fetchone()
        return row['count'] if row else 0

    def get_upcoming_matches(self, days_ahead: int = 1) -> list:
        # Sadece BUGÜN ve YARIN (today + 1 gün) arasındaki BAŞLAMAMIŞ maçlar
        today = date.today()
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

    def get_league_stats(self, tournament_id: int) -> Dict[str, float]:
        query = "SELECT * FROM league_analytics WHERE tournament_id = %s"
        self.cur.execute(query, (tournament_id,))
        row = self.cur.fetchone()
        if row:
            return {
                "avg_goals_home": row.get("avg_goals_home", 1.2),
                "avg_goals_away": row.get("avg_goals_away", 1.0)
            }
        return {"avg_goals_home": 1.2, "avg_goals_away": 1.0}

    def compute_expected_goals(self, home_stats: Dict, away_stats: Dict,
                               league_stats: Dict, form_home: Dict, form_away: Dict,
                               eff_home: Dict, eff_away: Dict) -> Tuple[float, float]:
        league_avg_home = league_stats["avg_goals_home"]
        league_avg_away = league_stats["avg_goals_away"]

        home_avg_for = home_stats.get("goals_for_home", home_stats.get("goals_for", 0)) / max(1, home_stats.get("matches_played_home", home_stats.get("matches_played", 1)))
        home_avg_against = home_stats.get("goals_against_home", home_stats.get("goals_against", 0)) / max(1, home_stats.get("matches_played_home", home_stats.get("matches_played", 1)))
        away_avg_for = away_stats.get("goals_for_away", away_stats.get("goals_for", 0)) / max(1, away_stats.get("matches_played_away", away_stats.get("matches_played", 1)))
        away_avg_against = away_stats.get("goals_against_away", away_stats.get("goals_against", 0)) / max(1, away_stats.get("matches_played_away", away_stats.get("matches_played", 1)))

        home_attack = safe_div(home_avg_for, league_avg_home, 1.0)
        away_defense = safe_div(away_avg_against, league_avg_away, 1.0)
        away_attack = safe_div(away_avg_for, league_avg_away, 1.0)
        home_defense = safe_div(home_avg_against, league_avg_home, 1.0)

        expected_home = home_attack * away_defense * league_avg_home
        expected_away = away_attack * home_defense * league_avg_away

        home_scoring_streak = form_home.get("current_scoring_streak", 0)
        away_scoring_streak = form_away.get("current_scoring_streak", 0)
        expected_home *= (1 + 0.05 * home_scoring_streak)
        expected_away *= (1 + 0.05 * away_scoring_streak)

        home_clean_streak = form_home.get("current_clean_sheet_streak", 0)
        away_clean_streak = form_away.get("current_clean_sheet_streak", 0)
        expected_home *= (1 - 0.05 * away_clean_streak)
        expected_away *= (1 - 0.05 * home_clean_streak)

        home_conv = eff_home.get("conversion_rate_pct", 0) / 100
        away_conv = eff_away.get("conversion_rate_pct", 0) / 100
        expected_home *= (1 + 0.2 * (home_conv - 0.1))
        expected_away *= (1 + 0.2 * (away_conv - 0.1))

        home_save = eff_home.get("save_rate_pct", 70) / 100
        away_save = eff_away.get("save_rate_pct", 70) / 100
        expected_home *= (1 - 0.2 * (away_save - 0.7))
        expected_away *= (1 - 0.2 * (home_save - 0.7))

        expected_home = max(0.1, expected_home)
        expected_away = max(0.1, expected_away)

        return expected_home, expected_away

    def predict_match(self, match: Dict[str, Any]) -> Dict[str, Any]:
        event_id = match["event_id"]
        home_team = match["home_team"]
        away_team = match["away_team"]
        tournament_id = match["tournament_id"]

        league_stats = self.get_league_stats(tournament_id)

        home_analytics = get_team_stat(self.cur, home_team, tournament_id, "Home", "team_analytics", ["goals_for as goals_for_home", "goals_against as goals_against_home", "matches_played as matches_played_home"])
        away_analytics = get_team_stat(self.cur, away_team, tournament_id, "Away", "team_analytics", ["goals_for as goals_for_away", "goals_against as goals_against_away", "matches_played as matches_played_away"])
        
        if not home_analytics["matches_played_home"]:
            home_analytics = get_team_stat(self.cur, home_team, tournament_id, "Overall", "team_analytics", ["goals_for as goals_for_home", "goals_against as goals_against_home", "matches_played as matches_played_home"])
        if not away_analytics["matches_played_away"]:
            away_analytics = get_team_stat(self.cur, away_team, tournament_id, "Overall", "team_analytics", ["goals_for as goals_for_away", "goals_against as goals_against_away", "matches_played as matches_played_away"])

        home_form = get_team_stat(self.cur, home_team, tournament_id, "Home", "team_form_analytics", ["current_scoring_streak", "current_clean_sheet_streak", "points_last_5"])
        away_form = get_team_stat(self.cur, away_team, tournament_id, "Away", "team_form_analytics", ["current_scoring_streak", "current_clean_sheet_streak", "points_last_5"])
        if not home_form.get("points_last_5"):
            home_form = get_team_stat(self.cur, home_team, tournament_id, "Overall", "team_form_analytics", ["current_scoring_streak", "current_clean_sheet_streak", "points_last_5"])
        if not away_form.get("points_last_5"):
            away_form = get_team_stat(self.cur, away_team, tournament_id, "Overall", "team_form_analytics", ["current_scoring_streak", "current_clean_sheet_streak", "points_last_5"])

        home_eff = get_team_stat(self.cur, home_team, tournament_id, "Home", "team_efficiency_analytics", ["conversion_rate_pct", "save_rate_pct"])
        away_eff = get_team_stat(self.cur, away_team, tournament_id, "Away", "team_efficiency_analytics", ["conversion_rate_pct", "save_rate_pct"])
        if not home_eff.get("conversion_rate_pct"):
            home_eff = get_team_stat(self.cur, home_team, tournament_id, "Overall", "team_efficiency_analytics", ["conversion_rate_pct", "save_rate_pct"])
        if not away_eff.get("conversion_rate_pct"):
            away_eff = get_team_stat(self.cur, away_team, tournament_id, "Overall", "team_efficiency_analytics", ["conversion_rate_pct", "save_rate_pct"])

        lambda_home, lambda_away = self.compute_expected_goals(
            home_analytics, away_analytics, league_stats,
            home_form, away_form, home_eff, away_eff
        )

        max_goals = 10
        score_probs = {}
        total_prob = 0.0
        for i in range(max_goals + 1):
            for j in range(max_goals + 1):
                prob = poisson_prob(lambda_home, i) * poisson_prob(lambda_away, j)
                score_probs[(i, j)] = prob
                total_prob += prob
                
        if total_prob > 0:
            for k in score_probs:
                score_probs[k] /= total_prob

        prob_1x = sum(prob for (i, j), prob in score_probs.items() if i >= j)
        prob_x2 = sum(prob for (i, j), prob in score_probs.items() if i <= j)
        prob_over_15 = sum(prob for (i, j), prob in score_probs.items() if i + j > 1.5)
        prob_under_35 = sum(prob for (i, j), prob in score_probs.items() if i + j < 3.5)
        prob_btts = sum(prob for (i, j), prob in score_probs.items() if i > 0 and j > 0)

        odds_1x = match.get("odds_1x")
        odds_x2 = match.get("odds_x2")
        odds_o15 = match.get("odds_o15")
        odds_u35 = match.get("odds_u35")
        odds_btts_yes = match.get("odds_btts_yes")

        value_1x = (prob_1x * odds_1x - 1) if odds_1x else None
        value_x2 = (prob_x2 * odds_x2 - 1) if odds_x2 else None
        value_o15 = (prob_over_15 * odds_o15 - 1) if odds_o15 else None
        value_u35 = (prob_under_35 * odds_u35 - 1) if odds_u35 else None
        value_btts = (prob_btts * odds_btts_yes - 1) if odds_btts_yes else None

        result = {
            "event_id": event_id,
            "home_team": home_team,
            "away_team": away_team,
            "start_utc": match["start_utc"],
            "start_time_utc": match["start_time_utc"],
            "expected_goals": (round(lambda_home, 2), round(lambda_away, 2)),
            "prob_1x": round(prob_1x * 100, 1),
            "prob_x2": round(prob_x2 * 100, 1),
            "prob_o15": round(prob_over_15 * 100, 1),
            "prob_u35": round(prob_under_35 * 100, 1),
            "prob_btts": round(prob_btts * 100, 1),
            "odds": {
                "1X": odds_1x, "X2": odds_x2,
                "O1.5": odds_o15, "U3.5": odds_u35,
                "BTTS": odds_btts_yes
            },
            "value": {
                "1X": round(value_1x, 2) if value_1x is not None else None,
                "X2": round(value_x2, 2) if value_x2 is not None else None,
                "O1.5": round(value_o15, 2) if value_o15 is not None else None,
                "U3.5": round(value_u35, 2) if value_u35 is not None else None,
                "BTTS": round(value_btts, 2) if value_btts is not None else None
            }
        }
        return result

    def save_prediction(self, pred: Dict[str, Any]):
        sql = """
            INSERT IGNORE INTO match_predictions (
                event_id, home_team, away_team, start_utc, start_time_utc,
                exp_goals_home, exp_goals_away, 
                prob_1x, prob_x2, prob_o15, prob_u35, prob_btts,
                value_1x, value_x2, value_o15, value_u35, value_btts
            ) VALUES (
                %(event_id)s, %(home_team)s, %(away_team)s, %(start_utc)s, %(start_time_utc)s,
                %(exp_goals_home)s, %(exp_goals_away)s,
                %(prob_1x)s, %(prob_x2)s, %(prob_o15)s, %(prob_u35)s, %(prob_btts)s,
                %(value_1x)s, %(value_x2)s, %(value_o15)s, %(value_u35)s, %(value_btts)s
            );
        """
        
        data = {
            "event_id": pred["event_id"],
            "home_team": pred["home_team"],
            "away_team": pred["away_team"],
            "start_utc": pred["start_utc"],
            "start_time_utc": pred["start_time_utc"],
            "exp_goals_home": pred["expected_goals"][0],
            "exp_goals_away": pred["expected_goals"][1],
            "prob_1x": pred["prob_1x"],
            "prob_x2": pred["prob_x2"],
            "prob_o15": pred["prob_o15"],
            "prob_u35": pred["prob_u35"],
            "prob_btts": pred["prob_btts"],
            "value_1x": pred["value"]["1X"],
            "value_x2": pred["value"]["X2"],
            "value_o15": pred["value"]["O1.5"],
            "value_u35": pred["value"]["U3.5"],
            "value_btts": pred["value"]["BTTS"]
        }
        self.cur.execute(sql, data)

    def run_predictions(self, days_ahead: int = 1):
        matches = self.get_upcoming_matches(days_ahead)
        print(f"\n{'='*80}")
        print(f"  {len(matches)} YENİ MAÇ İÇİN TAHMİNLER (BUGÜN VE YARIN)")
        print(f"{'='*80}\n")

        if len(matches) == 0:
            print("Tüm maçların tahmini zaten yapılmış veya yakında maç yok.")
            return

        for match in matches:
            try:
                pred = self.predict_match(match)
                self.print_prediction(pred)
                self.save_prediction(pred)
            except Exception as e:
                print(f"HATA - {match['home_team']} vs {match['away_team']}: {e}")

    def print_prediction(self, pred: Dict[str, Any]):
        print(f"\n{'-'*60}")
        print(f"{pred['home_team']} vs {pred['away_team']}  [{pred['start_utc']} {pred['start_time_utc'] or ''}]")
        print(f"{'-'*60}")
        print(f"Beklenen Goller: {pred['home_team']} {pred['expected_goals'][0]} - {pred['expected_goals'][1]} {pred['away_team']}")
        print(f"Çifte Şans     : 1X: %{pred['prob_1x']}  X2: %{pred['prob_x2']}")
        print(f"1.5 Alt/Üst    : Üst: %{pred['prob_o15']}  Alt: %{100 - pred['prob_o15']:.1f}")
        print(f"3.5 Alt/Üst    : Alt: %{pred['prob_u35']}  Üst: %{100 - pred['prob_u35']:.1f}")
        print(f"Karşılıklı Gol : Var: %{pred['prob_btts']}  Yok: %{100 - pred['prob_btts']:.1f}")

        if any(pred["odds"].values()):
            print("\nMevcut Oranlar:")
            o = pred["odds"]
            v = pred["value"]
            if o["1X"]:
                val = f" (Değer: {v['1X']:+.2f})" if v["1X"] is not None else ""
                print(f"  1X: {o['1X']:.2f}{val}")
            if o["X2"]:
                val = f" (Değer: {v['X2']:+.2f})" if v["X2"] is not None else ""
                print(f"  X2: {o['X2']:.2f}{val}")
            if o["O1.5"]:
                val = f" (Değer: {v['O1.5']:+.2f})" if v["O1.5"] is not None else ""
                print(f"  1.5 Üst: {o['O1.5']:.2f}{val}")
            if o["U3.5"]:
                val = f" (Değer: {v['U3.5']:+.2f})" if v["U3.5"] is not None else ""
                print(f"  3.5 Alt: {o['U3.5']:.2f}{val}")
            if o["BTTS"]:
                val = f" (Değer: {v['BTTS']:+.2f})" if v["BTTS"] is not None else ""
                print(f"  KG Var: {o['BTTS']:.2f}{val}")
        print(f"{'-'*60}")

if __name__ == "__main__":
    predictor = UltimatePredictor(CONFIG["db"])
    try:
        max_deneme = 3
        bekleme_suresi = 60 # Saniye cinsinden bekleme süresi

        for deneme in range(1, max_deneme + 1):
            baslangic_sayisi = predictor.get_prediction_count()
            print(f"\n>>> DENEME {deneme}/{max_deneme} <<<")
            print(f"Başlangıçtaki Toplam Tahmin Sayısı: {baslangic_sayisi}")

            # Tahminleri çalıştır
            predictor.run_predictions(days_ahead=1)

            bitis_sayisi = predictor.get_prediction_count()
            eklenen_sayi = bitis_sayisi - baslangic_sayisi

            print(f"Bitişteki Toplam Tahmin Sayısı: {bitis_sayisi}")
            print(f"Bu işlemde eklenen yeni tahmin sayısı: {eklenen_sayi}")

            if eklenen_sayi > 0:
                print("\n[BAŞARILI] Yeni tahminler başarıyla kaydedildi. İşlem tamamlandı.")
                break
            else:
                # Yeni kayıt eklenmediyse, gerçekten maç mı yok yoksa hata mı var kontrol edelim
                bekleyen_maclar = predictor.get_upcoming_matches(days_ahead=1)
                
                if len(bekleyen_maclar) == 0:
                    print("\n[BİLGİ] Veritabanına göre tahmin yapılacak yeni maç yok. Tekrarlamaya gerek yok.")
                    break
                else:
                    print(f"\n[UYARI] Tahmin bekleyen {len(bekleyen_maclar)} maç var ama veritabanına kayıt yapılamadı!")
                    if deneme < max_deneme:
                        print(f"{bekleme_suresi} saniye bekleniyor ve tekrar denenecek...")
                        time.sleep(bekleme_suresi)
                    else:
                        print("\n[HATA] Maksimum deneme sayısına ulaşıldı. İşlem başarısız.")
                        
    finally:
        predictor.close()
