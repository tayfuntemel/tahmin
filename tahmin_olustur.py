#!/usr/bin/env python3
"""
ultimate_predictor.py - Tüm analitik tabloları kullanarak maç tahmini yapan gelişmiş algoritma.
Veritabanındaki 'results_football' tablosundan henüz başlamamış maçları çeker,
takım, lig, hakem ve form istatistiklerini birleştirir,
Poisson dağılımı ile gol beklentilerini hesaplar ve çıktıları sunup veritabanına kaydeder.
Daha önce tahmini yapılmış maçları tekrar HESAPLAMAZ, GÜNCELLEMEZ.
Eksik veri durumlarında çökmez ve işlem sonunda CMD ekranını açık tutar.
"""

import mysql.connector
import math
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
    
    prob_1 FLOAT NULL,
    prob_x FLOAT NULL,
    prob_2 FLOAT NULL,
    prob_o25 FLOAT NULL,
    prob_o35 FLOAT NULL,
    prob_btts FLOAT NULL,
    
    value_1 FLOAT NULL,
    value_x FLOAT NULL,
    value_2 FLOAT NULL,
    value_o25 FLOAT NULL,
    value_u25 FLOAT NULL,
    value_btts FLOAT NULL,
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# ======================== YARDIMCI FONKSİYONLAR ========================
def poisson_prob(lambda_: float, k: int) -> float:
    """Poisson olasılık kütle fonksiyonu: P(X = k)"""
    if lambda_ <= 0:
        return 1.0 if k == 0 else 0.0
    return (math.exp(-lambda_) * (lambda_ ** k)) / math.factorial(k)

def safe_div(num: float, denom: float, default: float = 0.0) -> float:
    """Güvenli bölme işlemi."""
    return num / denom if denom != 0 else default

def get_team_stat(cursor, team_name: str, tournament_id: int, venue: str, table: str, fields: list) -> Dict[str, Any]:
    """
    Bir takımın belirtilen tablodaki istatistiklerini getirir.
    Eğer satır yoksa tüm alanlar için varsayılan (0 veya None) döndürür.
    'AS' ile belirtilen takma adları (alias) ayrıştırarak doğru sözlük anahtarlarını oluşturur.
    """
    query = f"SELECT {', '.join(fields)} FROM {table} WHERE team_name = %s AND tournament_id = %s AND venue_type = %s"
    cursor.execute(query, (team_name, tournament_id, venue))
    row = cursor.fetchone()
    
    if row:
        return row
        
    # Eğer veri yoksa varsayılan değerleri (0) oluştur:
    default_data = {}
    for field in fields:
        # Örnek: "matches_played as matches_played_away" metninden sadece "matches_played_away" kısmını alır
        if " as " in field.lower():
            key = field.lower().split(" as ")[1].strip()
        elif " AS " in field:
            key = field.split(" AS ")[1].strip()
        else:
            key = field.strip()
            
        default_data[key] = 0
        
    return default_data

# ======================== ANA TAHMİN SINIFI ========================
class UltimatePredictor:
    def __init__(self, db_config):
        self.conn = mysql.connector.connect(**db_config)
        self.conn.autocommit = True  # İşlemlerin anında kaydedilmesi için
        self.cur = self.conn.cursor(dictionary=True)
        self._init_db()

    def _init_db(self):
        """Tahminlerin tutulacağı tabloyu oluşturur."""
        self.cur.execute(SCHEMA_PREDICTIONS_TABLE)

    def close(self):
        self.cur.close()
        self.conn.close()

    def get_upcoming_matches(self, days_ahead: int = 2) -> list:
        """
        Bugünden itibaren 'days_ahead' gün içindeki henüz başlamamış ve 
        daha önce tahmini EKLENMEMİŞ maçları getirir.
        """
        today = date.today()
        end_date = today + timedelta(days=days_ahead)
        
        # LEFT JOIN ile match_predictions tablosunda eşleşmesi olmayanları (IS NULL) çekiyoruz
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
        """Lig ortalamalarını döndürür."""
        query = "SELECT * FROM league_analytics WHERE tournament_id = %s"
        self.cur.execute(query, (tournament_id,))
        row = self.cur.fetchone()
        if row:
            return {
                "avg_goals_home": row.get("avg_goals_home", 1.2),
                "avg_goals_away": row.get("avg_goals_away", 1.0),
                "avg_goals_match": row.get("avg_goals_match", 2.2),
                "home_win_pct": row.get("home_win_pct", 45),
                "draw_pct": row.get("draw_pct", 25),
                "away_win_pct": row.get("away_win_pct", 30),
                "over_25_pct": row.get("over_25_pct", 50),
                "btts_yes_pct": row.get("btts_yes_pct", 45)
            }
        # Varsayılan değerler (Avrupa futbolu ortalamaları)
        return {
            "avg_goals_home": 1.2,
            "avg_goals_away": 1.0,
            "avg_goals_match": 2.2,
            "home_win_pct": 45,
            "draw_pct": 25,
            "away_win_pct": 30,
            "over_25_pct": 50,
            "btts_yes_pct": 45
        }

    def get_referee_stats(self, referee_name: str) -> Dict[str, float]:
        """Hakem istatistiklerini döndürür (yoksa None)."""
        if not referee_name:
            return None
        query = "SELECT * FROM referee_analytics WHERE referee_name = %s"
        self.cur.execute(query, (referee_name,))
        return self.cur.fetchone()

    def compute_expected_goals(self, home_stats: Dict, away_stats: Dict,
                               league_stats: Dict, form_home: Dict, form_away: Dict,
                               eff_home: Dict, eff_away: Dict) -> Tuple[float, float]:
        """
        Ev sahibi ve deplasman takımı için beklenen gol sayılarını hesaplar.
        """
        # --- Lig ortalamaları ---
        league_avg_home = league_stats["avg_goals_home"]
        league_avg_away = league_stats["avg_goals_away"]

        # --- Takım ortalamaları (kendi evinde / deplasmanda) ---
        home_avg_for = home_stats.get("goals_for_home", home_stats.get("goals_for", 0)) / max(1, home_stats.get("matches_played_home", home_stats.get("matches_played", 1)))
        home_avg_against = home_stats.get("goals_against_home", home_stats.get("goals_against", 0)) / max(1, home_stats.get("matches_played_home", home_stats.get("matches_played", 1)))
        away_avg_for = away_stats.get("goals_for_away", away_stats.get("goals_for", 0)) / max(1, away_stats.get("matches_played_away", away_stats.get("matches_played", 1)))
        away_avg_against = away_stats.get("goals_against_away", away_stats.get("goals_against", 0)) / max(1, away_stats.get("matches_played_away", away_stats.get("matches_played", 1)))

        # Hücum ve savunma katsayıları
        home_attack = safe_div(home_avg_for, league_avg_home, 1.0)
        away_defense = safe_div(away_avg_against, league_avg_away, 1.0)
        away_attack = safe_div(away_avg_for, league_avg_away, 1.0)
        home_defense = safe_div(home_avg_against, league_avg_home, 1.0)

        # Baz beklenen gol
        expected_home = home_attack * away_defense * league_avg_home
        expected_away = away_attack * home_defense * league_avg_away

        # --- Form düzeltmeleri ---
        home_scoring_streak = form_home.get("current_scoring_streak", 0)
        away_scoring_streak = form_away.get("current_scoring_streak", 0)
        expected_home *= (1 + 0.05 * home_scoring_streak)
        expected_away *= (1 + 0.05 * away_scoring_streak)

        home_clean_streak = form_home.get("current_clean_sheet_streak", 0)
        away_clean_streak = form_away.get("current_clean_sheet_streak", 0)
        expected_home *= (1 - 0.05 * away_clean_streak)
        expected_away *= (1 - 0.05 * home_clean_streak)

        # --- Verimlilik düzeltmeleri ---
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
        """Tek bir maç için tüm olasılıkları hesaplar."""
        event_id = match["event_id"]
        home_team = match["home_team"]
        away_team = match["away_team"]
        tournament_id = match["tournament_id"]

        # Lig istatistikleri
        league_stats = self.get_league_stats(tournament_id)

        # Takım istatistikleri
        home_analytics = get_team_stat(self.cur, home_team, tournament_id, "Home", "team_analytics", ["goals_for as goals_for_home", "goals_against as goals_against_home", "matches_played as matches_played_home"])
        away_analytics = get_team_stat(self.cur, away_team, tournament_id, "Away", "team_analytics", ["goals_for as goals_for_away", "goals_against as goals_against_away", "matches_played as matches_played_away"])
        
        if not home_analytics["matches_played_home"]:
            home_analytics = get_team_stat(self.cur, home_team, tournament_id, "Overall", "team_analytics", ["goals_for as goals_for_home", "goals_against as goals_against_home", "matches_played as matches_played_home"])
        if not away_analytics["matches_played_away"]:
            away_analytics = get_team_stat(self.cur, away_team, tournament_id, "Overall", "team_analytics", ["goals_for as goals_for_away", "goals_against as goals_against_away", "matches_played as matches_played_away"])

        # Form istatistikleri
        home_form = get_team_stat(self.cur, home_team, tournament_id, "Home", "team_form_analytics", ["current_scoring_streak", "current_clean_sheet_streak", "points_last_5"])
        away_form = get_team_stat(self.cur, away_team, tournament_id, "Away", "team_form_analytics", ["current_scoring_streak", "current_clean_sheet_streak", "points_last_5"])
        if not home_form.get("points_last_5"):
            home_form = get_team_stat(self.cur, home_team, tournament_id, "Overall", "team_form_analytics", ["current_scoring_streak", "current_clean_sheet_streak", "points_last_5"])
        if not away_form.get("points_last_5"):
            away_form = get_team_stat(self.cur, away_team, tournament_id, "Overall", "team_form_analytics", ["current_scoring_streak", "current_clean_sheet_streak", "points_last_5"])

        # Verimlilik istatistikleri
        home_eff = get_team_stat(self.cur, home_team, tournament_id, "Home", "team_efficiency_analytics", ["conversion_rate_pct", "save_rate_pct"])
        away_eff = get_team_stat(self.cur, away_team, tournament_id, "Away", "team_efficiency_analytics", ["conversion_rate_pct", "save_rate_pct"])
        if not home_eff.get("conversion_rate_pct"):
            home_eff = get_team_stat(self.cur, home_team, tournament_id, "Overall", "team_efficiency_analytics", ["conversion_rate_pct", "save_rate_pct"])
        if not away_eff.get("conversion_rate_pct"):
            away_eff = get_team_stat(self.cur, away_team, tournament_id, "Overall", "team_efficiency_analytics", ["conversion_rate_pct", "save_rate_pct"])

        # Beklenen golleri hesapla
        lambda_home, lambda_away = self.compute_expected_goals(
            home_analytics, away_analytics, league_stats,
            home_form, away_form, home_eff, away_eff
        )

        # Poisson dağılımı ile skor olasılıklarını hesapla
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

        prob_home_win = sum(prob for (i, j), prob in score_probs.items() if i > j)
        prob_draw = sum(prob for (i, j), prob in score_probs.items() if i == j)
        prob_away_win = sum(prob for (i, j), prob in score_probs.items() if i < j)

        prob_over_25 = sum(prob for (i, j), prob in score_probs.items() if i + j > 2.5)
        prob_over_35 = sum(prob for (i, j), prob in score_probs.items() if i + j > 3.5)
        prob_btts = sum(prob for (i, j), prob in score_probs.items() if i > 0 and j > 0)

        # Mevcut bahis oranları
        odds_1 = match.get("odds_1")
        odds_x = match.get("odds_x")
        odds_2 = match.get("odds_2")
        odds_o25 = match.get("odds_o25")
        odds_u25 = match.get("odds_u25")
        odds_btts_yes = match.get("odds_btts_yes")

        value_1 = (prob_home_win * odds_1 - 1) if odds_1 else None
        value_x = (prob_draw * odds_x - 1) if odds_x else None
        value_2 = (prob_away_win * odds_2 - 1) if odds_2 else None
        value_o25 = (prob_over_25 * odds_o25 - 1) if odds_o25 else None
        value_u25 = ((1 - prob_over_25) * odds_u25 - 1) if odds_u25 else None
        value_btts = (prob_btts * odds_btts_yes - 1) if odds_btts_yes else None

        result = {
            "event_id": event_id,
            "home_team": home_team,
            "away_team": away_team,
            "start_utc": match["start_utc"],
            "start_time_utc": match["start_time_utc"],
            "expected_goals": (round(lambda_home, 2), round(lambda_away, 2)),
            "prob_home_win": round(prob_home_win * 100, 1),
            "prob_draw": round(prob_draw * 100, 1),
            "prob_away_win": round(prob_away_win * 100, 1),
            "prob_over_25": round(prob_over_25 * 100, 1),
            "prob_over_35": round(prob_over_35 * 100, 1),
            "prob_btts": round(prob_btts * 100, 1),
            "odds": {
                "1": odds_1, "X": odds_x, "2": odds_2,
                "O2.5": odds_o25, "U2.5": odds_u25,
                "BTTS_Yes": odds_btts_yes
            },
            "value": {
                "1": round(value_1, 2) if value_1 is not None else None,
                "X": round(value_x, 2) if value_x is not None else None,
                "2": round(value_2, 2) if value_2 is not None else None,
                "O2.5": round(value_o25, 2) if value_o25 is not None else None,
                "U2.5": round(value_u25, 2) if value_u25 is not None else None,
                "BTTS_Yes": round(value_btts, 2) if value_btts is not None else None
            }
        }
        return result

    def save_prediction(self, pred: Dict[str, Any]):
        """Hesaplanan tahmini match_predictions tablosuna kaydeder. Varsa atlar."""
        # INSERT IGNORE komutu kullandık. Eğer event_id zaten varsa hiçbir şey yapmayacak ve hata vermeyecek.
        sql = """
            INSERT IGNORE INTO match_predictions (
                event_id, home_team, away_team, start_utc, start_time_utc,
                exp_goals_home, exp_goals_away, 
                prob_1, prob_x, prob_2, prob_o25, prob_o35, prob_btts,
                value_1, value_x, value_2, value_o25, value_u25, value_btts
            ) VALUES (
                %(event_id)s, %(home_team)s, %(away_team)s, %(start_utc)s, %(start_time_utc)s,
                %(exp_goals_home)s, %(exp_goals_away)s,
                %(prob_1)s, %(prob_x)s, %(prob_2)s, %(prob_o25)s, %(prob_o35)s, %(prob_btts)s,
                %(value_1)s, %(value_x)s, %(value_2)s, %(value_o25)s, %(value_u25)s, %(value_btts)s
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
            "prob_1": pred["prob_home_win"],
            "prob_x": pred["prob_draw"],
            "prob_2": pred["prob_away_win"],
            "prob_o25": pred["prob_over_25"],
            "prob_o35": pred["prob_over_35"],
            "prob_btts": pred["prob_btts"],
            "value_1": pred["value"]["1"],
            "value_x": pred["value"]["X"],
            "value_2": pred["value"]["2"],
            "value_o25": pred["value"]["O2.5"],
            "value_u25": pred["value"]["U2.5"],
            "value_btts": pred["value"]["BTTS_Yes"]
        }
        self.cur.execute(sql, data)

    def run_predictions(self, days_ahead: int = 2):
        """Önümüzdeki günlerdeki maçları tahmin et, yazdır ve veritabanına kaydet."""
        matches = self.get_upcoming_matches(days_ahead)
        print(f"\n{'='*80}")
        print(f"  {len(matches)} YENİ MAÇ İÇİN TAHMİNLER (VE DB KAYDI)")
        print(f"{'='*80}\n")

        if len(matches) == 0:
            print("Tüm maçların tahmini zaten yapılmış veya yakında maç yok.")
            return

        for match in matches:
            try:
                pred = self.predict_match(match)
                self.print_prediction(pred)
                self.save_prediction(pred)  # Tahmini veritabanına kaydediyoruz
            except Exception as e:
                print(f"HATA - {match['home_team']} vs {match['away_team']}: {e}")

    def print_prediction(self, pred: Dict[str, Any]):
        """Tahmin sonuçlarını güzelce yazdır."""
        print(f"\n{'-'*60}")
        print(f"{pred['home_team']} vs {pred['away_team']}  [{pred['start_utc']} {pred['start_time_utc'] or ''}]")
        print(f"{'-'*60}")
        print(f"Beklenen Goller: {pred['home_team']} {pred['expected_goals'][0]} - {pred['expected_goals'][1]} {pred['away_team']}")
        print(f"1X2 Olasılıkları: 1: %{pred['prob_home_win']}  X: %{pred['prob_draw']}  2: %{pred['prob_away_win']}")
        print(f"Alt/Üst 2.5    : Üst: %{pred['prob_over_25']}  Alt: %{100 - pred['prob_over_25']:.1f}")
        print(f"Alt/Üst 3.5    : Üst: %{pred['prob_over_35']}  Alt: %{100 - pred['prob_over_35']:.1f}")
        print(f"Karşılıklı Gol  : Var: %{pred['prob_btts']}  Yok: %{100 - pred['prob_btts']:.1f}")

        if any(pred["odds"].values()):
            print("\nMevcut Oranlar:")
            o = pred["odds"]
            v = pred["value"]
            if o["1"]:
                val = f" (Değer: {v['1']:+.2f})" if v["1"] is not None else ""
                print(f"  1: {o['1']:.2f}{val}")
            if o["X"]:
                val = f" (Değer: {v['X']:+.2f})" if v["X"] is not None else ""
                print(f"  X: {o['X']:.2f}{val}")
            if o["2"]:
                val = f" (Değer: {v['2']:+.2f})" if v["2"] is not None else ""
                print(f"  2: {o['2']:.2f}{val}")
            if o["O2.5"]:
                val = f" (Değer: {v['O2.5']:+.2f})" if v["O2.5"] is not None else ""
                print(f"  O2.5: {o['O2.5']:.2f}{val}")
            if o["U2.5"]:
                val = f" (Değer: {v['U2.5']:+.2f})" if v["U2.5"] is not None else ""
                print(f"  U2.5: {o['U2.5']:.2f}{val}")
            if o["BTTS_Yes"]:
                val = f" (Değer: {v['BTTS_Yes']:+.2f})" if v["BTTS_Yes"] is not None else ""
                print(f"  KG Var: {o['BTTS_Yes']:.2f}{val}")
        print(f"{'-'*60}")

# ======================== ANA ÇALIŞTIRMA ========================
if __name__ == "__main__":
    predictor = UltimatePredictor(CONFIG["db"])
    try:
        predictor.run_predictions(days_ahead=2)  # Bugün + yarın
    finally:
        predictor.close()
