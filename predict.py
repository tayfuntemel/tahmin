#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tahmin scripti - cache tablolarını (league_stats, team_form_cache, referee_stats) kullanır.
Önce update_stats.py çalıştırılmış olmalıdır.
UTF-8 karakter desteği eklendi.
"""

import os
import datetime as dt
import mysql.connector
import numpy as np

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "charset": "utf8mb4",
    "use_unicode": True,
    "collation": "utf8mb4_unicode_ci"
}

MAJOR_TOURNAMENT_IDS = {
    1, 2, 3, 72, 84, 36, 37, 3739, 33, 34, 7372, 42, 41, 8343, 810,
    4, 5397, 62, 101, 39, 40, 38, 692, 280, 127, 83, 1449,
    169352, 5071, 28, 6720, 18, 3397, 3708, 82, 3034, 3284, 6230,
    54, 64, 29, 1060, 219, 652, 144, 1339, 1340, 1341, 5, 6, 12, 13, 19, 24, 27, 30, 31, 48, 49, 50, 52, 53, 55, 79, 102, 232, 384, 
    681, 877, 1061, 1107, 1427, 10812, 16753, 19232, 34363, 51702, 52653, 58560, 
    64475, 71900, 71901, 72112, 78740, 92016, 92614, 143625
}

class PredictionEngine:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = mysql.connector.connect(**DB_CONFIG)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor(dictionary=True)
        self._create_predictions_table()

    def _create_predictions_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                event_id BIGINT UNSIGNED NOT NULL,
                prediction_date DATE NOT NULL,
                match_date DATE NOT NULL,
                home_team VARCHAR(128) NOT NULL,
                away_team VARCHAR(128) NOT NULL,
                category_id INT NULL,
                model_over_prob FLOAT,
                model_btts_prob FLOAT,
                odds_over FLOAT,
                odds_btts FLOAT,
                edge_over FLOAT,
                edge_btts FLOAT,
                play_over BOOLEAN,
                play_btts BOOLEAN,
                form_score FLOAT,
                pressure_score FLOAT,
                h2h_score FLOAT,
                early_bonus INT,
                second_bonus INT,
                referee_penalty FLOAT,
                net_total_score FLOAT,
                actual_ft_home INT NULL,
                actual_ft_away INT NULL,
                actual_over25 BOOLEAN NULL,
                actual_btts BOOLEAN NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_prediction (event_id, prediction_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        print("[DB] predictions tablosu hazır.")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_league_stats(self, category_id):
        self.cursor.execute("SELECT * FROM league_stats WHERE category_id = %s", (category_id,))
        row = self.cursor.fetchone()
        if row:
            return row
        # Varsayılan
        return {'avg_goals': 2.5, 'avg_shot_on': 8.0, 'avg_corners': 9.0,
                'btts_ratio': 0.45, 'over25_ratio': 0.45}

    def get_team_form_cache(self, team_key):
        self.cursor.execute("SELECT * FROM team_form_cache WHERE team_key = %s", (team_key,))
        row = self.cursor.fetchone()
        if row:
            return row
        # Varsayılan form
        return {'last_10_avg_goals': 1.0, 'last_10_avg_shot_on': 4.0, 'last_10_avg_corners': 4.0,
                'last_10_btts_ratio': 0.4, 'home_last_5_avg_goals': 1.0, 'away_last_5_avg_goals': 1.0,
                'last_3_avg_shot_on': 4.0, 'last_3_avg_corners': 4.0, 'last_3_avg_possession': 45.0}

    def get_referee_penalty(self, referee_name, league_avg_goals):
        if not referee_name:
            return 1.0
        self.cursor.execute("SELECT avg_goals FROM referee_stats WHERE referee_name = %s", (referee_name,))
        row = self.cursor.fetchone()
        if row and row['avg_goals'] < league_avg_goals * 0.9:
            return 0.9
        return 1.0

    def get_h2h_stats(self, home_team, away_team, category_id, last_n=10):
        query = """
            SELECT ft_home, ft_away FROM results_football
            WHERE status IN ('finished', 'ended')
                AND category_id = %s
                AND ((home_team = %s AND away_team = %s) OR (home_team = %s AND away_team = %s))
            ORDER BY start_utc DESC LIMIT %s
        """
        self.cursor.execute(query, (category_id, home_team, away_team, away_team, home_team, last_n))
        matches = self.cursor.fetchall()
        if not matches:
            return None, None
        total = len(matches)
        btts = sum(1 for m in matches if (m['ft_home'] or 0) > 0 and (m['ft_away'] or 0) > 0) / total
        over25 = sum(1 for m in matches if (m['ft_home'] or 0) + (m['ft_away'] or 0) > 2.5) / total
        return btts, over25

    def calculate_form_score(self, home_cache, away_cache, league_avg_goals):
        general_avg = (home_cache['last_10_avg_goals'] + away_cache['last_10_avg_goals']) / 2.0
        norm_general = min(general_avg / league_avg_goals, 1.5) if league_avg_goals > 0 else 1.0
        general_score = min(31.5, general_avg * 7 * norm_general)
        specific_avg = (home_cache['home_last_5_avg_goals'] + away_cache['away_last_5_avg_goals']) / 2.0
        norm_specific = min(specific_avg / league_avg_goals, 1.5) if league_avg_goals > 0 else 1.0
        specific_score = min(13.5, specific_avg * 3 * norm_specific)
        return general_score + specific_score

    def calculate_attack_pressure(self, home_cache, away_cache, league_avg_shot_on, league_avg_corners):
        def pressure(cache):
            norm_shot = min(cache['last_3_avg_shot_on'] / league_avg_shot_on, 1.5) if league_avg_shot_on > 0 else 1.0
            norm_corn = min(cache['last_3_avg_corners'] / league_avg_corners, 1.5) if league_avg_corners > 0 else 1.0
            norm_poss = min(cache['last_3_avg_possession'] / 50.0, 1.5)
            return (
                cache['last_3_avg_shot_on'] * 3 * norm_shot +
                cache['last_3_avg_corners'] * 1.5 * norm_corn +
                cache['last_3_avg_shot_on'] * 1 * norm_shot +   # total shot approximated
                cache['last_3_avg_possession'] * 0.2 * norm_poss
            ) / 3.0
        home_p = pressure(home_cache)
        away_p = pressure(away_cache)
        return min(40.0, (home_p + away_p) / 2.0)

    def calculate_h2h_score(self, h2h_btts, h2h_over25, league_btts, league_over25):
        if h2h_btts is None:
            return 0.0
        norm_btts = min(h2h_btts / league_btts, 1.5) if league_btts > 0 else 1.0
        norm_over = min(h2h_over25 / league_over25, 1.5) if league_over25 > 0 else 1.0
        raw = ((h2h_btts * norm_btts + h2h_over25 * norm_over) / 2.0) * 15
        return min(15.0, raw)

    def calculate_early_bonus(self, home_cache, away_cache, league_avg_goals):
        league_first_half = league_avg_goals * 0.45
        home_fh = home_cache['last_10_avg_goals'] * 0.4
        away_fh = away_cache['last_10_avg_goals'] * 0.4
        if home_fh > league_first_half and away_fh > league_first_half:
            return 5
        elif (home_fh + away_fh) / 2.0 > league_first_half * 1.5:
            return 3
        return 0

    def calculate_second_half_bonus(self, home_cache, away_cache, league_btts):
        avg_tend = (home_cache['last_10_btts_ratio'] + away_cache['last_10_btts_ratio']) / 2.0
        if avg_tend > league_btts * 1.2:
            return 5
        elif avg_tend > league_btts * 1.1:
            return 2
        return 0

    def predict_match(self, match):
        home = match['home_team']
        away = match['away_team']
        cat_id = match['category_id'] or 0
        home_key = f"{home}|{cat_id}"
        away_key = f"{away}|{cat_id}"
        
        league = self.get_league_stats(cat_id)
        home_cache = self.get_team_form_cache(home_key)
        away_cache = self.get_team_form_cache(away_key)
        
        form_score = self.calculate_form_score(home_cache, away_cache, league['avg_goals'])
        pressure_score = self.calculate_attack_pressure(home_cache, away_cache, league['avg_shot_on'], league['avg_corners'])
        
        h2h_btts, h2h_over25 = self.get_h2h_stats(home, away, cat_id)
        h2h_score = self.calculate_h2h_score(h2h_btts, h2h_over25, league['btts_ratio'], league['over25_ratio'])
        
        early_bonus = self.calculate_early_bonus(home_cache, away_cache, league['avg_goals'])
        second_bonus = self.calculate_second_half_bonus(home_cache, away_cache, league['btts_ratio'])
        
        referee_penalty = self.get_referee_penalty(match.get('referee'), league['avg_goals'])
        
        raw_total = form_score + pressure_score + h2h_score + early_bonus + second_bonus
        net_total = raw_total * referee_penalty
        max_possible = 110.0
        model_prob = min(99.0, (net_total / max_possible) * 100)
        kg_prob = model_prob * 0.95
        
        over_odds = match.get('odds_o25')
        btts_odds = match.get('odds_btts_yes')
        
        result = {
            'home_team': home,
            'away_team': away,
            'date': match['start_utc'],
            'category_id': cat_id,
            'model_over_prob': model_prob,
            'model_btts_prob': kg_prob,
            'over_odds': over_odds,
            'btts_odds': btts_odds,
            'form_score': form_score,
            'pressure_score': pressure_score,
            'h2h_score': h2h_score,
            'early_bonus': early_bonus,
            'second_bonus': second_bonus,
            'referee_penalty': referee_penalty,
            'net_total': net_total,
            'league_stats': league
        }
        if over_odds and over_odds > 0:
            result['over_edge'] = model_prob - (100 / over_odds)
            result['over_play'] = result['over_edge'] >= 5
        else:
            result['over_edge'] = None
            result['over_play'] = False
        if btts_odds and btts_odds > 0:
            result['btts_edge'] = kg_prob - (100 / btts_odds)
            result['btts_play'] = result['btts_edge'] >= 5
        else:
            result['btts_edge'] = None
            result['btts_play'] = False
        return result

    def save_prediction(self, pred, event_id):
        sql = """
            INSERT INTO predictions 
            (event_id, prediction_date, match_date, home_team, away_team, category_id,
             model_over_prob, model_btts_prob, odds_over, odds_btts, edge_over, edge_btts,
             play_over, play_btts, form_score, pressure_score, h2h_score, early_bonus,
             second_bonus, referee_penalty, net_total_score)
            VALUES (%s, CURDATE(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            model_over_prob = VALUES(model_over_prob),
            model_btts_prob = VALUES(model_btts_prob),
            edge_over = VALUES(edge_over),
            edge_btts = VALUES(edge_btts),
            play_over = VALUES(play_over),
            play_btts = VALUES(play_btts),
            form_score = VALUES(form_score),
            pressure_score = VALUES(pressure_score),
            h2h_score = VALUES(h2h_score),
            early_bonus = VALUES(early_bonus),
            second_bonus = VALUES(second_bonus),
            referee_penalty = VALUES(referee_penalty),
            net_total_score = VALUES(net_total_score),
            updated_at = NOW()
        """
        self.cursor.execute(sql, (
            event_id, pred['date'], pred['home_team'], pred['away_team'], pred['category_id'],
            pred['model_over_prob'], pred['model_btts_prob'], pred['over_odds'], pred['btts_odds'],
            pred['over_edge'], pred['btts_edge'], pred['over_play'], pred['btts_play'],
            pred['form_score'], pred['pressure_score'], pred['h2h_score'], pred['early_bonus'],
            pred['second_bonus'], pred['referee_penalty'], pred['net_total']
        ))

    def run(self):
        self.connect()
        # Gelecek maçları al (bugün ve yarın, başlamamış, major turnuva)
        tz_tr = dt.timezone(dt.timedelta(hours=3))
        today = dt.datetime.now(tz_tr).date()
        tomorrow = today + dt.timedelta(days=1)
        ids_str = ','.join(map(str, MAJOR_TOURNAMENT_IDS))
        query = f"""
            SELECT event_id, start_utc, home_team, away_team, odds_o25, odds_btts_yes, category_id
            FROM results_football
            WHERE status IN ('notstarted', 'scheduled')
                AND start_utc IN (%s, %s)
                AND (tournament_id IN ({ids_str}) OR category_id IN ({ids_str}))
        """
        self.cursor.execute(query, (today, tomorrow))
        matches = self.cursor.fetchall()
        if not matches:
            print("Bugün veya yarın oynanacak major turnuva maçı bulunamadı.")
            self.close()
            return
        
        print(f"\n🔮 TAHMİN RAPORU - {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        for match in matches:
            # Hakem bilgisini al
            self.cursor.execute("SELECT referee FROM results_football WHERE event_id = %s", (match['event_id'],))
            ref_row = self.cursor.fetchone()
            match['referee'] = ref_row['referee'] if ref_row else None
            pred = self.predict_match(match)
            self.save_prediction(pred, match['event_id'])
            # Konsola rapor yazdır
            print("=" * 50)
            print(f"🏆 {pred['home_team']} vs {pred['away_team']} (Lig ID: {pred['category_id']}) - {pred['date']}")
            print(f"📊 2.5 Üst: %{pred['model_over_prob']:.1f} | KG Var: %{pred['model_btts_prob']:.1f}")
            if pred['over_odds']:
                edge_over = pred['over_edge']
                print(f"💰 Over 2.5: Oran {pred['over_odds']:.2f} -> Edge %{edge_over:.1f} -> {'OYNA' if pred['over_play'] else 'OYNAMA'}")
            if pred['btts_odds']:
                edge_btts = pred['btts_edge']
                print(f"💰 KG Var: Oran {pred['btts_odds']:.2f} -> Edge %{edge_btts:.1f} -> {'OYNA' if pred['btts_play'] else 'OYNAMA'}")
        print(f"\n✅ {len(matches)} maçın tahmini predictions tablosuna kaydedildi.")
        self.close()

if __name__ == "__main__":
    engine = PredictionEngine()
    engine.run()
