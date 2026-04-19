#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tahmin scripti - cache tablolarını (league_stats, team_form_cache, referee_stats) kullanır.
Master prompt'taki tüm kurallar uygulanır.
1.5 Üst marketi de eklenmiştir.

TARİH KISITLAMASI: PREDICT_FROM_DATE değişkeni ile belirtilen tarih ve bir sonraki gün
için tahmin yapılır. None ise bugün ve yarın kullanılır.
"""

import os
import datetime as dt
import mysql.connector

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

VALUE_EDGE_THRESHOLD = 0   # 0 = value kontrolü yok

# ========== KONFİGÜRASYON ==========
# Tahmin yapılacak başlangıç tarihi (bu tarih ve bir sonraki gün için tahmin yapılır).
# Örnek: "2026-04-20" -> 20 Nisan 2026 ve 21 Nisan 2026 maçları.
# None verilirse bugün ve yarın kullanılır.
PREDICT_FROM_DATE = "2026-04-10"   # <-- İhtiyacına göre değiştir
# ===================================

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
        # Ana tabloyu oluştur (ilk seferde)
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

        # 1.5 Üst için yeni sütunları ekle (eğer yoklarsa)
        new_columns = [
            ("model_over15_prob", "FLOAT"),
            ("odds_over15", "FLOAT"),
            ("edge_over15", "FLOAT"),
            ("play_over15", "BOOLEAN")
        ]
        for col_name, col_type in new_columns:
            try:
                self.cursor.execute(f"ALTER TABLE predictions ADD COLUMN {col_name} {col_type}")
                print(f"[DB] predictions tablosuna {col_name} sütunu eklendi.")
            except mysql.connector.Error as err:
                if err.errno == 1060:  # Duplicate column name
                    pass
                else:
                    print(f"[DB] Uyarı: {err}")
        print("[DB] predictions tablosu hazır (1.5 Üst sütunları dahil).")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_league_stats(self, category_id):
        self.cursor.execute("SELECT * FROM league_stats WHERE category_id = %s", (category_id,))
        row = self.cursor.fetchone()
        if row:
            return {
                'avg_goals': float(row['avg_goals']) if row['avg_goals'] is not None else 2.5,
                'avg_shot_on': float(row['avg_shot_on']) if row['avg_shot_on'] is not None else 8.0,
                'avg_total_shots': float(row['avg_total_shots']) if row['avg_total_shots'] is not None else 22.0,
                'avg_corners': float(row['avg_corners']) if row['avg_corners'] is not None else 9.0,
                'btts_ratio': float(row['btts_ratio']) if row['btts_ratio'] is not None else 0.45,
                'over25_ratio': float(row['over25_ratio']) if row['over25_ratio'] is not None else 0.45,
                'avg_first_half_goals': float(row['avg_first_half_goals']) if row['avg_first_half_goals'] is not None else 1.1,
                'zero_zero_comeback_ratio': float(row['zero_zero_comeback_ratio']) if row['zero_zero_comeback_ratio'] is not None else 0.15
            }
        return {
            'avg_goals': 2.5,
            'avg_shot_on': 8.0,
            'avg_total_shots': 22.0,
            'avg_corners': 9.0,
            'btts_ratio': 0.45,
            'over25_ratio': 0.45,
            'avg_first_half_goals': 1.1,
            'zero_zero_comeback_ratio': 0.15
        }

    def get_team_form_cache(self, team_key):
        self.cursor.execute("SELECT * FROM team_form_cache WHERE team_key = %s", (team_key,))
        row = self.cursor.fetchone()
        if row:
            return {
                'last_10_avg_goals': float(row['last_10_avg_goals']) if row['last_10_avg_goals'] is not None else 1.0,
                'last_10_avg_shot_on': float(row['last_10_avg_shot_on']) if row['last_10_avg_shot_on'] is not None else 4.0,
                'last_10_avg_corners': float(row['last_10_avg_corners']) if row['last_10_avg_corners'] is not None else 4.0,
                'last_10_btts_ratio': float(row['last_10_btts_ratio']) if row['last_10_btts_ratio'] is not None else 0.4,
                'last_10_avg_first_half_goals': float(row['last_10_avg_first_half_goals']) if row['last_10_avg_first_half_goals'] is not None else 0.5,
                'zero_zero_comeback_ratio': float(row['zero_zero_comeback_ratio']) if row['zero_zero_comeback_ratio'] is not None else 0.15,
                'home_last_5_avg_goals': float(row['home_last_5_avg_goals']) if row['home_last_5_avg_goals'] is not None else 1.0,
                'away_last_5_avg_goals': float(row['away_last_5_avg_goals']) if row['away_last_5_avg_goals'] is not None else 1.0,
                'last_3_avg_shot_on': float(row['last_3_avg_shot_on']) if row['last_3_avg_shot_on'] is not None else 4.0,
                'last_3_avg_total_shots': float(row['last_3_avg_total_shots']) if row['last_3_avg_total_shots'] is not None else 10.0,
                'last_3_avg_corners': float(row['last_3_avg_corners']) if row['last_3_avg_corners'] is not None else 4.0,
                'last_3_avg_possession': float(row['last_3_avg_possession']) if row['last_3_avg_possession'] is not None else 45.0,
                'last_match_shot_on': int(row['last_match_shot_on']) if row['last_match_shot_on'] is not None else 4
            }
        return {
            'last_10_avg_goals': 1.0,
            'last_10_avg_shot_on': 4.0,
            'last_10_avg_corners': 4.0,
            'last_10_btts_ratio': 0.4,
            'last_10_avg_first_half_goals': 0.5,
            'zero_zero_comeback_ratio': 0.15,
            'home_last_5_avg_goals': 1.0,
            'away_last_5_avg_goals': 1.0,
            'last_3_avg_shot_on': 4.0,
            'last_3_avg_total_shots': 10.0,
            'last_3_avg_corners': 4.0,
            'last_3_avg_possession': 45.0,
            'last_match_shot_on': 4
        }

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

    def calculate_attack_pressure(self, home_cache, away_cache, league_avg_shot_on, league_avg_total_shots, league_avg_corners):
        def pressure(cache):
            norm_shot_on = min(cache['last_3_avg_shot_on'] / league_avg_shot_on, 1.5) if league_avg_shot_on > 0 else 1.0
            norm_total_shots = min(cache['last_3_avg_total_shots'] / league_avg_total_shots, 1.5) if league_avg_total_shots > 0 else 1.0
            norm_corners = min(cache['last_3_avg_corners'] / league_avg_corners, 1.5) if league_avg_corners > 0 else 1.0
            norm_poss = min(cache['last_3_avg_possession'] / 50.0, 1.5)
            raw = (cache['last_3_avg_shot_on'] * 3 * norm_shot_on) + \
                  (cache['last_3_avg_corners'] * 1.5 * norm_corners) + \
                  (cache['last_3_avg_total_shots'] * 1 * norm_total_shots) + \
                  (cache['last_3_avg_possession'] * 0.2 * norm_poss)
            if cache['last_match_shot_on'] <= 1:
                raw -= (cache['last_3_avg_possession'] * 0.2 * norm_poss)
            return raw / 3.0
        home_p = pressure(home_cache)
        away_p = pressure(away_cache)
        total = (home_p + away_p) / 2.0
        return min(40.0, total)

    def calculate_h2h_score(self, h2h_btts, h2h_over25, league_btts, league_over25):
        if h2h_btts is None:
            return 0.0
        norm_btts = min(h2h_btts / league_btts, 1.5) if league_btts > 0 else 1.0
        norm_over = min(h2h_over25 / league_over25, 1.5) if league_over25 > 0 else 1.0
        raw = ((h2h_btts * norm_btts + h2h_over25 * norm_over) / 2.0) * 15
        return min(15.0, raw)

    def calculate_early_bonus(self, home_cache, away_cache, league_avg_first_half_goals):
        home_fh = home_cache['last_10_avg_first_half_goals']
        away_fh = away_cache['last_10_avg_first_half_goals']
        combined_fh = home_fh + away_fh
        if combined_fh > league_avg_first_half_goals * 1.2:
            return 5
        elif combined_fh > league_avg_first_half_goals:
            return 3
        return 0

    def calculate_second_half_bonus(self, home_cache, away_cache, league_zero_zero_comeback):
        home_zz = home_cache.get('zero_zero_comeback_ratio', 0)
        away_zz = away_cache.get('zero_zero_comeback_ratio', 0)
        avg_zz = (home_zz + away_zz) / 2.0
        if avg_zz > 0:
            if avg_zz > league_zero_zero_comeback * 1.2:
                return 5
            elif avg_zz > league_zero_zero_comeback:
                return 2
        else:
            home_btts = home_cache['last_10_btts_ratio']
            away_btts = away_cache['last_10_btts_ratio']
            avg_btts = (home_btts + away_btts) / 2.0
            if avg_btts > 0.55:
                return 3
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
        pressure_score = self.calculate_attack_pressure(home_cache, away_cache,
                                                         league['avg_shot_on'],
                                                         league['avg_total_shots'],
                                                         league['avg_corners'])
        h2h_btts, h2h_over25 = self.get_h2h_stats(home, away, cat_id)
        h2h_score = self.calculate_h2h_score(h2h_btts, h2h_over25, league['btts_ratio'], league['over25_ratio'])
        early_bonus = self.calculate_early_bonus(home_cache, away_cache, league['avg_first_half_goals'])
        second_bonus = self.calculate_second_half_bonus(home_cache, away_cache, league['zero_zero_comeback_ratio'])
        referee_penalty = self.get_referee_penalty(match.get('referee'), league['avg_goals'])

        raw_total = form_score + pressure_score + h2h_score + early_bonus + second_bonus
        net_total = raw_total * referee_penalty
        max_possible = 110.0
        model_over25_prob = min(99.0, (net_total / max_possible) * 100)
        model_btts_prob = model_over25_prob * 0.95

        # 1.5 Üst olasılığı: model_over25_prob'dan daha yüksek olmalı.
        model_over15_prob = model_over25_prob + (100 - model_over25_prob) * 0.3
        model_over15_prob = min(99.0, model_over15_prob)

        over25_odds = match.get('odds_o25')
        over15_odds = match.get('odds_o15')
        btts_odds = match.get('odds_btts_yes')

        result = {
            'home_team': home,
            'away_team': away,
            'date': match['start_utc'],
            'category_id': cat_id,
            'model_over_prob': model_over25_prob,
            'model_over15_prob': model_over15_prob,
            'model_btts_prob': model_btts_prob,
            'over_odds': over25_odds,
            'over15_odds': over15_odds,
            'btts_odds': btts_odds,
            'form_score': form_score,
            'pressure_score': pressure_score,
            'h2h_score': h2h_score,
            'early_bonus': early_bonus,
            'second_bonus': second_bonus,
            'referee_penalty': referee_penalty,
            'net_total': net_total,
        }

        # 2.5 Üst
        if over25_odds and over25_odds > 0:
            result['over_edge'] = model_over25_prob - (100 / over25_odds)
            result['over_play'] = result['over_edge'] >= VALUE_EDGE_THRESHOLD
        else:
            result['over_edge'] = None
            result['over_play'] = False

        # 1.5 Üst
        if over15_odds and over15_odds > 0:
            result['over15_edge'] = model_over15_prob - (100 / over15_odds)
            result['over15_play'] = result['over15_edge'] >= VALUE_EDGE_THRESHOLD
        else:
            result['over15_edge'] = None
            result['over15_play'] = False

        # KG Var
        if btts_odds and btts_odds > 0:
            result['btts_edge'] = model_btts_prob - (100 / btts_odds)
            result['btts_play'] = result['btts_edge'] >= VALUE_EDGE_THRESHOLD
        else:
            result['btts_edge'] = None
            result['btts_play'] = False

        return result

    def save_prediction(self, pred, event_id):
        sql = """
            INSERT INTO predictions 
            (event_id, prediction_date, match_date, home_team, away_team, category_id,
             model_over_prob, model_over15_prob, model_btts_prob,
             odds_over, odds_over15, odds_btts,
             edge_over, edge_over15, edge_btts,
             play_over, play_over15, play_btts,
             form_score, pressure_score, h2h_score, early_bonus,
             second_bonus, referee_penalty, net_total_score)
            VALUES (%s, CURDATE(), %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            model_over_prob = VALUES(model_over_prob),
            model_over15_prob = VALUES(model_over15_prob),
            model_btts_prob = VALUES(model_btts_prob),
            odds_over = VALUES(odds_over),
            odds_over15 = VALUES(odds_over15),
            odds_btts = VALUES(odds_btts),
            edge_over = VALUES(edge_over),
            edge_over15 = VALUES(edge_over15),
            edge_btts = VALUES(edge_btts),
            play_over = VALUES(play_over),
            play_over15 = VALUES(play_over15),
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
            pred['model_over_prob'], pred['model_over15_prob'], pred['model_btts_prob'],
            pred['over_odds'], pred['over15_odds'], pred['btts_odds'],
            pred['over_edge'], pred['over15_edge'], pred['btts_edge'],
            pred['over_play'], pred['over15_play'], pred['btts_play'],
            pred['form_score'], pred['pressure_score'], pred['h2h_score'], pred['early_bonus'],
            pred['second_bonus'], pred['referee_penalty'], pred['net_total']
        ))

    def run(self):
        self.connect()
        tz_tr = dt.timezone(dt.timedelta(hours=3))

        # Tahmin yapılacak tarih aralığını belirle
        if PREDICT_FROM_DATE:
            pred_start = dt.datetime.strptime(PREDICT_FROM_DATE, "%Y-%m-%d").date()
            pred_end = pred_start + dt.timedelta(days=1)
            date1 = pred_start
            date2 = pred_end
            print(f"🔮 Tahmin aralığı: {date1} ve {date2} (konfigürasyondan)")
        else:
            today = dt.datetime.now(tz_tr).date()
            tomorrow = today + dt.timedelta(days=1)
            date1 = today
            date2 = tomorrow
            print(f"🔮 Tahmin aralığı: bugün ({today}) ve yarın ({tomorrow})")

        ids_str = ','.join(map(str, MAJOR_TOURNAMENT_IDS))
        query = f"""
            SELECT event_id, start_utc, home_team, away_team,
                   odds_o25, odds_o15, odds_btts_yes, category_id
            FROM results_football
            WHERE status IN ('finished', 'ended')
                AND start_utc IN (%s, %s)
                AND (tournament_id IN ({ids_str}) OR category_id IN ({ids_str}))
        """
        self.cursor.execute(query, (date1, date2))
        matches = self.cursor.fetchall()
        if not matches:
            print(f"{date1} veya {date2} oynanacak major turnuva maçı bulunamadı.")
            self.close()
            return

        print(f"\n🔮 TAHMİN RAPORU - {dt.datetime.now().strftime('%Y-%m-%d %H:%M')} (Edge eşiği: {VALUE_EDGE_THRESHOLD})\n")
        for match in matches:
            self.cursor.execute("SELECT referee FROM results_football WHERE event_id = %s", (match['event_id'],))
            ref_row = self.cursor.fetchone()
            match['referee'] = ref_row['referee'] if ref_row else None
            pred = self.predict_match(match)
            self.save_prediction(pred, match['event_id'])

            print("=" * 50)
            print(f"🏆 {pred['home_team']} vs {pred['away_team']} (Lig ID: {pred['category_id']}) - {pred['date']}")
            print(f"📊 2.5 Üst: %{pred['model_over_prob']:.1f} | 1.5 Üst: %{pred['model_over15_prob']:.1f} | KG Var: %{pred['model_btts_prob']:.1f}")
            if pred['over_odds']:
                print(f"💰 Over 2.5: Oran {pred['over_odds']:.2f} -> Edge %{pred['over_edge']:.1f} -> {'OYNA' if pred['over_play'] else 'OYNAMA'}")
            if pred['over15_odds']:
                print(f"💰 Over 1.5: Oran {pred['over15_odds']:.2f} -> Edge %{pred['over15_edge']:.1f} -> {'OYNA' if pred['over15_play'] else 'OYNAMA'}")
            if pred['btts_odds']:
                print(f"💰 KG Var: Oran {pred['btts_odds']:.2f} -> Edge %{pred['btts_edge']:.1f} -> {'OYNA' if pred['btts_play'] else 'OYNAMA'}")
            print(f"📈 Puanlar: Form={pred['form_score']:.1f}, Baskı={pred['pressure_score']:.1f}, H2H={pred['h2h_score']:.1f}, Erken={pred['early_bonus']}, İkinciYarı={pred['second_bonus']}, Hakem={pred['referee_penalty']} -> Toplam {pred['net_total']:.1f}/110")

        print(f"\n✅ {len(matches)} maçın tahmini predictions tablosuna kaydedildi.")
        self.close()


if __name__ == "__main__":
    engine = PredictionEngine()
    engine.run()
