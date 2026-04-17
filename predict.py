#!/usr/bin/env python3
"""
Tahmin scripti - numpy kullanmaz, cache tablolarından okur, UTF-8 karakter desteği var
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
    1,2,3,72,84,36,37,3739,33,34,7372,42,41,8343,810,4,5397,62,101,39,40,38,692,280,127,83,1449,
    169352,5071,28,6720,18,3397,3708,82,3034,3284,6230,54,64,29,1060,219,652,144,1339,1340,1341,
    5,6,12,13,19,24,27,30,31,48,49,50,52,53,55,79,102,232,384,681,877,1061,1107,1427,10812,16753,
    19232,34363,51702,52653,58560,64475,71900,71901,72112,78740,92016,92614,143625
}

class Predictor:
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_pred (event_id, prediction_date)
            )
        """)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_league_stats(self, cat_id):
        self.cursor.execute("SELECT * FROM league_stats WHERE category_id = %s", (cat_id,))
        row = self.cursor.fetchone()
        if row:
            return row
        return {'avg_goals': 2.5, 'avg_shot_on': 8.0, 'avg_corners': 9.0,
                'btts_ratio': 0.45, 'over25_ratio': 0.45}

    def get_team_form(self, team_key):
        self.cursor.execute("SELECT * FROM team_form_cache WHERE team_key = %s", (team_key,))
        row = self.cursor.fetchone()
        if row:
            return row
        return {
            'last_10_avg_goals': 1.0, 'last_10_avg_shot_on': 4.0, 'last_10_avg_corners': 4.0,
            'last_10_btts_ratio': 0.4, 'home_last_5_avg_goals': 1.0, 'away_last_5_avg_goals': 1.0,
            'last_3_avg_shot_on': 4.0, 'last_3_avg_corners': 4.0, 'last_3_avg_possession': 45.0
        }

    def get_h2h(self, home, away, cat_id):
        self.cursor.execute("""
            SELECT ft_home, ft_away FROM results_football
            WHERE status IN ('finished','ended') AND category_id = %s
                AND ((home_team = %s AND away_team = %s) OR (home_team = %s AND away_team = %s))
            ORDER BY start_utc DESC LIMIT 10
        """, (cat_id, home, away, away, home))
        matches = self.cursor.fetchall()
        if not matches:
            return None, None
        total = len(matches)
        btts = sum(1 for m in matches if (m['ft_home'] or 0) > 0 and (m['ft_away'] or 0) > 0) / total
        over = sum(1 for m in matches if (m['ft_home'] or 0) + (m['ft_away'] or 0) > 2.5) / total
        return btts, over

    def get_referee_penalty(self, referee, league_avg_goals):
        if not referee:
            return 1.0
        self.cursor.execute("SELECT avg_goals FROM referee_stats WHERE referee_name = %s", (referee,))
        row = self.cursor.fetchone()
        if row and row['avg_goals'] and row['avg_goals'] < league_avg_goals * 0.9:
            return 0.9
        return 1.0

    def normalize(self, value, league_avg, max_ratio=1.5):
        if league_avg <= 0:
            return 1.0
        return min(value / league_avg, max_ratio)

    def calculate_form_score(self, home_f, away_f, league_avg_goals):
        general_avg = (home_f['last_10_avg_goals'] + away_f['last_10_avg_goals']) / 2.0
        norm_g = self.normalize(general_avg, league_avg_goals)
        general_score = min(31.5, general_avg * 7 * norm_g)
        specific_avg = (home_f['home_last_5_avg_goals'] + away_f['away_last_5_avg_goals']) / 2.0
        norm_s = self.normalize(specific_avg, league_avg_goals)
        specific_score = min(13.5, specific_avg * 3 * norm_s)
        return general_score + specific_score

    def calculate_pressure(self, home_f, away_f, league_avg_shot_on, league_avg_corners):
        def pressure_one(f):
            norm_shot = self.normalize(f['last_3_avg_shot_on'], league_avg_shot_on)
            norm_corn = self.normalize(f['last_3_avg_corners'], league_avg_corners)
            norm_poss = self.normalize(f['last_3_avg_possession'], 50.0)
            return (f['last_3_avg_shot_on'] * 3 * norm_shot +
                    f['last_3_avg_corners'] * 1.5 * norm_corn +
                    f['last_3_avg_shot_on'] * 1 * norm_shot +
                    f['last_3_avg_possession'] * 0.2 * norm_poss) / 3.0
        home_p = pressure_one(home_f)
        away_p = pressure_one(away_f)
        return min(40.0, (home_p + away_p) / 2.0)

    def calculate_h2h_score(self, btts, over, league_btts, league_over):
        if btts is None:
            return 0.0
        norm_b = self.normalize(btts, league_btts)
        norm_o = self.normalize(over, league_over)
        return min(15.0, ((btts * norm_b + over * norm_o) / 2.0) * 15)

    def calculate_bonuses(self, home_f, away_f, league_avg_goals, league_btts):
        league_fh = league_avg_goals * 0.45
        home_fh = home_f['last_10_avg_goals'] * 0.4
        away_fh = away_f['last_10_avg_goals'] * 0.4
        early = 0
        if home_fh > league_fh and away_fh > league_fh:
            early = 5
        elif (home_fh + away_fh)/2.0 > league_fh * 1.5:
            early = 3
        avg_tend = (home_f['last_10_btts_ratio'] + away_f['last_10_btts_ratio']) / 2.0
        second = 0
        if avg_tend > league_btts * 1.2:
            second = 5
        elif avg_tend > league_btts * 1.1:
            second = 2
        return early, second

    def predict_match(self, match):
        home, away = match['home_team'], match['away_team']
        cat_id = match['category_id'] or 0
        home_key = f"{home}|{cat_id}"
        away_key = f"{away}|{cat_id}"
        league = self.get_league_stats(cat_id)
        home_f = self.get_team_form(home_key)
        away_f = self.get_team_form(away_key)
        form_score = self.calculate_form_score(home_f, away_f, league['avg_goals'])
        pressure_score = self.calculate_pressure(home_f, away_f, league['avg_shot_on'], league['avg_corners'])
        btts_h2h, over_h2h = self.get_h2h(home, away, cat_id)
        h2h_score = self.calculate_h2h_score(btts_h2h, over_h2h, league['btts_ratio'], league['over25_ratio'])
        early_bonus, second_bonus = self.calculate_bonuses(home_f, away_f, league['avg_goals'], league['btts_ratio'])
        ref_penalty = self.get_referee_penalty(match.get('referee'), league['avg_goals'])
        raw_total = form_score + pressure_score + h2h_score + early_bonus + second_bonus
        net_total = raw_total * ref_penalty
        model_prob = min(99.0, (net_total / 110.0) * 100)
        kg_prob = model_prob * 0.95
        odds_over = match.get('odds_o25')
        odds_btts = match.get('odds_btts_yes')
        edge_over = None
        edge_btts = None
        play_over = False
        play_btts = False
        if odds_over and odds_over > 0:
            edge_over = model_prob - (100 / odds_over)
            play_over = edge_over >= 5
        if odds_btts and odds_btts > 0:
            edge_btts = kg_prob - (100 / odds_btts)
            play_btts = edge_btts >= 5
        return {
            'home_team': home, 'away_team': away, 'date': match['start_utc'],
            'category_id': cat_id, 'model_over_prob': model_prob, 'model_btts_prob': kg_prob,
            'odds_over': odds_over, 'odds_btts': odds_btts,
            'edge_over': edge_over, 'edge_btts': edge_btts,
            'play_over': play_over, 'play_btts': play_btts,
            'form_score': form_score, 'pressure_score': pressure_score, 'h2h_score': h2h_score,
            'early_bonus': early_bonus, 'second_bonus': second_bonus,
            'referee_penalty': ref_penalty, 'net_total': net_total
        }

    def save_prediction(self, pred, event_id):
        self.cursor.execute("""
            INSERT INTO predictions 
            (event_id, prediction_date, match_date, home_team, away_team, category_id,
             model_over_prob, model_btts_prob, odds_over, odds_btts, edge_over, edge_btts,
             play_over, play_btts, form_score, pressure_score, h2h_score, early_bonus,
             second_bonus, referee_penalty, net_total_score)
            VALUES (%s, CURDATE(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            model_over_prob=VALUES(model_over_prob), model_btts_prob=VALUES(model_btts_prob),
            odds_over=VALUES(odds_over), odds_btts=VALUES(odds_btts),
            edge_over=VALUES(edge_over), edge_btts=VALUES(edge_btts),
            play_over=VALUES(play_over), play_btts=VALUES(play_btts),
            form_score=VALUES(form_score), pressure_score=VALUES(pressure_score),
            h2h_score=VALUES(h2h_score), early_bonus=VALUES(early_bonus),
            second_bonus=VALUES(second_bonus), referee_penalty=VALUES(referee_penalty),
            net_total_score=VALUES(net_total_score), updated_at=NOW()
        """, (event_id, pred['date'], pred['home_team'], pred['away_team'], pred['category_id'],
              pred['model_over_prob'], pred['model_btts_prob'], pred['odds_over'], pred['odds_btts'],
              pred['edge_over'], pred['edge_btts'], pred['play_over'], pred['play_btts'],
              pred['form_score'], pred['pressure_score'], pred['h2h_score'], pred['early_bonus'],
              pred['second_bonus'], pred['referee_penalty'], pred['net_total']))

    def run(self):
        self.connect()
        tz_tr = dt.timezone(dt.timedelta(hours=3))
        today = dt.datetime.now(tz_tr).date()
        tomorrow = today + dt.timedelta(days=1)
        query = f"""
            SELECT event_id, start_utc, home_team, away_team, odds_o25, odds_btts_yes, category_id
            FROM results_football
            WHERE status IN ('notstarted', 'scheduled')
                AND start_utc IN (%s, %s)
                AND (tournament_id IN ({','.join(map(str, MAJOR_TOURNAMENT_IDS))}) 
                     OR category_id IN ({','.join(map(str, MAJOR_TOURNAMENT_IDS))}))
        """
        self.cursor.execute(query, (today, tomorrow))
        matches = self.cursor.fetchall()
        if not matches:
            print("Bugün veya yarın maç yok.")
            self.close()
            return
        for match in matches:
            self.cursor.execute("SELECT referee FROM results_football WHERE event_id = %s", (match['event_id'],))
            ref = self.cursor.fetchone()
            match['referee'] = ref['referee'] if ref else None
            pred = self.predict_match(match)
            self.save_prediction(pred, match['event_id'])
            print(f"✅ {pred['home_team']} vs {pred['away_team']} -> Over Edge: {pred['edge_over']:.1f}% / KG Edge: {pred['edge_btts']:.1f}%")
        self.close()

if __name__ == "__main__":
    predictor = Predictor()
    predictor.run()
