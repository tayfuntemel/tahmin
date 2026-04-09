#!/usr/bin/env python3
import os
import mysql.connector

CONFIG = {
    "db": {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306))
    }
}

SCHEMA_CREATE_ODDS_TABLE = """
CREATE TABLE IF NOT EXISTS odds_performance_analytics (
  id                  INT AUTO_INCREMENT PRIMARY KEY,
  filter_type         ENUM('League', 'Team') NOT NULL,
  team_name           VARCHAR(128) NULL,
  tournament_id       INT NOT NULL,
  market              VARCHAR(64) NOT NULL,
  odds_band           VARCHAR(32) NOT NULL,
  matches_played      INT DEFAULT 0,
  won_bets            INT DEFAULT 0,
  win_rate_pct        FLOAT DEFAULT 0,
  avg_odds            FLOAT DEFAULT 0,
  total_profit_units  FLOAT DEFAULT 0,
  yield_roi_pct       FLOAT DEFAULT 0,
  last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY idx_odds_filter (filter_type, team_name, tournament_id, market, odds_band)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class OddsAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.stats = {}

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_ODDS_TABLE)

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _get_odds_band(self, odds):
        if odds < 1.50: return "< 1.50"
        elif odds <= 1.99: return "1.50 - 1.99"
        elif odds <= 2.99: return "2.00 - 2.99"
        elif odds <= 4.99: return "3.00 - 4.99"
        else: return "5.00+"

    def _init_node(self):
        return {"matches": 0, "won": 0, "sum_odds": 0, "profit": 0.0}

    def _process_market(self, f_type, team_name, tour_id, market, odds, is_won):
        if not odds: return
        band = self._get_odds_band(odds)
        key = (f_type, team_name, tour_id, market, band)
        if key not in self.stats:
            self.stats[key] = self._init_node()
        node = self.stats[key]
        node["matches"] += 1
        node["sum_odds"] += odds
        if is_won:
            node["won"] += 1
            node["profit"] += (odds - 1.0)
        else:
            node["profit"] -= 1.0

    def analyze_odds(self):
        self.cur.execute("SELECT * FROM results_football WHERE status IN ('finished','ended') AND ft_home IS NOT NULL AND ft_away IS NOT NULL AND tournament_id IS NOT NULL")
        matches = self.cur.fetchall()
        for match in matches:
            t_id = match['tournament_id']
            h_team, a_team = match['home_team'], match['away_team']
            gh, ga = match['ft_home'], match['ft_away']
            home_won = gh > ga
            away_won = gh < ga
            draw = gh == ga
            over15 = (gh+ga) > 1.5
            over25 = (gh+ga) > 2.5
            over35 = (gh+ga) > 3.5
            btts_yes = (gh>0 and ga>0)
            btts_no = not btts_yes

            # Lig bazlı
            self._process_market('League', None, t_id, 'MS1', match.get('odds_1'), home_won)
            self._process_market('League', None, t_id, 'MS2', match.get('odds_2'), away_won)
            self._process_market('League', None, t_id, 'MS0', match.get('odds_x'), draw)
            self._process_market('League', None, t_id, 'O15', match.get('odds_o15'), over15)
            self._process_market('League', None, t_id, 'O25', match.get('odds_o25'), over25)
            self._process_market('League', None, t_id, 'O35', match.get('odds_o35'), over35)
            self._process_market('League', None, t_id, 'KG Var', match.get('odds_btts_yes'), btts_yes)
            self._process_market('League', None, t_id, 'KG Yok', match.get('odds_btts_no'), btts_no)

            # Takım bazlı (ev sahibi)
            self._process_market('Team', h_team, t_id, 'MS1', match.get('odds_1'), home_won)
            self._process_market('Team', h_team, t_id, 'O15', match.get('odds_o15'), over15)
            self._process_market('Team', h_team, t_id, 'O25', match.get('odds_o25'), over25)
            self._process_market('Team', h_team, t_id, 'O35', match.get('odds_o35'), over35)
            self._process_market('Team', h_team, t_id, 'KG Var', match.get('odds_btts_yes'), btts_yes)
            self._process_market('Team', h_team, t_id, 'KG Yok', match.get('odds_btts_no'), btts_no)

            # Takım bazlı (deplasman)
            self._process_market('Team', a_team, t_id, 'MS2', match.get('odds_2'), away_won)
            self._process_market('Team', a_team, t_id, 'O15', match.get('odds_o15'), over15)
            self._process_market('Team', a_team, t_id, 'O25', match.get('odds_o25'), over25)
            self._process_market('Team', a_team, t_id, 'O35', match.get('odds_o35'), over35)
            self._process_market('Team', a_team, t_id, 'KG Var', match.get('odds_btts_yes'), btts_yes)
            self._process_market('Team', a_team, t_id, 'KG Yok', match.get('odds_btts_no'), btts_no)

        insert_query = """
            INSERT INTO odds_performance_analytics 
            (filter_type, team_name, tournament_id, market, odds_band, 
             matches_played, won_bets, win_rate_pct, avg_odds, total_profit_units, yield_roi_pct)
            VALUES (%(filter_type)s, %(team_name)s, %(tournament_id)s, %(market)s, %(odds_band)s,
             %(matches)s, %(won)s, %(win_rate)s, %(avg_odds)s, %(profit)s, %(roi)s)
            ON DUPLICATE KEY UPDATE
             matches_played=VALUES(matches_played), won_bets=VALUES(won_bets), win_rate_pct=VALUES(win_rate_pct),
             avg_odds=VALUES(avg_odds), total_profit_units=VALUES(total_profit_units), yield_roi_pct=VALUES(yield_roi_pct)
        """
        count = 0
        for key, data in self.stats.items():
            f_type, team_name, tour_id, market, band = key
            mp = data["matches"]
            if mp == 0: continue
            win_rate = (data["won"] / mp) * 100
            avg_odds = data["sum_odds"] / mp
            roi = (data["profit"] / mp) * 100
            row = {
                "filter_type": f_type, "team_name": team_name, "tournament_id": tour_id,
                "market": market, "odds_band": band, "matches": mp, "won": data["won"],
                "win_rate": round(win_rate, 2), "avg_odds": round(avg_odds, 2),
                "profit": round(data["profit"], 2), "roi": round(roi, 2)
            }
            self.cur.execute(insert_query, row)
            count += 1
        print(f"[ORAN ANALİZİ] {count} kayıt güncellendi.")

if __name__ == "__main__":
    a = OddsAnalyzer(CONFIG["db"])
    try:
        a.connect()
        a.analyze_odds()
    finally:
        a.close()
