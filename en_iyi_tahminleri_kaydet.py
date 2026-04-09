#!/usr/bin/env python3
import mysql.connector
import time
import os

CONFIG = {
    "db": {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306))
    }
}

class BestBetSelector:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self._ensure_columns()

    def _ensure_columns(self):
        cols = [
            ("best_market_raw", "VARCHAR(32) NULL"),
            ("best_market_tr", "VARCHAR(64) NULL"),
            ("best_prob", "FLOAT NULL"),
            ("best_odds", "FLOAT NULL"),
            ("best_value", "FLOAT NULL")
        ]
        for col, typ in cols:
            try:
                self.cur.execute(f"ALTER TABLE match_predictions ADD COLUMN {col} {typ}")
            except:
                pass

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def get_pending_count(self):
        self.cur.execute("SELECT COUNT(*) as c FROM match_predictions WHERE best_market_raw IS NULL")
        return self.cur.fetchone()['c']

    def select_best_bets(self):
        # Sadece best_market_raw boş olanları al
        self.cur.execute("""
            SELECT mp.*, 
                   r.odds_1x, r.odds_x2, r.odds_o15, r.odds_u35, r.odds_btts_yes
            FROM match_predictions mp
            LEFT JOIN results_football r ON mp.event_id = r.event_id
            WHERE mp.best_market_raw IS NULL
        """)
        rows = self.cur.fetchall()
        if not rows:
            print("Güncellenecek maç yok.")
            return

        update_sql = """
            UPDATE match_predictions
            SET best_market_raw = %s, best_market_tr = %s,
                best_prob = %s, best_odds = %s, best_value = %s
            WHERE event_id = %s
        """
        count = 0
        for row in rows:
            markets = {
                '1X':   {'prob': row['prob_1x'],   'value': row['value_1x'],   'odds': row['odds_1x'], 'tr': '1x çifte şans'},
                'X2':   {'prob': row['prob_x2'],   'value': row['value_x2'],   'odds': row['odds_x2'], 'tr': 'x2 çifte şans'},
                'O15':  {'prob': row['prob_o15'],  'value': row['value_o15'],  'odds': row['odds_o15'], 'tr': '1.5 üst'},
                'U35':  {'prob': row['prob_u35'],  'value': row['value_u35'],  'odds': row['odds_u35'], 'tr': '3.5 alt'},
                'BTTS': {'prob': row['prob_btts'], 'value': row['value_btts'], 'odds': row['odds_btts_yes'], 'tr': 'kg var'}
            }

            best = None
            best_score = -999
            for key, m in markets.items():
                if m['odds'] is None or m['prob'] is None or m['value'] is None:
                    continue
                # Sadece oranı 1.60 ve üzeri olanları değerlendir
                if m['odds'] < 1.60:
                    continue
                if m['value'] <= 0:
                    continue
                # Güven skoru = value * (prob/100)
                score = m['value'] * (m['prob'] / 100.0)
                if score > best_score:
                    best_score = score
                    best = {
                        'raw': key,
                        'tr': m['tr'],
                        'prob': m['prob'],
                        'odds': m['odds'],
                        'value': m['value']
                    }
            if best:
                self.cur.execute(update_sql, (
                    best['raw'], best['tr'], best['prob'], best['odds'], best['value'], row['event_id']
                ))
                count += 1
            else:
                # Uygun bahis yoksa NO_BET yaz
                self.cur.execute(update_sql, ('NO_BET', 'Bahis Yok (Düşük Oran/Value)', 0, 0, 0, row['event_id']))
        print(f"{count} maça en iyi bahis atandı.")

if __name__ == "__main__":
    selector = BestBetSelector(CONFIG["db"])
    try:
        selector.connect()
        pending = selector.get_pending_count()
        if pending > 0:
            selector.select_best_bets()
        else:
            print("Bekleyen maç yok.")
    finally:
        selector.close()
