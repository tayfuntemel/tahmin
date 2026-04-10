#!/usr/bin/env python3
import mysql.connector
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
        self._ensure_market_sample_table()

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

    def _ensure_market_sample_table(self):
        """Geçmişte her marketin kaç kez oynandığını tutan tablo"""
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS market_sample_counts (
                market_raw VARCHAR(32) PRIMARY KEY,
                sample_count INT NOT NULL DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        # Güncelle: bitmiş maçlardaki best_market_raw sayılarını hesapla
        self.cur.execute("""
            INSERT INTO market_sample_counts (market_raw, sample_count)
            SELECT best_market_raw, COUNT(*) 
            FROM match_predictions mp
            JOIN results_football r ON mp.event_id = r.event_id
            WHERE r.status = 'finished' AND best_market_raw IS NOT NULL AND best_market_raw != 'NO_BET'
            GROUP BY best_market_raw
            ON DUPLICATE KEY UPDATE sample_count = VALUES(sample_count)
        """)

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def get_pending_count(self):
        self.cur.execute("SELECT COUNT(*) as c FROM match_predictions WHERE best_market_raw IS NULL")
        return self.cur.fetchone()['c']

    def select_best_bets(self):
        # Minimum örneklem sayısı (20 maç altındaki marketleri oynama)
        MIN_SAMPLE = 20

        self.cur.execute("""
            SELECT mp.*, 
                   r.odds_1, r.odds_x, r.odds_2,
                   r.odds_o15, r.odds_o25, r.odds_o35,
                   r.odds_btts_yes, r.odds_btts_no
            FROM match_predictions mp
            LEFT JOIN results_football r ON mp.event_id = r.event_id
            WHERE mp.best_market_raw IS NULL
        """)
        rows = self.cur.fetchall()
        if not rows:
            print("Güncellenecek maç yok.")
            return

        # Market sample count'ları çek
        sample_counts = {}
        self.cur.execute("SELECT market_raw, sample_count FROM market_sample_counts")
        for row in self.cur.fetchall():
            sample_counts[row['market_raw']] = row['sample_count']

        update_sql = """
            UPDATE match_predictions
            SET best_market_raw = %s, best_market_tr = %s,
                best_prob = %s, best_odds = %s, best_value = %s
            WHERE event_id = %s
        """
        count = 0
        for row in rows:
            markets = {
                'MS1':   {'prob': row['prob_ms1'],   'value': row['value_ms1'],   'odds': row['odds_1'], 'tr': 'ms1'},
                'MS0':   {'prob': row['prob_ms0'],   'value': row['value_ms0'],   'odds': row['odds_x'], 'tr': 'ms0'},
                'MS2':   {'prob': row['prob_ms2'],   'value': row['value_ms2'],   'odds': row['odds_2'], 'tr': 'ms2'},
                'O15':   {'prob': row['prob_o15'],   'value': row['value_o15'],   'odds': row['odds_o15'], 'tr': '1.5 üst'},
                'O25':   {'prob': row['prob_o25'],   'value': row['value_o25'],   'odds': row['odds_o25'], 'tr': '2.5 üst'},
                'O35':   {'prob': row['prob_o35'],   'value': row['value_o35'],   'odds': row['odds_o35'], 'tr': '3.5 üst'},
                'KG Var': {'prob': row['prob_btts_yes'], 'value': row['value_btts_yes'], 'odds': row['odds_btts_yes'], 'tr': 'kg var'},
                'KG Yok': {'prob': row['prob_btts_no'],  'value': row['value_btts_no'],  'odds': row['odds_btts_no'], 'tr': 'kg yok'}
            }

            best = None
            best_score = -999
            for key, m in markets.items():
                # Örneklem kontrolü
                if sample_counts.get(key, 0) < MIN_SAMPLE:
                    continue
                if m['odds'] is None or m['prob'] is None or m['value'] is None:
                    continue
                if m['odds'] < 1.60:
                    continue
                if m['value'] <= 0:
                    continue
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
                self.cur.execute(update_sql, ('NO_BET', 'bahis yok', 0, 0, 0, row['event_id']))
        print(f"{count} maça en iyi bahis atandı (minimum örneklem {MIN_SAMPLE}).")

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
