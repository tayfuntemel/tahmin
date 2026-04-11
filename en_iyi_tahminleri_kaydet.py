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

    def _ensure_columns(self):
        # Hedef tabloda gerekli sütunların varlığını garanti et
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
        # Sadece ihtiyacımız olan marketlerin oranlarını ve olasılıklarını çekiyoruz
        self.cur.execute("""
            SELECT mp.event_id,
                   mp.prob_ms1, mp.prob_ms0, mp.prob_ms2,
                   mp.prob_o15, mp.prob_o35,
                   r.odds_1x, r.odds_12, r.odds_x2,
                   r.odds_o15, r.odds_u35
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
            # 1X = Ev sahibi kazanır veya beraberlik
            prob_1x = (row['prob_ms1'] + row['prob_ms0']) / 100.0  # ondalık olasılık (0..1)
            # 12 = Ev sahibi veya deplasman kazanır (beraberlik yok)
            prob_12 = (row['prob_ms1'] + row['prob_ms2']) / 100.0
            # X2 = Deplasman kazanır veya beraberlik
            prob_x2 = (row['prob_ms0'] + row['prob_ms2']) / 100.0
            # 1.5 Üst (zaten doğrudan var)
            prob_o15 = row['prob_o15'] / 100.0
            # 3.5 Alt = 1 - (3.5 Üst olasılığı)
            prob_u35 = (100.0 - row['prob_o35']) / 100.0 if row['prob_o35'] is not None else 0.0

            markets = {
                '1X': {
                    'prob': prob_1x,
                    'odds': row['odds_1x'],
                    'tr': '1x (çifte şans)'
                },
                '12': {
                    'prob': prob_12,
                    'odds': row['odds_12'],
                    'tr': '12 (çifte şans)'
                },
                'X2': {
                    'prob': prob_x2,
                    'odds': row['odds_x2'],
                    'tr': 'x2 (çifte şans)'
                },
                'O15': {
                    'prob': prob_o15,
                    'odds': row['odds_o15'],
                    'tr': '1.5 üst'
                },
                'U35': {
                    'prob': prob_u35,
                    'odds': row['odds_u35'],
                    'tr': '3.5 alt'
                }
            }

            best = None
            best_score = -999999  # en düşük değerden başla

            for key, m in markets.items():
                # Oran veya olasılık eksikse atla
                if m['odds'] is None or m['prob'] is None:
                    continue

                # Verileri float'a çevir (güvenlik)
                try:
                    m_odds = float(m['odds'])
                    m_prob = float(m['prob'])
                except (ValueError, TypeError):
                    continue

                # Value = (Olasılık * Oran) - 1
                m_value = (m_prob * m_odds) - 1.0

                # HİÇBİR FİLTRE UYGULANMAZ:
                # - Oran 1.60 altı olsa bile değerlendirilir.
                # - Olasılık %45 altı veya %95 üstü olsa bile değerlendirilir.
                # - Value negatif olsa bile değerlendirilir.

                # Skor: Value ve olasılığın dengeli çarpımı (mevcut mantık korundu)
                score = m_value * m_prob
                if score > best_score:
                    best_score = score
                    best = {
                        'raw': key,
                        'tr': m['tr'],
                        'prob': round(m_prob * 100, 1),   # yüzde olarak sakla
                        'odds': m_odds,
                        'value': round(m_value, 3)
                    }

            if best:
                self.cur.execute(update_sql, (
                    best['raw'], best['tr'], best['prob'], best['odds'], best['value'], row['event_id']
                ))
                count += 1
            else:
                # Hiçbir market geçerli değilse (örneğin tüm oranlar eksik) NO_BET ata
                self.cur.execute(update_sql, ('NO_BET', 'bahis yok', 0, 0, 0, row['event_id']))

        print(f"{count} maça en iyi bahis atandı (marketler: 1X,12,X2,O15,U35).")

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
