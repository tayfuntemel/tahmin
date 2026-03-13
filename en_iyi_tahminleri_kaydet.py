#!/usr/bin/env python3
import mysql.connector

# --- VERİTABANI BAĞLANTISI ---
CONFIG = {
    "db": {
        "host": "netscout.fun",
        "user": "netscout_veri",
        "password": "i.34temel1",
        "database": "netscout_veri",
        "port": 3306
    }
}

class BestBetAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self._ensure_columns_exist()
        print("[EN İYİ TAHMİN] Veritabanı bağlantısı başarılı.")

    def _ensure_columns_exist(self):
        columns_to_add = [
            ("best_market_raw", "VARCHAR(32) NULL"),
            ("best_market_tr", "VARCHAR(64) NULL"),
            ("best_prob", "FLOAT NULL"),
            ("best_odds", "FLOAT NULL"),
            ("best_value", "FLOAT NULL")
        ]
        
        for col_name, col_type in columns_to_add:
            try:
                self.cur.execute(f"ALTER TABLE match_predictions ADD COLUMN {col_name} {col_type}")
            except mysql.connector.Error as err:
                if err.errno != 1060:
                    print(f"[HATA] Sütun eklenirken hata: {err}")

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def process_best_bets(self):
        print("[EN İYİ TAHMİN] Henüz tahmini oluşturulmamış yeni maçlar aranıyor...")
        
        # SADECE best_market_raw sütunu boş olan maçları ve yeni market oranlarını çekiyoruz
        query = """
            SELECT MP.*, 
                   R.odds_1x, R.odds_x2, R.odds_o15, R.odds_u35, R.odds_btts_yes
            FROM match_predictions MP
            LEFT JOIN results_football R ON MP.event_id = R.event_id 
            WHERE MP.best_market_raw IS NULL
        """
        self.cur.execute(query)
        matches = self.cur.fetchall()
        
        if not matches:
            print("[BİLGİ] Tahmin yapılacak yeni maç bulunamadı.")
            return

        print(f"[BİLGİ] {len(matches)} yeni maç için tahmin hesaplanıyor...")

        update_query = """
            UPDATE match_predictions 
            SET best_market_raw = %s, best_market_tr = %s, best_prob = %s, best_odds = %s, best_value = %s
            WHERE event_id = %s
        """
        
        count = 0
        for row in matches:
            markets = {
                '1X':   {'prob': row['prob_1x'],   'value': row['value_1x'],   'odds': row['odds_1x'], 'name': '1x çifte şans'},
                'X2':   {'prob': row['prob_x2'],   'value': row['value_x2'],   'odds': row['odds_x2'], 'name': 'x2 çifte şans'},
                'O15':  {'prob': row['prob_o15'],  'value': row['value_o15'],  'odds': row['odds_o15'], 'name': '1.5 üst'},
                'U35':  {'prob': row['prob_u35'],  'value': row['value_u35'],  'odds': row['odds_u35'], 'name': '3.5 alt'},
                'BTTS': {'prob': row['prob_btts'], 'value': row['value_btts'], 'odds': row['odds_btts_yes'], 'name': 'kg var'}
            }

            best_bet = None
            highest_value = -999

            for key, data in markets.items():
                try:
                    prob = float(data['prob']) if data['prob'] is not None else 0
                    odds = float(data['odds']) if data['odds'] is not None else 0
                    value = float(data['value']) if data['value'] is not None else -999
                except (ValueError, TypeError):
                    continue

                # Sadece oranı olanları (odds > 0) ve sistemde kâr ihtimali (value > 0) taşıyanları değerlendiriyoruz.
                if odds > 0 and value > 0:
                    if value > highest_value:
                        highest_value = value
                        best_bet = {
                            'market_raw': key,
                            'market_tr': data['name'],
                            'prob': prob,
                            'odds': odds,
                            'value': value
                        }

            if best_bet:
                self.cur.execute(update_query, (
                    best_bet['market_raw'], 
                    best_bet['market_tr'], 
                    best_bet['prob'], 
                    best_bet['odds'], 
                    best_bet['value'], 
                    row['event_id']
                ))
                count += 1
            else:
                self.cur.execute(update_query, ('NO_BET', 'Bahis Yok', 0, 0, 0, row['event_id']))

        print(f"[BAŞARILI] Toplam {count} maça başarıyla tahmin atandı.")

if __name__ == "__main__":
    analyzer = BestBetAnalyzer(CONFIG["db"])
    try:
        analyzer.connect()
        analyzer.process_best_bets()
    except Exception as e:
        print(f"[HATA] Tahmin oluşturulurken bir sorun oluştu: {e}")
    finally:
        analyzer.close()
