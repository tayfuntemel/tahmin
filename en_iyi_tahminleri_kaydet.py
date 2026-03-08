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
        """match_predictions tablosuna en iyi tahmin verilerini tutacak sütunları ekler."""
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
                # Sütun zaten varsa oluşan 1060 hatasını görmezden geliyoruz
                if err.errno != 1060:
                    print(f"[HATA] Sütun eklenirken hata: {err}")

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def process_best_bets(self):
        print("[EN İYİ TAHMİN] Henüz tahmini oluşturulmamış yeni maçlar aranıyor...")
        
        # SADECE best_market_raw sütunu boş olan (daha önce tahmin yapılmamış) maçları çekiyoruz
        query = """
            SELECT MP.*, 
                   R.odds_1, R.odds_x, R.odds_2, R.odds_o25, R.odds_u25, R.odds_btts_yes
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
            prob_u25 = 100 - float(row['prob_o25']) if row['prob_o25'] is not None else 0
            
            markets = {
                '1':    {'prob': row['prob_1'],   'value': row['value_1'],   'odds': row['odds_1'], 'name': 'ms 1'},
                'X':    {'prob': row['prob_x'],   'value': row['value_x'],   'odds': row['odds_x'], 'name': 'ms x'},
                '2':    {'prob': row['prob_2'],   'value': row['value_2'],   'odds': row['odds_2'], 'name': 'ms 2'},
                'O25':  {'prob': row['prob_o25'], 'value': row['value_o25'], 'odds': row['odds_o25'], 'name': '2.5 üst'},
                'U25':  {'prob': prob_u25,        'value': row['value_u25'], 'odds': row['odds_u25'], 'name': '2.5 alt'},
                'BTTS': {'prob': row['prob_btts'],'value': row['value_btts'],'odds': row['odds_btts_yes'], 'name': 'kg var'}
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

                # %70 ihtimal şartı KALDIRILDI. Sadece Oran >= 1.50 ve Değer (Value) > 0 şartı aranıyor.
                if odds >= 1.50 and value > 0:
                    if value > highest_value:
                        highest_value = value
                        best_bet = {
                            'market_raw': key,
                            'market_tr': data['name'],
                            'prob': prob,
                            'odds': odds,
                            'value': value
                        }

            # En iyi bahis bulunduysa kaydet, bulunamadıysa bir dahaki sefere tekrar taranmaması için "Yok" anlamında 'NO_BET' yaz
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
                # Maç incelendi ama uygun şartlarda bahis çıkmadı. Tekrar taranmasını önlemek için işaretliyoruz.
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
