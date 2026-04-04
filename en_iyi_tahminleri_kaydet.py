#!/usr/bin/env python3
import mysql.connector
import time
import os

# --- VERİTABANI BAĞLANTISI ---
# Bilgiler artık kodun içine yazılmıyor, güvenli bir şekilde GitHub Secrets'tan (Environment Variables) çekiliyor.
CONFIG = {
    "db": {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306))
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

    def get_last_updated_time(self):
        """Tablodaki en güncel updated_at zamanını döndürür."""
        self.cur.execute("SELECT MAX(updated_at) as last_time FROM match_predictions")
        row = self.cur.fetchone()
        return row['last_time'] if row and row['last_time'] else None

    def get_pending_count(self):
        """Henüz en iyi tahmini atanmamış (best_market_raw IS NULL) maç sayısını döndürür."""
        self.cur.execute("SELECT COUNT(*) as count FROM match_predictions WHERE best_market_raw IS NULL")
        row = self.cur.fetchone()
        return row['count'] if row else 0

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    # =========================================================
    # V3 FİNAL - KUSURSUZ KARAR MEKANİZMASI
    # =========================================================
    def calculate_confidence_score(self, market, prob, odds, value, exp_home, exp_away):
        # 1. ZEHİRLİ PAZAR: BTTS KESİN YASAK!
        if market == 'BTTS':
            return -999

        score = prob
        total_xg = exp_home + exp_away
        xg_diff = exp_home - exp_away

        # 2. Value Tuzağı Kontrolü
        if value <= 0:
            return -999
        elif 0.01 <= value <= 0.10:
            score += 15
        elif 0.10 < value <= 0.18:
            score += 5
        elif value > 0.20:
            score -= 25 

        # 3. Oran Mantığı Kalkanı
        if odds < 1.35:
            score -= 40
        elif 1.35 <= odds < 1.50:
            score += 15
        elif 1.50 <= odds <= 1.75:
            score -= 30
        elif odds > 1.75:
            score += 5

        # 4. xG Uyumu (1.5 Üst Oynama Şartı Zorlaştırıldı)
        if market == 'O15':
            if total_xg >= 2.8:
                score += 15
            else:
                return -999 # Beklenen gol 2.8'in altındaysa 1.5 Üst oynama!
        elif market == 'U35':
            if total_xg <= 2.2:
                score += 15
            elif total_xg > 2.8:
                score -= 20
        elif market == '1X':
            if xg_diff > 0.4:
                score += 15
            elif xg_diff < 0:
                score -= 20
        elif market == 'X2':
            if xg_diff < -0.4:
                score += 15
            elif xg_diff > 0:
                score -= 20

        return score

    def process_best_bets(self):
        print("[EN İYİ TAHMİN] Henüz tahmini oluşturulmamış yeni maçlar aranıyor...")
        
        # MP.* zaten exp_goals_home ve exp_goals_away içeriyor.
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

        print(f"[BİLGİ] {len(matches)} yeni maç için V3 Final Algoritması çalışıyor...")

        update_query = """
            UPDATE match_predictions 
            SET best_market_raw = %s, best_market_tr = %s, best_prob = %s, best_odds = %s, best_value = %s
            WHERE event_id = %s
        """
        
        count = 0
        for row in matches:
            exp_home = float(row['exp_goals_home'] or 0)
            exp_away = float(row['exp_goals_away'] or 0)

            markets = {
                '1X':   {'prob': row['prob_1x'],   'value': row['value_1x'],   'odds': row['odds_1x'], 'name': '1x çifte şans'},
                'X2':   {'prob': row['prob_x2'],   'value': row['value_x2'],   'odds': row['odds_x2'], 'name': 'x2 çifte şans'},
                'O15':  {'prob': row['prob_o15'],  'value': row['value_o15'],  'odds': row['odds_o15'], 'name': '1.5 üst'},
                'U35':  {'prob': row['prob_u35'],  'value': row['value_u35'],  'odds': row['odds_u35'], 'name': '3.5 alt'},
                'BTTS': {'prob': row['prob_btts'], 'value': row['value_btts'], 'odds': row['odds_btts_yes'], 'name': 'kg var'}
            }

            best_bet = None
            highest_score = 75.0 # Mükemmel barajımız

            for key, data in markets.items():
                try:
                    prob = float(data['prob']) if data['prob'] is not None else 0
                    odds = float(data['odds']) if data['odds'] is not None else 0
                    value = float(data['value']) if data['value'] is not None else -999
                except (ValueError, TypeError):
                    continue

                # Yeni V3 Final Puanlamasını Kullan
                market_score = self.calculate_confidence_score(key, prob, odds, value, exp_home, exp_away)

                if market_score > highest_score:
                    highest_score = market_score
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
                self.cur.execute(update_query, ('NO_BET', 'Bahis Yok (Riskli Bulundu)', 0, 0, 0, row['event_id']))

        print(f"[BAŞARILI] Toplam {count} maça başarıyla güvenli tahmin atandı.")

if __name__ == "__main__":
    analyzer = BestBetAnalyzer(CONFIG["db"])
    try:
        analyzer.connect()
        
        max_deneme = 3
        bekleme_suresi = 60 # Saniye
        
        for deneme in range(1, max_deneme + 1):
            print(f"\n>>> DENEME {deneme}/{max_deneme} <<<")
            
            bekleyen_mac = analyzer.get_pending_count()
            if bekleyen_mac == 0:
                print("[BİLGİ] 'best_market_raw' sütunu boş olan, yani güncellenmesi beklenen yeni maç yok. İşlem tamam.")
                break
                
            baslangic_zaman = analyzer.get_last_updated_time()
            print(f"Başlangıçtaki Son Güncelleme Zamanı: {baslangic_zaman}")
            print(f"Güncelleme Bekleyen Maç Sayısı: {bekleyen_mac}")
            
            # İşlemi çalıştır
            analyzer.process_best_bets()
            
            bitis_zaman = analyzer.get_last_updated_time()
            print(f"Bitişteki Son Güncelleme Zamanı: {bitis_zaman}")
            
            if baslangic_zaman != bitis_zaman:
                print("\n[BAŞARILI] Tablodaki 'updated_at' zamanı değişti, kayıtlar başarıyla güncellendi.")
                break
            else:
                kalan_mac = analyzer.get_pending_count()
                if kalan_mac == 0:
                    print("\n[BİLGİ] Bekleyen tüm maçlar işlendi veya zaten kalmamıştı.")
                    break
                else:
                    print(f"\n[UYARI] İşlem yapıldı ama updated_at zamanı DEĞİŞMEDİ! Halen güncellenmeyi bekleyen {kalan_mac} maç var.")
                    if deneme < max_deneme:
                        print(f"{bekleme_suresi} saniye bekleniyor ve tekrar denenecek...")
                        time.sleep(bekleme_suresi)
                    else:
                        print("\n[HATA] Maksimum deneme sayısına ulaşıldı. Tablo güncellenemedi!")

    except Exception as e:
        print(f"[HATA] Tahmin oluşturulurken bir sorun oluştu: {e}")
    finally:
        analyzer.close() 
