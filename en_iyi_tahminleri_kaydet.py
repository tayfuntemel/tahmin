#!/usr/bin/env python3
import mysql.connector
import time
import os

# --- VERİTABANI BAĞLANTISI ---
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
        """Veritabanına bağlanır ve gerekli sütunların varlığını kontrol eder."""
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self._ensure_columns_exist()
        print("[EN İYİ TAHMİN] Veritabanı bağlantısı başarılı.")

    def _ensure_columns_exist(self):
        """Tahminlerin yazılacağı sütunların tabloda olduğundan emin olur."""
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
                if err.errno != 1060: # 1060 hatası "Duplicate column name" yani sütun zaten var demektir.
                    print(f"[HATA] Sütun eklenirken hata: {err}")

    def get_last_updated_time(self):
        """Tablodaki en güncel updated_at zamanını döndürür."""
        self.cur.execute("SELECT MAX(updated_at) as last_time FROM match_predictions")
        row = self.cur.fetchone()
        return row['last_time'] if row and row['last_time'] else None

    def get_pending_count(self):
        """Henüz en iyi tahmini atanmamış maç sayısını döndürür."""
        self.cur.execute("SELECT COUNT(*) as count FROM match_predictions WHERE best_market_raw IS NULL")
        row = self.cur.fetchone()
        return row['count'] if row else 0

    def close(self):
        """Veritabanı bağlantısını güvenli bir şekilde kapatır."""
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    # =========================================================
    # V3 FİNAL - KUSURSUZ KARAR MEKANİZMASI (GENİŞLETİLMİŞ)
    # =========================================================
    def calculate_confidence_score(self, market, prob, odds, value, exp_home, exp_away):
        """Her bir bahis marketi için güven skoru hesaplar."""
        # 1. ZEHİRLİ PAZAR: BTTS KESİN YASAK!
        if market == 'BTTS':
            return -999

        score = prob
        total_xg = exp_home + exp_away
        xg_diff = exp_home - exp_away # Pozitifse Ev Sahibi, Negatifse Deplasman üstün

        # 2. Value (Değer) Tuzağı Kontrolü
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

        # 4. xG Uyumu (Marketlere Göre Özelleştirilmiş Kurallar)
        # --- ALT / ÜST MARKETLERİ ---
        if market == 'O15': # 1.5 Üst
            if total_xg >= 2.8:
                score += 15
            else:
                return -999
                
        elif market == 'U35': # 3.5 Alt
            if total_xg <= 2.2:
                score += 15
            elif total_xg > 2.8:
                score -= 20
                
        elif market == 'O25': # 2.5 Üst (YENİ)
            if total_xg >= 3.0:
                score += 20
            elif total_xg < 2.5:
                return -999 # Toplam beklenen gol 2.5'ten azsa oynama
                
        elif market == 'U25': # 2.5 Alt (YENİ)
            if total_xg <= 2.5:
                score += 20
            elif total_xg > 2.8:
                return -999 # Beklenen gol yüksekse risk alma

        # --- TARAF BAHİSLERİ (ÇİFTE ŞANS) ---
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

        # --- TARAF BAHİSLERİ (MAÇ SONUCU - YENİ) ---
        elif market == '1': # Maç Sonucu 1
            if xg_diff > 0.8: # Ev sahibinin bariz xG üstünlüğü olmalı
                score += 20
            elif xg_diff < 0.2:
                return -999 # Ev sahibi üstün değilse taraf bahsi risklidir
                
        elif market == 'X': # Maç Sonucu 0 (Beraberlik)
            if -0.3 <= xg_diff <= 0.3: # İki takım xG olarak başa baş ise
                score += 15
            else:
                score -= 20
                
        elif market == '2': # Maç Sonucu 2
            if xg_diff < -0.8: # Deplasmanın bariz xG üstünlüğü olmalı
                score += 20
            elif xg_diff > -0.2:
                return -999 # Deplasman üstün değilse uzak dur

        return score

    def process_best_bets(self):
        print("[EN İYİ TAHMİN] Henüz tahmini oluşturulmamış yeni maçlar aranıyor...")
        
        # SQL Sorgusu yeni oran sütunlarını içerecek şekilde genişletildi.
        query = """
            SELECT MP.*, 
                   R.odds_1x, R.odds_x2, R.odds_o15, R.odds_u35, R.odds_btts_yes,
                   R.odds_1, R.odds_x, R.odds_2, R.odds_o25, R.odds_u25
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

            # Tüm marketler (Eski + Yeni) sözlüğe eklendi
            # NOT: Veritabanındaki tahmin sütun adlarının 'prob_1', 'value_o25' vb. olduğu varsayılmıştır.
            markets = {
                '1X':   {'prob': row.get('prob_1x'),   'value': row.get('value_1x'),   'odds': row.get('odds_1x'), 'name': '1x çifte şans'},
                'X2':   {'prob': row.get('prob_x2'),   'value': row.get('value_x2'),   'odds': row.get('odds_x2'), 'name': 'x2 çifte şans'},
                'O15':  {'prob': row.get('prob_o15'),  'value': row.get('value_o15'),  'odds': row.get('odds_o15'), 'name': '1.5 üst'},
                'U35':  {'prob': row.get('prob_u35'),  'value': row.get('value_u35'),  'odds': row.get('odds_u35'), 'name': '3.5 alt'},
                'BTTS': {'prob': row.get('prob_btts'), 'value': row.get('value_btts'), 'odds': row.get('odds_btts_yes'), 'name': 'kg var'},
                
                # Yeni Eklenen Marketler
                '1':    {'prob': row.get('prob_1'),    'value': row.get('value_1'),    'odds': row.get('odds_1'), 'name': 'maç sonucu 1'},
                'X':    {'prob': row.get('prob_x'),    'value': row.get('value_x'),    'odds': row.get('odds_x'), 'name': 'maç sonucu 0'},
                '2':    {'prob': row.get('prob_2'),    'value': row.get('value_2'),    'odds': row.get('odds_2'), 'name': 'maç sonucu 2'},
                'O25':  {'prob': row.get('prob_o25'),  'value': row.get('value_o25'),  'odds': row.get('odds_o25'), 'name': '2.5 üst'},
                'U25':  {'prob': row.get('prob_u25'),  'value': row.get('value_u25'),  'odds': row.get('odds_u25'), 'name': '2.5 alt'}
            }

            best_bet = None
            highest_score = 75.0 # Mükemmel barajımız

            for key, data in markets.items():
                try:
                    # Eğer veri veritabanında yoksa (None dönerse) atla
                    if data['prob'] is None or data['odds'] is None or data['value'] is None:
                        continue
                        
                    prob = float(data['prob'])
                    odds = float(data['odds'])
                    value = float(data['value'])
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
