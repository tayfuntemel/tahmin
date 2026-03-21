#!/usr/bin/env python3
import os
import mysql.connector

# Mevcut veritabanı ayarlarımız
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

# Oran Analizlerinin kaydedileceği yeni tablonun şeması
SCHEMA_CREATE_ODDS_TABLE = """
CREATE TABLE IF NOT EXISTS odds_performance_analytics (
  id                  INT AUTO_INCREMENT PRIMARY KEY,
  filter_type         ENUM('League', 'Team') NOT NULL,
  team_name           VARCHAR(128) NULL,          -- filter_type League ise NULL kalır
  tournament_id       INT NOT NULL,
  
  market              VARCHAR(64) NOT NULL,       -- 'Match_Winner', 'Over_25' vs.
  odds_band           VARCHAR(32) NOT NULL,       -- '< 1.50', '1.50 - 1.99' vs.
  
  matches_played      INT DEFAULT 0,              -- Bu oran aralığında oynanan maç sayısı
  won_bets            INT DEFAULT 0,              -- Bu oran aralığında kazanan bahis sayısı
  win_rate_pct        FLOAT DEFAULT 0,            -- Kazanma yüzdesi (%)
  
  avg_odds            FLOAT DEFAULT 0,            -- Bu aralıktaki ortalama oran
  total_profit_units  FLOAT DEFAULT 0,            -- Her maça 1 birim (Örn: 100 TL) yatırılsaydı elde edilecek Kâr/Zarar
  yield_roi_pct       FLOAT DEFAULT 0,            -- Yatırım Getirisi (ROI) Yüzdesi
  
  last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY idx_odds_filter (filter_type, team_name, tournament_id, market, odds_band)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class OddsAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.stats = {} # Verileri toplayacağımız bellek

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_ODDS_TABLE)
        print("[ORAN ANALİZİ] Veritabanı bağlantısı başarılı ve 'odds_performance_analytics' tablosu hazır.")

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _get_odds_band(self, odds):
        """Oranın hangi aralığa (band) düştüğünü bulur."""
        if odds < 1.50: return "< 1.50"
        elif 1.50 <= odds <= 1.99: return "1.50 - 1.99"
        elif 2.00 <= odds <= 2.99: return "2.00 - 2.99"
        elif 3.00 <= odds <= 4.99: return "3.00 - 4.99"
        else: return "5.00+"

    def _init_node(self):
        return {
            "matches": 0, "won": 0, "sum_odds": 0, "profit": 0.0
        }

    def _process_market(self, f_type, team_name, tour_id, market, odds, is_won):
        if not odds: return
        
        band = self._get_odds_band(odds)
        key = (f_type, team_name, tour_id, market, band)
        
        if key not in self.stats:
            self.stats[key] = self._init_node()
            
        node = self.stats[key]
        node["matches"] += 1
        node["sum_odds"] += odds
        
        # 1 Birimlik (Örn: 100 TL) bahis varsayımı üzerinden Kâr/Zarar (Profit) hesabı
        # Eğer kaybederse yatırdığı 1 birimi kaybeder (-1).
        # Eğer kazanırsa (Oran - 1) kadar net kâr elde eder.
        if is_won:
            node["won"] += 1
            node["profit"] += (odds - 1.0)
        else:
            node["profit"] -= 1.0

    def analyze_odds(self):
        print("[ORAN ANALİZİ] Oranları ve sonuçları belli olan maçlar çekiliyor...")
        self.cur.execute("""
            SELECT * FROM results_football 
            WHERE status IN ('finished', 'ended') 
            AND ft_home IS NOT NULL AND ft_away IS NOT NULL
            AND tournament_id IS NOT NULL
        """)
        matches = self.cur.fetchall()
        print(f"[ORAN ANALİZİ] {len(matches)} maçın piyasa oranları taranıyor...")

        for match in matches:
            t_id = match['tournament_id']
            h_team = match['home_team']
            a_team = match['away_team']
            
            gf, ga = match['ft_home'], match['ft_away']
            
            home_won = (gf > ga)
            away_won = (gf < ga)
            over_25_won = ((gf + ga) > 2.5)
            
            # --- 1. LİG FİLTRESİ İÇİN KAYITLAR (Tüm lig genelindeki eğilim) ---
            self._process_market('League', None, t_id, 'Home_Win', match.get('odds_1'), home_won)
            self._process_market('League', None, t_id, 'Away_Win', match.get('odds_2'), away_won)
            self._process_market('League', None, t_id, 'Over_25', match.get('odds_o25'), over_25_won)
            
            # --- 2. TAKIM FİLTRESİ İÇİN KAYITLAR (Sadece o takımın oynadığı maçlardaki eğilimi) ---
            # Ev sahibinin galibiyet performansı
            self._process_market('Team', h_team, t_id, 'Match_Winner', match.get('odds_1'), home_won)
            self._process_market('Team', h_team, t_id, 'Over_25', match.get('odds_o25'), over_25_won)
            
            # Deplasman takımının galibiyet performansı
            self._process_market('Team', a_team, t_id, 'Match_Winner', match.get('odds_2'), away_won)
            self._process_market('Team', a_team, t_id, 'Over_25', match.get('odds_o25'), over_25_won)

        print("[ORAN ANALİZİ] Hesaplamalar tamamlandı. Kâr/Zarar (ROI) analizleri veritabanına yazılıyor...")
        
        insert_query = """
            INSERT INTO odds_performance_analytics 
            (filter_type, team_name, tournament_id, market, odds_band, 
             matches_played, won_bets, win_rate_pct, avg_odds, total_profit_units, yield_roi_pct)
            VALUES 
            (%(filter_type)s, %(team_name)s, %(tournament_id)s, %(market)s, %(odds_band)s,
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
            # ROI (Yield) Formülü: (Toplam Kâr / Toplam Yatırılan Birim) * 100
            # Her maça 1 birim yatırdığımızı varsayıyoruz, bu yüzden Toplam Yatırılan = mp
            roi = (data["profit"] / mp) * 100

            row = {
                "filter_type": f_type,
                "team_name": team_name,
                "tournament_id": tour_id,
                "market": market,
                "odds_band": band,
                
                "matches": mp,
                "won": data["won"],
                "win_rate": round(win_rate, 2),
                "avg_odds": round(avg_odds, 2),
                "profit": round(data["profit"], 2),
                "roi": round(roi, 2)
            }
            
            self.cur.execute(insert_query, row)
            count += 1

        print(f"[BAŞARILI] Toplam {count} adet Oran ve ROI analizi oluşturuldu/güncellendi.")

if __name__ == "__main__":
    analyzer = OddsAnalyzer(CONFIG["db"])
    try:
        analyzer.connect()
        analyzer.analyze_odds()
    except Exception as e:
        print(f"[HATA] Oran analizi sırasında bir sorun oluştu: {e}")
    finally:
        analyzer.close()
