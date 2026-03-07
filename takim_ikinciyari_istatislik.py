#!/usr/bin/env python3
import mysql.connector

# Mevcut veritabanı ayarlarımız
CONFIG = {
    "db": {
        "host": "netscout.fun",
        "user": "netscout_veri",
        "password": "i.34temel1",
        "database": "netscout_veri",
        "port": 3306
    }
}

# İkinci Yarı (Second-Half) analizlerinin kaydedileceği yeni tablonun şeması
SCHEMA_CREATE_SH_TABLE = """
CREATE TABLE IF NOT EXISTS team_second_half_analytics (
  id                  INT AUTO_INCREMENT PRIMARY KEY,
  team_name           VARCHAR(128) NOT NULL,
  tournament_id       INT NOT NULL,
  venue_type          ENUM('Overall', 'Home', 'Away') NOT NULL,
  
  matches_played      INT DEFAULT 0,
  
  -- Sadece İkinci Yarı Skoruna Göre Sonuçlar
  sh_wins             INT DEFAULT 0,
  sh_draws            INT DEFAULT 0,
  sh_losses           INT DEFAULT 0,
  
  -- İkinci Yarı Gol İstatistikleri
  sh_goals_for        INT DEFAULT 0,
  sh_goals_against    INT DEFAULT 0,
  sh_avg_goals_for    FLOAT DEFAULT 0,
  sh_avg_goals_against FLOAT DEFAULT 0,
  
  -- İkinci Yarı Yüzdelik Gol Oranları (%)
  sh_over_05_pct      FLOAT DEFAULT 0,
  sh_over_15_pct      FLOAT DEFAULT 0,
  sh_btts_yes_pct     FLOAT DEFAULT 0,
  
  last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY idx_team_tour_venue_sh (team_name, tournament_id, venue_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class SecondHalfAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.stats = {} # Analizleri bellekte tutacağımız sözlük

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_SH_TABLE)
        print("[İKİNCİ YARI ANALİZİ] Veritabanı bağlantısı başarılı ve 'team_second_half_analytics' tablosu hazır.")

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _init_team_struct(self):
        """Bir takım için temel İkinci Yarı veri yapısını oluşturur."""
        return {
            "matches": 0, "sh_wins": 0, "sh_draws": 0, "sh_losses": 0,
            "sh_goals_for": 0, "sh_goals_against": 0,
            "sh_over_05": 0, "sh_over_15": 0, "sh_btts": 0
        }

    def _get_team_node(self, team_name, tournament_id, venue_type):
        key = (team_name, tournament_id, venue_type)
        if key not in self.stats:
            self.stats[key] = self._init_team_struct()
        return self.stats[key]

    def _process_match(self, team_name, is_home, match, venue_type):
        tour_id = match['tournament_id']
        node = self._get_team_node(team_name, tour_id, venue_type)
        
        # Matematiksel işlemimiz: Maç Sonu - İlk Yarı = İkinci Yarı Skoru
        if is_home:
            sh_gf = match['ft_home'] - match['ht_home']
            sh_ga = match['ft_away'] - match['ht_away']
        else:
            sh_gf = match['ft_away'] - match['ht_away']
            sh_ga = match['ft_home'] - match['ht_home']
            
        node["matches"] += 1
        node["sh_goals_for"] += sh_gf
        node["sh_goals_against"] += sh_ga
        
        # Sadece 2. yarıdaki gollere göre galibiyet, beraberlik, mağlubiyet durumu
        if sh_gf > sh_ga:
            node["sh_wins"] += 1
        elif sh_gf == sh_ga:
            node["sh_draws"] += 1
        else:
            node["sh_losses"] += 1

        # İkinci Yarı Gol Metrikleri (Örn: Sadece ikinci yarıda 2 gol oldu mu?)
        total_sh_goals = sh_gf + sh_ga
        if total_sh_goals > 0.5: node["sh_over_05"] += 1
        if total_sh_goals > 1.5: node["sh_over_15"] += 1
        
        # İkinci yarıda iki takım da gol attı mı?
        if sh_gf > 0 and sh_ga > 0: node["sh_btts"] += 1

    def analyze_second_half(self):
        print("[İKİNCİ YARI ANALİZİ] Maçlar veritabanından çekiliyor...")
        # Sadece skorları eksiksiz olan maçları alıyoruz
        self.cur.execute("""
            SELECT * FROM results_football 
            WHERE status IN ('finished', 'ended') 
            AND ht_home IS NOT NULL AND ht_away IS NOT NULL
            AND ft_home IS NOT NULL AND ft_away IS NOT NULL
        """)
        matches = self.cur.fetchall()
        print(f"[İKİNCİ YARI ANALİZİ] Toplam {len(matches)} maç inceleniyor...")

        for match in matches:
            home_team = match['home_team']
            away_team = match['away_team']
            
            # Ev sahibi hesaplamaları
            self._process_match(home_team, True, match, "Home")
            self._process_match(home_team, True, match, "Overall")
            
            # Deplasman hesaplamaları
            self._process_match(away_team, False, match, "Away")
            self._process_match(away_team, False, match, "Overall")

        print("[İKİNCİ YARI ANALİZİ] Hesaplamalar tamamlandı. Veritabanına kaydediliyor...")
        
        insert_query = """
            INSERT INTO team_second_half_analytics 
            (team_name, tournament_id, venue_type, matches_played, 
             sh_wins, sh_draws, sh_losses, sh_goals_for, sh_goals_against, 
             sh_avg_goals_for, sh_avg_goals_against, 
             sh_over_05_pct, sh_over_15_pct, sh_btts_yes_pct)
            VALUES 
            (%(team_name)s, %(tournament_id)s, %(venue_type)s, %(matches_played)s,
             %(sh_wins)s, %(sh_draws)s, %(sh_losses)s, %(sh_goals_for)s, %(sh_goals_against)s,
             %(sh_avg_goals_for)s, %(sh_avg_goals_against)s,
             %(sh_over_05_pct)s, %(sh_over_15_pct)s, %(sh_btts_yes_pct)s)
            ON DUPLICATE KEY UPDATE
             matches_played=VALUES(matches_played),
             sh_wins=VALUES(sh_wins), sh_draws=VALUES(sh_draws), sh_losses=VALUES(sh_losses),
             sh_goals_for=VALUES(sh_goals_for), sh_goals_against=VALUES(sh_goals_against),
             sh_avg_goals_for=VALUES(sh_avg_goals_for), sh_avg_goals_against=VALUES(sh_avg_goals_against),
             sh_over_05_pct=VALUES(sh_over_05_pct), sh_over_15_pct=VALUES(sh_over_15_pct), sh_btts_yes_pct=VALUES(sh_btts_yes_pct)
        """

        count = 0
        for key, data in self.stats.items():
            team_name, tour_id, venue_type = key
            mp = data["matches"]
            if mp == 0: continue

            row = {
                "team_name": team_name,
                "tournament_id": tour_id,
                "venue_type": venue_type,
                "matches_played": mp,
                
                "sh_wins": data["sh_wins"],
                "sh_draws": data["sh_draws"],
                "sh_losses": data["sh_losses"],
                "sh_goals_for": data["sh_goals_for"],
                "sh_goals_against": data["sh_goals_against"],
                
                "sh_avg_goals_for": round(data["sh_goals_for"] / mp, 2),
                "sh_avg_goals_against": round(data["sh_goals_against"] / mp, 2),
                
                "sh_over_05_pct": round((data["sh_over_05"] / mp) * 100, 2),
                "sh_over_15_pct": round((data["sh_over_15"] / mp) * 100, 2),
                "sh_btts_yes_pct": round((data["sh_btts"] / mp) * 100, 2),
            }
            
            self.cur.execute(insert_query, row)
            count += 1

        print(f"[BAŞARILI] Toplam {count} takımın İKİNCİ YARI analizi oluşturuldu/güncellendi.")

if __name__ == "__main__":
    analyzer = SecondHalfAnalyzer(CONFIG["db"])
    try:
        analyzer.connect()
        analyzer.analyze_second_half()
    except Exception as e:
        print(f"[HATA] İkinci yarı analizi sırasında bir sorun oluştu: {e}")
    finally:
        analyzer.close()