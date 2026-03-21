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

# İlk Yarı (Half-Time) analizlerinin kaydedileceği yeni tablonun şeması
SCHEMA_CREATE_HT_TABLE = """
CREATE TABLE IF NOT EXISTS team_half_time_analytics (
  id                  INT AUTO_INCREMENT PRIMARY KEY,
  team_name           VARCHAR(128) NOT NULL,
  tournament_id       INT NOT NULL,
  venue_type          ENUM('Overall', 'Home', 'Away') NOT NULL,
  
  matches_played      INT DEFAULT 0,
  
  -- İlk Yarı Sonuçları
  ht_wins             INT DEFAULT 0,
  ht_draws            INT DEFAULT 0,
  ht_losses           INT DEFAULT 0,
  
  -- İlk Yarı Gol İstatistikleri
  ht_goals_for        INT DEFAULT 0,
  ht_goals_against    INT DEFAULT 0,
  ht_avg_goals_for    FLOAT DEFAULT 0,
  ht_avg_goals_against FLOAT DEFAULT 0,
  
  -- İlk Yarı Yüzdelik Gol Oranları (%)
  ht_over_05_pct      FLOAT DEFAULT 0,
  ht_over_15_pct      FLOAT DEFAULT 0,
  ht_btts_yes_pct     FLOAT DEFAULT 0,
  
  -- İY / MS (HT/FT) Dönüşüm Analizleri
  ht_win_ft_win       INT DEFAULT 0,  -- İlk yarı önde, maçı kazandı (1/1 veya 2/2)
  ht_win_ft_not_win   INT DEFAULT 0,  -- İlk yarı önde ama maçı kazanamadı (Puan kaybı)
  ht_lose_ft_win      INT DEFAULT 0,  -- İlk yarı geride, maçı kazandı (Geri dönüş / Comeback)
  ht_lose_ft_draw     INT DEFAULT 0,  -- İlk yarı geride, maçı berabere bitirdi
  
  last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY idx_team_tour_venue (team_name, tournament_id, venue_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class HalfTimeAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.stats = {} # Analizleri bellekte tutacağımız sözlük

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_HT_TABLE)
        print("[İLK YARI ANALİZİ] Veritabanı bağlantısı başarılı ve 'team_half_time_analytics' tablosu hazır.")

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _init_team_struct(self):
        """Bir takım için temel İlk Yarı veri yapısını oluşturur."""
        return {
            "matches": 0, "ht_wins": 0, "ht_draws": 0, "ht_losses": 0,
            "ht_goals_for": 0, "ht_goals_against": 0,
            "ht_over_05": 0, "ht_over_15": 0, "ht_btts": 0,
            "ht_win_ft_win": 0, "ht_win_ft_not_win": 0,
            "ht_lose_ft_win": 0, "ht_lose_ft_draw": 0
        }

    def _get_team_node(self, team_name, tournament_id, venue_type):
        key = (team_name, tournament_id, venue_type)
        if key not in self.stats:
            self.stats[key] = self._init_team_struct()
        return self.stats[key]

    def _process_match(self, team_name, is_home, match, venue_type):
        tour_id = match['tournament_id']
        node = self._get_team_node(team_name, tour_id, venue_type)
        
        # İlk yarı ve maç sonu skorlarını belirle
        if is_home:
            ht_gf, ht_ga = match['ht_home'], match['ht_away']
            ft_gf, ft_ga = match['ft_home'], match['ft_away']
        else:
            ht_gf, ht_ga = match['ht_away'], match['ht_home']
            ft_gf, ft_ga = match['ft_away'], match['ft_home']
            
        node["matches"] += 1
        node["ht_goals_for"] += ht_gf
        node["ht_goals_against"] += ht_ga
        
        # İlk Yarı Sonuçları (1X2)
        if ht_gf > ht_ga:
            node["ht_wins"] += 1
            # İY Önde kapatan takım MS ne yaptı?
            if ft_gf > ft_ga:
                node["ht_win_ft_win"] += 1
            else:
                node["ht_win_ft_not_win"] += 1
                
        elif ht_gf == ht_ga:
            node["ht_draws"] += 1
            
        else:
            node["ht_losses"] += 1
            # İY Geride kapatan takım MS ne yaptı?
            if ft_gf > ft_ga:
                node["ht_lose_ft_win"] += 1
            elif ft_gf == ft_ga:
                node["ht_lose_ft_draw"] += 1

        # İlk Yarı Gol Metrikleri
        total_ht_goals = ht_gf + ht_ga
        if total_ht_goals > 0.5: node["ht_over_05"] += 1
        if total_ht_goals > 1.5: node["ht_over_15"] += 1
        if ht_gf > 0 and ht_ga > 0: node["ht_btts"] += 1

    def analyze_half_time(self):
        print("[İLK YARI ANALİZİ] Maçlar veritabanından çekiliyor...")
        # Sadece ilk yarı ve maç sonu skoru belli olan bitmiş maçları alıyoruz
        self.cur.execute("""
            SELECT * FROM results_football 
            WHERE status IN ('finished', 'ended') 
            AND ht_home IS NOT NULL AND ht_away IS NOT NULL
            AND ft_home IS NOT NULL AND ft_away IS NOT NULL
        """)
        matches = self.cur.fetchall()
        print(f"[İLK YARI ANALİZİ] Toplam {len(matches)} maç inceleniyor...")

        for match in matches:
            home_team = match['home_team']
            away_team = match['away_team']
            
            # Ev sahibi hesaplamaları
            self._process_match(home_team, True, match, "Home")
            self._process_match(home_team, True, match, "Overall")
            
            # Deplasman hesaplamaları
            self._process_match(away_team, False, match, "Away")
            self._process_match(away_team, False, match, "Overall")

        print("[İLK YARI ANALİZİ] Hesaplamalar tamamlandı. Veritabanına kaydediliyor...")
        
        insert_query = """
            INSERT INTO team_half_time_analytics 
            (team_name, tournament_id, venue_type, matches_played, 
             ht_wins, ht_draws, ht_losses, ht_goals_for, ht_goals_against, 
             ht_avg_goals_for, ht_avg_goals_against, 
             ht_over_05_pct, ht_over_15_pct, ht_btts_yes_pct, 
             ht_win_ft_win, ht_win_ft_not_win, ht_lose_ft_win, ht_lose_ft_draw)
            VALUES 
            (%(team_name)s, %(tournament_id)s, %(venue_type)s, %(matches_played)s,
             %(ht_wins)s, %(ht_draws)s, %(ht_losses)s, %(ht_goals_for)s, %(ht_goals_against)s,
             %(ht_avg_goals_for)s, %(ht_avg_goals_against)s,
             %(ht_over_05_pct)s, %(ht_over_15_pct)s, %(ht_btts_yes_pct)s,
             %(ht_win_ft_win)s, %(ht_win_ft_not_win)s, %(ht_lose_ft_win)s, %(ht_lose_ft_draw)s)
            ON DUPLICATE KEY UPDATE
             matches_played=VALUES(matches_played),
             ht_wins=VALUES(ht_wins), ht_draws=VALUES(ht_draws), ht_losses=VALUES(ht_losses),
             ht_goals_for=VALUES(ht_goals_for), ht_goals_against=VALUES(ht_goals_against),
             ht_avg_goals_for=VALUES(ht_avg_goals_for), ht_avg_goals_against=VALUES(ht_avg_goals_against),
             ht_over_05_pct=VALUES(ht_over_05_pct), ht_over_15_pct=VALUES(ht_over_15_pct), ht_btts_yes_pct=VALUES(ht_btts_yes_pct),
             ht_win_ft_win=VALUES(ht_win_ft_win), ht_win_ft_not_win=VALUES(ht_win_ft_not_win), 
             ht_lose_ft_win=VALUES(ht_lose_ft_win), ht_lose_ft_draw=VALUES(ht_lose_ft_draw)
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
                
                "ht_wins": data["ht_wins"],
                "ht_draws": data["ht_draws"],
                "ht_losses": data["ht_losses"],
                "ht_goals_for": data["ht_goals_for"],
                "ht_goals_against": data["ht_goals_against"],
                
                "ht_avg_goals_for": round(data["ht_goals_for"] / mp, 2),
                "ht_avg_goals_against": round(data["ht_goals_against"] / mp, 2),
                
                "ht_over_05_pct": round((data["ht_over_05"] / mp) * 100, 2),
                "ht_over_15_pct": round((data["ht_over_15"] / mp) * 100, 2),
                "ht_btts_yes_pct": round((data["ht_btts"] / mp) * 100, 2),
                
                "ht_win_ft_win": data["ht_win_ft_win"],
                "ht_win_ft_not_win": data["ht_win_ft_not_win"],
                "ht_lose_ft_win": data["ht_lose_ft_win"],
                "ht_lose_ft_draw": data["ht_lose_ft_draw"]
            }
            
            self.cur.execute(insert_query, row)
            count += 1

        print(f"[BAŞARILI] Toplam {count} takımın İLK YARI analizi oluşturuldu/güncellendi.")

if __name__ == "__main__":
    analyzer = HalfTimeAnalyzer(CONFIG["db"])
    try:
        analyzer.connect()
        analyzer.analyze_half_time()
    except Exception as e:
        print(f"[HATA] İlk yarı analizi sırasında bir sorun oluştu: {e}")
    finally:
        analyzer.close()
