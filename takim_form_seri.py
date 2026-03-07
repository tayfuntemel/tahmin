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

# Form ve Seri analizlerinin kaydedileceği tablonun şeması
SCHEMA_CREATE_FORM_TABLE = """
CREATE TABLE IF NOT EXISTS team_form_analytics (
  id                  INT AUTO_INCREMENT PRIMARY KEY,
  team_name           VARCHAR(128) NOT NULL,
  tournament_id       INT NOT NULL,
  venue_type          ENUM('Overall', 'Home', 'Away') NOT NULL,
  
  -- Son 5 Maç Formu (Örn: 'G,B,G,M,G') ve Toplanan Puan (Maks 15)
  form_last_5         VARCHAR(32) DEFAULT '',
  points_last_5       INT DEFAULT 0,
  
  -- Aktif Sonuç Serileri (Şu an kaç maçtır devam ediyor?)
  current_win_streak        INT DEFAULT 0,
  current_unbeaten_streak   INT DEFAULT 0,
  current_losing_streak     INT DEFAULT 0,
  current_no_win_streak     INT DEFAULT 0, -- Kazanamama serisi
  
  -- Aktif Gol Serileri
  current_clean_sheet_streak INT DEFAULT 0, -- Gol yememe serisi
  current_scoring_streak     INT DEFAULT 0, -- Gol atma serisi
  current_over_25_streak     INT DEFAULT 0, -- 2.5 Üst bitme serisi
  
  last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY idx_team_tour_venue_form (team_name, tournament_id, venue_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class FormAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.stats = {} # Takımların zaman tünelini tutacağımız sözlük

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_FORM_TABLE)
        print("[FORM ANALİZİ] Veritabanı bağlantısı başarılı ve 'team_form_analytics' tablosu hazır.")

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _init_team_struct(self):
        """Bir takımın seri takip yapısını oluşturur."""
        return {
            "form_queue": [], # Son 5 maçı tutacak liste ['G', 'B', 'M', ...]
            "win_streak": 0,
            "unbeaten_streak": 0,
            "losing_streak": 0,
            "no_win_streak": 0,
            "clean_sheet_streak": 0,
            "scoring_streak": 0,
            "over_25_streak": 0
        }

    def _get_team_node(self, team_name, tournament_id, venue_type):
        key = (team_name, tournament_id, venue_type)
        if key not in self.stats:
            self.stats[key] = self._init_team_struct()
        return self.stats[key]

    def _process_match(self, team_name, is_home, match, venue_type):
        tour_id = match['tournament_id']
        node = self._get_team_node(team_name, tour_id, venue_type)
        
        # Takımın attığı ve yediği golleri belirle
        if is_home:
            gf, ga = match['ft_home'], match['ft_away']
        else:
            gf, ga = match['ft_away'], match['ft_home']
            
        # Maç sonucunu (G/B/M) harf olarak belirle
        result_char = 'B' # Beraberlik
        if gf > ga: result_char = 'G' # Galibiyet
        elif gf < ga: result_char = 'M' # Mağlubiyet

        # 1. Form Kuyruğunu Güncelle (Sadece son 5 maçı tutarız)
        node["form_queue"].append(result_char)
        if len(node["form_queue"]) > 5:
            node["form_queue"].pop(0) # En eski maçı listeden at
            
        # 2. Sonuç Serilerini Güncelle (Streak Logic)
        if result_char == 'G':
            node["win_streak"] += 1
            node["unbeaten_streak"] += 1
            node["losing_streak"] = 0
            node["no_win_streak"] = 0
        elif result_char == 'B':
            node["win_streak"] = 0
            node["unbeaten_streak"] += 1
            node["losing_streak"] = 0
            node["no_win_streak"] += 1
        elif result_char == 'M':
            node["win_streak"] = 0
            node["unbeaten_streak"] = 0
            node["losing_streak"] += 1
            node["no_win_streak"] += 1

        # 3. Gol Serilerini Güncelle
        # Gol yememe (Clean Sheet)
        if ga == 0: node["clean_sheet_streak"] += 1
        else: node["clean_sheet_streak"] = 0
        
        # Gol atma
        if gf > 0: node["scoring_streak"] += 1
        else: node["scoring_streak"] = 0
        
        # 2.5 Üst
        if (gf + ga) > 2.5: node["over_25_streak"] += 1
        else: node["over_25_streak"] = 0

    def analyze_form(self):
        print("[FORM ANALİZİ] Maçlar KRONOLOJİK sırayla (eskiden yeniye) çekiliyor...")
        # ORDER BY ile maçları oynanma sırasına göre alıyoruz ki seriler doğru hesaplansın.
        self.cur.execute("""
            SELECT * FROM results_football 
            WHERE status IN ('finished', 'ended') 
            AND ft_home IS NOT NULL AND ft_away IS NOT NULL
            ORDER BY start_utc ASC, start_time_utc ASC
        """)
        matches = self.cur.fetchall()
        print(f"[FORM ANALİZİ] {len(matches)} maç zaman tünelinden geçiriliyor...")

        for match in matches:
            home_team = match.get('home_team')
            away_team = match.get('away_team')
            
            if home_team and away_team:
                # Ev sahibi hesaplamaları
                self._process_match(home_team, True, match, "Home")
                self._process_match(home_team, True, match, "Overall")
                
                # Deplasman hesaplamaları
                self._process_match(away_team, False, match, "Away")
                self._process_match(away_team, False, match, "Overall")

        print("[FORM ANALİZİ] Zaman tüneli simülasyonu bitti. Güncel aktif seriler veritabanına kaydediliyor...")
        
        insert_query = """
            INSERT INTO team_form_analytics 
            (team_name, tournament_id, venue_type, form_last_5, points_last_5, 
             current_win_streak, current_unbeaten_streak, current_losing_streak, current_no_win_streak,
             current_clean_sheet_streak, current_scoring_streak, current_over_25_streak)
            VALUES 
            (%(team_name)s, %(tournament_id)s, %(venue_type)s, %(form_last_5)s, %(points_last_5)s,
             %(win_streak)s, %(unbeaten_streak)s, %(losing_streak)s, %(no_win_streak)s,
             %(clean_sheet_streak)s, %(scoring_streak)s, %(over_25_streak)s)
            ON DUPLICATE KEY UPDATE
             form_last_5=VALUES(form_last_5), points_last_5=VALUES(points_last_5),
             current_win_streak=VALUES(current_win_streak), current_unbeaten_streak=VALUES(current_unbeaten_streak),
             current_losing_streak=VALUES(current_losing_streak), current_no_win_streak=VALUES(current_no_win_streak),
             current_clean_sheet_streak=VALUES(current_clean_sheet_streak), current_scoring_streak=VALUES(current_scoring_streak),
             current_over_25_streak=VALUES(current_over_25_streak)
        """

        count = 0
        for key, data in self.stats.items():
            team_name, tour_id, venue_type = key
            
            # Form listesinden virgüllü metin ve puan hesabı yapıyoruz
            form_str = ",".join(data["form_queue"])
            pts = 0
            for char in data["form_queue"]:
                if char == 'G': pts += 3
                elif char == 'B': pts += 1
                
            row = {
                "team_name": team_name,
                "tournament_id": tour_id,
                "venue_type": venue_type,
                
                "form_last_5": form_str,
                "points_last_5": pts,
                
                "win_streak": data["win_streak"],
                "unbeaten_streak": data["unbeaten_streak"],
                "losing_streak": data["losing_streak"],
                "no_win_streak": data["no_win_streak"],
                
                "clean_sheet_streak": data["clean_sheet_streak"],
                "scoring_streak": data["scoring_streak"],
                "over_25_streak": data["over_25_streak"]
            }
            
            self.cur.execute(insert_query, row)
            count += 1

        print(f"[BAŞARILI] Toplam {count} takımın GÜNCEL FORM VE SERİ durumu oluşturuldu/güncellendi.")

if __name__ == "__main__":
    analyzer = FormAnalyzer(CONFIG["db"])
    try:
        analyzer.connect()
        analyzer.analyze_form()
    except Exception as e:
        print(f"[HATA] Form analizi sırasında bir sorun oluştu: {e}")
    finally:
        analyzer.close()