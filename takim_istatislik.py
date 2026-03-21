#!/usr/bin/env python3
import os
import mysql.connector
import json
from typing import Dict, Any

# Mevcut ayarlarını kullanıyoruz
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

# Analizlerin kaydedileceği yeni tablonun şeması
SCHEMA_CREATE_ANALYTICS_TABLE = """
CREATE TABLE IF NOT EXISTS team_analytics (
  id              INT AUTO_INCREMENT PRIMARY KEY,
  team_name       VARCHAR(128) NOT NULL,
  tournament_id   INT NOT NULL,
  category_id     INT NULL,
  venue_type      ENUM('Overall', 'Home', 'Away') NOT NULL,
  
  -- Temel Maç İstatistikleri
  matches_played  INT DEFAULT 0,
  wins            INT DEFAULT 0,
  draws           INT DEFAULT 0,
  losses          INT DEFAULT 0,
  goals_for       INT DEFAULT 0,
  goals_against   INT DEFAULT 0,
  
  -- Ortalama İstatistikler (Maç Başına)
  avg_possession  FLOAT DEFAULT 0,
  avg_shots       FLOAT DEFAULT 0,
  avg_shots_on    FLOAT DEFAULT 0,
  avg_corners     FLOAT DEFAULT 0,
  avg_fouls       FLOAT DEFAULT 0,
  avg_cards       FLOAT DEFAULT 0,
  
  -- Derin Analizler (JSON Formatında)
  referee_stats   JSON NULL,
  formation_stats JSON NULL,
  odds_stats      JSON NULL,
  
  last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  
  -- Aynı takımın aynı turnuvada ve aynı saha türündeki kaydının tekilleştirilmesi
  UNIQUE KEY idx_team_tour_venue (team_name, tournament_id, venue_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class Analyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.stats = {} # Tüm analizleri bellekte toplayacağımız sözlük

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_ANALYTICS_TABLE)
        print("[ANALİZ] Veritabanı bağlantısı başarılı ve 'team_analytics' tablosu hazır.")

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _init_team_struct(self):
        """Bir takım için temel veri yapısını oluşturur."""
        return {
            "matches_played": 0, "wins": 0, "draws": 0, "losses": 0,
            "goals_for": 0, "goals_against": 0,
            "possession": 0, "shots": 0, "shots_on": 0, "corners": 0, "fouls": 0,
            "referees": {}, # Örn: {"Cüneyt Çakır": {"matches": 1, "wins": 1, ...}}
            "formations": {}, # Örn: {"4-2-3-1": {"matches": 1, "goals": 2, ...}}
            "odds_o25_ranges": {} # Örn: {"1.50-1.75": {"matches": 2, "total_match_goals": 5}}
        }

    def _get_team_node(self, team_name, tournament_id, category_id, venue_type):
        """Sözlük içinde takımın ilgili düğümünü bulur veya oluşturur."""
        key = (team_name, tournament_id, category_id, venue_type)
        if key not in self.stats:
            self.stats[key] = self._init_team_struct()
        return self.stats[key]

    def _process_match_for_team(self, team_name, is_home, match, venue_type):
        """Bir maçın verilerini takımın istatistiklerine ekler."""
        tour_id = match['tournament_id']
        cat_id = match['category_id']
        
        # Takımın bu maçtaki istatistiklerini belirle
        if is_home:
            gf, ga = match['ft_home'], match['ft_away']
            poss, shots = match['poss_h'], match['shot_h']
            shots_on, corners = match['shot_on_h'], match['corn_h']
            fouls = match['fouls_h']
            formation = match['formation_h']
        else:
            gf, ga = match['ft_away'], match['ft_home']
            poss, shots = match['poss_a'], match['shot_a']
            shots_on, corners = match['shot_on_a'], match['corn_a']
            fouls = match['fouls_a']
            formation = match['formation_a']

        node = self._get_team_node(team_name, tour_id, cat_id, venue_type)
        
        # 1. Temel İstatistikler
        node["matches_played"] += 1
        if gf is not None and ga is not None:
            node["goals_for"] += gf
            node["goals_against"] += ga
            if gf > ga: node["wins"] += 1
            elif gf == ga: node["draws"] += 1
            else: node["losses"] += 1

        # 2. Ortalama İstatistikler (Toplamı alıyoruz, kaydederken böleceğiz)
        if poss: node["possession"] += poss
        if shots: node["shots"] += shots
        if shots_on: node["shots_on"] += shots_on
        if corners: node["corners"] += corners
        if fouls: node["fouls"] += fouls

        # 3. Hakem Analizi
        referee = match.get('referee')
        if referee:
            if referee not in node["referees"]:
                node["referees"][referee] = {"matches": 0, "wins": 0, "draws": 0, "losses": 0, "gf": 0, "ga": 0}
            r_node = node["referees"][referee]
            r_node["matches"] += 1
            if gf is not None and ga is not None:
                r_node["gf"] += gf
                r_node["ga"] += ga
                if gf > ga: r_node["wins"] += 1
                elif gf == ga: r_node["draws"] += 1
                else: r_node["losses"] += 1

        # 4. Diziliş (Formation) Analizi
        if formation:
            if formation not in node["formations"]:
                node["formations"][formation] = {"matches": 0, "wins": 0, "gf": 0, "ga": 0}
            f_node = node["formations"][formation]
            f_node["matches"] += 1
            if gf is not None and ga is not None:
                f_node["gf"] += gf
                f_node["ga"] += ga
                if gf > ga: f_node["wins"] += 1

        # 5. Oran Analizi (Üst 2.5 oranına göre maçtaki toplam gol trendi)
        o25 = match.get('odds_o25')
        if o25 and gf is not None and ga is not None:
            range_key = "Bilinmiyor"
            if o25 <= 1.50: range_key = "<= 1.50 (Çok Favori Üst)"
            elif 1.50 < o25 <= 1.80: range_key = "1.51 - 1.80"
            elif 1.80 < o25 <= 2.10: range_key = "1.81 - 2.10"
            else: range_key = "> 2.10 (Sürpriz Üst)"
            
            if range_key not in node["odds_o25_ranges"]:
                node["odds_o25_ranges"][range_key] = {"matches": 0, "total_match_goals": 0}
            
            node["odds_o25_ranges"][range_key]["matches"] += 1
            node["odds_o25_ranges"][range_key]["total_match_goals"] += (gf + ga)

    def analyze_and_save(self):
        print("[ANALİZ] Maçlar veritabanından çekiliyor...")
        # Sadece bitmiş ve gerekli temel verilere sahip maçları alıyoruz
        self.cur.execute("""
            SELECT * FROM results_football 
            WHERE status IN ('finished', 'ended') 
            AND home_team IS NOT NULL 
            AND away_team IS NOT NULL
            AND tournament_id IS NOT NULL
        """)
        matches = self.cur.fetchall()
        print(f"[ANALİZ] Toplam {len(matches)} maç inceleniyor...")

        # Bellekte hesaplamaları yap
        for match in matches:
            home_team = match['home_team']
            away_team = match['away_team']
            
            # Ev sahibi takım için (Home ve Overall)
            self._process_match_for_team(home_team, True, match, "Home")
            self._process_match_for_team(home_team, True, match, "Overall")
            
            # Deplasman takımı için (Away ve Overall)
            self._process_match_for_team(away_team, False, match, "Away")
            self._process_match_for_team(away_team, False, match, "Overall")

        print("[ANALİZ] Hesaplamalar tamamlandı. Veritabanına kaydediliyor...")
        
        # Veritabanına kaydet
        insert_query = """
            INSERT INTO team_analytics 
            (team_name, tournament_id, category_id, venue_type, matches_played, wins, draws, losses, goals_for, goals_against,
             avg_possession, avg_shots, avg_shots_on, avg_corners, avg_fouls,
             referee_stats, formation_stats, odds_stats)
            VALUES 
            (%(team_name)s, %(tournament_id)s, %(category_id)s, %(venue_type)s, %(matches_played)s, %(wins)s, %(draws)s, %(losses)s, %(goals_for)s, %(goals_against)s,
             %(avg_possession)s, %(avg_shots)s, %(avg_shots_on)s, %(avg_corners)s, %(avg_fouls)s,
             %(referee_stats)s, %(formation_stats)s, %(odds_stats)s)
            ON DUPLICATE KEY UPDATE
            matches_played=VALUES(matches_played), wins=VALUES(wins), draws=VALUES(draws), losses=VALUES(losses),
            goals_for=VALUES(goals_for), goals_against=VALUES(goals_against),
            avg_possession=VALUES(avg_possession), avg_shots=VALUES(avg_shots), avg_shots_on=VALUES(avg_shots_on),
            avg_corners=VALUES(avg_corners), avg_fouls=VALUES(avg_fouls),
            referee_stats=VALUES(referee_stats), formation_stats=VALUES(formation_stats), odds_stats=VALUES(odds_stats)
        """

        count = 0
        for key, data in self.stats.items():
            team_name, tour_id, cat_id, venue_type = key
            mp = data["matches_played"]
            if mp == 0: continue

            # Ortalamaları hesapla
            row = {
                "team_name": team_name,
                "tournament_id": tour_id,
                "category_id": cat_id,
                "venue_type": venue_type,
                "matches_played": mp,
                "wins": data["wins"],
                "draws": data["draws"],
                "losses": data["losses"],
                "goals_for": data["goals_for"],
                "goals_against": data["goals_against"],
                
                "avg_possession": round(data["possession"] / mp, 2),
                "avg_shots": round(data["shots"] / mp, 2),
                "avg_shots_on": round(data["shots_on"] / mp, 2),
                "avg_corners": round(data["corners"] / mp, 2),
                "avg_fouls": round(data["fouls"] / mp, 2),
                
                # Karmaşık verileri JSON formatına dönüştür
                "referee_stats": json.dumps(data["referees"], ensure_ascii=False),
                "formation_stats": json.dumps(data["formations"], ensure_ascii=False),
                "odds_stats": json.dumps(data["odds_o25_ranges"], ensure_ascii=False)
            }
            
            self.cur.execute(insert_query, row)
            count += 1

        print(f"[BAŞARILI] Toplam {count} takım analizi satırı oluşturuldu/güncellendi.")

if __name__ == "__main__":
    analyzer = Analyzer(CONFIG["db"])
    try:
        analyzer.connect()
        analyzer.analyze_and_save()
    except Exception as e:
        print(f"[HATA] Analiz sırasında bir sorun oluştu: {e}")
    finally:
        analyzer.close()
