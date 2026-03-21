#!/usr/bin/env python3
import os
import mysql.connector
from typing import Dict, Any

# Veritabanı yapılandırması (Mevcut ayarların)
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

# Lig analizlerinin kaydedileceği yeni tablonun şeması
SCHEMA_CREATE_LEAGUE_ANALYTICS = """
CREATE TABLE IF NOT EXISTS league_analytics (
  tournament_id     INT PRIMARY KEY,
  tournament_name   VARCHAR(128) NULL,
  category_name     VARCHAR(128) NULL,
  country           VARCHAR(64) NULL,
  
  -- Temel Maç İstatistikleri
  total_matches     INT DEFAULT 0,
  home_wins         INT DEFAULT 0,
  draws             INT DEFAULT 0,
  away_wins         INT DEFAULT 0,
  
  -- Yüzdelik Oranlar (%)
  home_win_pct      FLOAT DEFAULT 0,
  draw_pct          FLOAT DEFAULT 0,
  away_win_pct      FLOAT DEFAULT 0,
  over_25_pct       FLOAT DEFAULT 0,
  under_25_pct      FLOAT DEFAULT 0,
  btts_yes_pct      FLOAT DEFAULT 0,
  
  -- Gol Ortalamaları
  avg_goals_match   FLOAT DEFAULT 0,
  avg_goals_home    FLOAT DEFAULT 0,
  avg_goals_away    FLOAT DEFAULT 0,
  
  -- Ortalama Bahis Oranları
  avg_odds_1        FLOAT DEFAULT 0,
  avg_odds_x        FLOAT DEFAULT 0,
  avg_odds_2        FLOAT DEFAULT 0,
  avg_odds_o25      FLOAT DEFAULT 0,
  avg_odds_u25      FLOAT DEFAULT 0,
  avg_odds_btts_yes FLOAT DEFAULT 0,
  
  last_updated      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class LeagueAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.leagues = {} # Lig verilerini toplayacağımız sözlük

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_LEAGUE_ANALYTICS)
        print("[LİG ANALİZİ] Veritabanı bağlantısı başarılı ve 'league_analytics' tablosu hazır.")

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _init_league_struct(self, match):
        """Bir lig için temel veri yapısını oluşturur."""
        return {
            "tournament_name": match.get("tournament_name", "Bilinmiyor"),
            "category_name": match.get("category_name", "Bilinmiyor"),
            "country": match.get("country", "Bilinmiyor"),
            
            "matches": 0, "home_wins": 0, "draws": 0, "away_wins": 0,
            "goals_home": 0, "goals_away": 0, "total_goals": 0,
            "over_25_count": 0, "under_25_count": 0, "btts_yes_count": 0,
            
            # Oran ortalamaları için toplamlar ve sayımlar (her maçta oran olmayabilir)
            "sum_odds_1": 0, "count_odds_1": 0,
            "sum_odds_x": 0, "count_odds_x": 0,
            "sum_odds_2": 0, "count_odds_2": 0,
            "sum_odds_o25": 0, "count_odds_o25": 0,
            "sum_odds_u25": 0, "count_odds_u25": 0,
            "sum_odds_btts_yes": 0, "count_odds_btts_yes": 0
        }

    def analyze_leagues(self):
        print("[LİG ANALİZİ] Biten maçlar veritabanından çekiliyor...")
        self.cur.execute("""
            SELECT * FROM results_football 
            WHERE status IN ('finished', 'ended') 
            AND tournament_id IS NOT NULL
        """)
        matches = self.cur.fetchall()
        print(f"[LİG ANALİZİ] Toplam {len(matches)} maç inceleniyor...")

        # Verileri liglere göre grupla ve topla
        for match in matches:
            t_id = match['tournament_id']
            if t_id not in self.leagues:
                self.leagues[t_id] = self._init_league_struct(match)
                
            lg = self.leagues[t_id]
            lg["matches"] += 1
            
            # Skor ve Sonuç Analizi
            gh, ga = match.get('ft_home'), match.get('ft_away')
            if gh is not None and ga is not None:
                lg["goals_home"] += gh
                lg["goals_away"] += ga
                total_match_goals = gh + ga
                lg["total_goals"] += total_match_goals
                
                if gh > ga: lg["home_wins"] += 1
                elif gh == ga: lg["draws"] += 1
                else: lg["away_wins"] += 1
                
                if total_match_goals > 2.5: lg["over_25_count"] += 1
                else: lg["under_25_count"] += 1
                
                if gh > 0 and ga > 0: lg["btts_yes_count"] += 1

            # Oran Analizi (Sadece veri varsa ekle)
            def add_odds(field_name, sum_key, count_key):
                val = match.get(field_name)
                if val:
                    lg[sum_key] += val
                    lg[count_key] += 1

            add_odds('odds_1', 'sum_odds_1', 'count_odds_1')
            add_odds('odds_x', 'sum_odds_x', 'count_odds_x')
            add_odds('odds_2', 'sum_odds_2', 'count_odds_2')
            add_odds('odds_o25', 'sum_odds_o25', 'count_odds_o25')
            add_odds('odds_u25', 'sum_odds_u25', 'count_odds_u25')
            add_odds('odds_btts_yes', 'sum_odds_btts_yes', 'count_odds_btts_yes')

        print("[LİG ANALİZİ] Hesaplamalar tamamlandı. Veritabanına kaydediliyor...")
        
        insert_query = """
            INSERT INTO league_analytics 
            (tournament_id, tournament_name, category_name, country, total_matches, 
             home_wins, draws, away_wins, home_win_pct, draw_pct, away_win_pct, 
             over_25_pct, under_25_pct, btts_yes_pct, avg_goals_match, avg_goals_home, avg_goals_away, 
             avg_odds_1, avg_odds_x, avg_odds_2, avg_odds_o25, avg_odds_u25, avg_odds_btts_yes)
            VALUES 
            (%(tournament_id)s, %(tournament_name)s, %(category_name)s, %(country)s, %(total_matches)s,
             %(home_wins)s, %(draws)s, %(away_wins)s, %(home_win_pct)s, %(draw_pct)s, %(away_win_pct)s,
             %(over_25_pct)s, %(under_25_pct)s, %(btts_yes_pct)s, %(avg_goals_match)s, %(avg_goals_home)s, %(avg_goals_away)s,
             %(avg_odds_1)s, %(avg_odds_x)s, %(avg_odds_2)s, %(avg_odds_o25)s, %(avg_odds_u25)s, %(avg_odds_btts_yes)s)
            ON DUPLICATE KEY UPDATE
             total_matches=VALUES(total_matches), home_wins=VALUES(home_wins), draws=VALUES(draws), away_wins=VALUES(away_wins),
             home_win_pct=VALUES(home_win_pct), draw_pct=VALUES(draw_pct), away_win_pct=VALUES(away_win_pct),
             over_25_pct=VALUES(over_25_pct), under_25_pct=VALUES(under_25_pct), btts_yes_pct=VALUES(btts_yes_pct),
             avg_goals_match=VALUES(avg_goals_match), avg_goals_home=VALUES(avg_goals_home), avg_goals_away=VALUES(avg_goals_away),
             avg_odds_1=VALUES(avg_odds_1), avg_odds_x=VALUES(avg_odds_x), avg_odds_2=VALUES(avg_odds_2),
             avg_odds_o25=VALUES(avg_odds_o25), avg_odds_u25=VALUES(avg_odds_u25), avg_odds_btts_yes=VALUES(avg_odds_btts_yes)
        """

        count = 0
        for t_id, lg in self.leagues.items():
            tm = lg["matches"]
            if tm == 0: continue
            
            # Yüzdeleri ve Ortalamaları Hesapla
            row = {
                "tournament_id": t_id,
                "tournament_name": lg["tournament_name"],
                "category_name": lg["category_name"],
                "country": lg["country"],
                
                "total_matches": tm,
                "home_wins": lg["home_wins"],
                "draws": lg["draws"],
                "away_wins": lg["away_wins"],
                
                "home_win_pct": round((lg["home_wins"] / tm) * 100, 2),
                "draw_pct": round((lg["draws"] / tm) * 100, 2),
                "away_win_pct": round((lg["away_wins"] / tm) * 100, 2),
                
                "over_25_pct": round((lg["over_25_count"] / tm) * 100, 2),
                "under_25_pct": round((lg["under_25_count"] / tm) * 100, 2),
                "btts_yes_pct": round((lg["btts_yes_count"] / tm) * 100, 2),
                
                "avg_goals_match": round(lg["total_goals"] / tm, 2),
                "avg_goals_home": round(lg["goals_home"] / tm, 2),
                "avg_goals_away": round(lg["goals_away"] / tm, 2),
                
                # Oran ortalamaları (Eğer o ligde hiç oran açılmamışsa 0 kalır)
                "avg_odds_1": round(lg["sum_odds_1"] / lg["count_odds_1"], 2) if lg["count_odds_1"] > 0 else 0,
                "avg_odds_x": round(lg["sum_odds_x"] / lg["count_odds_x"], 2) if lg["count_odds_x"] > 0 else 0,
                "avg_odds_2": round(lg["sum_odds_2"] / lg["count_odds_2"], 2) if lg["count_odds_2"] > 0 else 0,
                "avg_odds_o25": round(lg["sum_odds_o25"] / lg["count_odds_o25"], 2) if lg["count_odds_o25"] > 0 else 0,
                "avg_odds_u25": round(lg["sum_odds_u25"] / lg["count_odds_u25"], 2) if lg["count_odds_u25"] > 0 else 0,
                "avg_odds_btts_yes": round(lg["sum_odds_btts_yes"] / lg["count_odds_btts_yes"], 2) if lg["count_odds_btts_yes"] > 0 else 0
            }
            
            self.cur.execute(insert_query, row)
            count += 1

        print(f"[BAŞARILI] Toplam {count} lig analizi oluşturuldu/güncellendi.")

if __name__ == "__main__":
    analyzer = LeagueAnalyzer(CONFIG["db"])
    try:
        analyzer.connect()
        analyzer.analyze_leagues()
    except Exception as e:
        print(f"[HATA] Lig analizi sırasında bir sorun oluştu: {e}")
    finally:
        analyzer.close()
