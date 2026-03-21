#!/usr/bin/env python3
import os
import mysql.connector

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

# Hakem analizlerinin kaydedileceği yeni tablonun şeması
SCHEMA_CREATE_REFEREE_ANALYTICS = """
CREATE TABLE IF NOT EXISTS referee_analytics (
  referee_name      VARCHAR(128) PRIMARY KEY,
  
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
  btts_yes_pct      FLOAT DEFAULT 0,
  
  -- Ortalamalar
  avg_goals_match   FLOAT DEFAULT 0,
  avg_fouls_match   FLOAT DEFAULT 0,
  
  last_updated      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class RefereeAnalyzer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None
        self.referees = {} # Hakem verilerini toplayacağımız sözlük

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(dictionary=True)
        self.cur.execute(SCHEMA_CREATE_REFEREE_ANALYTICS)
        print("[HAKEM ANALİZİ] Veritabanı bağlantısı başarılı ve 'referee_analytics' tablosu hazır.")

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

    def _init_referee_struct(self):
        """Bir hakem için temel veri yapısını oluşturur."""
        return {
            "matches": 0, "home_wins": 0, "draws": 0, "away_wins": 0,
            "total_goals": 0, "over_25_count": 0, "btts_yes_count": 0,
            "total_fouls": 0, "matches_with_foul_data": 0
        }

    def analyze_referees(self):
        print("[HAKEM ANALİZİ] Hakem atanmış bitmiş maçlar veritabanından çekiliyor...")
        # Sadece hakemi belli olan ve bitmiş maçları çekiyoruz
        self.cur.execute("""
            SELECT * FROM results_football 
            WHERE status IN ('finished', 'ended') 
            AND referee IS NOT NULL 
            AND referee != ''
        """)
        matches = self.cur.fetchall()
        print(f"[HAKEM ANALİZİ] Toplam {len(matches)} maç inceleniyor...")

        # Verileri hakemlere göre grupla ve topla
        for match in matches:
            ref_name = match['referee'].strip()
            
            if ref_name not in self.referees:
                self.referees[ref_name] = self._init_referee_struct()
                
            ref = self.referees[ref_name]
            ref["matches"] += 1
            
            # Skor ve Sonuç Analizi
            gh, ga = match.get('ft_home'), match.get('ft_away')
            if gh is not None and ga is not None:
                total_match_goals = gh + ga
                ref["total_goals"] += total_match_goals
                
                if gh > ga: ref["home_wins"] += 1
                elif gh == ga: ref["draws"] += 1
                else: ref["away_wins"] += 1
                
                if total_match_goals > 2.5: ref["over_25_count"] += 1
                
                if gh > 0 and ga > 0: ref["btts_yes_count"] += 1

            # Faul İstatistikleri
            fh, fa = match.get('fouls_h'), match.get('fouls_a')
            if fh is not None and fa is not None:
                ref["total_fouls"] += (fh + fa)
                ref["matches_with_foul_data"] += 1

        print("[HAKEM ANALİZİ] Hesaplamalar tamamlandı. Veritabanına kaydediliyor...")
        
        insert_query = """
            INSERT INTO referee_analytics 
            (referee_name, total_matches, home_wins, draws, away_wins, 
             home_win_pct, draw_pct, away_win_pct, over_25_pct, btts_yes_pct, 
             avg_goals_match, avg_fouls_match)
            VALUES 
            (%(referee_name)s, %(total_matches)s, %(home_wins)s, %(draws)s, %(away_wins)s,
             %(home_win_pct)s, %(draw_pct)s, %(away_win_pct)s, %(over_25_pct)s, %(btts_yes_pct)s, 
             %(avg_goals_match)s, %(avg_fouls_match)s)
            ON DUPLICATE KEY UPDATE
             total_matches=VALUES(total_matches), home_wins=VALUES(home_wins), draws=VALUES(draws), away_wins=VALUES(away_wins),
             home_win_pct=VALUES(home_win_pct), draw_pct=VALUES(draw_pct), away_win_pct=VALUES(away_win_pct),
             over_25_pct=VALUES(over_25_pct), btts_yes_pct=VALUES(btts_yes_pct),
             avg_goals_match=VALUES(avg_goals_match), avg_fouls_match=VALUES(avg_fouls_match)
        """

        count = 0
        for ref_name, ref in self.referees.items():
            tm = ref["matches"]
            if tm == 0: continue
            
            # Eğer hakemin faul istatistiği olan maçı varsa ona böl, yoksa 0 ata
            foul_matches = ref["matches_with_foul_data"]
            avg_fouls = round(ref["total_fouls"] / foul_matches, 2) if foul_matches > 0 else 0
            
            # Yüzdeleri ve Ortalamaları Hesapla
            row = {
                "referee_name": ref_name,
                "total_matches": tm,
                "home_wins": ref["home_wins"],
                "draws": ref["draws"],
                "away_wins": ref["away_wins"],
                
                "home_win_pct": round((ref["home_wins"] / tm) * 100, 2),
                "draw_pct": round((ref["draws"] / tm) * 100, 2),
                "away_win_pct": round((ref["away_wins"] / tm) * 100, 2),
                
                "over_25_pct": round((ref["over_25_count"] / tm) * 100, 2),
                "btts_yes_pct": round((ref["btts_yes_count"] / tm) * 100, 2),
                
                "avg_goals_match": round(ref["total_goals"] / tm, 2),
                "avg_fouls_match": avg_fouls
            }
            
            self.cur.execute(insert_query, row)
            count += 1

        print(f"[BAŞARILI] Toplam {count} hakem analizi oluşturuldu/güncellendi.")

if __name__ == "__main__":
    analyzer = RefereeAnalyzer(CONFIG["db"])
    try:
        analyzer.connect()
        analyzer.analyze_referees()
    except Exception as e:
        print(f"[HATA] Hakem analizi sırasında bir sorun oluştu: {e}")
    finally:
        analyzer.close()
