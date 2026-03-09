#!/usr/bin/env python3
import datetime as dt, time, json
import mysql.connector
from typing import Dict, Any, List
from playwright.sync_api import sync_playwright

CONFIG = {
    "db": {
        "host": "netscout.fun",
        "user": "netscout_veri",
        "password": "i.34temel1",
        "database": "netscout_veri",
        "port": 3306
    },
    "api": {
        "base_url": "https://api.sofascore.com/api/v1",
        "headers": {"Accept": "application/json, text/plain, */*"},
        "user_agent": "Mozilla/5.0"
    },
    "scraper": {
        "sleep_between_requests": 0.5 
    }
}

MAJOR_TOURNAMENT_IDS = {
    1, 2, 3, 72, 84, 36, 37, 3739, 33, 34, 7372, 42, 41, 8343, 810,
    4, 5397, 62, 101, 39, 40, 38, 692, 280, 127, 83, 1449,
    169352, 5071, 28, 6720, 18, 3397, 3708, 82, 3034, 3284, 6230,
    54, 64, 29, 1060, 219, 652, 144, 1339, 1340, 1341
}

SCHEMA_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS results_football (
  event_id        BIGINT UNSIGNED NOT NULL,
  start_utc       DATE NULL,
  start_time_utc  TIME NULL,
  status          VARCHAR(32) NULL,
  home_team       VARCHAR(128) NULL,
  away_team       VARCHAR(128) NULL,
  ht_home         INT NULL,
  ht_away         INT NULL,
  ft_home         INT NULL,
  ft_away         INT NULL,
  
  -- Temel İstatistikler
  poss_h          INT NULL, poss_a          INT NULL,
  corn_h          INT NULL, corn_a          INT NULL,
  shot_h          INT NULL, shot_a          INT NULL,
  shot_on_h       INT NULL, shot_on_a       INT NULL,
  
  -- Detaylı İstatistikler
  fouls_h         INT NULL, fouls_a         INT NULL,
  offsides_h      INT NULL, offsides_a      INT NULL,
  saves_h         INT NULL, saves_a         INT NULL,
  passes_h        INT NULL, passes_a        INT NULL,
  tackles_h       INT NULL, tackles_a       INT NULL,
  
  -- Ekstra Maç Bilgileri
  referee         VARCHAR(128) NULL,
  formation_h     VARCHAR(32) NULL,
  formation_a     VARCHAR(32) NULL,
  
  -- İddaa / Bahis Oranları (Ondalıklı - Decimal)
  odds_1          FLOAT NULL,
  odds_x          FLOAT NULL,
  odds_2          FLOAT NULL,
  odds_1x         FLOAT NULL,
  odds_12         FLOAT NULL,
  odds_x2         FLOAT NULL,
  odds_btts_yes   FLOAT NULL,
  odds_btts_no    FLOAT NULL,
  
  odds_o05        FLOAT NULL, odds_u05        FLOAT NULL,
  odds_o15        FLOAT NULL, odds_u15        FLOAT NULL,
  odds_o25        FLOAT NULL, odds_u25        FLOAT NULL,
  odds_o35        FLOAT NULL, odds_u35        FLOAT NULL,
  odds_o45        FLOAT NULL, odds_u45        FLOAT NULL,
  odds_o55        FLOAT NULL, odds_u55        FLOAT NULL,
  odds_o65        FLOAT NULL, odds_u65        FLOAT NULL,
  odds_o75        FLOAT NULL, odds_u75        FLOAT NULL,
  
  -- Turnuva Bilgileri
  tournament_id   INT NULL,
  tournament_name VARCHAR(128) NULL,
  category_id     INT NULL,
  category_name   VARCHAR(128) NULL,
  country         VARCHAR(64) NULL,
  last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  
  PRIMARY KEY (event_id),
  KEY idx_date (start_utc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class DB:
    def __init__(self, cfg):
        self.cfg = cfg
        self.conn = None
        self.cur = None

    def connect(self):
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        self.cur.execute(SCHEMA_CREATE_TABLE)
        print(f"[DB] Bağlantı başarılı ve tablo hazır.")

    def upsert_match(self, row: Dict[str, Any]):
        q = """
        INSERT INTO results_football
        (event_id, start_utc, start_time_utc, status, home_team, away_team,
         ht_home, ht_away, ft_home, ft_away,
         poss_h, poss_a, corn_h, corn_a, shot_h, shot_a, shot_on_h, shot_on_a,
         fouls_h, fouls_a, offsides_h, offsides_a, saves_h, saves_a, passes_h, passes_a, tackles_h, tackles_a,
         referee, formation_h, formation_a, 
         odds_1, odds_x, odds_2, odds_1x, odds_12, odds_x2, odds_btts_yes, odds_btts_no, 
         odds_o05, odds_u05, odds_o15, odds_u15, odds_o25, odds_u25, odds_o35, odds_u35, 
         odds_o45, odds_u45, odds_o55, odds_u55, odds_o65, odds_u65, odds_o75, odds_u75,
         tournament_id, tournament_name, category_id, category_name, country)
        VALUES
        (%(event_id)s, %(start_utc)s, %(start_time_utc)s, %(status)s, %(home_team)s, %(away_team)s,
         %(ht_home)s, %(ht_away)s, %(ft_home)s, %(ft_away)s,
         %(poss_h)s, %(poss_a)s, %(corn_h)s, %(corn_a)s, %(shot_h)s, %(shot_a)s, %(shot_on_h)s, %(shot_on_a)s,
         %(fouls_h)s, %(fouls_a)s, %(offsides_h)s, %(offsides_a)s, %(saves_h)s, %(saves_a)s, %(passes_h)s, %(passes_a)s, %(tackles_h)s, %(tackles_a)s,
         %(referee)s, %(formation_h)s, %(formation_a)s,
         %(odds_1)s, %(odds_x)s, %(odds_2)s, %(odds_1x)s, %(odds_12)s, %(odds_x2)s, %(odds_btts_yes)s, %(odds_btts_no)s,
         %(odds_o05)s, %(odds_u05)s, %(odds_o15)s, %(odds_u15)s, %(odds_o25)s, %(odds_u25)s, %(odds_o35)s, %(odds_u35)s, 
         %(odds_o45)s, %(odds_u45)s, %(odds_o55)s, %(odds_u55)s, %(odds_o65)s, %(odds_u65)s, %(odds_o75)s, %(odds_u75)s,
         %(tournament_id)s, %(tournament_name)s, %(category_id)s, %(category_name)s, %(country)s)
        ON DUPLICATE KEY UPDATE
          status = VALUES(status),
          ht_home = VALUES(ht_home), ht_away = VALUES(ht_away),
          ft_home = VALUES(ft_home), ft_away = VALUES(ft_away),
          poss_h = VALUES(poss_h), poss_a = VALUES(poss_a),
          corn_h = VALUES(corn_h), corn_a = VALUES(corn_a),
          shot_h = VALUES(shot_h), shot_a = VALUES(shot_a),
          shot_on_h = VALUES(shot_on_h), shot_on_a = VALUES(shot_on_a),
          fouls_h = VALUES(fouls_h), fouls_a = VALUES(fouls_a),
          offsides_h = VALUES(offsides_h), offsides_a = VALUES(offsides_a),
          saves_h = VALUES(saves_h), saves_a = VALUES(saves_a),
          passes_h = VALUES(passes_h), passes_a = VALUES(passes_a),
          tackles_h = VALUES(tackles_h), tackles_a = VALUES(tackles_a),
          referee = VALUES(referee),
          formation_h = VALUES(formation_h), formation_a = VALUES(formation_a),
          odds_1 = VALUES(odds_1), odds_x = VALUES(odds_x), odds_2 = VALUES(odds_2),
          odds_1x = VALUES(odds_1x), odds_12 = VALUES(odds_12), odds_x2 = VALUES(odds_x2),
          odds_btts_yes = VALUES(odds_btts_yes), odds_btts_no = VALUES(odds_btts_no),
          odds_o05 = VALUES(odds_o05), odds_u05 = VALUES(odds_u05),
          odds_o15 = VALUES(odds_o15), odds_u15 = VALUES(odds_u15),
          odds_o25 = VALUES(odds_o25), odds_u25 = VALUES(odds_u25),
          odds_o35 = VALUES(odds_o35), odds_u35 = VALUES(odds_u35),
          odds_o45 = VALUES(odds_o45), odds_u45 = VALUES(odds_u45),
          odds_o55 = VALUES(odds_o55), odds_u55 = VALUES(odds_u55),
          odds_o65 = VALUES(odds_o65), odds_u65 = VALUES(odds_u65),
          odds_o75 = VALUES(odds_o75), odds_u75 = VALUES(odds_u75);
        """
        self.cur.execute(q, row)

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()

class Scraper:
    def __init__(self, cfg):
        self.api = cfg
        self.browser = None
        self.page = None
        self.p = None

    def start(self):
        self.p = sync_playwright().start()
        self.browser = self.p.chromium.launch(headless=True)
        ctx = self.browser.new_context(user_agent=self.api["user_agent"], extra_http_headers=self.api["headers"])
        self.page = ctx.new_page()
        self.page.goto("https://www.sofascore.com/", wait_until="domcontentloaded")

    def stop(self):
        if self.browser: self.browser.close()
        if self.p: self.p.stop()

    def _fetch_json(self, url: str) -> dict:
        try:
            time.sleep(self.api.get("sleep_between_requests", 0.3))
            self.page.goto(url, wait_until="domcontentloaded", timeout=15000)
            data_text = self.page.evaluate("() => document.body.innerText")
            return json.loads(data_text)
        except Exception as e:
            return {}

    def get_detailed_stats(self, event_id) -> Dict[str, Any]:
        url = f"{self.api['base_url']}/event/{event_id}/statistics"
        data = self._fetch_json(url)
        
        res = {
            "poss_h": None, "poss_a": None, "corn_h": None, "corn_a": None, 
            "shot_h": None, "shot_a": None, "shot_on_h": None, "shot_on_a": None,
            "fouls_h": None, "fouls_a": None, "offsides_h": None, "offsides_a": None,
            "saves_h": None, "saves_a": None, "passes_h": None, "passes_a": None,
            "tackles_h": None, "tackles_a": None
        }
        
        if "statistics" not in data: 
            return res
            
        all_period = data["statistics"][0]
        for group in all_period.get("groups", []):
            for item in group.get("statisticsItems", []):
                name = item.get("name")
                h, a = item.get("homeValue"), item.get("awayValue")
                
                h_val = int(h.replace('%','')) if isinstance(h, str) and '%' in h else h
                a_val = int(a.replace('%','')) if isinstance(a, str) and '%' in a else a

                if name == "Ball possession": res["poss_h"], res["poss_a"] = h_val, a_val
                elif name == "Corner kicks": res["corn_h"], res["corn_a"] = h_val, a_val
                elif name == "Total shots": res["shot_h"], res["shot_a"] = h_val, a_val
                elif name == "Shots on target": res["shot_on_h"], res["shot_on_a"] = h_val, a_val
                elif name == "Fouls": res["fouls_h"], res["fouls_a"] = h_val, a_val
                elif name == "Offsides": res["offsides_h"], res["offsides_a"] = h_val, a_val
                elif name == "Goalkeeper saves": res["saves_h"], res["saves_a"] = h_val, a_val
                elif name == "Passes": res["passes_h"], res["passes_a"] = h_val, a_val
                elif name == "Tackles": res["tackles_h"], res["tackles_a"] = h_val, a_val
        return res

    def get_event_details(self, event_id) -> Dict[str, Any]:
        url = f"{self.api['base_url']}/event/{event_id}"
        data = self._fetch_json(url)
        ev = data.get("event", {})
        
        return {
            "referee": ev.get("referee", {}).get("name")
        }

    def get_incidents_and_lineups(self, event_id) -> Dict[str, Any]:
        res = {
            "formation_h": None,
            "formation_a": None
        }
            
        lin_url = f"{self.api['base_url']}/event/{event_id}/lineups"
        lin_data = self._fetch_json(lin_url)
        if lin_data:
            res["formation_h"] = lin_data.get("home", {}).get("formation")
            res["formation_a"] = lin_data.get("away", {}).get("formation")
            
        return res

    def get_odds(self, event_id) -> Dict[str, Any]:
        url = f"{self.api['base_url']}/event/{event_id}/odds/1/all"
        data = self._fetch_json(url)
        
        res = {
            "odds_1": None, "odds_x": None, "odds_2": None,
            "odds_1x": None, "odds_12": None, "odds_x2": None,
            "odds_btts_yes": None, "odds_btts_no": None,
            "odds_o05": None, "odds_u05": None,
            "odds_o15": None, "odds_u15": None,
            "odds_o25": None, "odds_u25": None,
            "odds_o35": None, "odds_u35": None,
            "odds_o45": None, "odds_u45": None,
            "odds_o55": None, "odds_u55": None,
            "odds_o65": None, "odds_u65": None,
            "odds_o75": None, "odds_u75": None
        }
        
        def get_decimal_odd(c_dict):
            """Hem ondalıklı hem kesirli oranı yakalayan esnek çevirici"""
            if c_dict.get("decimalValue"):
                try: return float(c_dict["decimalValue"])
                except: pass
                
            frac_str = c_dict.get("fractionalValue")
            if not frac_str: return None
            if str(frac_str).upper() == "EVS": return 2.0
            try:
                if '/' in str(frac_str):
                    n, d = str(frac_str).split('/')
                    return round((float(n) / float(d)) + 1, 2)
                return float(frac_str)
            except:
                return None

        markets = data.get("markets", [])
        for m in markets:
            m_name = str(m.get("marketName", "")).strip().lower()
            choices = m.get("choices", [])
            m_str = json.dumps(m).lower()
            
            # Full-Time (Maç Sonucu 1X2)
            if "full time" in m_name or "full-time" in m_name or "1x2" in m_name:
                for c in choices:
                    c_name = str(c.get("name", "")).strip().upper()
                    if c_name == "1": res["odds_1"] = get_decimal_odd(c)
                    elif c_name == "X": res["odds_x"] = get_decimal_odd(c)
                    elif c_name == "2": res["odds_2"] = get_decimal_odd(c)
            
            # Double Chance (Çifte Şans)
            elif "double chance" in m_name:
                for c in choices:
                    c_name = str(c.get("name", "")).strip().upper()
                    if c_name == "1X": res["odds_1x"] = get_decimal_odd(c)
                    elif c_name == "12": res["odds_12"] = get_decimal_odd(c)
                    elif c_name == "X2": res["odds_x2"] = get_decimal_odd(c)
            
            # Both Teams To Score (Karşılıklı Gol)
            elif "both teams to score" in m_name:
                for c in choices:
                    c_name = str(c.get("name", "")).strip().lower()
                    if c_name == "yes": res["odds_btts_yes"] = get_decimal_odd(c)
                    elif c_name == "no": res["odds_btts_no"] = get_decimal_odd(c)
            
            # Alt/Üst - İsimde "goals" veya "over/under" geçsin ama ilk yarı vs olmasın
            elif ("goals" in m_name or "over/under" in m_name) and "half" not in m_name and "team" not in m_name and "exact" not in m_name:
                for c in choices:
                    c_name = str(c.get("name", "")).strip().lower()
                    c_str = json.dumps(c).lower()
                    
                    line_found = None
                    baremler = ["0.5", "1.5", "2.5", "3.5", "4.5", "5.5", "6.5", "7.5"]
                    
                    # 1. Önce doğrudan seçeneğin JSON verisinde arıyoruz
                    for line in baremler:
                        if line in c_str:
                            line_found = line
                            break
                            
                    # 2. Seçenekte bulamazsak pazarın genel verisinde arıyoruz
                    if not line_found:
                        for line in baremler:
                            if line in m_str:
                                line_found = line
                                break
                    
                    # 3. İkisinde de yazmıyorsa ama bu "isMain" (Ana Barem) ise standart 2.5 kabul ediyoruz
                    if not line_found and m.get("isMain"):
                        line_found = "2.5"
                    
                    # Barem bulunduysa Over mi Under mi olduğunu saptayıp kaydediyoruz
                    if line_found:
                        val = get_decimal_odd(c)
                        
                        if "over" in c_name or "üst" in c_name or c_name == "o":
                            if line_found == "0.5": res["odds_o05"] = val
                            elif line_found == "1.5": res["odds_o15"] = val
                            elif line_found == "2.5": res["odds_o25"] = val
                            elif line_found == "3.5": res["odds_o35"] = val
                            elif line_found == "4.5": res["odds_o45"] = val
                            elif line_found == "5.5": res["odds_o55"] = val
                            elif line_found == "6.5": res["odds_o65"] = val
                            elif line_found == "7.5": res["odds_o75"] = val
                        elif "under" in c_name or "alt" in c_name or c_name == "u":
                            if line_found == "0.5": res["odds_u05"] = val
                            elif line_found == "1.5": res["odds_u15"] = val
                            elif line_found == "2.5": res["odds_u25"] = val
                            elif line_found == "3.5": res["odds_u35"] = val
                            elif line_found == "4.5": res["odds_u45"] = val
                            elif line_found == "5.5": res["odds_u55"] = val
                            elif line_found == "6.5": res["odds_u65"] = val
                            elif line_found == "7.5": res["odds_u75"] = val

        return res

    def by_date(self, date_str) -> List[Dict[str, Any]]:
        url = f"{self.api['base_url']}/sport/football/scheduled-events/{date_str}"
        data = self._fetch_json(url)
        return data.get("events", [])

    def parse(self, ev: Dict[str, Any], extra_data: Dict[str, Any]) -> Dict[str, Any]:
        ts = ev.get("startTimestamp")
        dt_utc = dt.datetime.fromtimestamp(ts, dt.timezone.utc) if isinstance(ts, int) else None
        status = (ev.get("status", {}).get("type") or "").lower()
        hs, as_ = ev.get("homeScore", {}) or {}, ev.get("awayScore", {}) or {}
        
        t = ev.get("tournament") or {}; u = ev.get("uniqueTournament") or {}
        cat = (t.get("category") or {}) if "category" in t else (u.get("category") or {})

        row = {
            "event_id": ev.get("id"),
            "start_utc": dt_utc.strftime("%Y-%m-%d") if dt_utc else None,
            "start_time_utc": dt_utc.strftime("%H:%M:%S") if dt_utc else None,
            "status": status,
            "home_team": (ev.get("homeTeam") or {}).get("name"),
            "away_team": (ev.get("awayTeam") or {}).get("name"),
            "ht_home": hs.get("period1"), "ht_away": as_.get("period1"),
            "ft_home": hs.get("normaltime") if hs.get("normaltime") is not None else hs.get("current"),
            "ft_away": as_.get("normaltime") if as_.get("normaltime") is not None else as_.get("current"),
            "tournament_id": u.get("id") or t.get("id"),
            "tournament_name": u.get("name") or t.get("name"),
            "category_id": cat.get("id"),
            "category_name": cat.get("name"),
            "country": (cat.get("country") or {}).get("name") if isinstance(cat.get("country"), dict) else cat.get("name")
        }
        
        row.update(extra_data) 
        return row

def main():
    db = DB(CONFIG["db"])
    db.connect()
    sc = Scraper(CONFIG["api"])
    sc.start()
    
    try:
        today = dt.date.today()
        # Sadece Dün, Bugün ve Yarın'ın tarihlerini içeren kesin bir liste oluşturuyoruz
        allowed_dates = [(today + dt.timedelta(days=i)).strftime("%Y-%m-%d") for i in [-1, 0, 1]]
        
        for date_str in allowed_dates:
            events = sc.by_date(date_str)
            
            p_count = 0
            for ev in events:
                # 1. KORUMA: Saat farklarından (UTC) dolayı istemediğimiz günlerin (örn: 11'i) sızmasını engelle
                ts = ev.get("startTimestamp")
                if ts:
                    dt_utc = dt.datetime.fromtimestamp(ts, dt.timezone.utc)
                    match_date_str = dt_utc.strftime("%Y-%m-%d")
                    
                    # Eğer maçın tarihi bizim izin verdiğimiz 3 günde yoksa, bu maçı es geç
                    if match_date_str not in allowed_dates:
                        continue
                
                t_id = ev.get("tournament", {}).get("id")
                u_id = ev.get("uniqueTournament", {}).get("id")
                
                if t_id in MAJOR_TOURNAMENT_IDS or u_id in MAJOR_TOURNAMENT_IDS:
                    ev_id = ev.get("id")
                    status = (ev.get("status", {}).get("type") or "").lower()
                    
                    # Ertelenmiş veya iptal edilmiş maçları atla
                    if status in ["postponed", "canceled"]:
                        continue
                    
                    extra_data = {
                        "poss_h": None, "poss_a": None, "corn_h": None, "corn_a": None, 
                        "shot_h": None, "shot_a": None, "shot_on_h": None, "shot_on_a": None,
                        "fouls_h": None, "fouls_a": None, "offsides_h": None, "offsides_a": None,
                        "saves_h": None, "saves_a": None, "passes_h": None, "passes_a": None,
                        "tackles_h": None, "tackles_a": None,
                        "referee": None,
                        "formation_h": None, "formation_a": None,
                        
                        # Tüm Oran Alanları İlk Değer Ataması
                        "odds_1": None, "odds_x": None, "odds_2": None,
                        "odds_1x": None, "odds_12": None, "odds_x2": None,
                        "odds_btts_yes": None, "odds_btts_no": None,
                        "odds_o05": None, "odds_u05": None,
                        "odds_o15": None, "odds_u15": None,
                        "odds_o25": None, "odds_u25": None,
                        "odds_o35": None, "odds_u35": None,
                        "odds_o45": None, "odds_u45": None,
                        "odds_o55": None, "odds_u55": None,
                        "odds_o65": None, "odds_u65": None,
                        "odds_o75": None, "odds_u75": None
                    }

                    # Oranlar (maçın durumundan bağımsız olarak çekilir)
                    odds = sc.get_odds(ev_id)
                    extra_data.update(odds)

                    # Maç başladıysa veya bittiyse tüm detayları (istatistik ve kadrolar) çek
                    if status in ["inprogress", "finished"]:
                        stats = sc.get_detailed_stats(ev_id)
                        extra_data.update(stats)
                        
                        details = sc.get_event_details(ev_id)
                        extra_data.update(details)
                        
                        inc_lin = sc.get_incidents_and_lineups(ev_id)
                        extra_data.update(inc_lin)

                    row = sc.parse(ev, extra_data)
                    db.upsert_match(row)
                    p_count += 1
            
            print(f"[BİLGİ] {date_str}: {p_count} majör maç işlendi ve veritabanına kaydedildi.")
            
    except Exception as e: 
        print(f"[HATA]: {e}")
    finally: 
        sc.stop()
        db.close()

if __name__ == "__main__":
    main()
