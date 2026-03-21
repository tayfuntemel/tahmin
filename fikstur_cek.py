#!/usr/bin/env python3
import os
import datetime as dt, time, json, sys
import mysql.connector
from typing import Dict, Any, List
from playwright.sync_api import sync_playwright

CONFIG = {
    "db": {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306))
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
  poss_h          INT NULL, poss_a          INT NULL,
  corn_h          INT NULL, corn_a          INT NULL,
  shot_h          INT NULL, shot_a          INT NULL,
  shot_on_h       INT NULL, shot_on_a       INT NULL,
  fouls_h         INT NULL, fouls_a         INT NULL,
  offsides_h      INT NULL, offsides_a      INT NULL,
  saves_h         INT NULL, saves_a         INT NULL,
  passes_h        INT NULL, passes_a        INT NULL,
  tackles_h       INT NULL, tackles_a       INT NULL,
  referee         VARCHAR(128) NULL,
  formation_h     VARCHAR(32) NULL,
  formation_a     VARCHAR(32) NULL,
  odds_1          FLOAT NULL, odds_x          FLOAT NULL, odds_2          FLOAT NULL,
  odds_1x         FLOAT NULL, odds_12         FLOAT NULL, odds_x2         FLOAT NULL,
  odds_btts_yes   FLOAT NULL, odds_btts_no    FLOAT NULL,
  odds_o05        FLOAT NULL, odds_u05        FLOAT NULL,
  odds_o15        FLOAT NULL, odds_u15        FLOAT NULL,
  odds_o25        FLOAT NULL, odds_u25        FLOAT NULL,
  odds_o35        FLOAT NULL, odds_u35        FLOAT NULL,
  odds_o45        FLOAT NULL, odds_u45        FLOAT NULL,
  odds_o55        FLOAT NULL, odds_u55        FLOAT NULL,
  odds_o65        FLOAT NULL, odds_u65        FLOAT NULL,
  odds_o75        FLOAT NULL, odds_u75        FLOAT NULL,
  tournament_id   INT NULL, tournament_name VARCHAR(128) NULL,
  category_id     INT NULL, category_name   VARCHAR(128) NULL,
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
         odds_1, odds_x, odds_2, odds_1x, odds_12, odds_x2, odds_btts_yes, odds_btts_no, 
         odds_o05, odds_u05, odds_o15, odds_u15, odds_o25, odds_u25, odds_o35, odds_u35, 
         odds_o45, odds_u45, odds_o55, odds_u55, odds_o65, odds_u65, odds_o75, odds_u75,
         tournament_id, tournament_name, category_id, category_name, country)
        VALUES
        (%(event_id)s, %(start_utc)s, %(start_time_utc)s, %(status)s, %(home_team)s, %(away_team)s,
         %(odds_1)s, %(odds_x)s, %(odds_2)s, %(odds_1x)s, %(odds_12)s, %(odds_x2)s, %(odds_btts_yes)s, %(odds_btts_no)s,
         %(odds_o05)s, %(odds_u05)s, %(odds_o15)s, %(odds_u15)s, %(odds_o25)s, %(odds_u25)s, %(odds_o35)s, %(odds_u35)s, 
         %(odds_o45)s, %(odds_u45)s, %(odds_o55)s, %(odds_u55)s, %(odds_o65)s, %(odds_u65)s, %(odds_o75)s, %(odds_u75)s,
         %(tournament_id)s, %(tournament_name)s, %(category_id)s, %(category_name)s, %(country)s)
        ON DUPLICATE KEY UPDATE
          status = VALUES(status),
          start_utc = VALUES(start_utc), 
          start_time_utc = VALUES(start_time_utc);
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

    def get_odds(self, event_id) -> Dict[str, Any]:
        url = f"{self.api['base_url']}/event/{event_id}/odds/1/all"
        data = self._fetch_json(url)
        
        res = {
            "odds_1": None, "odds_x": None, "odds_2": None, "odds_1x": None, "odds_12": None, "odds_x2": None,
            "odds_btts_yes": None, "odds_btts_no": None, "odds_o05": None, "odds_u05": None,
            "odds_o15": None, "odds_u15": None, "odds_o25": None, "odds_u25": None,
            "odds_o35": None, "odds_u35": None, "odds_o45": None, "odds_u45": None,
            "odds_o55": None, "odds_u55": None, "odds_o65": None, "odds_u65": None, "odds_o75": None, "odds_u75": None
        }
        
        def get_decimal_odd(c_dict):
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
            
            if "full time" in m_name or "full-time" in m_name or "1x2" in m_name:
                for c in choices:
                    c_name = str(c.get("name", "")).strip().upper()
                    if c_name == "1": res["odds_1"] = get_decimal_odd(c)
                    elif c_name == "X": res["odds_x"] = get_decimal_odd(c)
                    elif c_name == "2": res["odds_2"] = get_decimal_odd(c)
            elif "double chance" in m_name:
                for c in choices:
                    c_name = str(c.get("name", "")).strip().upper()
                    if c_name == "1X": res["odds_1x"] = get_decimal_odd(c)
                    elif c_name == "12": res["odds_12"] = get_decimal_odd(c)
                    elif c_name == "X2": res["odds_x2"] = get_decimal_odd(c)
            elif "both teams to score" in m_name:
                for c in choices:
                    c_name = str(c.get("name", "")).strip().lower()
                    if c_name == "yes": res["odds_btts_yes"] = get_decimal_odd(c)
                    elif c_name == "no": res["odds_btts_no"] = get_decimal_odd(c)
            elif ("goals" in m_name or "over/under" in m_name) and "half" not in m_name and "team" not in m_name and "exact" not in m_name:
                for c in choices:
                    c_name = str(c.get("name", "")).strip().lower()
                    c_str = json.dumps(c).lower()
                    
                    line_found = None
                    baremler = ["0.5", "1.5", "2.5", "3.5", "4.5", "5.5", "6.5", "7.5"]
                    
                    for line in baremler:
                        if line in c_str:
                            line_found = line; break
                    if not line_found:
                        for line in baremler:
                            if line in m_str:
                                line_found = line; break
                    if not line_found and m.get("isMain"):
                        line_found = "2.5"
                    
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
        
        # TÜRKİYE SAATİNE ÇEVİRME (UTC+3)
        tz_tr = dt.timezone(dt.timedelta(hours=3))
        dt_tr = dt.datetime.fromtimestamp(ts, tz_tr) if isinstance(ts, int) else None
        
        status = (ev.get("status", {}).get("type") or "").lower()
        t = ev.get("tournament") or {}; u = ev.get("uniqueTournament") or {}
        cat = (t.get("category") or {}) if "category" in t else (u.get("category") or {})

        row = {
            "event_id": ev.get("id"),
            "start_utc": dt_tr.strftime("%Y-%m-%d") if dt_tr else None,
            "start_time_utc": dt_tr.strftime("%H:%M:%S") if dt_tr else None,
            "status": status,
            "home_team": (ev.get("homeTeam") or {}).get("name"),
            "away_team": (ev.get("awayTeam") or {}).get("name"),
            "tournament_id": u.get("id") or t.get("id"),
            "tournament_name": u.get("name") or t.get("name"),
            "category_id": cat.get("id"),
            "category_name": cat.get("name"),
            "country": (cat.get("country") or {}).get("name") if isinstance(cat.get("country"), dict) else cat.get("name")
        }
        row.update(extra_data) 
        return row

def main():
    max_retries = 3  # İşlem başarısız olursa maksimum deneme sayısı
    attempt = 1
    
    while attempt <= max_retries:
        print(f"\n--- ÇALIŞTIRMA DENEMESİ {attempt}/{max_retries} ---")
        
        # Sınıfları döngü içinde başlatıyoruz ki her denemede temiz bir bağlantı kurulsun
        db = DB(CONFIG["db"])
        sc = Scraper(CONFIG["api"])
        total_processed_in_this_run = 0
        
        try:
            db.connect()
            sc.start()
            
            # TÜRKİYE SAATİNE GÖRE BUGÜN VE YARIN HESAPLAMASI (UTC+3)
            tz_tr = dt.timezone(dt.timedelta(hours=3))
            now_tr = dt.datetime.now(tz_tr)
            today_tr = now_tr.date()
            target_dates_tr = [today_tr, today_tr + dt.timedelta(days=1)] # Sadece TR Bugün ve TR Yarın
            
            # Saat farkından dolayı sınırda kalanları (gece maçları) kaçırmamak için dünden başlayıp yarından sonrasına kadar tarama yapıyoruz (-1, 0, 1)
            for i in [-1, 0, 1]:
                fetch_date = today_tr + dt.timedelta(days=i)
                date_str = fetch_date.strftime("%Y-%m-%d")
                events = sc.by_date(date_str)
                
                p_count = 0
                for ev in events:
                    # Olayın timestamp'ini direkt TR saatine çevirip kontrol ediyoruz
                    ts = ev.get("startTimestamp")
                    if not isinstance(ts, int):
                        continue
                    ev_dt_tr = dt.datetime.fromtimestamp(ts, tz_tr)
                    
                    # SADECE TR SAATİYLE BUGÜN VE YARIN OLANLARI İŞLE
                    if ev_dt_tr.date() not in target_dates_tr:
                        continue

                    t_id = ev.get("tournament", {}).get("id")
                    u_id = ev.get("uniqueTournament", {}).get("id")
                    
                    if t_id in MAJOR_TOURNAMENT_IDS or u_id in MAJOR_TOURNAMENT_IDS:
                        ev_id = ev.get("id")
                        status = (ev.get("status", {}).get("type") or "").lower()
                        
                        # SADECE BAŞLAMAMIŞ MAÇLAR
                        if status in ["notstarted", "scheduled"]:
                            extra_data = {
                                "odds_1": None, "odds_x": None, "odds_2": None, "odds_1x": None, "odds_12": None, "odds_x2": None,
                                "odds_btts_yes": None, "odds_btts_no": None, "odds_o05": None, "odds_u05": None,
                                "odds_o15": None, "odds_u15": None, "odds_o25": None, "odds_u25": None,
                                "odds_o35": None, "odds_u35": None, "odds_o45": None, "odds_u45": None,
                                "odds_o55": None, "odds_u55": None, "odds_o65": None, "odds_u65": None, "odds_o75": None, "odds_u75": None
                            }

                            # Oranları çek
                            odds = sc.get_odds(ev_id)
                            extra_data.update(odds)

                            row = sc.parse(ev, extra_data)
                            db.upsert_match(row)
                            p_count += 1
                            total_processed_in_this_run += 1
                
                print(f"[BAŞLAMAMIŞ MAÇLAR] TR Saati Taraması: {date_str} için API isteği yapıldı, {p_count} uygun maç işlendi.")
                
        except Exception as e: 
            print(f"[HATA]: İşlem sırasında bir sorun oluştu: {e}")
        finally: 
            # Tarayıcıyı ve DB bağlantısını her deneme sonunda düzgünce kapatıyoruz
            sc.stop()
            db.close()

        # Döngü sonu kontrolü: Eğer veri başarıyla çekildiyse döngüyü kır ve tamamen çık
        if total_processed_in_this_run > 0:
            print(f"\n[BAŞARILI] Toplam {total_processed_in_this_run} maç işlendi. Script sorunsuz tamamlandı.")
            break
        else:
            print(f"\n[UYARI] Bu denemede HİÇBİR maç verisi çekilemedi (Toplam 0 maç)!")
            if attempt < max_retries:
                print("15 saniye bekleniyor, ardından tekrar denenecek...")
                time.sleep(15)
            attempt += 1

    # Eğer 3 deneme de bittiyse ve hala hiç veri çekilemediyse GitHub Actions'ı hatalı bitir.
    # (Not: Yaz dönemlerinde veya fikstürün gerçekten boş olduğu nadir günlerde de bu hata tetiklenebilir.)
    if total_processed_in_this_run == 0:
        print("\n[KRİTİK HATA] Maksimum deneme sayısına ulaşıldı ancak hiçbir veri çekilemedi.")
        sys.exit(1)

if __name__ == "__main__":
    main()
