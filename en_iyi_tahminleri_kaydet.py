#!/usr/bin/env python3
import datetime as dt, time, json, sys
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
    5, 6, 12, 13, 19, 24, 27, 30, 31, 48, 49, 50, 52, 53, 55, 79, 102, 232, 384, 
    681, 877, 1061, 1107, 1427, 10812, 16753, 19232, 34363, 51702, 52653, 58560, 
    64475, 71900, 71901, 72112, 78740, 92016, 92614, 143625
}

# TABLO ADI "results_football_new" OLARAK AYARLANDI
SCHEMA_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS results_football_new (
  event_id        BIGINT UNSIGNED NOT NULL,
  start_utc       DATE NULL,
  start_time_utc  TIME NULL,
  status          VARCHAR(32) NULL,
  home_team       VARCHAR(128) NULL,
  away_team       VARCHAR(128) NULL,
  ht_home         INT NULL, ht_away         INT NULL,
  ft_home         INT NULL, ft_away         INT NULL,
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
  formation_h     VARCHAR(32) NULL, formation_a     VARCHAR(32) NULL,
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
        print(f"[DB] Bağlantı başarılı ve results_football_new tablosu hazır.")

    def upsert_match(self, row: Dict[str, Any]):
        # INSERT SORGUSU YENİ TABLOYA VE TÜM İSTATİSTİKLERE GÖRE GÜNCELLENDİ
        q = """
        INSERT INTO results_football_new
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
          start_utc = VALUES(start_utc), start_time_utc = VALUES(start_time_utc),
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
          formation_h = VALUES(formation_h), formation_a = VALUES(formation_a);
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

    # --- İSTATİSTİK ÇEKME FONKSİYONLARI BURAYA EKLENDİ ---
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
        referee = None
        if ev and isinstance(ev.get("referee"), dict):
            referee = ev["referee"].get("name")
        return {"referee": referee}

    def get_incidents_and_lineups(self, event_id) -> Dict[str, Any]:
        res = {"formation_h": None, "formation_a": None}
        lin_url = f"{self.api['base_url']}/event/{event_id}/lineups"
        lin_data = self._fetch_json(lin_url)
        if lin_data:
            home_data = lin_data.get("home")
            if isinstance(home_data, dict):
                res["formation_h"] = home_data.get("formation")
            away_data = lin_data.get("away")
            if isinstance(away_data, dict):
                res["formation_a"] = away_data.get("formation")
        return res

    # --- ORAN ÇEKME FONKSİYONU ---
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

        home_score = ev.get("homeScore", {})
        away_score = ev.get("awayScore", {})

        row = {
            "event_id": ev.get("id"),
            "start_utc": dt_tr.strftime("%Y-%m-%d") if dt_tr else None,
            "start_time_utc": dt_tr.strftime("%H:%M:%S") if dt_tr else None,
            "status": status,
            "home_team": (ev.get("homeTeam") or {}).get("name"),
            "away_team": (ev.get("awayTeam") or {}).get("name"),
            "ht_home": home_score.get("period1"), 
            "ht_away": away_score.get("period1"), 
            "ft_home": home_score.get("current"), 
            "ft_away": away_score.get("current"), 
            "tournament_id": u.get("id") or t.get("id"),
            "tournament_name": u.get("name") or t.get("name"),
            "category_id": cat.get("id"),
            "category_name": cat.get("name"),
            "country": (cat.get("country") or {}).get("name") if isinstance(cat.get("country"), dict) else cat.get("name")
        }
        row.update(extra_data) 
        return row

def main():
    max_retries = 3  
    attempt = 1
    
    while attempt <= max_retries:
        print(f"\n--- ÇALIŞTIRMA DENEMESİ {attempt}/{max_retries} ---")
        
        db = DB(CONFIG["db"])
        sc = Scraper(CONFIG["api"])
        total_processed_in_this_run = 0
        
        try:
            db.connect()
            sc.start()
            
            tz_tr = dt.timezone(dt.timedelta(hours=3))
            
            # --- TARİH ARALIĞI: 23 Ocak 2025 - 24 Mart 2026 ---
            start_date_tr = dt.date(2025, 9, 1)
            end_date_tr = dt.date(2026, 3, 24)
            
            dates_to_fetch = []
            curr_date = start_date_tr
            
            while curr_date <= end_date_tr:
                dates_to_fetch.append(curr_date)
                curr_date += dt.timedelta(days=1)
            
            print(f"[BİLGİ] {start_date_tr} ile {end_date_tr} arasındaki {len(dates_to_fetch)} gün taranacak.")
            
            for fetch_date in dates_to_fetch:
                date_str = fetch_date.strftime("%Y-%m-%d")
                events = sc.by_date(date_str)
                
                p_count = 0
                for ev in events:
                    ts = ev.get("startTimestamp")
                    if not isinstance(ts, int):
                        continue
                    ev_dt_tr = dt.datetime.fromtimestamp(ts, tz_tr)
                    
                    if not (start_date_tr <= ev_dt_tr.date() <= end_date_tr):
                        continue

                    # SADECE BİTMİŞ MAÇLAR KOŞULU
                    status = (ev.get("status", {}).get("type") or "").lower()
                    if status != "finished":
                        continue

                    t_id = ev.get("tournament", {}).get("id")
                    u_id = ev.get("uniqueTournament", {}).get("id")
                    
                    if t_id in MAJOR_TOURNAMENT_IDS or u_id in MAJOR_TOURNAMENT_IDS:
                        ev_id = ev.get("id")
                        
                        # Artık istatistik, hakem ve dizilişler için de yer ayırıyoruz
                        extra_data = {
                            # Oranlar
                            "odds_1": None, "odds_x": None, "odds_2": None, "odds_1x": None, "odds_12": None, "odds_x2": None,
                            "odds_btts_yes": None, "odds_btts_no": None, "odds_o05": None, "odds_u05": None,
                            "odds_o15": None, "odds_u15": None, "odds_o25": None, "odds_u25": None,
                            "odds_o35": None, "odds_u35": None, "odds_o45": None, "odds_u45": None,
                            "odds_o55": None, "odds_u55": None, "odds_o65": None, "odds_u65": None, "odds_o75": None, "odds_u75": None,
                            
                            # İstatistikler, Hakem ve Dizilişler
                            "poss_h": None, "poss_a": None, "corn_h": None, "corn_a": None, 
                            "shot_h": None, "shot_a": None, "shot_on_h": None, "shot_on_a": None,
                            "fouls_h": None, "fouls_a": None, "offsides_h": None, "offsides_a": None,
                            "saves_h": None, "saves_a": None, "passes_h": None, "passes_a": None,
                            "tackles_h": None, "tackles_a": None,
                            "referee": None,
                            "formation_h": None, "formation_a": None
                        }

                        # Tüm API verilerini sırayla çekiyoruz
                        odds = sc.get_odds(ev_id)
                        extra_data.update(odds)
                        
                        stats = sc.get_detailed_stats(ev_id)
                        extra_data.update(stats)
                        
                        details = sc.get_event_details(ev_id)
                        extra_data.update(details)
                        
                        inc_lin = sc.get_incidents_and_lineups(ev_id)
                        extra_data.update(inc_lin)

                        row = sc.parse(ev, extra_data)
                        db.upsert_match(row)
                        p_count += 1
                        total_processed_in_this_run += 1
                
                print(f"[TARAMA] {date_str} için API isteği yapıldı, {p_count} BİTMİŞ maç, oran ve istatistikleriyle işlendi.")
                
        except Exception as e: 
            print(f"[HATA]: İşlem sırasında bir sorun oluştu: {e}")
        finally: 
            sc.stop()
            db.close()

        if total_processed_in_this_run > 0:
            print(f"\n[BAŞARILI] Toplam {total_processed_in_this_run} BİTMİŞ maç tüm verileriyle işlendi. Script sorunsuz tamamlandı.")
            break
        else:
            print(f"\n[UYARI] Bu denemede HİÇBİR bitmiş maç verisi çekilemedi (Toplam 0 maç)!")
            if attempt < max_retries:
                print("15 saniye bekleniyor, ardından tekrar denenecek...")
                time.sleep(15)
            attempt += 1

    if total_processed_in_this_run == 0:
        print("\n[KRİTİK HATA] Maksimum deneme sayısına ulaşıldı ancak hiçbir veri çekilemedi.")
        sys.exit(1)

if __name__ == "__main__":
    main()
