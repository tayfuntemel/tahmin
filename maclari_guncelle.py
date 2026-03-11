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

# (Orijinal tablonun tamamen aynı şeması)
SCHEMA_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS results_football (
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
        print(f"[DB] Bağlantı başarılı ve tablo hazır.")

    def upsert_match(self, row: Dict[str, Any]):
        q = """
        INSERT INTO results_football
        (event_id, start_utc, start_time_utc, status, home_team, away_team,
         ht_home, ht_away, ft_home, ft_away,
         poss_h, poss_a, corn_h, corn_a, shot_h, shot_a, shot_on_h, shot_on_a,
         fouls_h, fouls_a, offsides_h, offsides_a, saves_h, saves_a, passes_h, passes_a, tackles_h, tackles_a,
         referee, formation_h, formation_a, 
         tournament_id, tournament_name, category_id, category_name, country)
        VALUES
        (%(event_id)s, %(start_utc)s, %(start_time_utc)s, %(status)s, %(home_team)s, %(away_team)s,
         %(ht_home)s, %(ht_away)s, %(ft_home)s, %(ft_away)s,
         %(poss_h)s, %(poss_a)s, %(corn_h)s, %(corn_a)s, %(shot_h)s, %(shot_a)s, %(shot_on_h)s, %(shot_on_a)s,
         %(fouls_h)s, %(fouls_a)s, %(offsides_h)s, %(offsides_a)s, %(saves_h)s, %(saves_a)s, %(passes_h)s, %(passes_a)s, %(tackles_h)s, %(tackles_a)s,
         %(referee)s, %(formation_h)s, %(formation_a)s,
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
        res = {"formation_h": None, "formation_a": None}
        lin_url = f"{self.api['base_url']}/event/{event_id}/lineups"
        lin_data = self._fetch_json(lin_url)
        if lin_data:
            res["formation_h"] = lin_data.get("home", {}).get("formation")
            res["formation_a"] = lin_data.get("away", {}).get("formation")
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
        hs, as_ = ev.get("homeScore", {}) or {}, ev.get("awayScore", {}) or {}
        
        t = ev.get("tournament") or {}; u = ev.get("uniqueTournament") or {}
        cat = (t.get("category") or {}) if "category" in t else (u.get("category") or {})

        row = {
            "event_id": ev.get("id"),
            "start_utc": dt_tr.strftime("%Y-%m-%d") if dt_tr else None,
            "start_time_utc": dt_tr.strftime("%H:%M:%S") if dt_tr else None,
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
        # TÜRKİYE SAATİNE GÖRE DÜN VE BUGÜN HESAPLAMASI (UTC+3)
        tz_tr = dt.timezone(dt.timedelta(hours=3))
        now_tr = dt.datetime.now(tz_tr)
        today_tr = now_tr.date()
        target_dates_tr = [today_tr - dt.timedelta(days=1), today_tr] # Sadece TR Dün ve TR Bugün
        
        # API sınırlarında kalan gece maçları için geniş tarama yapıyoruz (-2, -1, 0)
        for i in [-2, -1, 0]:
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
                
                # SADECE TR SAATİYLE DÜN VE BUGÜN OLANLARI İŞLE
                if ev_dt_tr.date() not in target_dates_tr:
                    continue

                t_id = ev.get("tournament", {}).get("id")
                u_id = ev.get("uniqueTournament", {}).get("id")
                
                if t_id in MAJOR_TOURNAMENT_IDS or u_id in MAJOR_TOURNAMENT_IDS:
                    ev_id = ev.get("id")
                    status = (ev.get("status", {}).get("type") or "").lower()
                    
                    # SADECE OYNANAN VEYA BİTMİŞ MAÇLAR
                    if status in ["inprogress", "finished", "ended"]:
                        extra_data = {
                            "poss_h": None, "poss_a": None, "corn_h": None, "corn_a": None, 
                            "shot_h": None, "shot_a": None, "shot_on_h": None, "shot_on_a": None,
                            "fouls_h": None, "fouls_a": None, "offsides_h": None, "offsides_a": None,
                            "saves_h": None, "saves_a": None, "passes_h": None, "passes_a": None,
                            "tackles_h": None, "tackles_a": None,
                            "referee": None,
                            "formation_h": None, "formation_a": None
                        }

                        # Detaylı İstatistikleri çek
                        stats = sc.get_detailed_stats(ev_id)
                        extra_data.update(stats)
                        
                        details = sc.get_event_details(ev_id)
                        extra_data.update(details)
                        
                        inc_lin = sc.get_incidents_and_lineups(ev_id)
                        extra_data.update(inc_lin)

                        row = sc.parse(ev, extra_data)
                        db.upsert_match(row)
                        p_count += 1
            
            print(f"[BİTMİŞ/CANLI MAÇLAR] TR Saati Taraması: {date_str} için API isteği yapıldı, {p_count} uygun maç işlendi.")
            
    except Exception as e: 
        print(f"[HATA]: {e}")
    finally: 
        sc.stop()
        db.close()

if __name__ == "__main__":
    main()
