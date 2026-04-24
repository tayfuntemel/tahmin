#!/usr/bin/env python3
import os
import datetime as dt
import time
import json
import sys
import mysql.connector
from typing import Dict, Any, List
from playwright.sync_api import sync_playwright, Route, Request, Response

CONFIG = {
    "db": {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306))
    },
    "scraper": {
        "headless": True,
        "timeout": 60000,
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
}

MAJOR_TOURNAMENT_IDS = {
    1, 2, 3, 72, 84, 36, 37, 3739, 33, 34, 7372, 42, 41, 8343, 810,
    4, 5397, 62, 101, 39, 40, 38, 692, 280, 127, 83, 1449,
    169352, 5071, 28, 6720, 18, 3397, 3708, 82, 3034, 3284, 6230,
    54, 64, 29, 1060, 219, 652, 144, 1339, 1340, 1341, 5, 6, 12, 13, 19, 24, 27, 30, 31, 48, 49, 50, 52, 53, 55, 79, 102, 232, 384,
    681, 877, 1061, 1107, 1427, 10812, 16753, 19232, 34363, 51702, 52653, 58560,
    64475, 71900, 71901, 72112, 78740, 92016, 92614, 143625
}

SCHEMA_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS results_football (
  event_id        BIGINT UNSIGNED NOT NULL,
  start_utc       DATE NULL,
  start_time_utc  TIME NULL,
  match_year      INT NULL,
  match_week      INT NULL,
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
        try:
            self.cur.execute("ALTER TABLE results_football ADD COLUMN match_year INT NULL, ADD COLUMN match_week INT NULL;")
            print("[DB] 'match_year' ve 'match_week' sütunları eklendi.")
        except mysql.connector.Error as err:
            if err.errno != 1060:
                print(f"[DB] Tablo güncellenirken uyarı: {err}")
        print("[DB] Bağlantı başarılı ve tablo hazır.")

    def upsert_match(self, row: Dict[str, Any]):
        q = """
        INSERT INTO results_football
        (event_id, start_utc, start_time_utc, match_year, match_week, status, home_team, away_team,
         odds_1, odds_x, odds_2, odds_1x, odds_12, odds_x2, odds_btts_yes, odds_btts_no, 
         odds_o05, odds_u05, odds_o15, odds_u15, odds_o25, odds_u25, odds_o35, odds_u35, 
         odds_o45, odds_u45, odds_o55, odds_u55, odds_o65, odds_u65, odds_o75, odds_u75,
         tournament_id, tournament_name, category_id, category_name, country)
        VALUES
        (%(event_id)s, %(start_utc)s, %(start_time_utc)s, %(match_year)s, %(match_week)s, %(status)s, %(home_team)s, %(away_team)s,
         %(odds_1)s, %(odds_x)s, %(odds_2)s, %(odds_1x)s, %(odds_12)s, %(odds_x2)s, %(odds_btts_yes)s, %(odds_btts_no)s,
         %(odds_o05)s, %(odds_u05)s, %(odds_o15)s, %(odds_u15)s, %(odds_o25)s, %(odds_u25)s, %(odds_o35)s, %(odds_u35)s, 
         %(odds_o45)s, %(odds_u45)s, %(odds_o55)s, %(odds_u55)s, %(odds_o65)s, %(odds_u65)s, %(odds_o75)s, %(odds_u75)s,
         %(tournament_id)s, %(tournament_name)s, %(category_id)s, %(category_name)s, %(country)s)
        ON DUPLICATE KEY UPDATE
          status = VALUES(status),
          start_utc = VALUES(start_utc), 
          start_time_utc = VALUES(start_time_utc),
          match_year = VALUES(match_year),
          match_week = VALUES(match_week);
        """
        self.cur.execute(q, row)

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()


class Scraper:
    def __init__(self, cfg):
        self.cfg = cfg
        self.playwright = None
        self.browser = None
        self.page = None
        self.captured_api_data = {}  # { url: response_json }

    def start(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.cfg["headless"])
        context = self.browser.new_context(
            user_agent=self.cfg["user_agent"],
            viewport={"width": 1280, "height": 800}
        )
        self.page = context.new_page()

        # --- API yanıtlarını yakalamak için dinleyici ---
        self.page.on("response", self.handle_response)
        # Ana sayfayı ziyaret et (oturum başlat)
        self.page.goto("https://www.sofascore.com/", timeout=self.cfg["timeout"])
        # Sayfanın stabil olması için bekle
        self.page.wait_for_timeout(3000)

    def handle_response(self, response: Response):
        """Belirli API URL'lerini yakala ve JSON'larını kaydet."""
        url = response.url
        if "/scheduled-events/" in url and "/api/v1/" in url:
            try:
                data = response.json()
                self.captured_api_data[url] = data
                print(f"[NETWORK] Yakalanan API: {url.split('?')[0]}")
            except:
                pass

    def get_matches_for_date(self, date_str: str) -> List[Dict]:
        """
        Belirtilen tarih için maçları al.
        Strateji: Tarih sayfasını ziyaret et, network'ten scheduled-events isteğini yakala.
        """
        self.captured_api_data.clear()
        # Sofascore'da tarih URL formatı: https://www.sofascore.com/tr/tarih/{date}
        # veya direkt ana sayfada tarih parametresi ile. Deneyelim.
        url = f"https://www.sofascore.com/tr/tarih/{date_str}"
        try:
            self.page.goto(url, timeout=self.cfg["timeout"])
        except:
            # Alternatif URL dene
            url = f"https://www.sofascore.com/tr/futbol/{date_str}"
            self.page.goto(url, timeout=self.cfg["timeout"])

        # Sayfanın yüklenmesi ve API isteklerinin tamamlanması için bekle
        self.page.wait_for_timeout(5000)

        # Yakalanan API verilerinden scheduled-events olanı bul
        for api_url, data in self.captured_api_data.items():
            if "scheduled-events" in api_url:
                events = data.get("events", [])
                if events:
                    return events
        return []

    def get_odds(self, event_id: int) -> Dict[str, Any]:
        """Maç sayfasını ziyaret et, odds API yanıtını yakala."""
        self.captured_api_data.clear()
        url = f"https://www.sofascore.com/tr/mac/{event_id}"
        self.page.goto(url, timeout=self.cfg["timeout"])
        self.page.wait_for_timeout(5000)

        odds_data = None
        for api_url, data in self.captured_api_data.items():
            if f"/event/{event_id}/odds/" in api_url:
                odds_data = data
                break

        # odds_data yoksa boş dict döndür
        if not odds_data:
            return self._empty_odds()

        # odds_data içinden istediğimiz oranları parse et
        return self._parse_odds(odds_data)

    def _empty_odds(self) -> Dict[str, Any]:
        return {
            "odds_1": None, "odds_x": None, "odds_2": None,
            "odds_1x": None, "odds_12": None, "odds_x2": None,
            "odds_btts_yes": None, "odds_btts_no": None,
            "odds_o05": None, "odds_u05": None, "odds_o15": None, "odds_u15": None,
            "odds_o25": None, "odds_u25": None, "odds_o35": None, "odds_u35": None,
            "odds_o45": None, "odds_u45": None, "odds_o55": None, "odds_u55": None,
            "odds_o65": None, "odds_u65": None, "odds_o75": None, "odds_u75": None
        }

    def _parse_odds(self, data: dict) -> Dict[str, Any]:
        res = self._empty_odds()
        markets = data.get("markets", [])
        for m in markets:
            m_name = m.get("marketName", "").lower()
            choices = m.get("choices", [])
            if "full time" in m_name or "1x2" in m_name:
                for c in choices:
                    name = c.get("name", "").upper()
                    dec = c.get("decimalValue")
                    if name == "1":
                        res["odds_1"] = float(dec) if dec else None
                    elif name == "X":
                        res["odds_x"] = float(dec) if dec else None
                    elif name == "2":
                        res["odds_2"] = float(dec) if dec else None
            elif "double chance" in m_name:
                for c in choices:
                    name = c.get("name", "").upper()
                    dec = c.get("decimalValue")
                    if name == "1X":
                        res["odds_1x"] = float(dec) if dec else None
                    elif name == "12":
                        res["odds_12"] = float(dec) if dec else None
                    elif name == "X2":
                        res["odds_x2"] = float(dec) if dec else None
            elif "both teams to score" in m_name:
                for c in choices:
                    name = c.get("name", "").lower()
                    dec = c.get("decimalValue")
                    if name == "yes":
                        res["odds_btts_yes"] = float(dec) if dec else None
                    elif name == "no":
                        res["odds_btts_no"] = float(dec) if dec else None
            elif "goals" in m_name or "over/under" in m_name:
                for c in choices:
                    line = c.get("line")
                    if not line:
                        continue
                    name = c.get("name", "").lower()
                    dec = c.get("decimalValue")
                    if "over" in name:
                        if line == 0.5: res["odds_o05"] = float(dec) if dec else None
                        elif line == 1.5: res["odds_o15"] = float(dec) if dec else None
                        elif line == 2.5: res["odds_o25"] = float(dec) if dec else None
                        elif line == 3.5: res["odds_o35"] = float(dec) if dec else None
                        elif line == 4.5: res["odds_o45"] = float(dec) if dec else None
                        elif line == 5.5: res["odds_o55"] = float(dec) if dec else None
                        elif line == 6.5: res["odds_o65"] = float(dec) if dec else None
                        elif line == 7.5: res["odds_o75"] = float(dec) if dec else None
                    elif "under" in name:
                        if line == 0.5: res["odds_u05"] = float(dec) if dec else None
                        elif line == 1.5: res["odds_u15"] = float(dec) if dec else None
                        elif line == 2.5: res["odds_u25"] = float(dec) if dec else None
                        elif line == 3.5: res["odds_u35"] = float(dec) if dec else None
                        elif line == 4.5: res["odds_u45"] = float(dec) if dec else None
                        elif line == 5.5: res["odds_u55"] = float(dec) if dec else None
                        elif line == 6.5: res["odds_u65"] = float(dec) if dec else None
                        elif line == 7.5: res["odds_u75"] = float(dec) if dec else None
        return res

    def parse_match(self, ev: Dict) -> Dict[str, Any]:
        ts = ev.get("startTimestamp")
        tz_tr = dt.timezone(dt.timedelta(hours=3))
        dt_tr = dt.datetime.fromtimestamp(ts, tz_tr) if isinstance(ts, int) else None
        match_year, match_week = None, None
        if dt_tr:
            match_year, match_week, _ = dt_tr.isocalendar()

        status = ev.get("status", {}).get("type", "").lower()
        home = ev.get("homeTeam", {})
        away = ev.get("awayTeam", {})
        tournament = ev.get("tournament", {})
        unique_tournament = ev.get("uniqueTournament", {})
        category = tournament.get("category", {}) or unique_tournament.get("category", {})
        # country bilgisi
        country_obj = category.get("country")
        if isinstance(country_obj, dict):
            country = country_obj.get("name")
        else:
            country = category.get("name")  # bazen direkt ülke adı olabilir

        row = {
            "event_id": ev.get("id"),
            "start_utc": dt_tr.strftime("%Y-%m-%d") if dt_tr else None,
            "start_time_utc": dt_tr.strftime("%H:%M:%S") if dt_tr else None,
            "match_year": match_year,
            "match_week": match_week,
            "status": status,
            "home_team": home.get("name"),
            "away_team": away.get("name"),
            "tournament_id": unique_tournament.get("id") or tournament.get("id"),
            "tournament_name": unique_tournament.get("name") or tournament.get("name"),
            "category_id": category.get("id"),
            "category_name": category.get("name"),
            "country": country
        }
        return row

    def stop(self):
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()


def main():
    max_retries = 3
    attempt = 1
    total_processed = 0

    while attempt <= max_retries:
        print(f"\n--- ÇALIŞTIRMA DENEMESİ {attempt}/{max_retries} ---")
        db = DB(CONFIG["db"])
        sc = Scraper(CONFIG["scraper"])
        processed_this_attempt = 0

        try:
            db.connect()
            sc.start()

            tz_tr = dt.timezone(dt.timedelta(hours=3))
            now_tr = dt.datetime.now(tz_tr)
            today_tr = now_tr.date()
            target_dates = [today_tr, today_tr + dt.timedelta(days=1)]

            for single_date in target_dates:
                date_str = single_date.strftime("%Y-%m-%d")
                print(f"\n[TARAMA] {date_str} için maçlar alınıyor...")
                events = sc.get_matches_for_date(date_str)

                if not events:
                    print(f"  {date_str} için maç bulunamadı veya API yakalanamadı.")
                    continue

                count = 0
                for ev in events:
                    # Sadece başlamamış maçlar
                    status = ev.get("status", {}).get("type", "").lower()
                    if status not in ["notstarted", "scheduled"]:
                        continue

                    t_id = ev.get("tournament", {}).get("id")
                    u_id = ev.get("uniqueTournament", {}).get("id")
                    if t_id not in MAJOR_TOURNAMENT_IDS and u_id not in MAJOR_TOURNAMENT_IDS:
                        continue

                    ev_id = ev.get("id")
                    home_name = ev.get("homeTeam", {}).get("name", "?")
                    away_name = ev.get("awayTeam", {}).get("name", "?")
                    print(f"  Maç {ev_id}: {home_name} vs {away_name} -> oranlar çekiliyor...")
                    odds = sc.get_odds(ev_id)
                    row = sc.parse_match(ev)
                    row.update(odds)
                    db.upsert_match(row)
                    count += 1
                    processed_this_attempt += 1
                    total_processed += 1

                print(f"[TAMAMLANDI] {date_str} için {count} maç işlendi.")

            if processed_this_attempt == 0:
                print("[UYARI] Hiç maç işlenemedi, tekrar deneniyor...")
                if attempt < max_retries:
                    time.sleep(15)
                attempt += 1
            else:
                print(f"\n[BAŞARILI] Bu denemede {processed_this_attempt} maç işlendi.")
                break

        except Exception as e:
            print(f"[HATA] {e}")
            attempt += 1
            time.sleep(15)
        finally:
            sc.stop()
            db.close()

    if total_processed == 0:
        print("\n[KRİTİK HATA] Hiç maç verisi çekilemedi.")
        sys.exit(1)


if __name__ == "__main__":
    main()
