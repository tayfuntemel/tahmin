#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import datetime as dt
from typing import Dict, Any, List, Optional, Set

import mysql.connector
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


CONFIG = {
    "db": {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306)),
    },
    "scraper": {
        "sleep_between_requests": float(os.getenv("SLEEP_BETWEEN_REQUESTS", "1.0")),
        "request_timeout": int(os.getenv("REQUEST_TIMEOUT", "60")),
    },
    "runtime": {
        "max_retries": int(os.getenv("MAX_RETRIES", "3")),
        "retry_wait_seconds": int(os.getenv("RETRY_WAIT_SECONDS", "15")),
    },
}


MAJOR_TOURNAMENT_IDS = {
    1, 2, 3, 72, 84, 36, 37, 3739, 33, 34, 7372, 42, 41, 8343, 810,
    4, 5397, 62, 101, 39, 40, 38, 692, 280, 127, 83, 1449,
    169352, 5071, 28, 6720, 18, 3397, 3708, 82, 3034, 3284, 6230,
    54, 64, 29, 1060, 219, 652, 144, 1339, 1340, 1341,
    5, 6, 12, 13, 19, 24, 27, 30, 31, 48, 49, 50, 52, 53, 55, 79,
    102, 232, 384, 681, 877, 1061, 1107, 1427, 10812, 16753, 19232,
    34363, 51702, 52653, 58560, 64475, 71900, 71901, 72112, 78740,
    92016, 92614, 143625,
}


ODDS_KEYS = [
    "odds_1", "odds_x", "odds_2", "odds_1x", "odds_12", "odds_x2",
    "odds_btts_yes", "odds_btts_no",
    "odds_o05", "odds_u05", "odds_o15", "odds_u15", "odds_o25", "odds_u25",
    "odds_o35", "odds_u35", "odds_o45", "odds_u45", "odds_o55", "odds_u55",
    "odds_o65", "odds_u65", "odds_o75", "odds_u75",
]


def _safe_table_name(raw_name: Optional[str]) -> str:
    name = (raw_name or "sofascore_fixtures_direct").strip()
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if not name or any(ch not in allowed for ch in name):
        raise ValueError("FIXTURE_TABLE_NAME sadece harf, rakam ve alt çizgi içerebilir.")
    return name


TABLE_NAME = _safe_table_name(os.getenv("FIXTURE_TABLE_NAME"))


SCHEMA_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
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
  poss_h          INT NULL,
  poss_a          INT NULL,
  corn_h          INT NULL,
  corn_a          INT NULL,
  shot_h          INT NULL,
  shot_a          INT NULL,
  shot_on_h       INT NULL,
  shot_on_a       INT NULL,
  fouls_h         INT NULL,
  fouls_a         INT NULL,
  offsides_h      INT NULL,
  offsides_a      INT NULL,
  saves_h         INT NULL,
  saves_a         INT NULL,
  passes_h        INT NULL,
  passes_a        INT NULL,
  tackles_h       INT NULL,
  tackles_a       INT NULL,
  referee         VARCHAR(128) NULL,
  formation_h     VARCHAR(32) NULL,
  formation_a     VARCHAR(32) NULL,
  odds_1          FLOAT NULL,
  odds_x          FLOAT NULL,
  odds_2          FLOAT NULL,
  odds_1x         FLOAT NULL,
  odds_12         FLOAT NULL,
  odds_x2         FLOAT NULL,
  odds_btts_yes   FLOAT NULL,
  odds_btts_no    FLOAT NULL,
  odds_o05        FLOAT NULL,
  odds_u05        FLOAT NULL,
  odds_o15        FLOAT NULL,
  odds_u15        FLOAT NULL,
  odds_o25        FLOAT NULL,
  odds_u25        FLOAT NULL,
  odds_o35        FLOAT NULL,
  odds_u35        FLOAT NULL,
  odds_o45        FLOAT NULL,
  odds_u45        FLOAT NULL,
  odds_o55        FLOAT NULL,
  odds_u55        FLOAT NULL,
  odds_o65        FLOAT NULL,
  odds_u65        FLOAT NULL,
  odds_o75        FLOAT NULL,
  odds_u75        FLOAT NULL,
  tournament_id   INT NULL,
  tournament_name VARCHAR(128) NULL,
  category_id     INT NULL,
  category_name   VARCHAR(128) NULL,
  country         VARCHAR(64) NULL,
  last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (event_id),
  KEY idx_date (start_utc),
  KEY idx_status (status),
  KEY idx_tournament (tournament_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


class DB:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.conn = None
        self.cur = None

    def connect(self) -> None:
        self.close(silent=True)
        self.conn = mysql.connector.connect(**self.cfg)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        self.cur.execute(SCHEMA_CREATE_TABLE)
        self._ensure_columns()
        print(f"[DB] Bağlantı başarılı ve tablo hazır: {TABLE_NAME}")

    def _ensure_columns(self) -> None:
        columns = {
            "match_year": "INT NULL",
            "match_week": "INT NULL",
        }
        for col, definition in columns.items():
            try:
                self.cur.execute(f"ALTER TABLE `{TABLE_NAME}` ADD COLUMN {col} {definition};")
            except mysql.connector.Error:
                pass

    def _ping_or_reconnect(self) -> None:
        try:
            self.conn.ping(reconnect=True, attempts=3, delay=1)
        except mysql.connector.Error:
            print("[DB] Bağlantı koptu, yeniden bağlanılıyor...")
            self.connect()

    def get_existing_fixture_event_ids(self) -> Set[int]:
        """Bugün/yarın için zaten kaydedilmiş başlamamış maçları önbelleğe alır."""
        try:
            self._ping_or_reconnect()
            self.cur.execute(
                f"""
                SELECT event_id
                FROM `{TABLE_NAME}`
                WHERE status IN ('notstarted', 'scheduled')
                  AND start_utc >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
                  AND start_utc <= DATE_ADD(CURDATE(), INTERVAL 2 DAY)
                """
            )
            return {int(row[0]) for row in self.cur.fetchall()}
        except Exception as e:
            print(f"[DB HATA] Kayıtlı fikstür ID'leri alınamadı: {e}")
            return set()

    def upsert_match(self, row: Dict[str, Any]) -> None:
        self._ping_or_reconnect()

        q = f"""
        INSERT INTO `{TABLE_NAME}`
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
          match_week = VALUES(match_week),
          home_team = VALUES(home_team),
          away_team = VALUES(away_team),
          odds_1 = VALUES(odds_1),
          odds_x = VALUES(odds_x),
          odds_2 = VALUES(odds_2),
          odds_1x = VALUES(odds_1x),
          odds_12 = VALUES(odds_12),
          odds_x2 = VALUES(odds_x2),
          odds_btts_yes = VALUES(odds_btts_yes),
          odds_btts_no = VALUES(odds_btts_no),
          odds_o05 = VALUES(odds_o05),
          odds_u05 = VALUES(odds_u05),
          odds_o15 = VALUES(odds_o15),
          odds_u15 = VALUES(odds_u15),
          odds_o25 = VALUES(odds_o25),
          odds_u25 = VALUES(odds_u25),
          odds_o35 = VALUES(odds_o35),
          odds_u35 = VALUES(odds_u35),
          odds_o45 = VALUES(odds_o45),
          odds_u45 = VALUES(odds_u45),
          odds_o55 = VALUES(odds_o55),
          odds_u55 = VALUES(odds_u55),
          odds_o65 = VALUES(odds_o65),
          odds_u65 = VALUES(odds_u65),
          odds_o75 = VALUES(odds_o75),
          odds_u75 = VALUES(odds_u75),
          tournament_id = VALUES(tournament_id),
          tournament_name = VALUES(tournament_name),
          category_id = VALUES(category_id),
          category_name = VALUES(category_name),
          country = VALUES(country);
        """
        self.cur.execute(q, row)

    def close(self, silent: bool = False) -> None:
        try:
            if self.cur:
                self.cur.close()
        except Exception as e:
            if not silent:
                print(f"[DB UYARI] Cursor kapatılamadı: {e}")
        try:
            if self.conn:
                self.conn.close()
        except Exception as e:
            if not silent:
                print(f"[DB UYARI] Bağlantı kapatılamadı: {e}")
        self.cur = None
        self.conn = None


class Scraper:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.base_url = "https://api.sofascore.com/api/v1"
        self.session = requests.Session()

        retry = Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=1.2,
            status_forcelist=(403, 408, 429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.session.headers.update(
            {
                "User-Agent": os.getenv(
                    "SOFASCORE_USER_AGENT",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36",
                ),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": os.getenv("SOFASCORE_ACCEPT_LANGUAGE", "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7"),
                "Origin": "https://www.sofascore.com",
                "Referer": "https://www.sofascore.com/",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
            }
        )

    def start(self) -> None:
        print("[SİSTEM] Doğrudan SofaScore API modu aktif. ScraperAPI kullanılmıyor.")

    def stop(self) -> None:
        try:
            self.session.close()
        finally:
            print("[SİSTEM] HTTP oturumu kapatıldı.")

    def _fetch_json(self, target_url: str) -> dict:
        time.sleep(self.cfg.get("sleep_between_requests", 1.0))

        endpoint = target_url.split("/api/v1/")[-1]
        print(f"[İSTEK] {endpoint} -> doğrudan SofaScore API üzerinden isteniyor...")

        try:
            response = self.session.get(
                target_url,
                timeout=self.cfg.get("request_timeout", 60),
            )

            if response.status_code != 200:
                print(f"[SOFASCORE HATASI] HTTP {response.status_code} - {response.text[:300]}")
                return {}

            content_type = response.headers.get("Content-Type", "")
            if "json" not in content_type.lower() and not response.text.lstrip().startswith(("{", "[")):
                print(f"[JSON HATASI] Cevap JSON görünmüyor. Content-Type={content_type} | {response.text[:200]}")
                return {}

            data = response.json()
            return data if isinstance(data, dict) else {}

        except requests.RequestException as e:
            print(f"[AĞ HATASI] URL: {target_url} | Detay: {e}")
            return {}
        except ValueError:
            print(f"[JSON HATASI] URL: {target_url} | Cevap JSON parse edilemedi.")
            return {}
        except Exception as e:
            print(f"[SİSTEM HATASI] URL: {target_url} | Detay: {e}")
            return {}

    @staticmethod
    def empty_odds() -> Dict[str, Optional[float]]:
        return {key: None for key in ODDS_KEYS}

    @staticmethod
    def _decimal_odd(choice: Dict[str, Any]) -> Optional[float]:
        decimal_value = choice.get("decimalValue")
        if decimal_value not in (None, ""):
            try:
                return float(decimal_value)
            except (TypeError, ValueError):
                pass

        fractional_value = choice.get("fractionalValue")
        if fractional_value in (None, ""):
            return None

        frac = str(fractional_value).strip().upper()
        if frac == "EVS":
            return 2.0

        try:
            if "/" in frac:
                numerator, denominator = frac.split("/", 1)
                return round((float(numerator) / float(denominator)) + 1, 2)
            return float(frac)
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    def get_odds(self, event_id: int) -> Dict[str, Optional[float]]:
        url = f"https://api.sofascore.com/api/v1/event/{event_id}/odds/1/all"
        data = self._fetch_json(url)
        res = self.empty_odds()

        markets = data.get("markets", []) if isinstance(data, dict) else []
        if not isinstance(markets, list):
            return res

        goal_lines = ["0.5", "1.5", "2.5", "3.5", "4.5", "5.5", "6.5", "7.5"]
        line_to_keys = {
            "0.5": ("odds_o05", "odds_u05"),
            "1.5": ("odds_o15", "odds_u15"),
            "2.5": ("odds_o25", "odds_u25"),
            "3.5": ("odds_o35", "odds_u35"),
            "4.5": ("odds_o45", "odds_u45"),
            "5.5": ("odds_o55", "odds_u55"),
            "6.5": ("odds_o65", "odds_u65"),
            "7.5": ("odds_o75", "odds_u75"),
        }

        for market in markets:
            if not isinstance(market, dict):
                continue

            market_name = str(market.get("marketName", "")).strip().lower()
            choices = market.get("choices", [])
            if not isinstance(choices, list):
                continue

            market_json = json.dumps(market, ensure_ascii=False).lower()

            if "full time" in market_name or "full-time" in market_name or "1x2" in market_name:
                for choice in choices:
                    name = str(choice.get("name", "")).strip().upper()
                    value = self._decimal_odd(choice)
                    if name == "1":
                        res["odds_1"] = value
                    elif name == "X":
                        res["odds_x"] = value
                    elif name == "2":
                        res["odds_2"] = value

            elif "double chance" in market_name:
                for choice in choices:
                    name = str(choice.get("name", "")).strip().upper()
                    value = self._decimal_odd(choice)
                    if name == "1X":
                        res["odds_1x"] = value
                    elif name == "12":
                        res["odds_12"] = value
                    elif name == "X2":
                        res["odds_x2"] = value

            elif "both teams to score" in market_name:
                for choice in choices:
                    name = str(choice.get("name", "")).strip().lower()
                    value = self._decimal_odd(choice)
                    if name == "yes":
                        res["odds_btts_yes"] = value
                    elif name == "no":
                        res["odds_btts_no"] = value

            elif (
                ("goals" in market_name or "over/under" in market_name)
                and "half" not in market_name
                and "team" not in market_name
                and "exact" not in market_name
            ):
                for choice in choices:
                    choice_name = str(choice.get("name", "")).strip().lower()
                    choice_json = json.dumps(choice, ensure_ascii=False).lower()

                    line_found = None
                    for line in goal_lines:
                        if line in choice_json:
                            line_found = line
                            break
                    if not line_found:
                        for line in goal_lines:
                            if line in market_json:
                                line_found = line
                                break
                    if not line_found and market.get("isMain"):
                        line_found = "2.5"

                    if not line_found:
                        continue

                    over_key, under_key = line_to_keys[line_found]
                    value = self._decimal_odd(choice)
                    if "over" in choice_name or "üst" in choice_name or choice_name == "o":
                        res[over_key] = value
                    elif "under" in choice_name or "alt" in choice_name or choice_name == "u":
                        res[under_key] = value

        return res

    def by_date(self, date_str: str) -> List[Dict[str, Any]]:
        url = f"https://api.sofascore.com/api/v1/sport/football/scheduled-events/{date_str}"
        data = self._fetch_json(url)
        events = data.get("events", []) if isinstance(data, dict) else []
        return events if isinstance(events, list) else []

    def parse(self, ev: Dict[str, Any], extra_data: Dict[str, Any]) -> Dict[str, Any]:
        ts = ev.get("startTimestamp")
        tz_tr = dt.timezone(dt.timedelta(hours=3))
        dt_tr = dt.datetime.fromtimestamp(ts, tz_tr) if isinstance(ts, int) else None

        match_year, match_week = None, None
        if dt_tr:
            match_year, match_week, _ = dt_tr.isocalendar()

        status = (ev.get("status", {}).get("type") or "").lower()
        tournament = ev.get("tournament") or {}
        unique_tournament = ev.get("uniqueTournament") or {}
        category = (tournament.get("category") or {}) if "category" in tournament else (unique_tournament.get("category") or {})

        country_value = None
        if isinstance(category.get("country"), dict):
            country_value = category.get("country", {}).get("name")
        else:
            country_value = category.get("name")

        row = {
            "event_id": ev.get("id"),
            "start_utc": dt_tr.strftime("%Y-%m-%d") if dt_tr else None,
            "start_time_utc": dt_tr.strftime("%H:%M:%S") if dt_tr else None,
            "match_year": match_year,
            "match_week": match_week,
            "status": status,
            "home_team": (ev.get("homeTeam") or {}).get("name"),
            "away_team": (ev.get("awayTeam") or {}).get("name"),
            "tournament_id": unique_tournament.get("id") or tournament.get("id"),
            "tournament_name": unique_tournament.get("name") or tournament.get("name"),
            "category_id": category.get("id"),
            "category_name": category.get("name"),
            "country": country_value,
        }
        row.update(extra_data)
        return row


def is_major_tournament(ev: Dict[str, Any]) -> bool:
    tournament_id = (ev.get("tournament") or {}).get("id")
    unique_tournament_id = (ev.get("uniqueTournament") or {}).get("id")
    return tournament_id in MAJOR_TOURNAMENT_IDS or unique_tournament_id in MAJOR_TOURNAMENT_IDS


def should_process_fixture(ev: Dict[str, Any], target_dates_tr: List[dt.date], tz_tr: dt.timezone) -> bool:
    ts = ev.get("startTimestamp")
    if not isinstance(ts, int):
        return False

    ev_dt_tr = dt.datetime.fromtimestamp(ts, tz_tr)
    if ev_dt_tr.date() not in target_dates_tr:
        return False

    if not is_major_tournament(ev):
        return False

    status = (ev.get("status", {}).get("type") or "").lower()
    return status in {"notstarted", "scheduled"}


def run_once() -> int:
    db = DB(CONFIG["db"])
    scraper = Scraper(CONFIG["scraper"])
    total_processed = 0

    try:
        db.connect()
        scraper.start()

        existing_fixture_ids = db.get_existing_fixture_event_ids()
        print(f"[BİLGİ] Veritabanından {len(existing_fixture_ids)} adet mevcut fikstür önbelleğe alındı.")

        tz_tr = dt.timezone(dt.timedelta(hours=3))
        today_tr = dt.datetime.now(tz_tr).date()
        target_dates_tr = [today_tr, today_tr + dt.timedelta(days=1)]

        # Gün sınırı kaymalarını kaçırmamak için -1, 0, +1 gün API'den çekiliyor,
        # fakat sadece Türkiye saatine göre bugün ve yarın oynanacak maçlar işleniyor.
        for offset in [-1, 0, 1]:
            fetch_date = today_tr + dt.timedelta(days=offset)
            date_str = fetch_date.strftime("%Y-%m-%d")
            events = scraper.by_date(date_str)

            print(f"[DEBUG] {date_str} tarihi için API'den {len(events)} adet ham etkinlik geldi.")

            processed_for_date = 0
            skipped_existing = 0

            for ev in events:
                if not should_process_fixture(ev, target_dates_tr, tz_tr):
                    continue

                event_id = ev.get("id")
                if not event_id:
                    continue

                # Aynı çalıştırma içinde veya DB'de kayıtlı olan fikstürler tekrar odds isteği yapmasın.
                # Maç saati/takım/status güncellenmesi istenirse bu kontrol kaldırılabilir.
                if int(event_id) in existing_fixture_ids:
                    skipped_existing += 1
                    continue

                extra_data = scraper.empty_odds()
                odds = scraper.get_odds(int(event_id))
                extra_data.update(odds)

                row = scraper.parse(ev, extra_data)
                db.upsert_match(row)

                existing_fixture_ids.add(int(event_id))
                processed_for_date += 1
                total_processed += 1

            print(
                f"[FİKSTÜR] TR bugün/yarın filtresiyle {date_str} tarandı: "
                f"{processed_for_date} yeni maç işlendi, {skipped_existing} kayıtlı maç atlandı."
            )

        return total_processed

    finally:
        scraper.stop()
        db.close()


def main() -> None:
    max_retries = CONFIG["runtime"]["max_retries"]
    retry_wait_seconds = CONFIG["runtime"]["retry_wait_seconds"]
    total_processed_last_attempt = 0

    for attempt in range(1, max_retries + 1):
        print(f"\n--- FİKSTÜR ÇALIŞTIRMA DENEMESİ {attempt}/{max_retries} ---")

        try:
            total_processed_last_attempt = run_once()
        except Exception as e:
            print(f"[HATA] İşlem sırasında sorun oluştu: {e}")
            total_processed_last_attempt = 0

        if total_processed_last_attempt > 0:
            print(f"\n[BAŞARILI] Toplam {total_processed_last_attempt} yeni fikstür maçı işlendi. Script tamamlandı.")
            return

        print("\n[UYARI] Bu denemede yeni fikstür verisi işlenmedi.")
        if attempt < max_retries:
            print(f"[BİLGİ] {retry_wait_seconds} saniye bekleniyor, ardından tekrar denenecek...")
            time.sleep(retry_wait_seconds)

    print("\n[KRİTİK HATA] Maksimum deneme sayısına ulaşıldı ancak yeni veri işlenemedi.")
    sys.exit(1)


if __name__ == "__main__":
    main()
