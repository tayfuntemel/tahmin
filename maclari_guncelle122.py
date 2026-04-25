#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import datetime as dt
from typing import Dict, Any, List, Optional, Set

import mysql.connector
import requests


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
        "work_minutes": int(os.getenv("WORK_MINUTES", "9")),
        "loop_wait_seconds": int(os.getenv("LOOP_WAIT_SECONDS", "60")),
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


STAT_KEYS = [
    "poss_h", "poss_a", "corn_h", "corn_a", "shot_h", "shot_a", "shot_on_h", "shot_on_a",
    "fouls_h", "fouls_a", "offsides_h", "offsides_a", "saves_h", "saves_a",
    "passes_h", "passes_a", "tackles_h", "tackles_a",
]


EXTRA_KEYS = STAT_KEYS + ["referee", "formation_h", "formation_a"]


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
        print("[DB] Bağlantı başarılı ve tablo hazır.")

    def _ensure_columns(self) -> None:
        columns = {
            "match_year": "INT NULL",
            "match_week": "INT NULL",
        }
        for column_name, definition in columns.items():
            try:
                self.cur.execute(f"ALTER TABLE results_football ADD COLUMN {column_name} {definition};")
            except mysql.connector.Error:
                pass

    def _ping_or_reconnect(self) -> None:
        try:
            self.conn.ping(reconnect=True, attempts=3, delay=1)
        except mysql.connector.Error:
            print("[DB] Bağlantı koptu, yeniden bağlanılıyor...")
            self.connect()

    def get_finished_event_ids(self) -> Set[int]:
        try:
            self._ping_or_reconnect()
            self.cur.execute("SELECT event_id FROM results_football WHERE status IN ('finished', 'ended')")
            return {int(row[0]) for row in self.cur.fetchall()}
        except Exception as e:
            print(f"[DB HATA] Biten maçlar alınırken hata oluştu: {e}")
            return set()

    def upsert_match(self, row: Dict[str, Any]) -> None:
        self._ping_or_reconnect()

        q = """
        INSERT INTO results_football
        (event_id, start_utc, start_time_utc, match_year, match_week, status, home_team, away_team,
         ht_home, ht_away, ft_home, ft_away,
         poss_h, poss_a, corn_h, corn_a, shot_h, shot_a, shot_on_h, shot_on_a,
         fouls_h, fouls_a, offsides_h, offsides_a, saves_h, saves_a, passes_h, passes_a, tackles_h, tackles_a,
         referee, formation_h, formation_a,
         tournament_id, tournament_name, category_id, category_name, country)
        VALUES
        (%(event_id)s, %(start_utc)s, %(start_time_utc)s, %(match_year)s, %(match_week)s, %(status)s, %(home_team)s, %(away_team)s,
         %(ht_home)s, %(ht_away)s, %(ft_home)s, %(ft_away)s,
         %(poss_h)s, %(poss_a)s, %(corn_h)s, %(corn_a)s, %(shot_h)s, %(shot_a)s, %(shot_on_h)s, %(shot_on_a)s,
         %(fouls_h)s, %(fouls_a)s, %(offsides_h)s, %(offsides_a)s, %(saves_h)s, %(saves_a)s, %(passes_h)s, %(passes_a)s, %(tackles_h)s, %(tackles_a)s,
         %(referee)s, %(formation_h)s, %(formation_a)s,
         %(tournament_id)s, %(tournament_name)s, %(category_id)s, %(category_name)s, %(country)s)
        ON DUPLICATE KEY UPDATE
          status = VALUES(status),
          start_utc = VALUES(start_utc),
          start_time_utc = VALUES(start_time_utc),
          match_year = VALUES(match_year),
          match_week = VALUES(match_week),
          home_team = VALUES(home_team),
          away_team = VALUES(away_team),
          ht_home = VALUES(ht_home),
          ht_away = VALUES(ht_away),
          ft_home = VALUES(ft_home),
          ft_away = VALUES(ft_away),
          poss_h = VALUES(poss_h),
          poss_a = VALUES(poss_a),
          corn_h = VALUES(corn_h),
          corn_a = VALUES(corn_a),
          shot_h = VALUES(shot_h),
          shot_a = VALUES(shot_a),
          shot_on_h = VALUES(shot_on_h),
          shot_on_a = VALUES(shot_on_a),
          fouls_h = VALUES(fouls_h),
          fouls_a = VALUES(fouls_a),
          offsides_h = VALUES(offsides_h),
          offsides_a = VALUES(offsides_a),
          saves_h = VALUES(saves_h),
          saves_a = VALUES(saves_a),
          passes_h = VALUES(passes_h),
          passes_a = VALUES(passes_a),
          tackles_h = VALUES(tackles_h),
          tackles_a = VALUES(tackles_a),
          referee = VALUES(referee),
          formation_h = VALUES(formation_h),
          formation_a = VALUES(formation_a),
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
        self.scraper_api_key = os.getenv("SCRAPER_API_KEY")
        if not self.scraper_api_key:
            print("[UYARI] SCRAPER_API_KEY ortam değişkeni boş görünüyor.")

    def start(self) -> None:
        print("[SİSTEM] ScraperAPI aktif! Maç güncelleme botu başlatılıyor...")

    def stop(self) -> None:
        print("[SİSTEM] ScraperAPI bağlantısı sorunsuz kapatıldı.")

    def _fetch_json(self, target_url: str) -> dict:
        time.sleep(self.cfg.get("sleep_between_requests", 1.0))

        try:
            payload = {
                "api_key": self.scraper_api_key,
                "url": target_url,
            }

            url_son_kisim = target_url.split("/")[-1]
            if len(url_son_kisim) > 20:
                print(f"[İSTEK] ...{url_son_kisim[-20:]} -> ScraperAPI üzerinden isteniyor...")
            else:
                print(f"[İSTEK] {url_son_kisim} -> ScraperAPI üzerinden isteniyor...")

            response = requests.get(
                "http://api.scraperapi.com",
                params=payload,
                timeout=self.cfg.get("request_timeout", 60),
            )

            if response.status_code != 200:
                print(f"[SCRAPERAPI HATASI] HTTP {response.status_code} - {response.text[:200]}")
                return {}

            try:
                data = response.json()
            except ValueError:
                print(f"[JSON HATASI] Cevap JSON değil: {response.text[:200]}")
                return {}

            if isinstance(data, dict) and "error" in data:
                print(f"[SOFASCORE ENGELİ] Hedef: {url_son_kisim} -> {data.get('error')}")
                return {}

            return data if isinstance(data, dict) else {}

        except requests.RequestException as e:
            print(f"[AĞ HATASI] URL: {target_url} | Detay: {e}")
            return {}
        except Exception as e:
            print(f"[SİSTEM HATASI] URL: {target_url} | Detay: {e}")
            return {}

    @staticmethod
    def empty_extra_data() -> Dict[str, Optional[Any]]:
        return {key: None for key in EXTRA_KEYS}

    @staticmethod
    def _to_int_or_none(value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            cleaned = value.strip().replace("%", "").replace(",", "")
            if cleaned == "":
                return None
            try:
                return int(float(cleaned))
            except ValueError:
                return None
        return None

    def get_detailed_stats(self, event_id: int) -> Dict[str, Optional[int]]:
        url = f"https://api.sofascore.com/api/v1/event/{event_id}/statistics"
        data = self._fetch_json(url)

        res = {key: None for key in STAT_KEYS}
        statistics = data.get("statistics", []) if isinstance(data, dict) else []
        if not statistics or not isinstance(statistics, list):
            return res

        # Genelde 0. index "ALL" periyodudur. Eğer yoksa tüm blokları dolaşır.
        stat_blocks = statistics[:1] if statistics else []
        if not stat_blocks:
            stat_blocks = statistics

        stat_name_map = {
            "ball possession": ("poss_h", "poss_a"),
            "corner kicks": ("corn_h", "corn_a"),
            "corners": ("corn_h", "corn_a"),
            "total shots": ("shot_h", "shot_a"),
            "shots": ("shot_h", "shot_a"),
            "shots on target": ("shot_on_h", "shot_on_a"),
            "fouls": ("fouls_h", "fouls_a"),
            "offsides": ("offsides_h", "offsides_a"),
            "goalkeeper saves": ("saves_h", "saves_a"),
            "saves": ("saves_h", "saves_a"),
            "passes": ("passes_h", "passes_a"),
            "tackles": ("tackles_h", "tackles_a"),
        }

        for block in stat_blocks:
            groups = block.get("groups", []) if isinstance(block, dict) else []
            if not isinstance(groups, list):
                continue

            for group in groups:
                items = group.get("statisticsItems", []) if isinstance(group, dict) else []
                if not isinstance(items, list):
                    continue

                for item in items:
                    if not isinstance(item, dict):
                        continue

                    name = str(item.get("name", "")).strip().lower()
                    if name not in stat_name_map:
                        continue

                    home_key, away_key = stat_name_map[name]
                    home_value = self._to_int_or_none(item.get("homeValue"))
                    away_value = self._to_int_or_none(item.get("awayValue"))
                    res[home_key] = home_value
                    res[away_key] = away_value

        return res

    def get_event_details(self, event_id: int) -> Dict[str, Optional[str]]:
        url = f"https://api.sofascore.com/api/v1/event/{event_id}"
        data = self._fetch_json(url)
        event = data.get("event", {}) if isinstance(data, dict) else {}
        referee = None
        if isinstance(event, dict):
            referee_data = event.get("referee") or {}
            if isinstance(referee_data, dict):
                referee = referee_data.get("name")
        return {"referee": referee}

    def get_lineups(self, event_id: int) -> Dict[str, Optional[str]]:
        url = f"https://api.sofascore.com/api/v1/event/{event_id}/lineups"
        data = self._fetch_json(url)

        res = {"formation_h": None, "formation_a": None}
        if not isinstance(data, dict) or not data:
            return res

        home = data.get("home") or {}
        away = data.get("away") or {}
        if isinstance(home, dict):
            res["formation_h"] = home.get("formation")
        if isinstance(away, dict):
            res["formation_a"] = away.get("formation")
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
        home_score = ev.get("homeScore") or {}
        away_score = ev.get("awayScore") or {}
        tournament = ev.get("tournament") or {}
        unique_tournament = ev.get("uniqueTournament") or {}
        category = (tournament.get("category") or {}) if "category" in tournament else (unique_tournament.get("category") or {})

        country_value = None
        if isinstance(category.get("country"), dict):
            country_value = category.get("country", {}).get("name")
        else:
            country_value = category.get("name")

        ft_home = home_score.get("normaltime") if home_score.get("normaltime") is not None else home_score.get("current")
        ft_away = away_score.get("normaltime") if away_score.get("normaltime") is not None else away_score.get("current")

        row = {
            "event_id": ev.get("id"),
            "start_utc": dt_tr.strftime("%Y-%m-%d") if dt_tr else None,
            "start_time_utc": dt_tr.strftime("%H:%M:%S") if dt_tr else None,
            "match_year": match_year,
            "match_week": match_week,
            "status": status,
            "home_team": (ev.get("homeTeam") or {}).get("name"),
            "away_team": (ev.get("awayTeam") or {}).get("name"),
            "ht_home": home_score.get("period1"),
            "ht_away": away_score.get("period1"),
            "ft_home": ft_home,
            "ft_away": ft_away,
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


def should_process_match(ev: Dict[str, Any], target_dates_tr: List[dt.date], tz_tr: dt.timezone) -> bool:
    ts = ev.get("startTimestamp")
    if not isinstance(ts, int):
        return False

    ev_dt_tr = dt.datetime.fromtimestamp(ts, tz_tr)
    if ev_dt_tr.date() not in target_dates_tr:
        return False

    if not is_major_tournament(ev):
        return False

    status = (ev.get("status", {}).get("type") or "").lower()
    return status in {"inprogress", "finished", "ended"}


def collect_match_extra_data(scraper: Scraper, event_id: int) -> Dict[str, Any]:
    extra_data = scraper.empty_extra_data()

    stats = scraper.get_detailed_stats(event_id)
    extra_data.update(stats)

    details = scraper.get_event_details(event_id)
    extra_data.update(details)

    lineups = scraper.get_lineups(event_id)
    extra_data.update(lineups)

    return extra_data


def run_loop() -> int:
    db = DB(CONFIG["db"])
    scraper = Scraper(CONFIG["scraper"])
    total_processed_all_loops = 0

    start_time = time.time()
    work_seconds = CONFIG["runtime"]["work_minutes"] * 60
    loop_wait_seconds = CONFIG["runtime"]["loop_wait_seconds"]

    try:
        db.connect()
        scraper.start()

        finished_events_in_db = db.get_finished_event_ids()
        print(f"[BİLGİ] Veritabanından {len(finished_events_in_db)} adet bitmiş maç önbelleğe alındı. Bunlar tekrar güncellenmeyecek.")

        while True:
            tz_tr = dt.timezone(dt.timedelta(hours=3))
            today_tr = dt.datetime.now(tz_tr).date()
            target_dates_tr = [today_tr - dt.timedelta(days=1), today_tr]

            total_processed_this_loop = 0

            # Gün sınırı kaymalarını kaçırmamak için -2, -1, 0 gün API'den çekiliyor,
            # fakat Türkiye saatine göre sadece dün ve bugün oynanan maçlar işleniyor.
            for offset in [-2, -1, 0]:
                fetch_date = today_tr + dt.timedelta(days=offset)
                date_str = fetch_date.strftime("%Y-%m-%d")
                events = scraper.by_date(date_str)

                processed_for_date = 0
                skipped_finished = 0

                for ev in events:
                    if not should_process_match(ev, target_dates_tr, tz_tr):
                        continue

                    event_id = ev.get("id")
                    if not event_id:
                        continue

                    status = (ev.get("status", {}).get("type") or "").lower()

                    if status in {"finished", "ended"} and int(event_id) in finished_events_in_db:
                        skipped_finished += 1
                        continue

                    extra_data = collect_match_extra_data(scraper, int(event_id))
                    row = scraper.parse(ev, extra_data)
                    db.upsert_match(row)

                    processed_for_date += 1
                    total_processed_this_loop += 1
                    total_processed_all_loops += 1

                    if status in {"finished", "ended"}:
                        finished_events_in_db.add(int(event_id))

                if processed_for_date > 0 or skipped_finished > 0:
                    print(
                        f"[{time.strftime('%H:%M:%S')}] {date_str} -> "
                        f"{processed_for_date} maç güncellendi, {skipped_finished} bitmiş maç atlandı."
                    )

            print(f"[{time.strftime('%H:%M:%S')}] Tur tamamlandı. Toplam {total_processed_this_loop} yeni maç/istatistik işlendi.")

            elapsed = time.time() - start_time
            if elapsed >= work_seconds:
                print(f"\n[BİLGİ] {CONFIG['runtime']['work_minutes']} dakikalık çalışma süresi doldu. Güvenle kapanıyor...")
                break

            remaining_minutes = (work_seconds - elapsed) / 60
            print(f"[BİLGİ] {loop_wait_seconds} saniye bekleniyor... Kalan çalışma süresi: {remaining_minutes:.1f} dakika\n")
            time.sleep(loop_wait_seconds)

        return total_processed_all_loops

    finally:
        scraper.stop()
        db.close()


def main() -> None:
    try:
        total_processed = run_loop()
        print(f"\n[SONUÇ] Script tamamlandı. Toplam {total_processed} maç/istatistik işlendi.")
        if total_processed == 0:
            print("[UYARI] Bu çalıştırmada uygun inprogress/finished maç bulunamadı veya hepsi zaten işlenmişti.")
    except Exception as e:
        print(f"[KRİTİK HATA] Script beklenmeyen hata ile durdu: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
