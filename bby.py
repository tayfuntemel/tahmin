#!/usr/bin/env python3
import os
import datetime as dt
import time
import json
import sys
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
    "scraper": {
        "headless": True,          # GitHub’da headless çalışır
        "timeout": 90000,          # 90 saniye (yavaş bağlantılar için)
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
}

# Aynı MAJOR_TOURNAMENT_IDS ve SCHEMA_CREATE_TABLE (değişmedi)
# ... (buraya sizin orijinal MAJOR_TOURNAMENT_IDS ve SCHEMA_CREATE_TABLE’ınızı koyun)

class DB:
    # ... (öncekiyle aynı, değişiklik yok)

class Scraper:
    def __init__(self, cfg):
        self.cfg = cfg
        self.playwright = None
        self.browser = None
        self.page = None

    def start(self):
        self.playwright = sync_playwright().start()
        # Chromium’u stealth eklentisi olmadan da çalıştırabiliriz, ancak daha gizli olması için argüman ekleyelim
        self.browser = self.playwright.chromium.launch(
            headless=self.cfg["headless"],
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = self.browser.new_context(
            user_agent=self.cfg["user_agent"],
            viewport={"width": 1280, "height": 800},
            locale="tr-TR"
        )
        self.page = context.new_page()
        # Ana sayfayı ziyaret et (çerezleri başlat)
        self.page.goto("https://www.sofascore.com/", timeout=self.cfg["timeout"])
        self.page.wait_for_timeout(3000)

    def get_matches_for_date(self, date_str: str) -> List[Dict]:
        """Tarih sayfasını açar ve içindeki maç verilerini JSON olarak döndürür."""
        # Sofascore’da tarih URL’si: /tr/tarih/2026-04-24
        url = f"https://www.sofascore.com/tr/tarih/{date_str}"
        try:
            self.page.goto(url, timeout=self.cfg["timeout"])
        except:
            # Alternatif URL dene
            url = f"https://www.sofascore.com/tr/futbol/{date_str}"
            self.page.goto(url, timeout=self.cfg["timeout"])

        # Sayfanın yüklenmesi için bekle (özellikle dynamic içerik)
        self.page.wait_for_timeout(5000)

        # Sayfadaki tüm script etiketlerini al
        scripts = self.page.evaluate("""() => {
            const results = [];
            const scripts = document.querySelectorAll('script[type="application/json"]');
            for (let s of scripts) {
                try {
                    const data = JSON.parse(s.innerText);
                    results.push(data);
                } catch(e) {}
            }
            return results;
        }""")

        # İçinde "scheduledEvents" veya "events" olan JSON’u bul
        for data in scripts:
            events = self._extract_events_from_json(data)
            if events:
                return events

        # Eğer bulamazsa, `window.__NUXT__` değişkenini dene
        try:
            nuxt = self.page.evaluate("() => window.__NUXT__")
            if nuxt:
                events = self._extract_events_from_json(nuxt)
                if events:
                    return events
        except:
            pass

        return []

    def _extract_events_from_json(self, data):
        """Rekürsif olarak 'scheduledEvents' veya 'events' anahtarını ara."""
        if isinstance(data, dict):
            if "scheduledEvents" in data and isinstance(data["scheduledEvents"], list):
                return data["scheduledEvents"]
            if "events" in data and isinstance(data["events"], list):
                return data["events"]
            for v in data.values():
                res = self._extract_events_from_json(v)
                if res:
                    return res
        elif isinstance(data, list):
            for item in data:
                res = self._extract_events_from_json(item)
                if res:
                    return res
        return None

    def get_odds(self, event_id: int) -> Dict[str, Any]:
        """Maç detay sayfasına gidip oranları al."""
        url = f"https://www.sofascore.com/tr/mac/{event_id}"
        self.page.goto(url, timeout=self.cfg["timeout"])
        self.page.wait_for_timeout(5000)

        # Sayfadaki JSON script etiketlerini ve __NUXT__’u tara
        scripts = self.page.evaluate("""() => {
            const results = [];
            const scripts = document.querySelectorAll('script[type="application/json"]');
            for (let s of scripts) {
                try {
                    const data = JSON.parse(s.innerText);
                    results.push(data);
                } catch(e) {}
            }
            return results;
        }""")

        odds_data = None
        for data in scripts:
            # Oran verileri genelde "oddsMarketGroups" içinde gelir
            if "oddsMarketGroups" in str(data):
                odds_data = data
                break
        if not odds_data:
            try:
                nuxt = self.page.evaluate("() => window.__NUXT__")
                if nuxt:
                    odds_data = nuxt
            except:
                pass

        if not odds_data:
            return self._empty_odds()

        return self._parse_odds(odds_data)

    def _empty_odds(self):
        return {
            "odds_1": None, "odds_x": None, "odds_2": None, "odds_1x": None,
            "odds_12": None, "odds_x2": None, "odds_btts_yes": None, "odds_btts_no": None,
            "odds_o05": None, "odds_u05": None, "odds_o15": None, "odds_u15": None,
            "odds_o25": None, "odds_u25": None, "odds_o35": None, "odds_u35": None,
            "odds_o45": None, "odds_u45": None, "odds_o55": None, "odds_u55": None,
            "odds_o65": None, "odds_u65": None, "odds_o75": None, "odds_u75": None
        }

    def _parse_odds(self, data: dict) -> Dict[str, Any]:
        res = self._empty_odds()
        # Derin arama ile "oddsMarketGroups" listesini bul
        market_groups = self._deep_search(data, "oddsMarketGroups")
        if not market_groups:
            return res

        for group in market_groups:
            market_name = group.get("marketName", "").lower()
            choices = group.get("choices", [])
            if "full time" in market_name:
                for c in choices:
                    name = c.get("name", "").upper()
                    dec = c.get("decimalValue")
                    if name == "1":
                        res["odds_1"] = float(dec) if dec else None
                    elif name == "X":
                        res["odds_x"] = float(dec) if dec else None
                    elif name == "2":
                        res["odds_2"] = float(dec) if dec else None
            elif "double chance" in market_name:
                for c in choices:
                    name = c.get("name", "").upper()
                    dec = c.get("decimalValue")
                    if name == "1X":
                        res["odds_1x"] = float(dec) if dec else None
                    elif name == "12":
                        res["odds_12"] = float(dec) if dec else None
                    elif name == "X2":
                        res["odds_x2"] = float(dec) if dec else None
            elif "both teams to score" in market_name:
                for c in choices:
                    name = c.get("name", "").lower()
                    dec = c.get("decimalValue")
                    if name == "yes":
                        res["odds_btts_yes"] = float(dec) if dec else None
                    elif name == "no":
                        res["odds_btts_no"] = float(dec) if dec else None
            elif "over/under" in market_name or "goals" in market_name:
                for c in choices:
                    line = c.get("line")
                    if not line:
                        continue
                    name = c.get("name", "").lower()
                    dec = c.get("decimalValue")
                    val = float(dec) if dec else None
                    if "over" in name:
                        if line == 0.5: res["odds_o05"] = val
                        elif line == 1.5: res["odds_o15"] = val
                        elif line == 2.5: res["odds_o25"] = val
                        elif line == 3.5: res["odds_o35"] = val
                        elif line == 4.5: res["odds_o45"] = val
                        elif line == 5.5: res["odds_o55"] = val
                        elif line == 6.5: res["odds_o65"] = val
                        elif line == 7.5: res["odds_o75"] = val
                    elif "under" in name:
                        if line == 0.5: res["odds_u05"] = val
                        elif line == 1.5: res["odds_u15"] = val
                        elif line == 2.5: res["odds_u25"] = val
                        elif line == 3.5: res["odds_u35"] = val
                        elif line == 4.5: res["odds_u45"] = val
                        elif line == 5.5: res["odds_u55"] = val
                        elif line == 6.5: res["odds_u65"] = val
                        elif line == 7.5: res["odds_u75"] = val
        return res

    def _deep_search(self, obj, key):
        if isinstance(obj, dict):
            if key in obj:
                return obj[key]
            for v in obj.values():
                res = self._deep_search(v, key)
                if res is not None:
                    return res
        elif isinstance(obj, list):
            for item in obj:
                res = self._deep_search(item, key)
                if res is not None:
                    return res
        return None

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
        country = category.get("country", {})
        if isinstance(country, dict):
            country_name = country.get("name")
        else:
            country_name = category.get("name")
        return {
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
            "country": country_name
        }

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

        try:
            db.connect()
            sc.start()

            tz_tr = dt.timezone(dt.timedelta(hours=3))
            now_tr = dt.datetime.now(tz_tr)
            today = now_tr.date()
            target_dates = [today, today + dt.timedelta(days=1)]

            for date_obj in target_dates:
                date_str = date_obj.strftime("%Y-%m-%d")
                print(f"\n[TARAMA] {date_str} için maçlar alınıyor...")
                events = sc.get_matches_for_date(date_str)

                if not events:
                    print(f"  {date_str} için hiç maç bulunamadı.")
                    continue

                count = 0
                for ev in events:
                    # Başlamamış maçları ve majör turnuvaları filtrele
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
                    total_processed += 1

                print(f"  {date_str} için {count} maç işlendi.")

            if total_processed == 0:
                print("[UYARI] Hiç maç işlenemedi, tekrar deneniyor...")
                attempt += 1
                if attempt <= max_retries:
                    time.sleep(15)
            else:
                print(f"\n[BAŞARILI] Toplam {total_processed} maç işlendi.")
                break

        except Exception as e:
            print(f"[HATA] {e}")
            attempt += 1
            time.sleep(15)
        finally:
            sc.stop()
            db.close()

    if total_processed == 0:
        print("[KRİTİK HATA] Hiç veri çekilemedi.")
        sys.exit(1)


if __name__ == "__main__":
    main()
