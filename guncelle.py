import sys
import subprocess
import time
import re
import os
from datetime import datetime, timedelta

def kütüphaneleri_kur():
    gerekli = ['requests', 'pymysql']
    for k in gerekli:
        try:
            __import__(k)
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", k])

kütüphaneleri_kur()
import requests
import pymysql

HEDEF_LIGLER = [
    "ENGLISH-PREMIER-LEAGUE", "SPANISH-LALIGA", "LALIGA",
    "GERMAN-BUNDESLIGA", "ITALIAN-SERIE-A", "FRENCH-LIGUE-1", "LIGUE-1",
    "TURKISH-SUPER-LIG", "DUTCH-EREDIVISIE", "EREDIVISIE",
    "PORTUGUESE-PRIMEIRA-LIGA", "PRIMEIRA-LIGA", "BRASILEIRO-SERIE-A",
    "UEFA-CHAMPIONS-LEAGUE", "CHAMPIONS-LEAGUE", "UEFA-EUROPA-LEAGUE", "EUROPA-LEAGUE"
]

# GITHUB SECRETS ENTEGRASYONU
CONFIG = {
    "db": {
        "host": os.environ.get("DB_HOST", "netscout.fun"),
        "user": os.environ.get("DB_USER", "netscout_veri"),
        "password": os.environ.get("DB_PASSWORD", "i.34temel1"),
        "database": os.environ.get("DB_NAME", "netscout_veri"),
        "port": int(os.environ.get("DB_PORT", 3306))
    }
}

STAT_MAPPING = {
    "possessionPct": "top_oynama", "totalShots": "sut", "shotsOnTarget": "isabetli",
    "wonCorners": "korner", "foulsCommitted": "faul", "yellowCards": "sari",
    "redCards": "kirmizi", "offsides": "ofsayt", "saves": "kurtaris",
    "shotPct": "sut_yuzde", "penaltyKickGoals": "pen_gol", "penaltyKickShots": "pen_sut",
    "accuratePasses": "isabetli_pas", "totalPasses": "toplam_pas", "passPct": "pas_yuzde",
    "accurateCrosses": "isabetli_orta", "totalCrosses": "toplam_orta", "crossPct": "orta_yuzde",
    "totalLongBalls": "toplam_uzun", "accurateLongBalls": "isabet_uzun", "longballPct": "uzun_yuzde",
    "blockedShots": "engel_sut", "effectiveTackles": "basarili_mud", "totalTackles": "toplam_mud",
    "tacklePct": "mud_yuzde", "interceptions": "pas_arasi", "effectiveClearance": "basarili_uzak",
    "totalClearance": "toplam_uzak"
}

def db_baglantisi_kur():
    try:
        conn = pymysql.connect(
            host=CONFIG["db"]["host"], user=CONFIG["db"]["user"],
            password=CONFIG["db"]["password"], database=CONFIG["db"]["database"],
            port=CONFIG["db"]["port"], autocommit=True
        )
        return conn
    except Exception as e:
        print(f"Veritabanı hatası: {e}")
        return None

def sezon_ayikla(season_info):
    if isinstance(season_info, dict):
        if "year" in season_info: return str(season_info["year"])
        elif "displayName" in season_info: return str(season_info["displayName"])
    return "-"

def canli_motoru_baslat(conn):
    cursor = conn.cursor()
    db_sutunlar = []
    for db_name in STAT_MAPPING.values():
        db_sutunlar.extend([f"ev_{db_name}", f"dep_{db_name}"])
    
    sutunlar_str = ", ".join(db_sutunlar)
    degerler_str = ", ".join(["%s"] * len(db_sutunlar))
    
    update_str_list = ["ev_skor = VALUES(ev_skor)", "dep_skor = VALUES(dep_skor)", "ev_iy_skor = VALUES(ev_iy_skor)", "dep_iy_skor = VALUES(dep_iy_skor)", "durum = VALUES(durum)"]
    for col in db_sutunlar: update_str_list.append(f"{col} = VALUES({col})")
    update_str = ",\n        ".join(update_str_list)
    
    insert_update_sql = f"""
    INSERT INTO fikstur (tarih, lig, sezon, ev_sahibi, deplasman, ev_skor, dep_skor, ev_iy_skor, dep_iy_skor, durum, {sutunlar_str}) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, {degerler_str})
    ON DUPLICATE KEY UPDATE {update_str}
    """

    bugun = datetime.now()
    gunler = [1, 0] 
    
    for i in gunler:
        hedef_tarih = bugun - timedelta(days=i)
        api_tarih = hedef_tarih.strftime("%Y%m%d")
        db_tarih = hedef_tarih.strftime("%Y-%m-%d")
        
        url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?dates={api_tarih}"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                veri = response.json()
                events = veri.get("events", [])
                for event in events:
                    season_info = event.get("season", {})
                    ham_lig = season_info.get("slug", "BILINMEYEN").upper()
                    temiz_lig = re.sub(r'^\d{4}(?:-\d{2})?-', '', ham_lig)
                    
                    if temiz_lig not in HEDEF_LIGLER: continue 
                    
                    mac_id = event["id"]
                    sezon = sezon_ayikla(season_info)
                    durum = event.get("status", {}).get("type", {}).get("shortDetail", "")
                    
                    if "Canceled" in durum or "Postponed" in durum or durum == "": continue
                        
                    ev_sahibi, deplasman, ev_skor, dep_skor, ev_iy_skor, dep_iy_skor, ev_id, dep_id = "", "", "-", "-", "-", "-", "", ""

                    if "competitions" in event and len(event["competitions"][0]["competitors"]) == 2:
                        ev_veri = event["competitions"][0]["competitors"][0]
                        dep_veri = event["competitions"][0]["competitors"][1]
                        ev_sahibi = ev_veri["team"]["name"]
                        ev_id = str(ev_veri["team"]["id"])
                        ev_skor = str(ev_veri.get("score", "-"))
                        deplasman = dep_veri["team"]["name"]
                        dep_id = str(dep_veri["team"]["id"])
                        dep_skor = str(dep_veri.get("score", "-"))
                        
                    stats = {f"ev_{db_name}": "-" for db_name in STAT_MAPPING.values()}
                    stats.update({f"dep_{db_name}": "-" for db_name in STAT_MAPPING.values()})
                    
                    if "Scheduled" not in durum:
                        summary_url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/all/summary?event={mac_id}"
                        try:
                            time.sleep(0.2) 
                            sum_resp = requests.get(summary_url, timeout=10)
                            if sum_resp.status_code == 200:
                                sum_veri = sum_resp.json()
                                header_comps = sum_veri.get("header", {}).get("competitions", [])
                                if header_comps:
                                    for comp_takim in header_comps[0].get("competitors", []):
                                        c_id = str(comp_takim.get("id", ""))
                                        if "linescores" in comp_takim and len(comp_takim["linescores"]) > 0:
                                            deger = comp_takim["linescores"][0].get("displayValue", "-")
                                            if c_id == ev_id: ev_iy_skor = deger
                                            elif c_id == dep_id: dep_iy_skor = deger

                                boxscore = sum_veri.get("boxscore", {}).get("teams", [])
                                if boxscore:
                                    for takim in boxscore:
                                        t_id = str(takim.get("team", {}).get("id", ""))
                                        for stat in takim.get("statistics", []):
                                            if stat.get("name", "") in STAT_MAPPING:
                                                db_isim = STAT_MAPPING[stat.get("name", "")]
                                                if t_id == ev_id: stats[f"ev_{db_isim}"] = str(stat.get("displayValue", "-"))
                                                elif t_id == dep_id: stats[f"dep_{db_isim}"] = str(stat.get("displayValue", "-"))
                        except Exception: pass 

                    veriler = [db_tarih, temiz_lig, sezon, ev_sahibi, deplasman, ev_skor, dep_skor, ev_iy_skor, dep_iy_skor, durum]
                    for db_name in STAT_MAPPING.values():
                        veriler.append(stats[f"ev_{db_name}"])
                        veriler.append(stats[f"dep_{db_name}"])

                    cursor.execute(insert_update_sql, tuple(veriler))
                    print(f"[+] Eşitlendi: {ev_sahibi} {ev_skor}-{dep_skor} {deplasman} ({durum})")
        except Exception as e:
            print(f"Hata: {e}")

if __name__ == "__main__":
    baglanti = db_baglantisi_kur()
    if baglanti:
        canli_motoru_baslat(baglanti)
        baglanti.close()
