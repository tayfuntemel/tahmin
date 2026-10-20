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

def sayiya_cevir(deger):
    if deger is None or deger == "-" or deger == "":
        return None
    try:
        return float(str(deger).replace("%", "").strip())
    except:
        return None

# SADECE DURUM 'FT' OLDUĞUNDA KAZANDI / KAYBETTI OLARAK VERİTABANINA İŞLER
def mac_sonucu_degerlendir(secilen_tahmin, durum_str, ev_skor, dep_skor, ev_iy_skor, dep_iy_skor, ev_korner, dep_korner, ev_sari, dep_sari):
    durum_up = str(durum_str or '').upper().strip()
    if 'CANCELED' in durum_up or 'POSTPONED' in durum_up:
        return 'IPTAL'
    if 'FT' not in durum_up:
        return 'BEKLIYOR'

    ev_s = sayiya_cevir(ev_skor)
    dep_s = sayiya_cevir(dep_skor)
    if ev_s is None or dep_s is None:
        return 'BEKLIYOR'

    ev = int(ev_s)
    dep = int(dep_s)
    toplam = ev + dep

    ev_iy_s = sayiya_cevir(ev_iy_skor)
    dep_iy_s = sayiya_cevir(dep_iy_skor)
    has_iy = (ev_iy_s is not None and dep_iy_s is not None)
    ev_iy = int(ev_iy_s) if has_iy else 0
    dep_iy = int(dep_iy_s) if has_iy else 0

    ev_kor_s = sayiya_cevir(ev_korner)
    dep_kor_s = sayiya_cevir(dep_korner)
    has_kor = (ev_kor_s is not None and dep_kor_s is not None)

    ev_sari_s = sayiya_cevir(ev_sari)
    dep_sari_s = sayiya_cevir(dep_sari)
    has_kart = (ev_sari_s is not None and dep_sari_s is not None)

    if secilen_tahmin == 'Maç Sonu 1': return 'KAZANDI' if ev > dep else 'KAYBETTI'
    if secilen_tahmin == 'Maç Sonu 2': return 'KAZANDI' if dep > ev else 'KAYBETTI'
    if secilen_tahmin == 'MS 1 ve 1.5 Üst': return 'KAZANDI' if (ev > dep and toplam >= 2) else 'KAYBETTI'
    if secilen_tahmin == 'MS 2 ve 1.5 Üst': return 'KAZANDI' if (dep > ev and toplam >= 2) else 'KAYBETTI'
    if secilen_tahmin == '1X ve 1.5 Üst': return 'KAZANDI' if (ev >= dep and toplam >= 2) else 'KAYBETTI'
    if secilen_tahmin == 'X2 ve 1.5 Üst': return 'KAZANDI' if (dep >= ev and toplam >= 2) else 'KAYBETTI'
    if secilen_tahmin == 'Ev 1.5 Gol Üst': return 'KAZANDI' if ev >= 2 else 'KAYBETTI'
    if secilen_tahmin == 'Dep 1.5 Gol Üst': return 'KAZANDI' if dep >= 2 else 'KAYBETTI'
    if secilen_tahmin == 'KG Var ve 2.5 Üst': return 'KAZANDI' if (ev > 0 and dep > 0 and toplam >= 3) else 'KAYBETTI'
    if secilen_tahmin == '1X Çifte Şans': return 'KAZANDI' if ev >= dep else 'KAYBETTI'
    if secilen_tahmin == 'X2 Çifte Şans': return 'KAZANDI' if dep >= ev else 'KAYBETTI'
    if secilen_tahmin == '1.5 Gol Üst': return 'KAZANDI' if toplam >= 2 else 'KAYBETTI'
    if secilen_tahmin == '2.5 Gol Üst': return 'KAZANDI' if toplam >= 3 else 'KAYBETTI'
    if secilen_tahmin == '2.5 Gol Alt': return 'KAZANDI' if toplam <= 2 else 'KAYBETTI'
    if secilen_tahmin == '3.5 Gol Alt': return 'KAZANDI' if toplam <= 3 else 'KAYBETTI'
    if secilen_tahmin == 'Karşılıklı Gol Var': return 'KAZANDI' if (ev > 0 and dep > 0) else 'KAYBETTI'
    if secilen_tahmin == 'İlk Yarı 0.5 Üst':
        return ('KAZANDI' if (ev_iy + dep_iy) >= 1 else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if secilen_tahmin == 'İlk Yarı 1':
        return ('KAZANDI' if ev_iy > dep_iy else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if secilen_tahmin == 'İlk Yarı 2':
        return ('KAZANDI' if dep_iy > ev_iy else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if secilen_tahmin == '2. Yarı 1.5 Üst':
        return ('KAZANDI' if (toplam - (ev_iy + dep_iy)) >= 2 else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if secilen_tahmin == '8.5 Korner Üst':
        return ('KAZANDI' if (int(ev_kor_s) + int(dep_kor_s)) >= 9 else 'KAYBETTI') if has_kor else 'BEKLIYOR'
    if secilen_tahmin == '9.5 Korner Üst':
        return ('KAZANDI' if (int(ev_kor_s) + int(dep_kor_s)) >= 10 else 'KAYBETTI') if has_kor else 'BEKLIYOR'
    if secilen_tahmin == '3.5 Kart Üst':
        return ('KAZANDI' if (int(ev_sari_s) + int(dep_sari_s)) >= 4 else 'KAYBETTI') if has_kart else 'BEKLIYOR'
    if secilen_tahmin == '4.5 Kart Üst':
        return ('KAZANDI' if (int(ev_sari_s) + int(dep_sari_s)) >= 5 else 'KAYBETTI') if has_kart else 'BEKLIYOR'

    return 'BEKLIYOR'

def tahmin_sonuclarini_veritabanina_isle(conn):
    try:
        dict_cursor = conn.cursor(pymysql.cursors.DictCursor)
        dict_cursor.execute("""
            SELECT t.id as tahmin_id, t.secilen_tahmin, t.sonuc as mevcut_sonuc,
                   f.durum, f.ev_skor, f.dep_skor, f.ev_iy_skor, f.dep_iy_skor,
                   f.ev_korner, f.dep_korner, f.ev_sari, f.dep_sari
            FROM tahminler t
            JOIN fikstur f ON f.id = t.fikstur_id
            WHERE t.secilen_tahmin IS NOT NULL
        """)
        satirlar = dict_cursor.fetchall()
        for s in satirlar:
            hesaplanan = mac_sonucu_degerlendir(
                s['secilen_tahmin'], s['durum'],
                s['ev_skor'], s['dep_skor'],
                s['ev_iy_skor'], s['dep_iy_skor'],
                s['ev_korner'], s['dep_korner'],
                s['ev_sari'], s['dep_sari']
            )
            if hesaplanan != s['mevcut_sonuc']:
                dict_cursor.execute("UPDATE tahminler SET sonuc = %s WHERE id = %s", (hesaplanan, s['tahmin_id']))
    except Exception as e:
        print(f"Tahmin sonuçlandırma uyarısı: {e}")

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

    # Fikstür güncellendikten hemen sonra FT olan maçların tahminlerini KAZANDI / KAYBETTI olarak veritabanına işle
    tahmin_sonuclarini_veritabanina_isle(conn)

if __name__ == "__main__":
    baglanti = db_baglantisi_kur()
    if baglanti:
        canli_motoru_baslat(baglanti)
        baglanti.close()
