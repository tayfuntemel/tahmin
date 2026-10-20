import pymysql
import math
import traceback
import os
from datetime import datetime, timedelta

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

# --- MATEMATİKSEL MODELLER (POISSON + DIXON-COLES) ---
def poisson_ihtimal(beklenen, gerceklesen):
    if beklenen <= 0:
        return 1.0 if gerceklesen == 0 else 0.0
    try:
        return ((beklenen ** gerceklesen) * math.exp(-beklenen)) / math.factorial(gerceklesen)
    except:
        return 0.0

def dixon_coles_duzeltmesi(ev_gol, dep_gol, ev_xg, dep_xg, rho=-0.11):
    if ev_gol == 0 and dep_gol == 0:
        return 1.0 - (ev_xg * dep_xg * rho)
    elif ev_gol == 0 and dep_gol == 1:
        return 1.0 + (ev_xg * rho)
    elif ev_gol == 1 and dep_gol == 0:
        return 1.0 + (dep_xg * rho)
    elif ev_gol == 1 and dep_gol == 1:
        return 1.0 - rho
    return 1.0

def db_baglantisi_kur():
    conn = pymysql.connect(
        host=CONFIG["db"]["host"], user=CONFIG["db"]["user"],
        password=CONFIG["db"]["password"], database=CONFIG["db"]["database"],
        port=CONFIG["db"]["port"], autocommit=True, cursorclass=pymysql.cursors.DictCursor
    )
    cursor = conn.cursor()
    tahminler_tablo_sql = """
    CREATE TABLE IF NOT EXISTS tahminler (
        id INT AUTO_INCREMENT PRIMARY KEY,
        fikstur_id INT NOT NULL,
        olusturma_tarihi TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        durum VARCHAR(50),
        beklenen_ev_gol DECIMAL(5,2), beklenen_dep_gol DECIMAL(5,2),
        p_ms_1 DECIMAL(5,2), p_ms_x DECIMAL(5,2), p_ms_2 DECIMAL(5,2),
        p_iy_1 DECIMAL(5,2), p_iy_x DECIMAL(5,2), p_iy_2 DECIMAL(5,2),
        p_gol_1_5_ust DECIMAL(5,2), p_gol_2_5_ust DECIMAL(5,2), p_gol_3_5_ust DECIMAL(5,2), p_kg_var DECIMAL(5,2),
        p_iy_0_5_ust DECIMAL(5,2), p_iy_1_5_ust DECIMAL(5,2),
        beklenen_korner DECIMAL(5,2), p_korner_8_5_ust DECIMAL(5,2), p_korner_9_5_ust DECIMAL(5,2),
        UNIQUE KEY unique_fikstur (fikstur_id)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """
    cursor.execute(tahminler_tablo_sql)
    return conn

def sayiya_cevir(deger):
    if deger is None or deger == "-" or deger == "":
        return None
    try:
        temiz = str(deger).replace("%", "").strip()
        return float(temiz)
    except:
        return None

def takim_performansi_getir(cursor, takim_adi, mac_tarihi, saha="genel"):
    if saha == "ev":
        sorgu = "SELECT * FROM fikstur WHERE ev_sahibi = %s AND tarih < %s AND durum LIKE '%%FT%%' ORDER BY tarih DESC LIMIT 15"
        cursor.execute(sorgu, (takim_adi, mac_tarihi))
    elif saha == "dep":
        sorgu = "SELECT * FROM fikstur WHERE deplasman = %s AND tarih < %s AND durum LIKE '%%FT%%' ORDER BY tarih DESC LIMIT 15"
        cursor.execute(sorgu, (takim_adi, mac_tarihi))
    else:
        sorgu = "SELECT * FROM fikstur WHERE (ev_sahibi = %s OR deplasman = %s) AND tarih < %s AND durum LIKE '%%FT%%' ORDER BY tarih DESC LIMIT 15"
        cursor.execute(sorgu, (takim_adi, takim_adi, mac_tarihi))
        
    maclar = cursor.fetchall()
    
    # İç/Dış sahada 4 maçtan az veri varsa (Avrupa kupaları veya sezon başı), genel forma geri dön
    if len(maclar) < 4 and saha != "genel":
        return takim_performansi_getir(cursor, takim_adi, mac_tarihi, saha="genel")
        
    if len(maclar) < 3:
        return None

    stats = {
        "gol_atti": [], "gol_yedi": [], 
        "iy_gol_atti": [], "iy_gol_yedi": [], 
        "korner_atti": [], "korner_yedi": [], 
        "kg_var": []
    }
    form_stats = {k: [] for k in stats.keys()}

    for index, m in enumerate(maclar):
        is_ev = (m['ev_sahibi'] == takim_adi)
        attigi = sayiya_cevir(m['ev_skor']) if is_ev else sayiya_cevir(m['dep_skor'])
        yedigi = sayiya_cevir(m['dep_skor']) if is_ev else sayiya_cevir(m['ev_skor'])
        iy_attigi = sayiya_cevir(m['ev_iy_skor']) if is_ev else sayiya_cevir(m['dep_iy_skor'])
        iy_yedigi = sayiya_cevir(m['dep_iy_skor']) if is_ev else sayiya_cevir(m['ev_iy_skor'])
        kor_atti = sayiya_cevir(m['ev_korner']) if is_ev else sayiya_cevir(m['dep_korner'])
        kor_yedi = sayiya_cevir(m['dep_korner']) if is_ev else sayiya_cevir(m['ev_korner'])

        # Şut Kalitesi (Sentetik xG) Verileri
        sut_atti = sayiya_cevir(m.get('ev_sut')) if is_ev else sayiya_cevir(m.get('dep_sut'))
        sut_yedi = sayiya_cevir(m.get('dep_sut')) if is_ev else sayiya_cevir(m.get('ev_sut'))
        isabet_atti = sayiya_cevir(m.get('ev_isabetli')) if is_ev else sayiya_cevir(m.get('dep_isabetli'))
        isabet_yedi = sayiya_cevir(m.get('dep_isabetli')) if is_ev else sayiya_cevir(m.get('ev_isabetli'))
        pen_atti = sayiya_cevir(m.get('ev_pen_gol')) if is_ev else sayiya_cevir(m.get('dep_pen_gol'))
        pen_yedi = sayiya_cevir(m.get('dep_pen_gol')) if is_ev else sayiya_cevir(m.get('ev_pen_gol'))

        # Gerçek gol ile Şut Kalitesini (xG) harmanla (Şans faktörünü kırar)
        efektif_attigi = attigi
        if attigi is not None and isabet_atti is not None and sut_atti is not None and sut_atti > 0:
            pen_duzeltme = (pen_atti * 0.25) if pen_atti else 0.0
            isabetsiz_atti = max(0.0, sut_atti - isabet_atti)
            sut_xg_atti = (isabet_atti * 0.29) + (isabetsiz_atti * 0.035)
            efektif_attigi = (( max(0.0, attigi - pen_duzeltme) ) * 0.62) + (sut_xg_atti * 0.38)

        efektif_yedigi = yedigi
        if yedigi is not None and isabet_yedi is not None and sut_yedi is not None and sut_yedi > 0:
            pen_y_duzeltme = (pen_yedi * 0.25) if pen_yedi else 0.0
            isabetsiz_yedi = max(0.0, sut_yedi - isabet_yedi)
            sut_xg_yedi = (isabet_yedi * 0.29) + (isabetsiz_yedi * 0.035)
            efektif_yedigi = (( max(0.0, yedigi - pen_y_duzeltme) ) * 0.62) + (sut_xg_yedi * 0.38)

        kg = 1.0 if (attigi is not None and yedigi is not None and attigi > 0 and yedigi > 0) else 0.0

        for dct in [stats, (form_stats if index < 5 else None)]:
            if dct is None: continue
            if efektif_attigi is not None: dct['gol_atti'].append(efektif_attigi)
            if efektif_yedigi is not None: dct['gol_yedi'].append(efektif_yedigi)
            if iy_attigi is not None: dct['iy_gol_atti'].append(iy_attigi)
            if iy_yedigi is not None: dct['iy_gol_yedi'].append(iy_yedigi)
            if kor_atti is not None: dct['korner_atti'].append(kor_atti)
            if kor_yedi is not None: dct['korner_yedi'].append(kor_yedi)
            dct['kg_var'].append(kg)

    agirlikli_ort = {}
    for key in stats.keys():
        genel_lst = stats[key]
        form_lst = form_stats[key]
        if len(genel_lst) > 0:
            genel_o = sum(genel_lst) / len(genel_lst)
            if len(form_lst) > 0:
                form_o = sum(form_lst) / len(form_lst)
                if form_o > (genel_o * 1.85) and genel_o > 0:
                    form_o = genel_o * 1.45
                agirlikli_ort[key] = (form_o * 0.58) + (genel_o * 0.42)
            else:
                agirlikli_ort[key] = genel_o
        else:
            agirlikli_ort[key] = None
            
    return agirlikli_ort

def tahminleri_hesapla_ve_kaydet(cursor, fikstur_id, ev, dep, ev_ort, dep_ort):
    if not ev_ort or not dep_ort or ev_ort['gol_atti'] is None or dep_ort['gol_atti'] is None:
        cursor.execute("INSERT INTO tahminler (fikstur_id, durum) VALUES (%s, 'YETERSİZ VERİ') ON DUPLICATE KEY UPDATE durum = 'YETERSİZ VERİ'", (fikstur_id,))
        return

    # Ev sahibi saha avantajı çarpanı (%4) ile xG hesabı
    ev_xG = max(0.25, ((ev_ort['gol_atti'] * 1.04) + (dep_ort['gol_yedi'] * 0.96)) / 2)
    dep_xG = max(0.20, ((dep_ort['gol_atti'] * 0.96) + (ev_ort['gol_yedi'] * 1.04)) / 2)
    
    ev_iy_xG = ((ev_ort['iy_gol_atti'] or (ev_xG * 0.44)) + (dep_ort['iy_gol_yedi'] or (dep_xG * 0.44))) / 2
    dep_iy_xG = ((dep_ort['iy_gol_atti'] or (dep_xG * 0.44)) + (ev_ort['iy_gol_yedi'] or (ev_xG * 0.44))) / 2

    # 8x8 Dixon-Coles 2 Boyutlu Skor Matrisi
    p_1 = p_x = p_2 = 0.0
    p_1_5_ust = p_2_5_ust = p_3_5_ust = pois_kg_var = 0.0
    toplam_matris = 0.0

    for i in range(8):
        for j in range(8):
             ham_ihtimal = poisson_ihtimal(ev_xG, i) * poisson_ihtimal(dep_xG, j)
             dc_carpan = dixon_coles_duzeltmesi(i, j, ev_xG, dep_xG)
             ihtimal = max(0.0, ham_ihtimal * dc_carpan)
             toplam_matris += ihtimal

    for i in range(8):
        for j in range(8):
            ham_ihtimal = poisson_ihtimal(ev_xG, i) * poisson_ihtimal(dep_xG, j)
            dc_carpan = dixon_coles_duzeltmesi(i, j, ev_xG, dep_xG)
            ihtimal = max(0.0, ham_ihtimal * dc_carpan) / (toplam_matris if toplam_matris > 0 else 1.0)
            
            if i > j: p_1 += ihtimal
            elif i == j: p_x += ihtimal
            else: p_2 += ihtimal

            if (i + j) >= 2: p_1_5_ust += ihtimal
            if (i + j) >= 3: p_2_5_ust += ihtimal
            if (i + j) >= 4: p_3_5_ust += ihtimal
            if i > 0 and j > 0: pois_kg_var += ihtimal

    p_iy_1 = p_iy_x = p_iy_2 = 0.0
    for i in range(5): 
        for j in range(5):
            ihtimal = poisson_ihtimal(ev_iy_xG, i) * poisson_ihtimal(dep_iy_xG, j)
            if i > j: p_iy_1 += ihtimal
            elif i == j: p_iy_x += ihtimal
            else: p_iy_2 += ihtimal

    tarihsel_kg = (ev_ort['kg_var'] + dep_ort['kg_var']) / 2
    p_kg_var = (tarihsel_kg * 0.45) + (pois_kg_var * 0.55)

    p_iy_0_g = poisson_ihtimal(ev_iy_xG + dep_iy_xG, 0)
    p_iy_1_g = poisson_ihtimal(ev_iy_xG + dep_iy_xG, 1)
    p_iy_0_5_ust = 1.0 - p_iy_0_g
    p_iy_1_5_ust = 1.0 - (p_iy_0_g + p_iy_1_g)

    beklenen_korner, p_korner_8_5, p_korner_9_5 = None, None, None
    if ev_ort['korner_atti'] is not None and dep_ort['korner_yedi'] is not None and dep_ort['korner_atti'] is not None and ev_ort['korner_yedi'] is not None:
        beklenen_korner = ((ev_ort['korner_atti'] + dep_ort['korner_yedi']) / 2) + ((dep_ort['korner_atti'] + ev_ort['korner_yedi']) / 2)
        p_korner_8_5 = 1.0 - sum([poisson_ihtimal(beklenen_korner, k) for k in range(9)])
        p_korner_9_5 = 1.0 - sum([poisson_ihtimal(beklenen_korner, k) for k in range(10)])

    sql = """
        INSERT INTO tahminler 
        (fikstur_id, durum, beklenen_ev_gol, beklenen_dep_gol, p_ms_1, p_ms_x, p_ms_2, p_iy_1, p_iy_x, p_iy_2, 
         p_gol_1_5_ust, p_gol_2_5_ust, p_gol_3_5_ust, p_kg_var, p_iy_0_5_ust, p_iy_1_5_ust, 
         beklenen_korner, p_korner_8_5_ust, p_korner_9_5_ust)
        VALUES (%s, 'HAZIR', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            durum='HAZIR', beklenen_ev_gol=VALUES(beklenen_ev_gol), beklenen_dep_gol=VALUES(beklenen_dep_gol),
            p_ms_1=VALUES(p_ms_1), p_ms_x=VALUES(p_ms_x), p_ms_2=VALUES(p_ms_2), p_iy_1=VALUES(p_iy_1), p_iy_x=VALUES(p_iy_x), p_iy_2=VALUES(p_iy_2),
            p_gol_1_5_ust=VALUES(p_gol_1_5_ust), p_gol_2_5_ust=VALUES(p_gol_2_5_ust), p_gol_3_5_ust=VALUES(p_gol_3_5_ust), p_kg_var=VALUES(p_kg_var),
            p_iy_0_5_ust=VALUES(p_iy_0_5_ust), p_iy_1_5_ust=VALUES(p_iy_1_5_ust),
            beklenen_korner=VALUES(beklenen_korner), p_korner_8_5_ust=VALUES(p_korner_8_5_ust), p_korner_9_5_ust=VALUES(p_korner_9_5_ust)
    """
    degerler = (
        fikstur_id, round(ev_xG, 2), round(dep_xG, 2),
        round(p_1*100, 2), round(p_x*100, 2), round(p_2*100, 2), round(p_iy_1*100, 2), round(p_iy_x*100, 2), round(p_iy_2*100, 2),
        round(p_1_5_ust*100, 2), round(p_2_5_ust*100, 2), round(p_3_5_ust*100, 2), round(p_kg_var*100, 2),
        round(p_iy_0_5_ust*100, 2), round(p_iy_1_5_ust*100, 2),
        round(beklenen_korner, 2) if beklenen_korner is not None else None,
        round(p_korner_8_5*100, 2) if p_korner_8_5 is not None else None, round(p_korner_9_5*100, 2) if p_korner_9_5 is not None else None
    )
    cursor.execute(sql, degerler)
    print(f"[{fikstur_id}] ✅ {ev} vs {dep} -> Hibrit Quant Analizi Kaydedildi!")

def tahmin_motorunu_baslat():
    try:
        conn = db_baglantisi_kur()
        cursor = conn.cursor()
        bugun = datetime.now().strftime("%Y-%m-%d")
        yarin = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        sonraki = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
        
        cursor.execute("SELECT id, ev_sahibi, deplasman, tarih FROM fikstur WHERE tarih IN (%s, %s, %s) AND durum NOT LIKE '%%FT%%'", (bugun, yarin, sonraki))
        gelecek_maclar = cursor.fetchall()
        
        for mac in gelecek_maclar:
            ev_ort = takim_performansi_getir(cursor, mac['ev_sahibi'], mac['tarih'], saha="ev")
            dep_ort = takim_performansi_getir(cursor, mac['deplasman'], mac['tarih'], saha="dep")
            tahminleri_hesapla_ve_kaydet(cursor, mac['id'], mac['ev_sahibi'], mac['deplasman'], ev_ort, dep_ort)

        conn.close()
    except Exception as e:
        print(traceback.format_exc())

if __name__ == "__main__":
    tahmin_motorunu_baslat()
