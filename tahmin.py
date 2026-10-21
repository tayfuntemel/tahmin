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

def tr_kucuk(metin):
    if metin is None:
        return ""
    harita = str.maketrans("İIĞÜŞÖÇ", "iığüşöç")
    return str(metin).translate(harita).lower()

def sayiya_cevir(deger):
    if deger is None or deger == "-" or deger == "":
        return None
    try:
        temiz = str(deger).replace("%", "").strip()
        return float(temiz)
    except:
        return None

# --- İSTATİSTİKSEL YOĞUNLUK (MEDYAN + ÜSTEL AĞIRLIKLI ORTALAMA) MOTORU ---
def medyan_hesapla(liste):
    if not liste:
        return None
    sirali = sorted(liste)
    n = len(sirali)
    orta = n // 2
    if n % 2 == 1:
        return sirali[orta]
    return (sirali[orta - 1] + sirali[orta]) / 2.0

def yogunluk_merkezi_hesapla(deger_agirlik_listesi, medyan_agirligi=0.35):
    """
    Aykırı skorların (Örn: 8-1, 6-0) ortalamayı bozmasını engeller.
    Üstel zaman ağırlıklı ortalama (%65) ile Medyan yoğunluk merkezini (%35) birleştirir.
    """
    if not deger_agirlik_listesi:
        return None
    toplam_agirlik = sum(w for _, w in deger_agirlik_listesi)
    if toplam_agirlik <= 0:
        return None
    agirlikli_ort = sum(v * w for v, w in deger_agirlik_listesi) / toplam_agirlik
    saf_degerler = [v for v, _ in deger_agirlik_listesi]
    med = medyan_hesapla(saf_degerler)
    if med is None:
        return agirlikli_ort
    return (agirlikli_ort * (1.0 - medyan_agirligi)) + (med * medyan_agirligi)

def oran_yogunlugu_hesapla(ikili_agirlik_listesi):
    """Geçmiş maçlarda bir olayın (Örn: İY 1, Korner > 9.5, Kart > 4.5) gerçekleşme yoğunluğunu (0.0 - 1.0) verir."""
    if not ikili_agirlik_listesi:
        return None
    toplam_w = sum(w for _, w in ikili_agirlik_listesi)
    if toplam_w <= 0:
        return None
    return sum(v * w for v, w in ikili_agirlik_listesi) / toplam_w

# --- MATEMATİKSEL MODELLER (POISSON + DIXON-COLES + NEGATİF BİNOM) ---
def poisson_ihtimal(beklenen, gerceklesen):
    if beklenen <= 0:
        return 1.0 if gerceklesen == 0 else 0.0
    try:
        return ((beklenen ** gerceklesen) * math.exp(-beklenen)) / math.factorial(gerceklesen)
    except:
        return 0.0

def dixon_coles_duzeltmesi(ev_gol, dep_gol, ev_xg, dep_xg, rho=-0.10):
    if ev_gol == 0 and dep_gol == 0:
        return 1.0 - (ev_xg * dep_xg * rho)
    elif ev_gol == 0 and dep_gol == 1:
        return 1.0 + (ev_xg * rho)
    elif ev_gol == 1 and dep_gol == 0:
        return 1.0 + (dep_xg * rho)
    elif ev_gol == 1 and dep_gol == 1:
        return 1.0 - rho
    return 1.0

def asiri_dagilim_olasilik(beklenen_deger, baraj, dagilim_katsayisi=1.35):
    if beklenen_deger is None or beklenen_deger <= 0:
        return None
    mu = float(beklenen_deger)
    var = mu * dagilim_katsayisi
    r = (mu * mu) / (var - mu)
    p = mu / var
    cdf = 0.0
    limit = int(math.floor(baraj))
    for k in range(limit + 1):
        log_comb = math.lgamma(r + k) - math.lgamma(k + 1) - math.lgamma(r)
        prob_k = math.exp(log_comb + (r * math.log(p)) + (k * math.log(1.0 - p)))
        cdf += prob_k
    over = (1.0 - cdf) * 100.0
    return max(1.0, min(95.0, round(over, 1)))

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
        beklenen_kart DECIMAL(5,2), p_kart_3_5_ust DECIMAL(5,2), p_kart_4_5_ust DECIMAL(5,2),
        p_kirmizi_kart DECIMAL(5,2), p_penalti_olur DECIMAL(5,2),
        secilen_tahmin VARCHAR(60) DEFAULT NULL,
        tahmin_olasilik DECIMAL(5,2) DEFAULT NULL,
        kategori VARCHAR(20) DEFAULT NULL,
        guven_skoru DECIMAL(6,2) DEFAULT NULL,
        kupon_maci TINYINT(1) DEFAULT 0,
        sonuc VARCHAR(20) DEFAULT 'BEKLIYOR',
        ai_rapor TEXT DEFAULT NULL,
        UNIQUE KEY unique_fikstur (fikstur_id)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """
    cursor.execute(tahminler_tablo_sql)

    yeni_sutunlar = [
        ("beklenen_kart", "DECIMAL(5,2) DEFAULT NULL"),
        ("p_kart_3_5_ust", "DECIMAL(5,2) DEFAULT NULL"),
        ("p_kart_4_5_ust", "DECIMAL(5,2) DEFAULT NULL"),
        ("p_kirmizi_kart", "DECIMAL(5,2) DEFAULT NULL"),
        ("p_penalti_olur", "DECIMAL(5,2) DEFAULT NULL"),
        ("secilen_tahmin", "VARCHAR(60) DEFAULT NULL"),
        ("tahmin_olasilik", "DECIMAL(5,2) DEFAULT NULL"),
        ("kategori", "VARCHAR(20) DEFAULT NULL"),
        ("guven_skoru", "DECIMAL(6,2) DEFAULT NULL"),
        ("kupon_maci", "TINYINT(1) DEFAULT 0"),
        ("sonuc", "VARCHAR(20) DEFAULT 'BEKLIYOR'"),
        ("ai_rapor", "TEXT DEFAULT NULL")
    ]
    for sutun_adi, veri_tipi in yeni_sutunlar:
        try:
            cursor.execute(f"ALTER TABLE tahminler ADD COLUMN {sutun_adi} {veri_tipi}")
        except Exception:
            pass

    return conn

# --- LİG BAZLI GERÇEK ORTALAMA (BASELINE) MOTORU ---
_lig_baz_cache = {}
def lig_baz_istatistiklerini_getir(cursor, lig, mac_tarihi):
    cache_key = f"{lig}_{mac_tarihi}"
    if cache_key in _lig_baz_cache:
        return _lig_baz_cache[cache_key]

    cursor.execute("""
        SELECT ev_skor, dep_skor, ev_iy_skor, dep_iy_skor, ev_korner, dep_korner, ev_sari, dep_sari
        FROM fikstur
        WHERE lig = %s AND tarih < %s AND durum LIKE '%%FT%%'
        ORDER BY tarih DESC LIMIT 120
    """, (lig, mac_tarihi))
    rows = cursor.fetchall()

    ev_g, dep_g, iy_ev_g, iy_dep_g, kor_l, kart_l = [], [], [], [], [], []
    for r in rows:
        eg, dg = sayiya_cevir(r['ev_skor']), sayiya_cevir(r['dep_skor'])
        if eg is not None and dg is not None:
            ev_g.append(eg)
            dep_g.append(dg)
        ieg, idg = sayiya_cevir(r['ev_iy_skor']), sayiya_cevir(r['dep_iy_skor'])
        if ieg is not None and idg is not None:
            iy_ev_g.append(ieg)
            iy_dep_g.append(idg)
        ek, dk = sayiya_cevir(r['ev_korner']), sayiya_cevir(r['dep_korner'])
        if ek is not None and dk is not None and (ek + dk) > 0:
            kor_l.append(ek + dk)
        es, ds = sayiya_cevir(r['ev_sari']), sayiya_cevir(r['dep_sari'])
        if es is not None and ds is not None and (es + ds) > 0:
            kart_l.append(es + ds)

    baz = {
        'ev_gol': (sum(ev_g) / len(ev_g)) if len(ev_g) >= 15 else 1.48,
        'dep_gol': (sum(dep_g) / len(dep_g)) if len(dep_g) >= 15 else 1.22,
        'iy_ev_gol': (sum(iy_ev_g) / len(iy_ev_g)) if len(iy_ev_g) >= 15 else 0.65,
        'iy_dep_gol': (sum(iy_dep_g) / len(iy_dep_g)) if len(iy_dep_g) >= 15 else 0.52,
        'korner': (sum(kor_l) / len(kor_l)) if len(kor_l) >= 15 else 9.60,
        'kart': (sum(kart_l) / len(kart_l)) if len(kart_l) >= 15 else 4.10
    }
    _lig_baz_cache[cache_key] = baz
    return baz

# --- 67 SÜTUNLU YOĞUNLUK VE TAKTİKSEL PROFİL ÇIKARICI ---
def _ham_maclari_analiz_et(maclar, takim_adi):
    if len(maclar) < 3:
        return None

    veriler = {
        "gol_atti": [], "gol_yedi": [],
        "iy_gol_atti": [], "iy_gol_yedi": [],
        "2y_gol_atti": [], "2y_gol_yedi": [],
        "korner_atti": [], "korner_yedi": [], "toplam_korner": [],
        "kart_aldi": [], "kart_yedirdi": [], "toplam_kart": [],
        "kirmizi_etki": [], "penalti_etki": [],
        "kaleci_kurtaris_orani": [], "sut_blok_orani": [], "tempo_endeksi": [],
        # Geçmiş Maçlardaki Gerçekleşme Yoğunlukları (Binary Hit-Rates)
        "yogunluk_iy_onde": [], "yogunluk_iy_berabere": [],
        "yogunluk_iy_1_1": [], "yogunluk_iy_x_1": [],
        "yogunluk_kg_var": [], "yogunluk_25_ust": [],
        "yogunluk_iy_05_ust": [], "yogunluk_iy_15_ust": [], "yogunluk_iy_kg": [],
        "yogunluk_korner_95": [], "yogunluk_korner_105": [],
        "yogunluk_kart_35": [], "yogunluk_kart_45": []
    }

    for index, m in enumerate(maclar):
        is_ev = (m['ev_sahibi'] == takim_adi)
        attigi = sayiya_cevir(m['ev_skor']) if is_ev else sayiya_cevir(m['dep_skor'])
        yedigi = sayiya_cevir(m['dep_skor']) if is_ev else sayiya_cevir(m['ev_skor'])
        iy_attigi = sayiya_cevir(m['ev_iy_skor']) if is_ev else sayiya_cevir(m['dep_iy_skor'])
        iy_yedigi = sayiya_cevir(m['dep_iy_skor']) if is_ev else sayiya_cevir(m['ev_iy_skor'])

        if attigi is None or yedigi is None:
            continue

        # Üstel Zaman Ağırlığı: En güncel maç 1.00, geriye gittikçe 0.89^index ile azalır
        zaman_w = 0.89 ** index

        ev_sut_ham = sayiya_cevir(m.get('ev_sut'))
        dep_sut_ham = sayiya_cevir(m.get('dep_sut'))
        istatistik_var = not ((ev_sut_ham is None or ev_sut_ham == 0) and (dep_sut_ham is None or dep_sut_ham == 0))

        kirmizi_ev = sayiya_cevir(m.get('ev_kirmizi')) if istatistik_var else 0.0
        kirmizi_dep = sayiya_cevir(m.get('dep_kirmizi')) if istatistik_var else 0.0
        mac_kirmizi = (kirmizi_ev or 0.0) + (kirmizi_dep or 0.0)

        # Kırmızı kart çıkan maçlarda şut/gol/korner sapmasını önlemek için o maçın ağırlığını %45 düşür
        efektif_w = zaman_w * (0.55 if mac_kirmizi > 0 else 1.0)

        sut_atti = (ev_sut_ham if is_ev else dep_sut_ham) if istatistik_var else None
        sut_yedi = (dep_sut_ham if is_ev else ev_sut_ham) if istatistik_var else None
        isabet_atti = (sayiya_cevir(m.get('ev_isabetli')) if is_ev else sayiya_cevir(m.get('dep_isabetli'))) if istatistik_var else None
        isabet_yedi = (sayiya_cevir(m.get('dep_isabetli')) if is_ev else sayiya_cevir(m.get('ev_isabetli'))) if istatistik_var else None
        engel_atti = (sayiya_cevir(m.get('ev_engel_sut')) if is_ev else sayiya_cevir(m.get('dep_engel_sut'))) if istatistik_var else None
        engel_yedi = (sayiya_cevir(m.get('dep_engel_sut')) if is_ev else sayiya_cevir(m.get('ev_engel_sut'))) if istatistik_var else None
        kurtaris_yapti = (sayiya_cevir(m.get('ev_kurtaris')) if is_ev else sayiya_cevir(m.get('dep_kurtaris'))) if istatistik_var else None
        rakip_kurtaris = (sayiya_cevir(m.get('dep_kurtaris')) if is_ev else sayiya_cevir(m.get('ev_kurtaris'))) if istatistik_var else None

        orta_toplam = (sayiya_cevir(m.get('ev_toplam_orta')) if is_ev else sayiya_cevir(m.get('dep_toplam_orta'))) if istatistik_var else None
        orta_isabet = (sayiya_cevir(m.get('ev_isabetli_orta')) if is_ev else sayiya_cevir(m.get('dep_isabetli_orta'))) if istatistik_var else None
        uzaklastirma = (sayiya_cevir(m.get('ev_basarili_uzak')) if is_ev else sayiya_cevir(m.get('dep_basarili_uzak'))) if istatistik_var else None
        uzun_top = (sayiya_cevir(m.get('ev_toplam_uzun')) if is_ev else sayiya_cevir(m.get('dep_toplam_uzun'))) if istatistik_var else None

        pen_gol_atti = min(2.0, sayiya_cevir(m.get('ev_pen_gol')) or 0.0) if is_ev else min(2.0, sayiya_cevir(m.get('dep_pen_gol')) or 0.0)
        pen_gol_yedi = min(2.0, sayiya_cevir(m.get('dep_pen_gol')) or 0.0) if is_ev else min(2.0, sayiya_cevir(m.get('ev_pen_gol')) or 0.0)
        pen_sut_ev = sayiya_cevir(m.get('ev_pen_sut')) or 0.0
        pen_sut_dep = sayiya_cevir(m.get('dep_pen_sut')) or 0.0
        pen_gol_ev = sayiya_cevir(m.get('ev_pen_gol')) or 0.0
        pen_gol_dep = sayiya_cevir(m.get('dep_pen_gol')) or 0.0

        # 1. GERÇEKÇİ SENTETİK xG (İsabetli Şut + Engellenmeyen Şut + Rakip Kaleci Kurtarışı + İsabetli Orta)
        efektif_attigi = min(4.2, attigi)
        if istatistik_var and isabet_atti is not None and sut_atti is not None and sut_atti > 0:
            blok_a = engel_atti or 0.0
            acik_isabetsiz = max(0.0, sut_atti - isabet_atti - blok_a)
            kurt_etkisi = (rakip_kurtaris or 0.0) * 0.065
            orta_etkisi = ((orta_isabet or 0.0) * 0.035) if (orta_isabet and orta_isabet > 0) else 0.0
            xg_ham = (isabet_atti * 0.265) + (acik_isabetsiz * 0.042) + (blok_a * 0.018) + kurt_etkisi + orta_etkisi
            gercek_pen_haric = max(0.0, min(4.2, attigi) - (pen_gol_atti * 0.28))
            efektif_attigi = (gercek_pen_haric * 0.52) + (xg_ham * 0.48)

        efektif_yedigi = min(4.2, yedigi)
        if istatistik_var and isabet_yedi is not None and sut_yedi is not None and sut_yedi > 0:
            blok_y = engel_yedi or 0.0
            acik_isabetsiz_y = max(0.0, sut_yedi - isabet_yedi - blok_y)
            kurt_etkisi_y = (kurtaris_yapti or 0.0) * 0.055
            xg_ham_y = (isabet_yedi * 0.265) + (acik_isabetsiz_y * 0.042) + (blok_y * 0.018) + kurt_etkisi_y
            gercek_pen_haric_y = max(0.0, min(4.2, yedigi) - (pen_gol_yedi * 0.28))
            efektif_yedigi = (gercek_pen_haric_y * 0.52) + (xg_ham_y * 0.48)

        veriler["gol_atti"].append((efektif_attigi, efektif_w))
        veriler["gol_yedi"].append((efektif_yedigi, efektif_w))

        # 2. KALECİ KURTARIŞ DİRENCİ VE SAVUNMA BLOK ORANI
        if istatistik_var:
            kaleye_gelen = (kurtaris_yapti or 0.0) + yedigi
            kurt_orani = ((kurtaris_yapti or 0.0) / kaleye_gelen) if kaleye_gelen > 0 else 0.68
            veriler["kaleci_kurtaris_orani"].append((kurt_orani, efektif_w))

            if sut_yedi and sut_yedi > 0:
                blok_orani = min(0.65, (engel_yedi or 0.0) / sut_yedi)
                veriler["sut_blok_orani"].append((blok_orani, efektif_w))

            # Maç Tempo Endeksi (Toplam Şut + Korner + Uzun Top yoğunluğu)
            toplam_mac_sut = (sut_atti or 11.0) + (sut_yedi or 11.0)
            uzun_etki = ((uzun_top or 48.0) / 50.0) if (uzun_top and uzun_top > 0) else 0.95
            tempo = (toplam_mac_sut / 24.0) * 0.75 + (uzun_etki * 0.25)
            veriler["tempo_endeksi"].append((tempo, efektif_w))

        # 3. İLK YARI VE İKİNCİ YARI YOĞUNLUKLARI
        if iy_attigi is not None and iy_yedigi is not None:
            iy_a_sinirli = min(3.0, iy_attigi)
            iy_y_sinirli = min(3.0, iy_yedigi)
            veriler["iy_gol_atti"].append((iy_a_sinirli, efektif_w))
            veriler["iy_gol_yedi"].append((iy_y_sinirli, efektif_w))
            veriler["2y_gol_atti"].append((max(0.0, min(3.5, attigi - iy_attigi)), efektif_w))
            veriler["2y_gol_yedi"].append((max(0.0, min(3.5, yedigi - iy_yedigi)), efektif_w))

            veriler["yogunluk_iy_onde"].append((1.0 if iy_attigi > iy_yedigi else 0.0, zaman_w))
            veriler["yogunluk_iy_berabere"].append((1.0 if iy_attigi == iy_yedigi else 0.0, zaman_w))
            veriler["yogunluk_iy_1_1"].append((1.0 if (iy_attigi > iy_yedigi and attigi > yedigi) else 0.0, zaman_w))
            veriler["yogunluk_iy_x_1"].append((1.0 if (iy_attigi == iy_yedigi and attigi > yedigi) else 0.0, zaman_w))
            veriler["yogunluk_iy_05_ust"].append((1.0 if (iy_attigi + iy_yedigi) >= 1 else 0.0, zaman_w))
            veriler["yogunluk_iy_15_ust"].append((1.0 if (iy_attigi + iy_yedigi) >= 2 else 0.0, zaman_w))
            veriler["yogunluk_iy_kg"].append((1.0 if (iy_attigi > 0 and iy_yedigi > 0) else 0.0, zaman_w))

        veriler["yogunluk_kg_var"].append((1.0 if (attigi > 0 and yedigi > 0) else 0.0, zaman_w))
        veriler["yogunluk_25_ust"].append((1.0 if (attigi + yedigi) >= 3 else 0.0, zaman_w))

        # 4. KORNER YOĞUNLUĞU (Gerçek Korner + Bloklanan Şut + Orta Baskısı)
        kor_atti = (sayiya_cevir(m.get('ev_korner')) if is_ev else sayiya_cevir(m.get('dep_korner'))) if istatistik_var else None
        kor_yedi = (sayiya_cevir(m.get('dep_korner')) if is_ev else sayiya_cevir(m.get('ev_korner'))) if istatistik_var else None
        if kor_atti is not None and kor_yedi is not None and (kor_atti + kor_yedi) > 0:
            ham_toplam_kor = kor_atti + kor_yedi
            if sut_atti is not None:
                baski_kor = (sut_atti * 0.17) + ((engel_atti or 0.0) * 0.36) + ((orta_toplam * 0.12) if (orta_toplam and orta_toplam > 0) else 1.15)
                kor_atti_ef = (kor_atti * 0.70) + (baski_kor * 0.30)
            else:
                kor_atti_ef = kor_atti
            veriler["korner_atti"].append((kor_atti_ef, efektif_w))
            veriler["korner_yedi"].append((kor_yedi, efektif_w))
            veriler["toplam_korner"].append((ham_toplam_kor, efektif_w))
            veriler["yogunluk_korner_95"].append((1.0 if ham_toplam_kor >= 10 else 0.0, zaman_w))
            veriler["yogunluk_korner_105"].append((1.0 if ham_toplam_kor >= 11 else 0.0, zaman_w))

        # 5. KART VE FAUL YOĞUNLUĞU
        sari_aldi = (sayiya_cevir(m.get('ev_sari')) if is_ev else sayiya_cevir(m.get('dep_sari'))) if istatistik_var else None
        sari_yedirdi = (sayiya_cevir(m.get('dep_sari')) if is_ev else sayiya_cevir(m.get('ev_sari'))) if istatistik_var else None
        faul_yapti = (sayiya_cevir(m.get('ev_faul')) if is_ev else sayiya_cevir(m.get('dep_faul'))) if istatistik_var else None
        faul_maruz = (sayiya_cevir(m.get('dep_faul')) if is_ev else sayiya_cevir(m.get('ev_faul'))) if istatistik_var else None

        if sari_aldi is not None and sari_yedirdi is not None:
            ham_toplam_sari = sari_aldi + sari_yedirdi
            ef_sari_a = (sari_aldi * 0.68) + (((faul_yapti / 6.0) * 0.32) if (faul_yapti and faul_yapti > 0) else (sari_aldi * 0.32))
            ef_sari_y = (sari_yedirdi * 0.68) + (((faul_maruz / 6.0) * 0.32) if (faul_maruz and faul_maruz > 0) else (sari_yedirdi * 0.32))
            veriler["kart_aldi"].append((ef_sari_a, zaman_w))
            veriler["kart_yedirdi"].append((ef_sari_y, zaman_w))
            veriler["toplam_kart"].append((ham_toplam_sari, zaman_w))
            veriler["yogunluk_kart_35"].append((1.0 if ham_toplam_sari >= 4 else 0.0, zaman_w))
            veriler["yogunluk_kart_45"].append((1.0 if ham_toplam_sari >= 5 else 0.0, zaman_w))

            toplam_faul = (faul_yapti or 12.0) + (faul_maruz or 12.0)
            sertlik_ek = 0.09 if (toplam_faul >= 28 or ham_toplam_sari >= 5) else 0.02
            kirmizi_agirlik = min(1.0, (min(2.0, mac_kirmizi) * 0.60) + sertlik_ek)
            veriler["kirmizi_etki"].append((kirmizi_agirlik, zaman_w))

        if istatistik_var:
            gercek_pen = min(2.0, max(pen_sut_ev, pen_gol_ev) + max(pen_sut_dep, pen_gol_dep))
            ceza_baski = 0.11 if ((isabet_atti or 0) >= 6 or (orta_isabet or 0) >= 6 or (sut_atti or 0) >= 18) else 0.03
            veriler["penalti_etki"].append((min(1.15, (gercek_pen * 0.58) + ceza_baski), zaman_w))

    sonuc = {}
    for k, lst in veriler.items():
        if k.startswith("yogunluk_"):
            sonuc[k] = oran_yogunlugu_hesapla(lst)
        else:
            sonuc[k] = yogunluk_merkezi_hesapla(lst, medyan_agirligi=0.35)
    return sonuc

def takim_performansi_getir(cursor, takim_adi, mac_tarihi, saha="ev"):
    """
    Saha içi (Ev/Deplasman) performans yoğunluğu (%65) ile Genel form yoğunluğunu (%35)
    birleştirerek hem iç/dış saha karakterini korur hem de örneklem sapmasını sıfırlar.
    """
    if saha == "ev":
        cursor.execute("SELECT * FROM fikstur WHERE ev_sahibi = %s AND tarih < %s AND durum LIKE '%%FT%%' ORDER BY tarih DESC LIMIT 12", (takim_adi, mac_tarihi))
    else:
        cursor.execute("SELECT * FROM fikstur WHERE deplasman = %s AND tarih < %s AND durum LIKE '%%FT%%' ORDER BY tarih DESC LIMIT 12", (takim_adi, mac_tarihi))
    saha_maclar = cursor.fetchall()

    cursor.execute("SELECT * FROM fikstur WHERE (ev_sahibi = %s OR deplasman = %s) AND tarih < %s AND durum LIKE '%%FT%%' ORDER BY tarih DESC LIMIT 16", (takim_adi, takim_adi, mac_tarihi))
    genel_maclar = cursor.fetchall()

    if len(genel_maclar) < 3:
        return None

    saha_profil = _ham_maclari_analiz_et(saha_maclar, takim_adi) if len(saha_maclar) >= 3 else None
    genel_profil = _ham_maclari_analiz_et(genel_maclar, takim_adi)

    if not genel_profil:
        return None
    if not saha_profil:
        return genel_profil

    saha_agirlik = 0.65 if len(saha_maclar) >= 5 else 0.50
    genel_agirlik = 1.0 - saha_agirlik

    harman = {}
    for k in genel_profil.keys():
        sv = saha_profil.get(k)
        gv = genel_profil.get(k)
        if sv is not None and gv is not None:
            harman[k] = (sv * saha_agirlik) + (gv * genel_agirlik)
        else:
            harman[k] = sv if sv is not None else gv
    return harman

# --- YOĞUNLUK VE MATRİS KONSENSÜSLÜ KARAR MOTORU ---
def quant_karar_motoru(ev_xg, dep_xg, ev_iy_xg, dep_iy_xg, ev_ort, dep_ort, p_ms_1, p_ms_x, p_ms_2, p_iy_1, p_iy_x, p_iy_2, p_o15, p_o25, p_o35, hist_btts, p_iy_o05, p_iy_o15, exp_corners, exp_cards, p_kart_35_db, p_kart_45_db, p_kirmizi, p_penalti):
    tot_xg = ev_xg + dep_xg
    xg_diff = ev_xg - dep_xg
    iy_tot_xg = ev_iy_xg + dep_iy_xg

    ev_2y_xg = max(0.15, ev_xg - ev_iy_xg)
    dep_2y_xg = max(0.12, dep_xg - dep_iy_xg)

    # 1. Yarı 5x5 ve 2. Yarı 6x6 Dixon-Coles Matrisi
    mat_iy = [[0.0]*5 for _ in range(5)]
    sum_iy = 0.0
    for i1 in range(5):
        for j1 in range(5):
            p1 = poisson_ihtimal(ev_iy_xg, i1) * poisson_ihtimal(dep_iy_xg, j1)
            tau1 = dixon_coles_duzeltmesi(i1, j1, ev_iy_xg, dep_iy_xg, -0.09)
            val1 = max(0.0, p1 * tau1)
            mat_iy[i1][j1] = val1
            sum_iy += val1

    mat_2y = [[0.0]*6 for _ in range(6)]
    sum_2y = 0.0
    for i2 in range(6):
        for j2 in range(6):
            p2 = poisson_ihtimal(ev_2y_xg, i2) * poisson_ihtimal(dep_2y_xg, j2)
            tau2 = dixon_coles_duzeltmesi(i2, j2, ev_2y_xg, dep_2y_xg, -0.08)
            val2 = max(0.0, p2 * tau2)
            mat_2y[i2][j2] = val2
            sum_2y += val2

    iy_ms_probs = {
        '1/1': 0.0, 'x/1': 0.0, '2/1': 0.0,
        '1/x': 0.0, 'x/x': 0.0, '2/x': 0.0,
        '1/2': 0.0, 'x/2': 0.0, '2/2': 0.0
    }
    p_ms1_kg = p_ms2_kg = p_ms1_o25 = p_ms2_o25 = p_kg_o25 = p_iy_kg = p_iki_yari_kg = 0.0

    if sum_iy > 0 and sum_2y > 0:
        for i1 in range(5):
            for j1 in range(5):
                prob_1h = mat_iy[i1][j1] / sum_iy
                iy_res = '1' if i1 > j1 else ('x' if i1 == j1 else '2')
                if i1 > 0 and j1 > 0:
                    p_iy_kg += prob_1h * 100.0

                for i2 in range(6):
                    for j2 in range(6):
                        prob_2h = mat_2y[i2][j2] / sum_2y
                        joint_prob = prob_1h * prob_2h * 100.0

                        ft_ev = i1 + i2
                        ft_dep = j1 + j2
                        ft_tot = ft_ev + ft_dep
                        ms_res = '1' if ft_ev > ft_dep else ('x' if ft_ev == ft_dep else '2')

                        iy_ms_probs[f"{iy_res}/{ms_res}"] += joint_prob

                        if ft_ev > ft_dep and ft_dep > 0: p_ms1_kg += joint_prob
                        if ft_dep > ft_ev and ft_ev > 0: p_ms2_kg += joint_prob
                        if ft_ev > ft_dep and ft_tot >= 3: p_ms1_o25 += joint_prob
                        if ft_dep > ft_ev and ft_tot >= 3: p_ms2_o25 += joint_prob
                        if ft_ev > 0 and ft_dep > 0 and ft_tot >= 3: p_kg_o25 += joint_prob
                        if i1 > 0 and j1 > 0 and i2 > 0 and j2 > 0: p_iki_yari_kg += joint_prob

    # GEÇMİŞ MAÇLARDAKİ GERÇEKLEŞME YOĞUNLUKLARI (DENSITY BLENDING)
    ev_iy_onde_yog = (ev_ort.get('yogunluk_iy_onde') or 0.32) * 100.0
    dep_iy_onde_yog = (dep_ort.get('yogunluk_iy_onde') or 0.28) * 100.0
    ev_1_1_yog = (ev_ort.get('yogunluk_iy_1_1') or 0.25) * 100.0
    dep_2_2_yog = (dep_ort.get('yogunluk_iy_1_1') or 0.22) * 100.0
    ort_iy_ber_yog = (((ev_ort.get('yogunluk_iy_berabere') or 0.42) + (dep_ort.get('yogunluk_iy_berabere') or 0.42)) / 2.0) * 100.0

    ev_kurt = ev_ort.get('kaleci_kurtaris_orani') or 0.68
    dep_kurt = dep_ort.get('kaleci_kurtaris_orani') or 0.68
    ev_blok = ev_ort.get('sut_blok_orani') or 0.25
    dep_blok = dep_ort.get('sut_blok_orani') or 0.25

    # Savunma Duvarı Katsayısı (Rakip kaleci kurtarış oranı + şut bloklama oranı yüksekse İY X/1 veya X/2 yoğunluğu artar)
    dep_duvar = (dep_kurt * 0.65) + (dep_blok * 0.35)
    ev_duvar = (ev_kurt * 0.65) + (ev_blok * 0.35)

    p_iy_ms_1_1 = min(85.0, (iy_ms_probs['1/1'] * 0.58) + (ev_1_1_yog * 0.24) + (min(ev_iy_onde_yog, p_ms_1) * 0.18))
    p_iy_ms_2_2 = min(85.0, (iy_ms_probs['2/2'] * 0.58) + (dep_2_2_yog * 0.24) + (min(dep_iy_onde_yog, p_ms_2) * 0.18))

    x1_duvar_bonus = 2.6 if (dep_duvar >= 0.54 and ev_2y_xg >= 0.98) else 0.0
    x2_duvar_bonus = 2.6 if (ev_duvar >= 0.54 and dep_2y_xg >= 0.98) else 0.0

    p_iy_ms_x_1 = min(65.0, (iy_ms_probs['x/1'] * 0.68) + ((ort_iy_ber_yog * (p_ms_1 / 100.0)) * 0.32) + x1_duvar_bonus)
    p_iy_ms_x_2 = min(65.0, (iy_ms_probs['x/2'] * 0.68) + ((ort_iy_ber_yog * (p_ms_2 / 100.0)) * 0.32) + x2_duvar_bonus)
    p_iy_ms_x_x = min(65.0, (iy_ms_probs['x/x'] * 0.70) + ((ort_iy_ber_yog * (p_ms_x / 100.0)) * 0.30))

    # Korner ve Kart: Matris Olasılığı (%65) + Geçmiş Maç Yoğunluğu (%35) Konsensüsü
    nb_kor_105 = asiri_dagilim_olasilik(exp_corners, 10.5, 1.35)
    kor_105_yog = (((ev_ort.get('yogunluk_korner_105') or 0.35) + (dep_ort.get('yogunluk_korner_105') or 0.35)) / 2.0) * 100.0
    p_kor_105 = ((nb_kor_105 * 0.68) + (kor_105_yog * 0.32)) if nb_kor_105 is not None else None

    nb_kart_45 = asiri_dagilim_olasilik(exp_cards, 4.5, 1.28)
    kart_45_yog = (((ev_ort.get('yogunluk_kart_45') or 0.35) + (dep_ort.get('yogunluk_kart_45') or 0.35)) / 2.0) * 100.0
    if nb_kart_45 is not None and p_kart_45_db is not None:
        p_kart_45 = (nb_kart_45 * 0.45) + (p_kart_45_db * 0.30) + (kart_45_yog * 0.25)
    else:
        p_kart_45 = nb_kart_45 or p_kart_45_db

    # İlk Yarı Gol ve KG Yoğunluk Harmanı
    iy_15_yog = (((ev_ort.get('yogunluk_iy_15_ust') or 0.32) + (dep_ort.get('yogunluk_iy_15_ust') or 0.32)) / 2.0) * 100.0
    p_iy_o15_cal = ((p_iy_o15 * 0.68) + (iy_15_yog * 0.32)) if p_iy_o15 is not None else 0.0

    iy_kg_yog = (((ev_ort.get('yogunluk_iy_kg') or 0.20) + (dep_ort.get('yogunluk_iy_kg') or 0.20)) / 2.0) * 100.0
    p_iy_kg_cal = (p_iy_kg * 0.70) + (iy_kg_yog * 0.30)

    pois_btts = (1.0 - math.exp(-ev_xg)) * (1.0 - math.exp(-dep_xg)) * 100.0
    true_btts = (hist_btts * 0.50) + (pois_btts * 0.50)
    y25_yog = (((ev_ort.get('yogunluk_25_ust') or 0.50) + (dep_ort.get('yogunluk_25_ust') or 0.50)) / 2.0) * 100.0
    p_kg_o25_cal = (p_kg_o25 * 0.55) + (min(true_btts, p_o25) * 0.25) + (min(hist_btts, y25_yog) * 0.20)

    candidates = {
        'iy/ms 1/1': {
            'prob': p_iy_ms_1_1, 'min': 41.0, 'oran': '2.25 - 3.10',
            'valid': (ev_xg >= 1.95 and xg_diff >= 0.85 and dep_xg <= 1.10 and ev_iy_xg >= 0.84 and ev_iy_onde_yog >= 34.0),
            'weight': 1.95, 'bonus': 16.0
        },
        'iy/ms 2/2': {
            'prob': p_iy_ms_2_2, 'min': 42.0, 'oran': '2.40 - 3.40',
            'valid': (dep_xg >= 1.95 and (-xg_diff) >= 0.85 and ev_xg <= 1.05 and dep_iy_xg >= 0.84 and dep_iy_onde_yog >= 34.0),
            'weight': 1.95, 'bonus': 16.0
        },
        'iy/ms x/1': {
            'prob': p_iy_ms_x_1, 'min': 23.2, 'oran': '4.10 - 5.20',
            'valid': (ev_xg >= 1.58 and xg_diff >= 0.55 and dep_xg <= 1.02 and iy_tot_xg <= 1.24 and ev_2y_xg >= 0.92 and ort_iy_ber_yog >= 38.0),
            'weight': 2.20, 'bonus': 15.5
        },
        'iy/ms x/2': {
            'prob': p_iy_ms_x_2, 'min': 23.2, 'oran': '4.50 - 5.80',
            'valid': (dep_xg >= 1.58 and (-xg_diff) >= 0.55 and ev_xg <= 1.02 and iy_tot_xg <= 1.24 and dep_2y_xg >= 0.92 and ort_iy_ber_yog >= 38.0),
            'weight': 2.20, 'bonus': 15.5
        },
        'iy/ms x/x': {
            'prob': p_iy_ms_x_x, 'min': 24.2, 'oran': '4.20 - 5.25',
            'valid': (tot_xg <= 2.18 and abs(xg_diff) <= 0.26 and iy_tot_xg <= 1.00 and ort_iy_ber_yog >= 42.0),
            'weight': 2.15, 'bonus': 15.0
        },
        'iy/ms 2/1': {
            'prob': iy_ms_probs['2/1'], 'min': 7.6, 'oran': '18.00 - 25.00',
            'valid': (ev_xg >= 2.15 and dep_iy_xg >= 0.72 and ev_2y_xg >= 1.38),
            'weight': 2.55, 'bonus': 14.0
        },
        'iy/ms 1/2': {
            'prob': iy_ms_probs['1/2'], 'min': 7.6, 'oran': '20.00 - 28.00',
            'valid': (dep_xg >= 2.15 and ev_iy_xg >= 0.72 and dep_2y_xg >= 1.38),
            'weight': 2.55, 'bonus': 14.0
        },
        'ms 1 ve kg var': {
            'prob': p_ms1_kg, 'min': 34.5, 'oran': '2.85 - 3.75',
            'valid': (ev_xg >= 2.10 and dep_xg >= 1.15 and xg_diff >= 0.65 and hist_btts >= 58.0),
            'weight': 2.00, 'bonus': 15.0
        },
        'ms 2 ve kg var': {
            'prob': p_ms2_kg, 'min': 34.5, 'oran': '3.10 - 4.10',
            'valid': (dep_xg >= 2.10 and ev_xg >= 1.15 and (-xg_diff) >= 0.65 and hist_btts >= 58.0),
            'weight': 2.00, 'bonus': 15.0
        },
        'ms 1 ve 2.5 üst': {
            'prob': p_ms1_o25, 'min': 47.5, 'oran': '2.05 - 2.65',
            'valid': (ev_xg >= 2.15 and xg_diff >= 0.85 and tot_xg >= 3.18 and y25_yog >= 52.0),
            'weight': 1.85, 'bonus': 14.5
        },
        'ms 2 ve 2.5 üst': {
            'prob': p_ms2_o25, 'min': 47.5, 'oran': '2.20 - 2.85',
            'valid': (dep_xg >= 2.15 and (-xg_diff) >= 0.85 and tot_xg >= 3.18 and y25_yog >= 52.0),
            'weight': 1.85, 'bonus': 14.5
        },
        'iki yarıda da kg var': {
            'prob': p_iki_yari_kg, 'min': 14.0, 'oran': '11.00 - 15.00',
            'valid': (ev_xg >= 1.85 and dep_xg >= 1.85 and iy_tot_xg >= 1.65 and hist_btts >= 65.0),
            'weight': 2.40, 'bonus': 15.5
        },
        'ilk yarı kg var': {
            'prob': p_iy_kg_cal, 'min': 32.0, 'oran': '3.60 - 4.50',
            'valid': (ev_iy_xg >= 0.80 and dep_iy_xg >= 0.80 and tot_xg >= 3.30),
            'weight': 2.05, 'bonus': 14.5
        },
        'ilk yarı 1.5 üst': {
            'prob': p_iy_o15_cal, 'min': 49.0, 'oran': '2.25 - 2.75',
            'valid': (p_iy_o15 is not None and iy_tot_xg >= 1.58 and tot_xg >= 3.35 and iy_15_yog >= 38.0),
            'weight': 1.80, 'bonus': 14.0
        },
        'kg var ve 2.5 üst': {
            'prob': p_kg_o25_cal, 'min': 59.0, 'oran': '1.95 - 2.35',
            'valid': (ev_xg >= 1.45 and dep_xg >= 1.45 and tot_xg >= 3.25 and hist_btts >= 64.0 and abs(xg_diff) <= 0.65),
            'weight': 1.70, 'bonus': 13.0
        },
        '10.5 korner üst': {
            'prob': p_kor_105 or 0, 'min': 60.0, 'oran': '2.15 - 2.60',
            'valid': (exp_corners is not None and exp_corners >= 11.75 and kor_105_yog >= 42.0),
            'weight': 1.75, 'bonus': 13.5
        },
        '4.5 sarı kart üst': {
            'prob': p_kart_45 or 0, 'min': 62.5, 'oran': '2.05 - 2.50',
            'valid': (exp_cards is not None and exp_cards >= 5.15 and abs(xg_diff) <= 0.90 and kart_45_yog >= 44.0),
            'weight': 1.75, 'bonus': 13.5
        },
        'penaltı olur': {
            'prob': p_penalti or 0, 'min': 40.5, 'oran': '2.75 - 3.30',
            'valid': (p_penalti is not None and p_penalti >= 40.5 and tot_xg >= 2.90),
            'weight': 1.90, 'bonus': 14.0
        },
        'kırmızı kart çıkar': {
            'prob': p_kirmizi or 0, 'min': 35.5, 'oran': '3.40 - 4.20',
            'valid': (p_kirmizi is not None and p_kirmizi >= 35.5 and exp_cards is not None and exp_cards >= 4.90),
            'weight': 1.90, 'bonus': 14.0
        }
    }

    passed = {}
    for market, rule in candidates.items():
        if rule['valid'] and rule['prob'] >= rule['min']:
            edge = rule['prob'] - rule['min']
            score = rule['bonus'] + (edge * rule['weight'])
            passed[market] = {
                'market': market,
                'prob': round(rule['prob'], 1),
                'edge': round(edge, 1),
                'oran': rule['oran'],
                'score': round(score, 2)
            }

    if not passed:
        en_yakin_iy_ms = max(iy_ms_probs, key=iy_ms_probs.get)
        fb_market = f"iy/ms {en_yakin_iy_ms}"
        fb_prob = round(iy_ms_probs[en_yakin_iy_ms], 1)
        ai_report = (
            "<div class='text-[10px] text-zinc-300 font-bold mb-1'>karar gerekçesi (pas / baraj altı):</div>"
            f"<p class='text-[10px] text-zinc-500 mb-2'>geçmiş maçların yoğunluk (medyan) ve 4d matris kontrolünde hiçbir yüksek oranlı market güven barajını aşamadı. en yakın ihtimal <b>{fb_market}</b> (%{fb_prob}) kaldı.</p>"
        )
        return {
            'secilen_tahmin': fb_market,
            'tahmin_olasilik': fb_prob,
            'kategori': 'RISKLI',
            'guven_skoru': round(fb_prob * 0.3, 2),
            'ai_rapor': ai_report
        }

    passed_list = sorted(passed.values(), key=lambda x: x['score'], reverse=True)
    winner = passed_list[0]
    runner_up = passed_list[1] if len(passed_list) > 1 else None

    kategori = 'BANKO' if (winner['score'] >= 22.0 or winner['edge'] >= 4.5) else 'IDEAL'

    ek_bilgi = f"ms xg: {ev_xg:.2f}-{dep_xg:.2f} | iy xg: {ev_iy_xg:.2f}-{dep_iy_xg:.2f}"
    if exp_corners is not None: ek_bilgi += f" | korner: {exp_corners:.1f}"
    if exp_cards is not None: ek_bilgi += f" | kart: {exp_cards:.1f}"

    ai_report = (
        f"<div class='text-[10px] text-zinc-300 font-bold mb-1'>neden {winner['market']} seçildi? (tahmini oran: {winner['oran']})</div>"
        f"<p class='text-[10px] text-zinc-500 mb-2'>geçmiş maçlardaki yoğunluk (medyan) analizi ve lig bazlı çarpımsal 4d matriste güven sınırını <b class='text-emerald-400'>+{winner['edge']} puan</b> aştı ({ek_bilgi}).</p>"
    )
    if runner_up:
        fark = round(winner['score'] - runner_up['score'], 1)
        ai_report += (
            f"<div class='text-[10px] text-zinc-300 font-bold mb-1'>en yakın yüksek oranlı alternatif: {runner_up['market']}</div>"
            f"<p class='text-[10px] text-zinc-500'>sistem alternatif olarak <b>{runner_up['market']}</b> (%{runner_up['prob']} • oran: {runner_up['oran']}) tercihini de onayladı ancak ana seçim <b>+{fark} quant puanı</b> farkla öne geçti.</p>"
        )
    else:
        ai_report += (
            "<div class='text-[10px] text-zinc-300 font-bold mb-1'>tekil yoğunluk onayı</div>"
            "<p class='text-[10px] text-zinc-500'>diğer marketler geçmiş maç yoğunluğu ve kaleci/savunma direnci filtresine takıldı.</p>"
        )

    return {
        'secilen_tahmin': winner['market'],
        'tahmin_olasilik': winner['prob'],
        'kategori': kategori,
        'guven_skoru': winner['score'],
        'ai_rapor': ai_report
    }

def mac_sonucu_hesapla(secilen_tahmin, m):
    durum_str = str(m.get('durum') or '').upper().strip()
    if 'CANCELED' in durum_str or 'POSTPONED' in durum_str:
        return 'IPTAL'
    if 'FT' not in durum_str:
        return 'BEKLIYOR'

    ev_s = sayiya_cevir(m.get('ev_skor'))
    dep_s = sayiya_cevir(m.get('dep_skor'))
    if ev_s is None or dep_s is None:
        return 'BEKLIYOR'

    ev = int(ev_s)
    dep = int(dep_s)
    toplam = ev + dep

    ev_iy_s = sayiya_cevir(m.get('ev_iy_skor'))
    dep_iy_s = sayiya_cevir(m.get('dep_iy_skor'))
    has_iy = (ev_iy_s is not None and dep_iy_s is not None)
    ev_iy = int(ev_iy_s) if has_iy else 0
    dep_iy = int(dep_iy_s) if has_iy else 0

    ev_kor_s = sayiya_cevir(m.get('ev_korner'))
    dep_kor_s = sayiya_cevir(m.get('dep_korner'))
    has_kor = (ev_kor_s is not None and dep_kor_s is not None)

    ev_sari_s = sayiya_cevir(m.get('ev_sari'))
    dep_sari_s = sayiya_cevir(m.get('dep_sari'))
    has_kart = (ev_sari_s is not None and dep_sari_s is not None)

    ev_kirmizi_s = sayiya_cevir(m.get('ev_kirmizi'))
    dep_kirmizi_s = sayiya_cevir(m.get('dep_kirmizi'))
    has_kirmizi = (ev_kirmizi_s is not None and dep_kirmizi_s is not None)

    ev_pen_sut = sayiya_cevir(m.get('ev_pen_sut'))
    dep_pen_sut = sayiya_cevir(m.get('dep_pen_sut'))
    ev_pen_gol = sayiya_cevir(m.get('ev_pen_gol'))
    dep_pen_gol = sayiya_cevir(m.get('dep_pen_gol'))
    has_pen = (ev_pen_sut is not None and dep_pen_sut is not None) or (ev_pen_gol is not None and dep_pen_gol is not None)
    toplam_pen = int(max(ev_pen_sut or 0, ev_pen_gol or 0) + max(dep_pen_sut or 0, dep_pen_gol or 0))

    t = tr_kucuk(secilen_tahmin)

    if t == 'iy/ms 1/1': return ('KAZANDI' if (ev_iy > dep_iy and ev > dep) else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == 'iy/ms 2/2': return ('KAZANDI' if (dep_iy > ev_iy and dep > ev) else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == 'iy/ms x/1': return ('KAZANDI' if (ev_iy == dep_iy and ev > dep) else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == 'iy/ms x/2': return ('KAZANDI' if (ev_iy == dep_iy and dep > ev) else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == 'iy/ms x/x': return ('KAZANDI' if (ev_iy == dep_iy and ev == dep) else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == 'iy/ms 1/x': return ('KAZANDI' if (ev_iy > dep_iy and ev == dep) else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == 'iy/ms 2/x': return ('KAZANDI' if (dep_iy > ev_iy and ev == dep) else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == 'iy/ms 2/1': return ('KAZANDI' if (dep_iy > ev_iy and ev > dep) else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == 'iy/ms 1/2': return ('KAZANDI' if (ev_iy > dep_iy and dep > ev) else 'KAYBETTI') if has_iy else 'BEKLIYOR'

    if t == 'ms 1 ve kg var': return 'KAZANDI' if (ev > dep and ev > 0 and dep > 0) else 'KAYBETTI'
    if t == 'ms 2 ve kg var': return 'KAZANDI' if (dep > ev and ev > 0 and dep > 0) else 'KAYBETTI'
    if t == 'ms 1 ve 2.5 üst': return 'KAZANDI' if (ev > dep and toplam >= 3) else 'KAYBETTI'
    if t == 'ms 2 ve 2.5 üst': return 'KAZANDI' if (dep > ev and toplam >= 3) else 'KAYBETTI'
    if t == 'kg var ve 2.5 üst': return 'KAZANDI' if (ev > 0 and dep > 0 and toplam >= 3) else 'KAYBETTI'
    if t == 'ilk yarı kg var': return ('KAZANDI' if (ev_iy > 0 and dep_iy > 0) else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == 'ilk yarı 1.5 üst': return ('KAZANDI' if (ev_iy + dep_iy) >= 2 else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == 'iki yarıda da kg var': return ('KAZANDI' if (ev_iy > 0 and dep_iy > 0 and (ev - ev_iy) > 0 and (dep - dep_iy) > 0) else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == 'kg yok': return 'KAZANDI' if (ev == 0 or dep == 0) else 'KAYBETTI'

    if t == '10.5 korner üst': return ('KAZANDI' if (int(ev_kor_s) + int(dep_kor_s)) >= 11 else 'KAYBETTI') if has_kor else 'BEKLIYOR'
    if t == '9.5 korner üst': return ('KAZANDI' if (int(ev_kor_s) + int(dep_kor_s)) >= 10 else 'KAYBETTI') if has_kor else 'BEKLIYOR'
    if t == '8.5 korner üst': return ('KAZANDI' if (int(ev_kor_s) + int(dep_kor_s)) >= 9 else 'KAYBETTI') if has_kor else 'BEKLIYOR'
    if t in ('3.5 kart üst', '3.5 sarı kart üst'): return ('KAZANDI' if (int(ev_sari_s) + int(dep_sari_s)) >= 4 else 'KAYBETTI') if has_kart else 'BEKLIYOR'
    if t in ('4.5 kart üst', '4.5 sarı kart üst'): return ('KAZANDI' if (int(ev_sari_s) + int(dep_sari_s)) >= 5 else 'KAYBETTI') if has_kart else 'BEKLIYOR'
    if t == 'kırmızı kart çıkar': return ('KAZANDI' if (int(ev_kirmizi_s) + int(dep_kirmizi_s)) >= 1 else 'KAYBETTI') if has_kirmizi else 'BEKLIYOR'
    if t == 'penaltı olur': return ('KAZANDI' if toplam_pen >= 1 else 'KAYBETTI') if has_pen else 'BEKLIYOR'

    return 'BEKLIYOR'

def tahminleri_hesapla_ve_kaydet(cursor, mac, ev_ort, dep_ort):
    fikstur_id = mac['id']
    ev = mac['ev_sahibi']
    dep = mac['deplasman']
    lig = mac.get('lig') or 'BILINMEYEN'
    mac_tarihi = str(mac['tarih'])

    if not ev_ort or not dep_ort or ev_ort['gol_atti'] is None or dep_ort['gol_atti'] is None:
        cursor.execute("INSERT INTO tahminler (fikstur_id, durum) VALUES (%s, 'YETERSİZ VERİ') ON DUPLICATE KEY UPDATE durum = 'YETERSİZ VERİ'", (fikstur_id,))
        return

    # 1. LİG BAZLI ÇARPIMSAL GÜÇ MODELİ + KALECİ / BLOK / TEMPO DÜZELTMESİ
    lig_baz = lig_baz_istatistiklerini_getir(cursor, lig, mac_tarihi)
    ort_lig_gol = (lig_baz['ev_gol'] + lig_baz['dep_gol']) / 2.0

    carpimsal_ev = (ev_ort['gol_atti'] * dep_ort['gol_yedi']) / max(0.85, ort_lig_gol)
    dogrusal_ev = (ev_ort['gol_atti'] * 1.04 + dep_ort['gol_yedi'] * 0.96) / 2.0
    ham_ev_xg = (carpimsal_ev * 0.55) + (dogrusal_ev * 0.45)

    carpimsal_dep = (dep_ort['gol_atti'] * ev_ort['gol_yedi']) / max(0.85, ort_lig_gol)
    dogrusal_dep = (dep_ort['gol_atti'] * 0.96 + ev_ort['gol_yedi'] * 1.04) / 2.0
    ham_dep_xg = (carpimsal_dep * 0.55) + (dogrusal_dep * 0.45)

    # Tempo ve Kaleci Kurtarış Direnci Çarpanı
    mac_tempo = ((ev_ort.get('tempo_endeksi') or 1.0) + (dep_ort.get('tempo_endeksi') or 1.0)) / 2.0
    tempo_carpan = max(0.88, min(1.14, 0.70 + (mac_tempo * 0.30)))

    dep_kaleci_direnc = max(0.88, min(1.10, 1.0 + (0.68 - (dep_ort.get('kaleci_kurtaris_orani') or 0.68)) * 0.45))
    ev_kaleci_direnc = max(0.88, min(1.10, 1.0 + (0.68 - (ev_ort.get('kaleci_kurtaris_orani') or 0.68)) * 0.45))

    ev_xG = round(max(0.25, ham_ev_xg * tempo_carpan * dep_kaleci_direnc), 2)
    dep_xG = round(max(0.20, ham_dep_xg * tempo_carpan * ev_kaleci_direnc), 2)

    ev_iy_ham = ((ev_ort['iy_gol_atti'] or (ev_xG * 0.44)) + (dep_ort['iy_gol_yedi'] or (dep_xG * 0.44))) / 2.0
    dep_iy_ham = ((dep_ort['iy_gol_atti'] or (dep_xG * 0.44)) + (ev_ort['iy_gol_yedi'] or (ev_xG * 0.44))) / 2.0
    ev_iy_xG = round(max(0.12, (ev_iy_ham * 0.65) + ((ev_xG * 0.44) * 0.35)), 2)
    dep_iy_xG = round(max(0.10, (dep_iy_ham * 0.65) + ((dep_xG * 0.44) * 0.35)), 2)

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

    tarihsel_kg = ((ev_ort.get('yogunluk_kg_var') or 0.50) + (dep_ort.get('yogunluk_kg_var') or 0.50)) / 2.0
    p_kg_var = (tarihsel_kg * 0.45) + (pois_kg_var * 0.55)

    p_iy_0_g = poisson_ihtimal(ev_iy_xG + dep_iy_xG, 0)
    p_iy_1_g = poisson_ihtimal(ev_iy_xG + dep_iy_xG, 1)
    p_iy_0_5_ust = 1.0 - p_iy_0_g
    p_iy_1_5_ust = 1.0 - (p_iy_0_g + p_iy_1_g)

    beklenen_korner, p_korner_8_5, p_korner_9_5 = None, None, None
    if ev_ort['korner_atti'] is not None and dep_ort['korner_yedi'] is not None and dep_ort['korner_atti'] is not None and ev_ort['korner_yedi'] is not None:
        ham_korner = ((ev_ort['korner_atti'] + dep_ort['korner_yedi']) / 2.0) + ((dep_ort['korner_atti'] + ev_ort['korner_yedi']) / 2.0)
        beklenen_korner = round((ham_korner * 0.78) + (lig_baz['korner'] * 0.22), 2)
        p_korner_8_5 = round((1.0 - sum([poisson_ihtimal(beklenen_korner, k) for k in range(9)])) * 100, 2)
        p_korner_9_5 = round((1.0 - sum([poisson_ihtimal(beklenen_korner, k) for k in range(10)])) * 100, 2)

    beklenen_kart, p_kart_3_5, p_kart_4_5 = None, None, None
    if ev_ort['kart_aldi'] is not None and dep_ort['kart_yedirdi'] is not None and dep_ort['kart_aldi'] is not None and ev_ort['kart_yedirdi'] is not None:
        ham_kart = ((ev_ort['kart_aldi'] + dep_ort['kart_yedirdi']) / 2.0) + ((dep_ort['kart_aldi'] + ev_ort['kart_yedirdi']) / 2.0)
        beklenen_kart = round((ham_kart * 0.80) + (lig_baz['kart'] * 0.20), 2)
        p_kart_3_5 = round((1.0 - sum([poisson_ihtimal(beklenen_kart, k) for k in range(4)])) * 100, 2)
        p_kart_4_5 = round((1.0 - sum([poisson_ihtimal(beklenen_kart, k) for k in range(5)])) * 100, 2)

    p_kirmizi_kart = None
    if ev_ort['kirmizi_etki'] is not None and dep_ort['kirmizi_etki'] is not None:
        lambda_kirmizi = (ev_ort['kirmizi_etki'] + dep_ort['kirmizi_etki']) / 2.0
        p_kirmizi_kart = round((1.0 - math.exp(-lambda_kirmizi)) * 100, 2)

    p_penalti_olur = None
    if ev_ort['penalti_etki'] is not None and dep_ort['penalti_etki'] is not None:
        lambda_pen = (ev_ort['penalti_etki'] + dep_ort['penalti_etki']) / 2.0
        p_penalti_olur = round((1.0 - math.exp(-lambda_pen)) * 100, 2)

    karar = quant_karar_motoru(
        ev_xG, dep_xG, ev_iy_xG, dep_iy_xG, ev_ort, dep_ort,
        round(p_1*100, 2), round(p_x*100, 2), round(p_2*100, 2),
        round(p_iy_1*100, 2), round(p_iy_x*100, 2), round(p_iy_2*100, 2),
        round(p_1_5_ust*100, 2), round(p_2_5_ust*100, 2), round(p_3_5_ust*100, 2),
        round(p_kg_var*100, 2), round(p_iy_0_5_ust*100, 2), round(p_iy_1_5_ust*100, 2),
        beklenen_korner, beklenen_kart, p_kart_3_5, p_kart_4_5,
        p_kirmizi_kart, p_penalti_olur
    )

    sonuc_durumu = mac_sonucu_hesapla(karar['secilen_tahmin'], mac)

    sql = """
        INSERT INTO tahminler 
        (fikstur_id, durum, beklenen_ev_gol, beklenen_dep_gol, p_ms_1, p_ms_x, p_ms_2, p_iy_1, p_iy_x, p_iy_2, 
         p_gol_1_5_ust, p_gol_2_5_ust, p_gol_3_5_ust, p_kg_var, p_iy_0_5_ust, p_iy_1_5_ust, 
         beklenen_korner, p_korner_8_5_ust, p_korner_9_5_ust, beklenen_kart, p_kart_3_5_ust, p_kart_4_5_ust,
         p_kirmizi_kart, p_penalti_olur,
         secilen_tahmin, tahmin_olasilik, kategori, guven_skoru, sonuc, ai_rapor)
        VALUES (%s, 'HAZIR', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            durum='HAZIR', beklenen_ev_gol=VALUES(beklenen_ev_gol), beklenen_dep_gol=VALUES(beklenen_dep_gol),
            p_ms_1=VALUES(p_ms_1), p_ms_x=VALUES(p_ms_x), p_ms_2=VALUES(p_ms_2), p_iy_1=VALUES(p_iy_1), p_iy_x=VALUES(p_iy_x), p_iy_2=VALUES(p_iy_2),
            p_gol_1_5_ust=VALUES(p_gol_1_5_ust), p_gol_2_5_ust=VALUES(p_gol_2_5_ust), p_gol_3_5_ust=VALUES(p_gol_3_5_ust), p_kg_var=VALUES(p_kg_var),
            p_iy_0_5_ust=VALUES(p_iy_0_5_ust), p_iy_1_5_ust=VALUES(p_iy_1_5_ust),
            beklenen_korner=VALUES(beklenen_korner), p_korner_8_5_ust=VALUES(p_korner_8_5_ust), p_korner_9_5_ust=VALUES(p_korner_9_5_ust),
            beklenen_kart=VALUES(beklenen_kart), p_kart_3_5_ust=VALUES(p_kart_3_5_ust), p_kart_4_5_ust=VALUES(p_kart_4_5_ust),
            p_kirmizi_kart=VALUES(p_kirmizi_kart), p_penalti_olur=VALUES(p_penalti_olur),
            secilen_tahmin=VALUES(secilen_tahmin), tahmin_olasilik=VALUES(tahmin_olasilik),
            kategori=VALUES(kategori), guven_skoru=VALUES(guven_skoru), sonuc=VALUES(sonuc), ai_rapor=VALUES(ai_rapor)
    """
    degerler = (
        fikstur_id, ev_xG, dep_xG,
        round(p_1*100, 2), round(p_x*100, 2), round(p_2*100, 2), round(p_iy_1*100, 2), round(p_iy_x*100, 2), round(p_iy_2*100, 2),
        round(p_1_5_ust*100, 2), round(p_2_5_ust*100, 2), round(p_3_5_ust*100, 2), round(p_kg_var*100, 2),
        round(p_iy_0_5_ust*100, 2), round(p_iy_1_5_ust*100, 2),
        beklenen_korner, p_korner_8_5, p_korner_9_5,
        beklenen_kart, p_kart_3_5, p_kart_4_5,
        p_kirmizi_kart, p_penalti_olur,
        karar['secilen_tahmin'], karar['tahmin_olasilik'], karar['kategori'], karar['guven_skoru'], sonuc_durumu, karar['ai_rapor']
    )
    cursor.execute(sql, degerler)
    print(f"[{fikstur_id}] ✅ {ev} vs {dep} -> {karar['secilen_tahmin']} (%{karar['tahmin_olasilik']}) kaydedildi!")

def gunluk_kuponu_kilitle(cursor, tarih):
    cursor.execute("""
        SELECT COUNT(*) as kilitli_sayisi
        FROM tahminler t
        JOIN fikstur f ON f.id = t.fikstur_id
        WHERE f.tarih = %s AND t.kupon_maci = 1 AND (f.durum LIKE '%%FT%%' OR f.ev_skor != '-')
    """, (tarih,))
    row = cursor.fetchone()
    if row and row['kilitli_sayisi'] > 0:
        return

    cursor.execute("""
        UPDATE tahminler t
        JOIN fikstur f ON f.id = t.fikstur_id
        SET t.kupon_maci = 0
        WHERE f.tarih = %s
    """, (tarih,))

    cursor.execute("""
        SELECT t.id
        FROM tahminler t
        JOIN fikstur f ON f.id = t.fikstur_id
        WHERE f.tarih = %s AND t.durum = 'HAZIR' AND t.kategori IN ('BANKO', 'IDEAL')
        ORDER BY CASE WHEN t.kategori = 'BANKO' THEN 1 ELSE 2 END ASC, t.guven_skoru DESC
        LIMIT 1
    """, (tarih,))
    secilenler = cursor.fetchall()

    for s in secilenler:
        cursor.execute("UPDATE tahminler SET kupon_maci = 1 WHERE id = %s", (s['id'],))

def tahmin_motorunu_baslat():
    try:
        conn = db_baglantisi_kur()
        cursor = conn.cursor()
        bugun = datetime.now().strftime("%Y-%m-%d")
        yarin = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        cursor.execute("""
            SELECT f.* 
            FROM fikstur f
            WHERE f.tarih IN (%s, %s) AND f.durum NOT LIKE '%%FT%%'
        """, (bugun, yarin))
        maclar = cursor.fetchall()
        
        for mac in maclar:
            ev_ort = takim_performansi_getir(cursor, mac['ev_sahibi'], mac['tarih'], saha="ev")
            dep_ort = takim_performansi_getir(cursor, mac['deplasman'], mac['tarih'], saha="dep")
            tahminleri_hesapla_ve_kaydet(cursor, mac, ev_ort, dep_ort)

        for t_gun in [bugun, yarin]:
            gunluk_kuponu_kilitle(cursor, t_gun)

        cursor.execute("""
            SELECT t.id as tahmin_id, t.secilen_tahmin, f.durum, f.ev_skor, f.dep_skor, 
                   f.ev_iy_skor, f.dep_iy_skor, f.ev_korner, f.dep_korner, f.ev_sari, f.dep_sari,
                   f.ev_kirmizi, f.dep_kirmizi, f.ev_pen_sut, f.dep_pen_sut, f.ev_pen_gol, f.dep_pen_gol
            FROM tahminler t
            JOIN fikstur f ON f.id = t.fikstur_id
            WHERE t.secilen_tahmin IS NOT NULL AND t.sonuc = 'BEKLIYOR'
        """)
        bekleyenler = cursor.fetchall()
        for b in bekleyenler:
            yeni_sonuc = mac_sonucu_hesapla(b['secilen_tahmin'], b)
            if yeni_sonuc != 'BEKLIYOR':
                cursor.execute("UPDATE tahminler SET sonuc = %s WHERE id = %s", (yeni_sonuc, b['tahmin_id']))

        conn.close()
    except Exception:
        print(traceback.format_exc())

if __name__ == "__main__":
    tahmin_motorunu_baslat()
