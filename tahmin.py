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

def asiri_dagilim_olasilik(beklenen_deger, baraj, dagilim_katsayisi=1.40):
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

def sayiya_cevir(deger):
    if deger is None or deger == "-" or deger == "":
        return None
    try:
        temiz = str(deger).replace("%", "").strip()
        return float(temiz)
    except:
        return None

# --- MAÇ SONUCU DOĞRULAMA MOTORU (SADECE 'FT' İSE SONUÇLANDIRIR) ---
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
    toplam_pen = int((ev_pen_sut or ev_pen_gol or 0) + (dep_pen_sut or dep_pen_gol or 0))

    t = tr_kucuk(secilen_tahmin)

    if t == 'maç sonu 1': return 'KAZANDI' if ev > dep else 'KAYBETTI'
    if t == 'maç sonu 2': return 'KAZANDI' if dep > ev else 'KAYBETTI'
    if t == 'ms 1 ve 1.5 üst': return 'KAZANDI' if (ev > dep and toplam >= 2) else 'KAYBETTI'
    if t == 'ms 2 ve 1.5 üst': return 'KAZANDI' if (dep > ev and toplam >= 2) else 'KAYBETTI'
    if t == '1x ve 1.5 üst': return 'KAZANDI' if (ev >= dep and toplam >= 2) else 'KAYBETTI'
    if t == 'x2 ve 1.5 üst': return 'KAZANDI' if (dep >= ev and toplam >= 2) else 'KAYBETTI'
    if t == 'ev 1.5 gol üst': return 'KAZANDI' if ev >= 2 else 'KAYBETTI'
    if t == 'dep 1.5 gol üst': return 'KAZANDI' if dep >= 2 else 'KAYBETTI'
    if t == 'kg var ve 2.5 üst': return 'KAZANDI' if (ev > 0 and dep > 0 and toplam >= 3) else 'KAYBETTI'
    if t == '1x çifte şans': return 'KAZANDI' if ev >= dep else 'KAYBETTI'
    if t == 'x2 çifte şans': return 'KAZANDI' if dep >= ev else 'KAYBETTI'
    if t == '1.5 gol üst': return 'KAZANDI' if toplam >= 2 else 'KAYBETTI'
    if t == '2.5 gol üst': return 'KAZANDI' if toplam >= 3 else 'KAYBETTI'
    if t == '2.5 gol alt': return 'KAZANDI' if toplam <= 2 else 'KAYBETTI'
    if t == '3.5 gol alt': return 'KAZANDI' if toplam <= 3 else 'KAYBETTI'
    if t == 'karşılıklı gol var': return 'KAZANDI' if (ev > 0 and dep > 0) else 'KAYBETTI'
    if t == 'ilk yarı 0.5 üst':
        return ('KAZANDI' if (ev_iy + dep_iy) >= 1 else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == 'ilk yarı 1':
        return ('KAZANDI' if ev_iy > dep_iy else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == 'ilk yarı 2':
        return ('KAZANDI' if dep_iy > ev_iy else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == '2. yarı 1.5 üst':
        return ('KAZANDI' if (toplam - (ev_iy + dep_iy)) >= 2 else 'KAYBETTI') if has_iy else 'BEKLIYOR'
    if t == '8.5 korner üst':
        return ('KAZANDI' if (int(ev_kor_s) + int(dep_kor_s)) >= 9 else 'KAYBETTI') if has_kor else 'BEKLIYOR'
    if t == '9.5 korner üst':
        return ('KAZANDI' if (int(ev_kor_s) + int(dep_kor_s)) >= 10 else 'KAYBETTI') if has_kor else 'BEKLIYOR'
    if t == '10.5 korner üst':
        return ('KAZANDI' if (int(ev_kor_s) + int(dep_kor_s)) >= 11 else 'KAYBETTI') if has_kor else 'BEKLIYOR'
    if t in ('3.5 kart üst', '3.5 sarı kart üst'):
        return ('KAZANDI' if (int(ev_sari_s) + int(dep_sari_s)) >= 4 else 'KAYBETTI') if has_kart else 'BEKLIYOR'
    if t in ('4.5 kart üst', '4.5 sarı kart üst'):
        return ('KAZANDI' if (int(ev_sari_s) + int(dep_sari_s)) >= 5 else 'KAYBETTI') if has_kart else 'BEKLIYOR'
    if t == 'kırmızı kart çıkar':
        return ('KAZANDI' if (int(ev_kirmizi_s) + int(dep_kirmizi_s)) >= 1 else 'KAYBETTI') if has_kirmizi else 'BEKLIYOR'
    if t == 'penaltı olur':
        return ('KAZANDI' if toplam_pen >= 1 else 'KAYBETTI') if has_pen else 'BEKLIYOR'

    return 'BEKLIYOR'

# --- QUANT KARAR MOTORU ---
def quant_karar_motoru(ev_xg, dep_xg, p_ms_1, p_ms_x, p_ms_2, p_iy_1, p_iy_2, p_o15, p_o25, p_o35, hist_btts, p_iy_o05, exp_corners, exp_cards, p_kart_35_db, p_kart_45_db, p_kirmizi, p_penalti):
    tot_xg = ev_xg + dep_xg
    xg_diff = ev_xg - dep_xg

    matrix = [[0.0]*8 for _ in range(8)]
    matrix_sum = 0.0
    for i in range(8):
        for j in range(8):
            p = poisson_ihtimal(ev_xg, i) * poisson_ihtimal(dep_xg, j)
            tau = dixon_coles_duzeltmesi(i, j, ev_xg, dep_xg, -0.10)
            val = max(0.0, p * tau)
            matrix[i][j] = val
            matrix_sum += val

    p_ms1_o15 = p_ms2_o15 = p_1x_o15 = p_x2_o15 = p_kg_o25 = p_ev_o15 = p_dep_o15 = 0.0
    mat_1 = mat_x = mat_2 = 0.0

    if matrix_sum > 0:
        for i in range(8):
            for j in range(8):
                prob = (matrix[i][j] / matrix_sum) * 100.0
                tot = i + j
                if i > j: mat_1 += prob
                elif i == j: mat_x += prob
                else: mat_2 += prob

                if i > j and tot >= 2: p_ms1_o15 += prob
                if j > i and tot >= 2: p_ms2_o15 += prob
                if i >= j and tot >= 2: p_1x_o15 += prob
                if j >= i and tot >= 2: p_x2_o15 += prob
                if i > 0 and j > 0 and tot >= 3: p_kg_o25 += prob
                if i >= 2: p_ev_o15 += prob
                if j >= 2: p_dep_o15 += prob

    sum_1x2 = p_ms_1 + p_ms_x + p_ms_2
    if sum_1x2 > 0:
        norm_1 = ((p_ms_1 / sum_1x2) * 50.0) + (mat_1 * 0.50)
        norm_x = ((p_ms_x / sum_1x2) * 50.0) + (mat_x * 0.50)
        norm_2 = ((p_ms_2 / sum_1x2) * 50.0) + (mat_2 * 0.50)
    else:
        norm_1, norm_x, norm_2 = mat_1, mat_x, mat_2

    p_1x = norm_1 + norm_x
    p_x2 = norm_2 + norm_x
    p_u25 = 100.0 - p_o25
    p_u35 = 100.0 - p_o35

    sh_xg = max(0.20, tot_xg * 0.56)
    p_2y_o15 = (1.0 - (poisson_ihtimal(sh_xg, 0) + poisson_ihtimal(sh_xg, 1))) * 100.0

    pois_btts = (1.0 - math.exp(-ev_xg)) * (1.0 - math.exp(-dep_xg)) * 100.0
    true_btts = (hist_btts * 0.50) + (pois_btts * 0.50)
    p_kg_o25_cal = (p_kg_o25 * 0.65) + (min(true_btts, p_o25) * 0.35)

    p_kor_85  = asiri_dagilim_olasilik(exp_corners, 8.5, 1.40)
    p_kor_95  = asiri_dagilim_olasilik(exp_corners, 9.5, 1.40)
    p_kor_105 = asiri_dagilim_olasilik(exp_corners, 10.5, 1.40)

    nb_kart_35 = asiri_dagilim_olasilik(exp_cards, 3.5, 1.30)
    nb_kart_45 = asiri_dagilim_olasilik(exp_cards, 4.5, 1.30)
    p_kart_35 = ((nb_kart_35 * 0.55) + (p_kart_35_db * 0.45)) if (nb_kart_35 is not None and p_kart_35_db is not None) else (nb_kart_35 or p_kart_35_db)
    p_kart_45 = ((nb_kart_45 * 0.55) + (p_kart_45_db * 0.45)) if (nb_kart_45 is not None and p_kart_45_db is not None) else (nb_kart_45 or p_kart_45_db)

    candidates = {
        # --- YÜKSEK ORANLI KOMBO VE ÖZEL MARKETLER ---
        'ms 1 ve 1.5 üst': {'prob': p_ms1_o15, 'min': 53.5, 'valid': (ev_xg >= 1.95 and xg_diff >= 0.85 and dep_xg <= 1.05 and tot_xg >= 2.65), 'weight': 1.85, 'bonus': 13.5},
        'ms 2 ve 1.5 üst': {'prob': p_ms2_o15, 'min': 54.5, 'valid': (dep_xg >= 1.95 and (-xg_diff) >= 0.90 and ev_xg <= 1.00 and tot_xg >= 2.65), 'weight': 1.85, 'bonus': 13.5},
        'ev 1.5 gol üst': {'prob': p_ev_o15, 'min': 63.0, 'valid': (ev_xg >= 2.05 and xg_diff >= 0.65), 'weight': 1.70, 'bonus': 12.0},
        'dep 1.5 gol üst': {'prob': p_dep_o15, 'min': 64.0, 'valid': (dep_xg >= 2.05 and (-xg_diff) >= 0.70), 'weight': 1.70, 'bonus': 12.0},
        'kg var ve 2.5 üst': {'prob': p_kg_o25_cal, 'min': 56.5, 'valid': (ev_xg >= 1.40 and dep_xg >= 1.40 and tot_xg >= 3.15 and hist_btts >= 65.0 and abs(xg_diff) <= 0.75), 'weight': 1.80, 'bonus': 12.5},
        '1x ve 1.5 üst': {'prob': p_1x_o15, 'min': 67.5, 'valid': (ev_xg >= 1.60 and xg_diff >= 0.50 and tot_xg >= 2.75 and dep_xg <= 1.20), 'weight': 1.60, 'bonus': 11.0},
        'x2 ve 1.5 üst': {'prob': p_x2_o15, 'min': 68.5, 'valid': (dep_xg >= 1.60 and (-xg_diff) >= 0.55 and tot_xg >= 2.75 and ev_xg <= 1.15), 'weight': 1.60, 'bonus': 11.0},
        '2. yarı 1.5 üst': {'prob': p_2y_o15, 'min': 61.5, 'valid': (tot_xg >= 3.35 and ev_xg >= 1.30 and dep_xg >= 1.30), 'weight': 1.65, 'bonus': 11.0},
        'ilk yarı 1': {'prob': p_iy_1 or 0, 'min': 52.0, 'valid': (p_iy_1 is not None and ev_xg >= 2.15 and xg_diff >= 1.05 and dep_xg <= 0.85), 'weight': 1.75, 'bonus': 11.5},
        'ilk yarı 2': {'prob': p_iy_2 or 0, 'min': 53.0, 'valid': (p_iy_2 is not None and dep_xg >= 2.15 and (-xg_diff) >= 1.10 and ev_xg <= 0.80), 'weight': 1.75, 'bonus': 11.5},

        # --- SARI KART, KORNER, KIRMIZI KART VE PENALTI MARKETLERİ ---
        '10.5 korner üst': {'prob': p_kor_105 or 0, 'min': 58.0, 'valid': (exp_corners is not None and exp_corners >= 11.85), 'weight': 1.80, 'bonus': 13.0},
        '9.5 korner üst': {'prob': p_kor_95 or 0, 'min': 62.5, 'valid': (exp_corners is not None and exp_corners >= 11.05), 'weight': 1.70, 'bonus': 12.0},
        '8.5 korner üst': {'prob': p_kor_85 or 0, 'min': 70.0, 'valid': (exp_corners is not None and exp_corners >= 10.45), 'weight': 1.50, 'bonus': 10.0},
        '4.5 sarı kart üst': {'prob': p_kart_45 or 0, 'min': 60.5, 'valid': (exp_cards is not None and exp_cards >= 5.15 and abs(xg_diff) <= 0.95), 'weight': 1.75, 'bonus': 12.5},
        '3.5 sarı kart üst': {'prob': p_kart_35 or 0, 'min': 68.5, 'valid': (exp_cards is not None and exp_cards >= 4.45 and abs(xg_diff) <= 1.10), 'weight': 1.60, 'bonus': 11.0},
        'penaltı olur': {'prob': p_penalti or 0, 'min': 38.5, 'valid': (p_penalti is not None and p_penalti >= 38.5 and tot_xg >= 2.85), 'weight': 1.95, 'bonus': 13.5},
        'kırmızı kart çıkar': {'prob': p_kirmizi or 0, 'min': 34.0, 'valid': (p_kirmizi is not None and p_kirmizi >= 34.0 and exp_cards is not None and exp_cards >= 4.85 and abs(xg_diff) <= 0.85), 'weight': 1.95, 'bonus': 13.5},

        # --- KLASİK ANA MARKETLER ---
        'maç sonu 1': {'prob': norm_1, 'min': 59.0, 'valid': (ev_xg >= 1.80 and xg_diff >= 0.80 and dep_xg <= 1.00), 'weight': 1.65, 'bonus': 11.0},
        'maç sonu 2': {'prob': norm_2, 'min': 60.5, 'valid': (dep_xg >= 1.80 and (-xg_diff) >= 0.85 and ev_xg <= 0.95), 'weight': 1.65, 'bonus': 11.0},
        '2.5 gol üst': {'prob': p_o25, 'min': 64.5, 'valid': (tot_xg >= 3.20 and ((ev_xg >= 1.25 and dep_xg >= 1.25) or (tot_xg >= 3.45 and p_o15 >= 85.0))), 'weight': 1.60, 'bonus': 10.5},
        '2.5 gol alt': {'prob': p_u25, 'min': 65.5, 'valid': (tot_xg <= 1.95 and max(ev_xg, dep_xg) <= 1.15 and true_btts <= 42.0), 'weight': 1.55, 'bonus': 10.0},
        'karşılıklı gol var': {'prob': true_btts, 'min': 62.5, 'valid': (ev_xg >= 1.25 and dep_xg >= 1.25 and hist_btts >= 64.0 and abs(xg_diff) <= 0.75), 'weight': 1.55, 'bonus': 10.0},

        # --- DÜŞÜK ORANLI DESTEK MARKETLERİ ---
        'ilk yarı 0.5 üst': {'prob': p_iy_o05 or 0, 'min': 77.5, 'valid': (p_iy_o05 is not None and tot_xg >= 3.05 and p_o15 >= 82.0), 'weight': 1.25, 'bonus': 6.5},
        '1.5 gol üst': {'prob': p_o15, 'min': 80.0, 'valid': (tot_xg >= 2.85 and (ev_xg >= 1.50 or dep_xg >= 1.50 or (ev_xg >= 1.30 and dep_xg >= 1.30))), 'weight': 1.10, 'bonus': 5.0},
        '3.5 gol alt': {'prob': p_u35, 'min': 83.5, 'valid': (tot_xg <= 2.20 and max(ev_xg, dep_xg) <= 1.35 and true_btts <= 48.0), 'weight': 1.05, 'bonus': 4.5},
        '1x çifte şans': {'prob': p_1x, 'min': 83.0, 'valid': (ev_xg >= 1.50 and xg_diff >= 0.55 and dep_xg <= 1.05), 'weight': 1.00, 'bonus': 4.0},
        'x2 çifte şans': {'prob': p_x2, 'min': 83.0, 'valid': (dep_xg >= 1.50 and (-xg_diff) >= 0.55 and ev_xg <= 1.00), 'weight': 1.00, 'bonus': 4.0}
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
                'score': round(score, 2)
            }

    # Yüksek oranlı ikizi varsa düşük oranlıyı baskıla
    if any(k in passed for k in ['ms 1 ve 1.5 üst', '1x ve 1.5 üst', 'ev 1.5 gol üst']):
        passed.pop('1x çifte şans', None)
        passed.pop('1.5 gol üst', None)
    if any(k in passed for k in ['ms 2 ve 1.5 üst', 'x2 ve 1.5 üst', 'dep 1.5 gol üst']):
        passed.pop('x2 çifte şans', None)
        passed.pop('1.5 gol üst', None)
    if any(k in passed for k in ['kg var ve 2.5 üst', '2.5 gol üst']):
        passed.pop('1.5 gol üst', None)
    if '2.5 gol alt' in passed:
        passed.pop('3.5 gol alt', None)
    if '10.5 korner üst' in passed:
        passed.pop('9.5 korner üst', None)
        passed.pop('8.5 korner üst', None)
    elif '9.5 korner üst' in passed:
        passed.pop('8.5 korner üst', None)
    if '4.5 sarı kart üst' in passed:
        passed.pop('3.5 sarı kart üst', None)

    if not passed:
        fallbacks = {
            '3.5 gol alt': p_u35 if tot_xg <= 2.65 else 0,
            '1.5 gol üst': p_o15 if tot_xg >= 2.40 else 0,
            '1x çifte şans': p_1x if xg_diff >= 0.15 else 0,
            'x2 çifte şans': p_x2 if xg_diff <= -0.15 else 0,
            '2.5 gol alt': p_u25
        }
        best_fb = max(fallbacks, key=fallbacks.get)
        fb_prob = round(fallbacks[best_fb], 1)
        ai_report = (
            "<div class='text-[10px] text-zinc-300 font-bold mb-1'>karar gerekçesi (riskli maç):</div>"
            "<p class='text-[10px] text-zinc-500 mb-2'>hiçbir yüksek değerli piyasa tahmini algoritmanın güvenlik barajını geçemedi. maç kaotik olarak işaretlendi.</p>"
        )
        return {
            'secilen_tahmin': best_fb,
            'tahmin_olasilik': fb_prob,
            'kategori': 'RISKLI',
            'guven_skoru': round(fb_prob * 0.5, 2),
            'ai_rapor': ai_report
        }

    passed_list = sorted(passed.values(), key=lambda x: x['score'], reverse=True)
    winner = passed_list[0]
    runner_up = passed_list[1] if len(passed_list) > 1 else None

    kategori = 'BANKO' if (winner['score'] >= 17.5 or winner['edge'] >= 5.5) else 'IDEAL'

    ek_bilgi = f"xg: {ev_xg:.2f} - {dep_xg:.2f}"
    if exp_corners is not None: ek_bilgi += f" | korner: {exp_corners:.1f}"
    if exp_cards is not None: ek_bilgi += f" | kart: {exp_cards:.1f}"

    ai_report = (
        f"<div class='text-[10px] text-zinc-300 font-bold mb-1'>neden {winner['market']} seçildi?</div>"
        f"<p class='text-[10px] text-zinc-500 mb-2'>2d dixon-coles ve negatif binom matrisinde güven barajını <b class='text-emerald-400'>+{winner['edge']} puan</b> aşarak en yüksek oran/isabet (value) değerini üretti ({ek_bilgi}).</p>"
    )
    if runner_up:
        fark = round(winner['score'] - runner_up['score'], 1)
        ai_report += (
            f"<div class='text-[10px] text-zinc-300 font-bold mb-1'>en yakın alternatif: {runner_up['market']}</div>"
            f"<p class='text-[10px] text-zinc-500'>sistem alternatif olarak <b>{runner_up['market']}</b> (%{runner_up['prob']}) tercihini de onayladı ancak ana seçim <b>+{fark} quant puanı</b> farkla daha değerli bulundu.</p>"
        )
    else:
        ai_report += (
            "<div class='text-[10px] text-zinc-300 font-bold mb-1'>tekil onay</div>"
            "<p class='text-[10px] text-zinc-500'>diğer marketler çapraz risk vetosuna takıldı, yalnızca bu market tüm güvenlik kalkanlarını geçti.</p>"
        )

    return {
        'secilen_tahmin': winner['market'],
        'tahmin_olasilik': winner['prob'],
        'kategori': kategori,
        'guven_skoru': winner['score'],
        'ai_rapor': ai_report
    }

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
    
    if len(maclar) < 4 and saha != "genel":
        return takim_performansi_getir(cursor, takim_adi, mac_tarihi, saha="genel")
        
    if len(maclar) < 3:
        return None

    stats = {
        "gol_atti": [], "gol_yedi": [], 
        "iy_gol_atti": [], "iy_gol_yedi": [], 
        "korner_atti": [], "korner_yedi": [], 
        "kart_aldi": [], "kart_yedirdi": [],
        "kirmizi_etki": [], "penalti_etki": [],
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

        sut_atti = sayiya_cevir(m.get('ev_sut')) if is_ev else sayiya_cevir(m.get('dep_sut'))
        sut_yedi = sayiya_cevir(m.get('dep_sut')) if is_ev else sayiya_cevir(m.get('ev_sut'))
        isabet_atti = sayiya_cevir(m.get('ev_isabetli')) if is_ev else sayiya_cevir(m.get('dep_isabetli'))
        isabet_yedi = sayiya_cevir(m.get('dep_isabetli')) if is_ev else sayiya_cevir(m.get('ev_isabetli'))
        orta_atti = sayiya_cevir(m.get('ev_isabetli_orta')) if is_ev else sayiya_cevir(m.get('dep_isabetli_orta'))
        pen_atti = sayiya_cevir(m.get('ev_pen_gol')) if is_ev else sayiya_cevir(m.get('dep_pen_gol'))
        pen_yedi = sayiya_cevir(m.get('dep_pen_gol')) if is_ev else sayiya_cevir(m.get('ev_pen_gol'))
        pen_sut_ev = sayiya_cevir(m.get('ev_pen_sut'))
        pen_sut_dep = sayiya_cevir(m.get('dep_pen_sut'))

        efektif_attigi = attigi
        if attigi is not None and isabet_atti is not None and sut_atti is not None and sut_atti > 0:
            pen_duzeltme = (pen_atti * 0.25) if pen_atti else 0.0
            isabetsiz_atti = max(0.0, sut_atti - isabet_atti)
            sut_xg_atti = (isabet_atti * 0.29) + (isabetsiz_atti * 0.035)
            efektif_attigi = ((max(0.0, attigi - pen_duzeltme)) * 0.62) + (sut_xg_atti * 0.38)

        efektif_yedigi = yedigi
        if yedigi is not None and isabet_yedi is not None and sut_yedi is not None and sut_yedi > 0:
            pen_y_duzeltme = (pen_yedi * 0.25) if pen_yedi else 0.0
            isabetsiz_yedi = max(0.0, sut_yedi - isabet_yedi)
            sut_xg_yedi = (isabet_yedi * 0.29) + (isabetsiz_yedi * 0.035)
            efektif_yedigi = ((max(0.0, yedigi - pen_y_duzeltme)) * 0.62) + (sut_xg_yedi * 0.38)

        sari_aldi = sayiya_cevir(m.get('ev_sari')) if is_ev else sayiya_cevir(m.get('dep_sari'))
        sari_yedirdi = sayiya_cevir(m.get('dep_sari')) if is_ev else sayiya_cevir(m.get('ev_sari'))
        faul_yapti = sayiya_cevir(m.get('ev_faul')) if is_ev else sayiya_cevir(m.get('dep_faul'))
        faul_maruz = sayiya_cevir(m.get('dep_faul')) if is_ev else sayiya_cevir(m.get('ev_faul'))

        efektif_kart_aldi = sari_aldi
        if sari_aldi is not None and faul_yapti is not None and faul_yapti > 0:
            efektif_kart_aldi = (sari_aldi * 0.70) + ((faul_yapti / 6.2) * 0.30)

        efektif_kart_yedirdi = sari_yedirdi
        if sari_yedirdi is not None and faul_maruz is not None and faul_maruz > 0:
            efektif_kart_yedirdi = (sari_yedirdi * 0.70) + ((faul_maruz / 6.2) * 0.30)

        # Kırmızı Kart Eğilimi (Geçmiş kırmızı kartlar + yüksek sarı kart/faul sertliği)
        kirmizi_ev = sayiya_cevir(m.get('ev_kirmizi'))
        kirmizi_dep = sayiya_cevir(m.get('dep_kirmizi'))
        if kirmizi_ev is not None and kirmizi_dep is not None:
            toplam_kirmizi = kirmizi_ev + kirmizi_dep
            sertlik_ek = 0.08 if ((sari_aldi or 0) + (sari_yedirdi or 0)) >= 5 else 0.02
            kirmizi_agirlik = min(1.0, (toplam_kirmizi * 0.65) + sertlik_ek)
        else:
            kirmizi_agirlik = None

        # Penaltı Eğilimi (Geçmiş penaltı şutları/golleri + ceza sahası içi baskı)
        if pen_sut_ev is not None and pen_sut_dep is not None:
            mac_pen = pen_sut_ev + pen_sut_dep
            baski_ek = 0.10 if ((isabet_atti or 0) >= 6 or (orta_atti or 0) >= 6) else 0.03
            pen_agirlik = min(1.2, (mac_pen * 0.65) + baski_ek)
        elif pen_atti is not None and pen_yedi is not None:
            mac_pen = pen_atti + pen_yedi
            pen_agirlik = min(1.2, (mac_pen * 0.70) + 0.05)
        else:
            pen_agirlik = None

        kg = 1.0 if (attigi is not None and yedigi is not None and attigi > 0 and yedigi > 0) else 0.0

        for dct in [stats, (form_stats if index < 5 else None)]:
            if dct is None: continue
            if efektif_attigi is not None: dct['gol_atti'].append(efektif_attigi)
            if efektif_yedigi is not None: dct['gol_yedi'].append(efektif_yedigi)
            if iy_attigi is not None: dct['iy_gol_atti'].append(iy_attigi)
            if iy_yedigi is not None: dct['iy_gol_yedi'].append(iy_yedigi)
            if kor_atti is not None: dct['korner_atti'].append(kor_atti)
            if kor_yedi is not None: dct['korner_yedi'].append(kor_yedi)
            if efektif_kart_aldi is not None: dct['kart_aldi'].append(efektif_kart_aldi)
            if efektif_kart_yedirdi is not None: dct['kart_yedirdi'].append(efektif_kart_yedirdi)
            if kirmizi_agirlik is not None: dct['kirmizi_etki'].append(kirmizi_agirlik)
            if pen_agirlik is not None: dct['penalti_etki'].append(pen_agirlik)
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

def tahminleri_hesapla_ve_kaydet(cursor, mac, ev_ort, dep_ort):
    fikstur_id = mac['id']
    ev = mac['ev_sahibi']
    dep = mac['deplasman']

    if not ev_ort or not dep_ort or ev_ort['gol_atti'] is None or dep_ort['gol_atti'] is None:
        cursor.execute("INSERT INTO tahminler (fikstur_id, durum) VALUES (%s, 'YETERSİZ VERİ') ON DUPLICATE KEY UPDATE durum = 'YETERSİZ VERİ'", (fikstur_id,))
        return

    ev_xG = round(max(0.25, ((ev_ort['gol_atti'] * 1.04) + (dep_ort['gol_yedi'] * 0.96)) / 2), 2)
    dep_xG = round(max(0.20, ((dep_ort['gol_atti'] * 0.96) + (ev_ort['gol_yedi'] * 1.04)) / 2), 2)
    
    ev_iy_xG = ((ev_ort['iy_gol_atti'] or (ev_xG * 0.44)) + (dep_ort['iy_gol_yedi'] or (dep_xG * 0.44))) / 2
    dep_iy_xG = ((dep_ort['iy_gol_atti'] or (dep_xG * 0.44)) + (ev_ort['iy_gol_yedi'] or (ev_xG * 0.44))) / 2

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
        beklenen_korner = round(((ev_ort['korner_atti'] + dep_ort['korner_yedi']) / 2) + ((dep_ort['korner_atti'] + ev_ort['korner_yedi']) / 2), 2)
        p_korner_8_5 = round((1.0 - sum([poisson_ihtimal(beklenen_korner, k) for k in range(9)])) * 100, 2)
        p_korner_9_5 = round((1.0 - sum([poisson_ihtimal(beklenen_korner, k) for k in range(10)])) * 100, 2)

    beklenen_kart, p_kart_3_5, p_kart_4_5 = None, None, None
    if ev_ort['kart_aldi'] is not None and dep_ort['kart_yedirdi'] is not None and dep_ort['kart_aldi'] is not None and ev_ort['kart_yedirdi'] is not None:
        beklenen_kart = round(((ev_ort['kart_aldi'] + dep_ort['kart_yedirdi']) / 2) + ((dep_ort['kart_aldi'] + ev_ort['kart_yedirdi']) / 2), 2)
        p_kart_3_5 = round((1.0 - sum([poisson_ihtimal(beklenen_kart, k) for k in range(4)])) * 100, 2)
        p_kart_4_5 = round((1.0 - sum([poisson_ihtimal(beklenen_kart, k) for k in range(5)])) * 100, 2)

    p_kirmizi_kart = None
    if ev_ort['kirmizi_etki'] is not None and dep_ort['kirmizi_etki'] is not None:
        lambda_kirmizi = (ev_ort['kirmizi_etki'] + dep_ort['kirmizi_etki']) / 2
        p_kirmizi_kart = round((1.0 - math.exp(-lambda_kirmizi)) * 100, 2)

    p_penalti_olur = None
    if ev_ort['penalti_etki'] is not None and dep_ort['penalti_etki'] is not None:
        lambda_pen = (ev_ort['penalti_etki'] + dep_ort['penalti_etki']) / 2
        p_penalti_olur = round((1.0 - math.exp(-lambda_pen)) * 100, 2)

    karar = quant_karar_motoru(
        ev_xG, dep_xG,
        round(p_1*100, 2), round(p_x*100, 2), round(p_2*100, 2),
        round(p_iy_1*100, 2), round(p_iy_2*100, 2),
        round(p_1_5_ust*100, 2), round(p_2_5_ust*100, 2), round(p_3_5_ust*100, 2),
        round(p_kg_var*100, 2), round(p_iy_0_5_ust*100, 2),
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
        LIMIT 4
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
        
        # SADECE BUGÜN VE YARIN İÇİN TAHMİN OLUŞTURUR
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

        # Sadece Bugün ve Yarın İçin Günün Kuponunu Kilitle
        for t_gun in [bugun, yarin]:
            gunluk_kuponu_kilitle(cursor, t_gun)

        # FT olan maçların sonuçlarını güncelle
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
