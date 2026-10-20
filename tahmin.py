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

def asiri_dagilim_olasilik(beklenen_deger, baraj, dagilim_katsayisi=1.45):
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

    # Mevcut tabloya yeni sütunları otomatik ekle (Veri kaybı olmadan)
    yeni_sutunlar = [
        ("beklenen_kart", "DECIMAL(5,2) DEFAULT NULL"),
        ("p_kart_3_5_ust", "DECIMAL(5,2) DEFAULT NULL"),
        ("p_kart_4_5_ust", "DECIMAL(5,2) DEFAULT NULL"),
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

# --- QUANT KARAR MOTORU (VERİTABANINA YAZILACAK KARARI ÜRETİR) ---
def quant_karar_motoru(ev_xg, dep_xg, p_ms_1, p_ms_x, p_ms_2, p_iy_1, p_iy_2, p_o15, p_o25, p_o35, hist_btts, p_iy_o05, exp_corners, exp_cards, p_kart_35_db, p_kart_45_db):
    tot_xg = ev_xg + dep_xg
    xg_diff = ev_xg - dep_xg

    # 8x8 2 Boyutlu Dixon-Coles Matrisi
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

    p_kor_85 = asiri_dagilim_olasilik(exp_corners, 8.5, 1.45)
    p_kor_95 = asiri_dagilim_olasilik(exp_corners, 9.5, 1.45)

    nb_kart_35 = asiri_dagilim_olasilik(exp_cards, 3.5, 1.35)
    nb_kart_45 = asiri_dagilim_olasilik(exp_cards, 4.5, 1.35)
    p_kart_35 = ((nb_kart_35 * 0.55) + (p_kart_35_db * 0.45)) if (nb_kart_35 is not None and p_kart_35_db is not None) else (nb_kart_35 or p_kart_35_db)
    p_kart_45 = ((nb_kart_45 * 0.55) + (p_kart_45_db * 0.45)) if (nb_kart_45 is not None and p_kart_45_db is not None) else (nb_kart_45 or p_kart_45_db)

    candidates = {
        'MS 1 ve 1.5 Üst': {'prob': p_ms1_o15, 'min': 53.5, 'valid': (ev_xg >= 1.95 and xg_diff >= 0.85 and dep_xg <= 1.05 and tot_xg >= 2.65), 'weight': 1.85, 'bonus': 13.5},
        'MS 2 ve 1.5 Üst': {'prob': p_ms2_o15, 'min': 54.5, 'valid': (dep_xg >= 1.95 and (-xg_diff) >= 0.90 and ev_xg <= 1.00 and tot_xg >= 2.65), 'weight': 1.85, 'bonus': 13.5},
        'Ev 1.5 Gol Üst': {'prob': p_ev_o15, 'min': 63.0, 'valid': (ev_xg >= 2.05 and xg_diff >= 0.65), 'weight': 1.70, 'bonus': 12.0},
        'Dep 1.5 Gol Üst': {'prob': p_dep_o15, 'min': 64.0, 'valid': (dep_xg >= 2.05 and (-xg_diff) >= 0.70), 'weight': 1.70, 'bonus': 12.0},
        'KG Var ve 2.5 Üst': {'prob': p_kg_o25_cal, 'min': 56.5, 'valid': (ev_xg >= 1.40 and dep_xg >= 1.40 and tot_xg >= 3.15 and hist_btts >= 65.0 and abs(xg_diff) <= 0.75), 'weight': 1.80, 'bonus': 12.5},
        '1X ve 1.5 Üst': {'prob': p_1x_o15, 'min': 67.5, 'valid': (ev_xg >= 1.60 and xg_diff >= 0.50 and tot_xg >= 2.75 and dep_xg <= 1.20), 'weight': 1.60, 'bonus': 11.0},
        'X2 ve 1.5 Üst': {'prob': p_x2_o15, 'min': 68.5, 'valid': (dep_xg >= 1.60 and (-xg_diff) >= 0.55 and tot_xg >= 2.75 and ev_xg <= 1.15), 'weight': 1.60, 'bonus': 11.0},
        '2. Yarı 1.5 Üst': {'prob': p_2y_o15, 'min': 61.5, 'valid': (tot_xg >= 3.35 and ev_xg >= 1.30 and dep_xg >= 1.30), 'weight': 1.65, 'bonus': 11.0},
        'İlk Yarı 1': {'prob': p_iy_1 or 0, 'min': 52.0, 'valid': (p_iy_1 is not None and ev_xg >= 2.15 and xg_diff >= 1.05 and dep_xg <= 0.85), 'weight': 1.75, 'bonus': 11.5},
        'İlk Yarı 2': {'prob': p_iy_2 or 0, 'min': 53.0, 'valid': (p_iy_2 is not None and dep_xg >= 2.15 and (-xg_diff) >= 1.10 and ev_xg <= 0.80), 'weight': 1.75, 'bonus': 11.5},
        '4.5 Kart Üst': {'prob': p_kart_45 or 0, 'min': 62.5, 'valid': (exp_cards is not None and exp_cards >= 5.35 and abs(xg_diff) <= 0.90), 'weight': 1.65, 'bonus': 11.0},
        'Maç Sonu 1': {'prob': norm_1, 'min': 59.0, 'valid': (ev_xg >= 1.80 and xg_diff >= 0.80 and dep_xg <= 1.00), 'weight': 1.65, 'bonus': 11.0},
        'Maç Sonu 2': {'prob': norm_2, 'min': 60.5, 'valid': (dep_xg >= 1.80 and (-xg_diff) >= 0.85 and ev_xg <= 0.95), 'weight': 1.65, 'bonus': 11.0},
        '2.5 Gol Üst': {'prob': p_o25, 'min': 64.5, 'valid': (tot_xg >= 3.20 and ((ev_xg >= 1.25 and dep_xg >= 1.25) or (tot_xg >= 3.45 and p_o15 >= 85.0))), 'weight': 1.60, 'bonus': 10.5},
        '2.5 Gol Alt': {'prob': p_u25, 'min': 65.5, 'valid': (tot_xg <= 1.95 and max(ev_xg, dep_xg) <= 1.15 and true_btts <= 42.0), 'weight': 1.55, 'bonus': 10.0},
        'Karşılıklı Gol Var': {'prob': true_btts, 'min': 62.5, 'valid': (ev_xg >= 1.25 and dep_xg >= 1.25 and hist_btts >= 64.0 and abs(xg_diff) <= 0.75), 'weight': 1.55, 'bonus': 10.0},
        '9.5 Korner Üst': {'prob': p_kor_95 or 0, 'min': 64.0, 'valid': (exp_corners is not None and exp_corners >= 11.30), 'weight': 1.50, 'bonus': 9.5},
        '3.5 Kart Üst': {'prob': p_kart_35 or 0, 'min': 71.0, 'valid': (exp_cards is not None and exp_cards >= 4.65 and abs(xg_diff) <= 1.05), 'weight': 1.45, 'bonus': 9.0},
        'İlk Yarı 0.5 Üst': {'prob': p_iy_o05 or 0, 'min': 77.5, 'valid': (p_iy_o05 is not None and tot_xg >= 3.05 and p_o15 >= 82.0), 'weight': 1.25, 'bonus': 6.5},
        '8.5 Korner Üst': {'prob': p_kor_85 or 0, 'min': 73.5, 'valid': (exp_corners is not None and exp_corners >= 10.80), 'weight': 1.15, 'bonus': 5.5},
        '1.5 Gol Üst': {'prob': p_o15, 'min': 80.0, 'valid': (tot_xg >= 2.85 and (ev_xg >= 1.50 or dep_xg >= 1.50 or (ev_xg >= 1.30 and dep_xg >= 1.30))), 'weight': 1.10, 'bonus': 5.0},
        '3.5 Gol Alt': {'prob': p_u35, 'min': 83.5, 'valid': (tot_xg <= 2.20 and max(ev_xg, dep_xg) <= 1.35 and true_btts <= 48.0), 'weight': 1.05, 'bonus': 4.5},
        '1X Çifte Şans': {'prob': p_1x, 'min': 83.0, 'valid': (ev_xg >= 1.50 and xg_diff >= 0.55 and dep_xg <= 1.05), 'weight': 1.00, 'bonus': 4.0},
        'X2 Çifte Şans': {'prob': p_x2, 'min': 83.0, 'valid': (dep_xg >= 1.50 and (-xg_diff) >= 0.55 and ev_xg <= 1.00), 'weight': 1.00, 'bonus': 4.0}
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
    if any(k in passed for k in ['MS 1 ve 1.5 Üst', '1X ve 1.5 Üst', 'Ev 1.5 Gol Üst']):
        passed.pop('1X Çifte Şans', None)
        passed.pop('1.5 Gol Üst', None)
    if any(k in passed for k in ['MS 2 ve 1.5 Üst', 'X2 ve 1.5 Üst', 'Dep 1.5 Gol Üst']):
        passed.pop('X2 Çifte Şans', None)
        passed.pop('1.5 Gol Üst', None)
    if any(k in passed for k in ['KG Var ve 2.5 Üst', '2.5 Gol Üst']):
        passed.pop('1.5 Gol Üst', None)
    if '2.5 Gol Alt' in passed:
        passed.pop('3.5 Gol Alt', None)
    if '9.5 Korner Üst' in passed:
        passed.pop('8.5 Korner Üst', None)
    if '4.5 Kart Üst' in passed:
        passed.pop('3.5 Kart Üst', None)

    if not passed:
        fallbacks = {
            '3.5 Gol Alt': p_u35 if tot_xg <= 2.65 else 0,
            '1.5 Gol Üst': p_o15 if tot_xg >= 2.40 else 0,
            '1X Çifte Şans': p_1x if xg_diff >= 0.15 else 0,
            'X2 Çifte Şans': p_x2 if xg_diff <= -0.15 else 0,
            '2.5 Gol Alt': p_u25
        }
        best_fb = max(fallbacks, key=fallbacks.get)
        fb_prob = round(fallbacks[best_fb], 1)
        ai_report = (
            "<div class='text-[10px] text-zinc-300 font-bold mb-1'>Karar Gerekçesi (Riskli Maç):</div>"
            "<p class='text-[10px] text-zinc-500 mb-2'>Hiçbir yüksek değerli piyasa tahmini algoritmanın güvenlik barajını geçemedi. Maç kaotik olarak işaretlendi.</p>"
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

    ek_bilgi = f"xG: {ev_xg:.2f} - {dep_xg:.2f}"
    if exp_corners is not None: ek_bilgi += f" | Korner: {exp_corners:.1f}"
    if exp_cards is not None: ek_bilgi += f" | Kart: {exp_cards:.1f}"

    ai_report = (
        f"<div class='text-[10px] text-zinc-300 font-bold mb-1'>Neden {winner['market']} Seçildi?</div>"
        f"<p class='text-[10px] text-zinc-500 mb-2'>2D Dixon-Coles ve Negatif Binom matrisinde güven barajını <b class='text-emerald-400'>+{winner['edge']} puan</b> aşarak en yüksek oran/isabet (Value) değerini üretti ({ek_bilgi}).</p>"
    )
    if runner_up:
        fark = round(winner['score'] - runner_up['score'], 1)
        ai_report += (
            f"<div class='text-[10px] text-zinc-300 font-bold mb-1'>En Yakın Alternatif: {runner_up['market']}</div>"
            f"<p class='text-[10px] text-zinc-500'>Sistem alternatif olarak <b>{runner_up['market']}</b> (%{runner_up['prob']}) tercihini de onayladı ancak ana seçim <b>+{fark} Quant puanı</b> farkla daha değerli bulundu.</p>"
        )
    else:
        ai_report += (
            "<div class='text-[10px] text-zinc-300 font-bold mb-1'>Tekil Onay</div>"
            "<p class='text-[10px] text-zinc-500'>Diğer marketler çapraz risk vetosuna takıldı, yalnızca bu market tüm güvenlik kalkanlarını geçti.</p>"
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
        pen_atti = sayiya_cevir(m.get('ev_pen_gol')) if is_ev else sayiya_cevir(m.get('dep_pen_gol'))
        pen_yedi = sayiya_cevir(m.get('dep_pen_gol')) if is_ev else sayiya_cevir(m.get('ev_pen_gol'))

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

    # Karar Motorunu Çalıştır ve Veritabanı İçin Hazırla
    karar = quant_karar_motoru(
        ev_xG, dep_xG,
        round(p_1*100, 2), round(p_x*100, 2), round(p_2*100, 2),
        round(p_iy_1*100, 2), round(p_iy_2*100, 2),
        round(p_1_5_ust*100, 2), round(p_2_5_ust*100, 2), round(p_3_5_ust*100, 2),
        round(p_kg_var*100, 2), round(p_iy_0_5_ust*100, 2),
        beklenen_korner, beklenen_kart, p_kart_3_5, p_kart_4_5
    )

    sonuc_durumu = mac_sonucu_hesapla(karar['secilen_tahmin'], mac)

    sql = """
        INSERT INTO tahminler 
        (fikstur_id, durum, beklenen_ev_gol, beklenen_dep_gol, p_ms_1, p_ms_x, p_ms_2, p_iy_1, p_iy_x, p_iy_2, 
         p_gol_1_5_ust, p_gol_2_5_ust, p_gol_3_5_ust, p_kg_var, p_iy_0_5_ust, p_iy_1_5_ust, 
         beklenen_korner, p_korner_8_5_ust, p_korner_9_5_ust, beklenen_kart, p_kart_3_5_ust, p_kart_4_5_ust,
         secilen_tahmin, tahmin_olasilik, kategori, guven_skoru, sonuc, ai_rapor)
        VALUES (%s, 'HAZIR', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            durum='HAZIR', beklenen_ev_gol=VALUES(beklenen_ev_gol), beklenen_dep_gol=VALUES(beklenen_dep_gol),
            p_ms_1=VALUES(p_ms_1), p_ms_x=VALUES(p_ms_x), p_ms_2=VALUES(p_ms_2), p_iy_1=VALUES(p_iy_1), p_iy_x=VALUES(p_iy_x), p_iy_2=VALUES(p_iy_2),
            p_gol_1_5_ust=VALUES(p_gol_1_5_ust), p_gol_2_5_ust=VALUES(p_gol_2_5_ust), p_gol_3_5_ust=VALUES(p_gol_3_5_ust), p_kg_var=VALUES(p_kg_var),
            p_iy_0_5_ust=VALUES(p_iy_0_5_ust), p_iy_1_5_ust=VALUES(p_iy_1_5_ust),
            beklenen_korner=VALUES(beklenen_korner), p_korner_8_5_ust=VALUES(p_korner_8_5_ust), p_korner_9_5_ust=VALUES(p_korner_9_5_ust),
            beklenen_kart=VALUES(beklenen_kart), p_kart_3_5_ust=VALUES(p_kart_3_5_ust), p_kart_4_5_ust=VALUES(p_kart_4_5_ust),
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
        karar['secilen_tahmin'], karar['tahmin_olasilik'], karar['kategori'], karar['guven_skoru'], sonuc_durumu, karar['ai_rapor']
    )
    cursor.execute(sql, degerler)
    print(f"[{fikstur_id}] ✅ {ev} vs {dep} -> {karar['secilen_tahmin']} (%{karar['tahmin_olasilik']} - {karar['kategori']}) Veritabanına Yazıldı!")

def gunluk_kuponu_kilitle(cursor, tarih):
    # O tarihte daha önce kupon oluşturulmuş ve içindeki maçlardan biri başlamış/bitmişse kupona DOKUNMA (Geçmişi koru)
    cursor.execute("""
        SELECT COUNT(*) as kilitli_sayisi
        FROM tahminler t
        JOIN fikstur f ON f.id = t.fikstur_id
        WHERE f.tarih = %s AND t.kupon_maci = 1 AND (f.durum LIKE '%%FT%%' OR f.ev_skor != '-')
    """, (tarih,))
    row = cursor.fetchone()
    if row and row['kilitli_sayisi'] > 0:
        return

    # Henüz maçlar başlamadıysa veya ilk kez kupon seçiliyorsa en iyi 1-4 maçı seç ve kupon_maci = 1 yap
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
        dun = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        bugun = datetime.now().strftime("%Y-%m-%d")
        yarin = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        sonraki = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
        
        # Bitmemiş maçları VEYA veritabanında henüz secilen_tahmin'i yazılmamış geçmiş maçları çek
        cursor.execute("""
            SELECT f.* 
            FROM fikstur f
            LEFT JOIN tahminler t ON f.id = t.fikstur_id
            WHERE f.tarih IN (%s, %s, %s, %s) 
              AND (f.durum NOT LIKE '%%FT%%' OR t.secilen_tahmin IS NULL)
        """, (dun, bugun, yarin, sonraki))
        maclar = cursor.fetchall()
        
        for mac in maclar:
            ev_ort = takim_performansi_getir(cursor, mac['ev_sahibi'], mac['tarih'], saha="ev")
            dep_ort = takim_performansi_getir(cursor, mac['deplasman'], mac['tarih'], saha="dep")
            tahminleri_hesapla_ve_kaydet(cursor, mac, ev_ort, dep_ort)

        # Her gün için Günün Kuponunu (1-4 maç) veritabanında kilitle
        for t_gun in [dun, bugun, yarin, sonraki]:
            gunluk_kuponu_kilitle(cursor, t_gun)

        # FT olan tüm maçların sonucunu (KAZANDI / KAYBETTI) veritabanında güncelle
        cursor.execute("""
            SELECT t.id as tahmin_id, t.secilen_tahmin, f.durum, f.ev_skor, f.dep_skor, 
                   f.ev_iy_skor, f.dep_iy_skor, f.ev_korner, f.dep_korner, f.ev_sari, f.dep_sari
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
