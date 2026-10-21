import sys
import subprocess
import math
import os
from datetime import datetime, timedelta

def kutuphaneleri_kur():
    gerekli = ['pymysql']
    for k in gerekli:
        try:
            __import__(k)
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", k])

kutuphaneleri_kur()
import pymysql
import pymysql.cursors

# =========================================================================
# VERİTABANI AYARLARI (guncelle.py ve cek_2.py İLE TAM UYUMLU)
# =========================================================================
CONFIG = {
    "db": {
        "host": os.environ.get("DB_HOST", "netscout.fun"),
        "user": os.environ.get("DB_USER", "netscout_veri"),
        "password": os.environ.get("DB_PASSWORD", "i.34temel1"),
        "database": os.environ.get("DB_NAME", "netscout_veri"),
        "port": int(os.environ.get("DB_PORT", 3306))
    }
}

def db_baglantisi_kur():
    try:
        return pymysql.connect(
            host=CONFIG["db"]["host"],
            user=CONFIG["db"]["user"],
            password=CONFIG["db"]["password"],
            database=CONFIG["db"]["database"],
            port=CONFIG["db"]["port"],
            charset='utf8mb4',
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor
        )
    except Exception as e:
        print(f"Veritabanı bağlantı hatası: {e}")
        return None

def flt(val, default=None):
    if val is None or val == "" or val == "-":
        return default
    try:
        return float(str(val).replace("%", "").strip())
    except:
        return default

# =========================================================================
# POISSON VE DIXON-COLES OLASILIK FONKSİYONLARI
# =========================================================================
def poisson_pmf(k, lam):
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (math.exp(-lam) * (lam ** k)) / math.factorial(k)

def skor_matrisi_hesapla(lam_ev, lam_dep, max_gol=7):
    """Dixon-Coles düşük skor düzeltmeli Bivariate Poisson matrisi"""
    matris = {}
    rho = -0.08  # Beraberlik ve alt skor korelasyon düzeltmesi
    toplam_olasilik = 0.0

    for i in range(max_gol + 1):
        for j in range(max_gol + 1):
            p = poisson_pmf(i, lam_ev) * poisson_pmf(j, lam_dep)
            if i == 0 and j == 0:
                tau = 1.0 - (lam_ev * lam_dep * rho)
            elif i == 0 and j == 1:
                tau = 1.0 + (lam_ev * rho)
            elif i == 1 and j == 0:
                tau = 1.0 + (lam_dep * rho)
            elif i == 1 and j == 1:
                tau = 1.0 - rho
            else:
                tau = 1.0
            p_duzeltilmis = max(0.0, p * tau)
            matris[(i, j)] = p_duzeltilmis
            toplam_olasilik += p_duzeltilmis

    if toplam_olasilik > 0:
        for k in matris:
            matris[k] /= toplam_olasilik
    return matris

def poisson_ust_olasiligi(lam_toplam, baraj):
    """Örn: baraj=8.5 ise 9 ve üzeri olma olasılığı"""
    alt_limit = int(math.floor(baraj))
    alt_olasilik = sum(poisson_pmf(k, lam_toplam) for k in range(alt_limit + 1))
    return max(0.0, min(1.0, 1.0 - alt_olasilik))

# =========================================================================
# 1. ADIM: FT OLAN TÜM GEÇMİŞ MAÇLARI ÇEK VE TAKIM PROFİLLERİNİ ÇIKAR
# =========================================================================
def gecmis_verileri_analiz_et(conn, referans_tarih):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM fikstur 
        WHERE durum LIKE '%%FT%%' AND tarih < %s
        ORDER BY tarih ASC, id ASC
    """, (referans_tarih,))
    ft_maclar = cursor.fetchall()

    takim_gecmisi = {}
    lig_ortalamalari = {}

    for m in ft_maclar:
        ev_skor = flt(m.get('ev_skor'))
        dep_skor = flt(m.get('dep_skor'))
        if ev_skor is None or dep_skor is None:
            continue

        lig = m.get('lig', 'GENEL')
        ev = m.get('ev_sahibi')
        dep = m.get('deplasman')

        ev_iy = flt(m.get('ev_iy_skor'), ev_skor * 0.44)
        dep_iy = flt(m.get('dep_iy_skor'), dep_skor * 0.44)

        # Sıfır (0) veri anomalisini tespit et (Süper Lig & Brezilya Serie A koruması)
        ev_sut = flt(m.get('ev_sut'), 0)
        dep_sut = flt(m.get('dep_sut'), 0)
        temel_veri_var = (ev_sut + dep_sut) > 0

        ev_pas = flt(m.get('ev_toplam_pas'), 0)
        dep_pas = flt(m.get('dep_toplam_pas'), 0)
        detay_veri_var = (ev_pas + dep_pas) > 0

        if lig not in lig_ortalamalari:
            lig_ortalamalari[lig] = {'ev_gol': [], 'dep_gol': [], 'korner': [], 'kart': []}
        
        lig_ortalamalari[lig]['ev_gol'].append(ev_skor)
        lig_ortalamalari[lig]['dep_gol'].append(dep_skor)
        if temel_veri_var:
            lig_ortalamalari[lig]['korner'].append(flt(m.get('ev_korner'), 4.5) + flt(m.get('dep_korner'), 4.0))
            lig_ortalamalari[lig]['kart'].append(flt(m.get('ev_sari'), 2.0) + flt(m.get('dep_sari'), 2.0))

        # Ev ve Deplasman kayıtlarını oluştur
        for takim, saha, kendi_on, rakip_on in [(ev, 'EV', 'ev_', 'dep_'), (dep, 'DEP', 'dep_', 'ev_')]:
            if takim not in takim_gecmisi:
                takim_gecmisi[takim] = []

            kayit = {
                'saha': saha,
                'lig': lig,
                'gol': flt(m.get(f'{kendi_on}skor'), 0),
                'yenen_gol': flt(m.get(f'{rakip_on}skor'), 0),
                'iy_gol': ev_iy if saha == 'EV' else dep_iy,
                'iy_yenen': dep_iy if saha == 'EV' else ev_iy,
                'temel_var': temel_veri_var,
                'detay_var': detay_veri_var,
                'top_oynama': flt(m.get(f'{kendi_on}top_oynama'), 50.0),
                'sut': flt(m.get(f'{kendi_on}sut'), 12.0),
                'yenen_sut': flt(m.get(f'{rakip_on}sut'), 12.0),
                'isabetli': flt(m.get(f'{kendi_on}isabetli'), 4.2),
                'yenen_isabetli': flt(m.get(f'{rakip_on}isabetli'), 4.2),
                'sut_yuzde': flt(m.get(f'{kendi_on}sut_yuzde'), 0.35),
                'korner': flt(m.get(f'{kendi_on}korner'), 4.8),
                'yenen_korner': flt(m.get(f'{rakip_on}korner'), 4.5),
                'faul': flt(m.get(f'{kendi_on}faul'), 12.5),
                'sari': flt(m.get(f'{kendi_on}sari'), 2.0),
                'rakip_sari': flt(m.get(f'{rakip_on}sari'), 2.0),
                'kurtaris': flt(m.get(f'{kendi_on}kurtaris'), 3.0),
                'engel_sut': flt(m.get(f'{kendi_on}engel_sut'), 3.2),
                'rakip_engel_sut': flt(m.get(f'{rakip_on}engel_sut'), 3.2),
                'pas_yuzde': flt(m.get(f'{kendi_on}pas_yuzde'), 0.82),
                'toplam_orta': flt(m.get(f'{kendi_on}toplam_orta'), 18.0),
                'toplam_uzak': flt(m.get(f'{kendi_on}toplam_uzak'), 24.0),
                'basarili_mud': flt(m.get(f'{kendi_on}basarili_mud'), 9.5)
            }
            takim_gecmisi[takim].append(kayit)

    return takim_gecmisi, lig_ortalamalari

def agirlikli_ortalama(mac_listesi, anahtar, varsayilan, sadece_temel=False, sadece_detay=False):
    uygun = []
    for m in mac_listesi:
        if sadece_temel and not m['temel_var']:
            continue
        if sadece_detay and not m['detay_var']:
            continue
        if m.get(anahtar) is not None:
            uygun.append(m[anahtar])

    if not uygun:
        return varsayilan

    # Son 10 maça üstel ağırlık ver (en güncel maç en yüksek ağırlığa sahip)
    son_maclar = uygun[-10:]
    toplam_agirlik = 0.0
    agirlikli_toplam = 0.0
    for idx, deger in enumerate(son_maclar):
        agirlik = 1.25 ** idx
        agirlikli_toplam += deger * agirlik
        toplam_agirlik += agirlik

    return agirlikli_toplam / toplam_agirlik

def takim_profili_cikar(takim_adi, saha_tipi, takim_gecmisi):
    tum_maclar = takim_gecmisi.get(takim_adi, [])
    saha_maclari = [m for m in tum_maclar if m['saha'] == saha_tipi]

    def harmanla(anahtar, varsayilan, temel=False, detay=False):
        genel_ort = agirlikli_ortalama(tum_maclar, anahtar, varsayilan, temel, detay)
        if len(saha_maclari) >= 2:
            saha_ort = agirlikli_ortalama(saha_maclari, anahtar, genel_ort, temel, detay)
            return (saha_ort * 0.60) + (genel_ort * 0.40)
        return genel_ort

    return {
        'mac_sayisi': len(tum_maclar),
        'gol': harmanla('gol', 1.35),
        'yenen_gol': harmanla('yenen_gol', 1.25),
        'iy_gol': harmanla('iy_gol', 0.60),
        'iy_yenen': harmanla('iy_yenen', 0.55),
        'top_oynama': harmanla('top_oynama', 50.0, temel=True),
        'sut': harmanla('sut', 12.5, temel=True),
        'yenen_sut': harmanla('yenen_sut', 12.0, temel=True),
        'isabetli': harmanla('isabetli', 4.4, temel=True),
        'yenen_isabetli': harmanla('yenen_isabetli', 4.2, temel=True),
        'sut_yuzde': harmanla('sut_yuzde', 0.35, temel=True),
        'korner': harmanla('korner', 4.8, temel=True),
        'yenen_korner': harmanla('yenen_korner', 4.5, temel=True),
        'faul': harmanla('faul', 12.5, temel=True),
        'sari': harmanla('sari', 2.1, temel=True),
        'rakip_sari': harmanla('rakip_sari', 2.0, temel=True),
        'kurtaris': harmanla('kurtaris', 3.0, temel=True),
        'engel_sut': harmanla('engel_sut', 3.2, temel=True),
        'toplam_orta': harmanla('toplam_orta', 18.0, detay=True),
        'toplam_uzak': harmanla('toplam_uzak', 24.0, detay=True),
        'pas_yuzde': harmanla('pas_yuzde', 0.82, detay=True)
    }

# =========================================================================
# 2. ADIM: MAÇIN TAHMİNİ İSTATİSTİKLERİNİ VE SKORUNU SİMÜLE ET
# =========================================================================
def mac_simule_et(ev_adi, dep_adi, lig, takim_gecmisi, lig_ortalamalari):
    ev_p = takim_profili_cikar(ev_adi, 'EV', takim_gecmisi)
    dep_p = takim_profili_cikar(dep_adi, 'DEP', takim_gecmisi)

    # 1. Tahmini Topa Sahip Olma
    toplam_top = ev_p['top_oynama'] + dep_p['top_oynama']
    tahmini_ev_top = round((ev_p['top_oynama'] / toplam_top) * 100, 1) if toplam_top > 0 else 50.0
    tahmini_dep_top = round(100.0 - tahmini_ev_top, 1)

    # 2. Tahmini Şut ve İsabetli Şut (Rakip Savunma Duvarı Düzeltmeli)
    tahmini_ev_sut = (ev_p['sut'] * 0.55) + (dep_p['yenen_sut'] * 0.45)
    tahmini_dep_sut = (dep_p['sut'] * 0.55) + (ev_p['yenen_sut'] * 0.45)

    # Rakibin uzaklaştırma ve şut bloklama direnci yüksekse isabetli şutu kırp
    dep_duvar_katsayisi = max(0.82, min(1.15, 1.0 - ((dep_p['toplam_uzak'] - 25.0) * 0.004)))
    ev_duvar_katsayisi = max(0.82, min(1.15, 1.0 - ((ev_p['toplam_uzak'] - 25.0) * 0.004)))

    tahmini_ev_isabet = ((ev_p['isabetli'] * 0.55) + (dep_p['yenen_isabetli'] * 0.45)) * dep_duvar_katsayisi
    tahmini_dep_isabet = ((dep_p['isabetli'] * 0.55) + (ev_p['yenen_isabetli'] * 0.45)) * ev_duvar_katsayisi

    # 3. Sentetik xG (Gol Beklentisi): Geçmiş Gol + İsabetli Şut Kalitesi + Pas Üstünlüğü
    ev_sut_xg = tahmini_ev_isabet * ((ev_p['sut_yuzde'] * 0.5) + 0.16)
    dep_sut_xg = tahmini_dep_isabet * ((dep_p['sut_yuzde'] * 0.5) + 0.16)

    ev_klasik_xg = (ev_p['gol'] * 0.55) + (dep_p['yenen_gol'] * 0.45)
    dep_klasik_xg = (dep_p['gol'] * 0.55) + (ev_p['yenen_gol'] * 0.45)

    # İç saha avantajı (+0.08) ve karma xG
    lam_ev = max(0.25, (ev_klasik_xg * 0.58) + (ev_sut_xg * 0.42) + 0.08)
    lam_dep = max(0.20, (dep_klasik_xg * 0.58) + (dep_sut_xg * 0.42))

    # İlk Yarı Gol Beklentisi
    lam_iy_ev = max(0.10, (ev_p['iy_gol'] * 0.55 + dep_p['iy_yenen'] * 0.45) * 0.6 + (lam_ev * 0.42) * 0.4)
    lam_iy_dep = max(0.08, (dep_p['iy_gol'] * 0.55 + ev_p['iy_yenen'] * 0.45) * 0.6 + (lam_dep * 0.42) * 0.4)

    # 4. Tahmini Korner (Orta + Şut + Bloklanan Şut Etkisi)
    ev_baz_korner = (ev_p['korner'] * 0.55) + (dep_p['yenen_korner'] * 0.45)
    dep_baz_korner = (dep_p['korner'] * 0.55) + (ev_p['yenen_korner'] * 0.45)
    orta_etkisi_ev = (ev_p['toplam_orta'] - 18.0) * 0.06
    orta_etkisi_dep = (dep_p['toplam_orta'] - 16.0) * 0.06

    tahmini_ev_korner = max(1.5, ev_baz_korner + orta_etkisi_ev)
    tahmini_dep_korner = max(1.2, dep_baz_korner + orta_etkisi_dep)
    toplam_korner_beklentisi = tahmini_ev_korner + tahmini_dep_korner

    # 5. Tahmini Sarı Kart (Faul ve Oyun Dengesi Etkisi)
    ev_kart = (ev_p['sari'] * 0.6) + (dep_p['rakip_sari'] * 0.4)
    dep_kart = (dep_p['sari'] * 0.6) + (ev_p['rakip_sari'] * 0.4)
    faul_etkisi = max(-0.4, min(0.8, ((ev_p['faul'] + dep_p['faul']) - 24.0) * 0.08))
    toplam_kart_beklentisi = max(1.5, ev_kart + dep_kart + faul_etkisi)

    # 6. Skor Matrisi ve En Yakın Skorun Tespiti
    matris = skor_matrisi_hesapla(lam_ev, lam_dep)
    iy_matris = skor_matrisi_hesapla(lam_iy_ev, lam_iy_dep, max_gol=4)

    # En olası 1. ve 2. skoru bul, beklentiye göre en gerçekçi skoru seç
    sirali_skorlar = sorted(matris.items(), key=lambda x: x[1], reverse=True)
    en_yakin_skor = sirali_skorlar[0][0]
    en_yakin_iy = sorted(iy_matris.items(), key=lambda x: x[1], reverse=True)[0][0]

    # 7. Market Olasılıklarını Hesapla (guncelle.py & index.php ile %100 uyumlu marketler)
    p_ms1 = sum(p for (i, j), p in matris.items() if i > j)
    p_ms0 = sum(p for (i, j), p in matris.items() if i == j)
    p_ms2 = sum(p for (i, j), p in matris.items() if j > i)

    p_1x = p_ms1 + p_ms0
    p_x2 = p_ms2 + p_ms0

    p_15_ust = sum(p for (i, j), p in matris.items() if (i + j) >= 2)
    p_25_ust = sum(p for (i, j), p in matris.items() if (i + j) >= 3)
    p_25_alt = sum(p for (i, j), p in matris.items() if (i + j) <= 2)
    p_35_alt = sum(p for (i, j), p in matris.items() if (i + j) <= 3)
    p_kg_var = sum(p for (i, j), p in matris.items() if i > 0 and j > 0)

    p_ms1_15_ust = sum(p for (i, j), p in matris.items() if i > j and (i + j) >= 2)
    p_ms2_15_ust = sum(p for (i, j), p in matris.items() if j > i and (i + j) >= 2)
    p_1x_15_ust  = sum(p for (i, j), p in matris.items() if i >= j and (i + j) >= 2)
    p_x2_15_ust  = sum(p for (i, j), p in matris.items() if j >= i and (i + j) >= 2)

    p_ev_15_ust  = sum(p for (i, j), p in matris.items() if i >= 2)
    p_dep_15_ust = sum(p for (i, j), p in matris.items() if j >= 2)

    p_iy_05_ust  = 1.0 - iy_matris.get((0, 0), 0.0)
    p_korner_85  = poisson_ust_olasiligi(toplam_korner_beklentisi, 8.5)
    p_korner_95  = poisson_ust_olasiligi(toplam_korner_beklentisi, 9.5)
    p_kart_35    = poisson_ust_olasiligi(toplam_kart_beklentisi, 3.5)

    # 8. "2-3 Maçta Ortalama 2.00 Oran" İçin Aday Marketler ve Tahmini Bürolar Oranı
    # (Her bir maçta hedef oran: ~1.30 - 1.52 arası, yüksek isabet koridoru)
    aday_marketler = [
        ('1x ve 1.5 üst', p_1x_15_ust, 1.38),
        ('x2 ve 1.5 üst', p_x2_15_ust, 1.42),
        ('1.5 gol üst', p_15_ust, 1.30),
        ('ilk yarı 0.5 üst', p_iy_05_ust, 1.36),
        ('ms 1 ve 1.5 üst', p_ms1_15_ust, 1.48),
        ('ms 2 ve 1.5 üst', p_ms2_15_ust, 1.52),
        ('ev 1.5 gol üst', p_ev_15_ust, 1.46),
        ('dep 1.5 gol üst', p_dep_15_ust, 1.50),
        ('3.5 gol alt', p_35_alt, 1.32),
        ('8.5 korner üst', p_korner_85, 1.40),
        ('9.5 korner üst', p_korner_95, 1.52),
        ('3.5 kart üst', p_kart_35, 1.42),
        ('maç sonu 1', p_ms1, 1.45),
        ('maç sonu 2', p_ms2, 1.50),
        ('karşılıklı gol var', p_kg_var, 1.55),
        ('2.5 gol üst', p_25_ust, 1.55)
    ]

    # Veri yeterliliği katsayısı (Takımların geçmiş maç sayısına göre güven düzeltmesi)
    veri_kalitesi = min(1.0, (ev_p['mac_sayisi'] + dep_p['mac_sayisi']) / 12.0)

    en_iyi_secim = None
    en_yuksek_skor = -1.0

    for market_adi, ham_olasilik, baz_oran in aday_marketler:
        # 2-3 maçta ~2.00 oran hedefi için ideal olasılık koridoru: %70 - %89
        # Çok aşırı düşük oranlı (örn. %95 olasılıklı 1.08 oranlı) veya çok riskli (%50) olanları cezalandır
        if 0.72 <= ham_olasilik <= 0.90:
            koridor_bonusu = 1.12
        elif 0.66 <= ham_olasilik < 0.72:
            koridor_bonusu = 1.00
        elif ham_olasilik > 0.90:
            # 1.5 gol üst veya 3.5 alt çok yüksekse onun yerine kombo marketi (1x ve 1.5 üst vb.) öne çıkar
            koridor_bonusu = 0.88 if market_adi in ('1.5 gol üst', '3.5 gol alt') else 1.05
        else:
            koridor_bonusu = 0.75

        deger_puani = (ham_olasilik * 100.0) * koridor_bonusu * (0.85 + 0.15 * veri_kalitesi)

        if deger_puani > en_yuksek_skor:
            en_yuksek_skor = deger_puani
            # Gerçekçi bahis oranı tahmini (Marj dahil)
            tahmini_oran = round(max(1.25, min(1.75, (1.0 / max(0.45, ham_olasilik)) * 0.93)), 2)
            en_iyi_secim = {
                'market': market_adi,
                'olasilik': round(min(96.0, ham_olasilik * 100.0), 1),
                'guven': round(min(98.0, deger_puani), 1),
                'tahmini_oran': tahmini_oran
            }

    # Kategori Sınıflandırması (index.php BANKO / IDEAL / RISKLI yapısı)
    olasilik_yuzde = en_iyi_secim['olasilik']
    if olasilik_yuzde >= 78.0 and veri_kalitesi >= 0.6:
        kategori = 'BANKO'
    elif olasilik_yuzde >= 70.0 and veri_kalitesi >= 0.4:
        kategori = 'IDEAL'
    else:
        kategori = 'RISKLI'

    # index.php arayüzü için detaylı AI Raporu (Tamamı küçük harf tasarımla uyumlu)
    ai_rapor = (
        f"<div class='space-y-1.5 text-[10px] text-zinc-300 leading-relaxed'>"
        f"<div class='flex justify-between border-b border-zinc-800 pb-1'>"
        f"<span class='text-zinc-500 font-bold'>tahmini skor / iy:</span>"
        f"<span class='text-emerald-400 font-black'>{en_yakin_skor[0]}-{en_yakin_skor[1]} (iy: {en_yakin_iy[0]}-{en_yakin_iy[1]})</span>"
        f"</div>"
        f"<div class='flex justify-between border-b border-zinc-800 pb-1'>"
        f"<span class='text-zinc-500 font-bold'>gol beklentisi (xg):</span>"
        f"<span class='text-zinc-200 font-bold'>{lam_ev:.2f} vs {lam_dep:.2f}</span>"
        f"</div>"
        f"<div class='flex justify-between border-b border-zinc-800 pb-1'>"
        f"<span class='text-zinc-500 font-bold'>tahmini şut / isabet:</span>"
        f"<span class='text-zinc-200 font-bold'>{tahmini_ev_sut:.1f} ({tahmini_ev_isabet:.1f}) - {tahmini_dep_sut:.1f} ({tahmini_dep_isabet:.1f})</span>"
        f"</div>"
        f"<div class='flex justify-between border-b border-zinc-800 pb-1'>"
        f"<span class='text-zinc-500 font-bold'>topa sahip olma / korner:</span>"
        f"<span class='text-zinc-200 font-bold'>%{tahmini_ev_top}-%%{tahmini_dep_top} • {toplam_korner_beklentisi:.1f} korner</span>"
        f"</div>"
        f"<div class='flex justify-between'>"
        f"<span class='text-zinc-500 font-bold'>tahmini oran / kart:</span>"
        f"<span class='text-amber-400 font-bold'>~{en_iyi_secim['tahmini_oran']:.2f} oran • {toplam_kart_beklentisi:.1f} sarı kart</span>"
        f"</div>"
        f"</div>"
    )

    return {
        'secilen_tahmin': en_iyi_secim['market'],
        'tahmin_olasilik': en_iyi_secim['olasilik'],
        'kategori': kategori,
        'guven_skoru': en_iyi_secim['guven'],
        'tahmini_oran': en_iyi_secim['tahmini_oran'],
        'ai_rapor': ai_rapor
    }

# =========================================================================
# 3. ADIM: GÜNLÜK 2-3 MAÇLIK (~2.00 ORANLI) KUPONU SEÇ VE VERİTABANINA İŞLE
# =========================================================================
def tahmin_motorunu_calistir():
    conn = db_baglantisi_kur()
    if not conn:
        return

    cursor = conn.cursor()

    # Tahminler tablosu yoksa otomatik oluştur (Mevcut yapıyla birebir uyumlu)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS `tahminler` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `fikstur_id` int(11) NOT NULL,
            `secilen_tahmin` varchar(100) DEFAULT NULL,
            `tahmin_olasilik` float DEFAULT NULL,
            `kategori` varchar(50) DEFAULT 'RISKLI',
            `guven_skoru` float DEFAULT NULL,
            `ai_rapor` text DEFAULT NULL,
            `kupon_maci` tinyint(1) DEFAULT 0,
            `durum` varchar(50) DEFAULT 'HAZIR',
            `sonuc` varchar(50) DEFAULT 'BEKLIYOR',
            PRIMARY KEY (`id`),
            UNIQUE KEY `idx_fikstur_id` (`fikstur_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)

    bugun = datetime.now()
    hedef_tarihler = [
        (bugun + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in [0, 1]  # Bugün ve Yarın
    ]

    for hedef_tarih in hedef_tarihler:
        print(f"\n[*] {hedef_tarih} tarihi için FT geçmiş verileri taranıyor...")
        takim_gecmisi, lig_ortalamalari = gecmis_verileri_analiz_et(conn, hedef_tarih)

        cursor.execute("""
            SELECT * FROM fikstur 
            WHERE tarih = %s AND durum NOT LIKE '%%Canceled%%' AND durum NOT LIKE '%%Postponed%%'
        """, (hedef_tarih,))
        gunun_maclari = cursor.fetchall()

        if not gunun_maclari:
            print(f"[-] {hedef_tarih} tarihinde maç bulunamadı.")
            continue

        gunun_tahminleri = []
        for mac in gunun_maclari:
            analiz = mac_simule_et(
                mac['ev_sahibi'], mac['deplasman'], mac['lig'],
                takim_gecmisi, lig_ortalamalari
            )
            analiz['fikstur_id'] = mac['id']
            analiz['ev_sahibi'] = mac['ev_sahibi']
            analiz['deplasman'] = mac['deplasman']
            gunun_tahminleri.append(analiz)

        # -----------------------------------------------------------------
        # 2 VEYA 3 MAÇLIK ORTALAMA ~2.00 ORANLI KUPON SEÇİCİ
        # -----------------------------------------------------------------
        onayli_adaylar = [t for t in gunun_tahminleri if t['kategori'] in ('BANKO', 'IDEAL')]
        onayli_adaylar.sort(key=lambda x: (x['kategori'] == 'BANKO', x['guven_skoru']), reverse=True)

        secilen_kupon_idleri = set()
        carpim_oran = 1.0

        for aday in onayli_adaylar:
            if len(secilen_kupon_idleri) < 3:
                secilen_kupon_idleri.add(aday['fikstur_id'])
                carpim_oran *= aday['tahmini_oran']
                # Eğer 2 veya 3 maçta toplam oran 1.90'ı geçtiyse kuponu tamamla
                if len(secilen_kupon_idleri) >= 2 and carpim_oran >= 1.90:
                    break

        # Veritabanına kaydet / güncelle
        for t in gunun_tahminleri:
            kupon_mu = 1 if t['fikstur_id'] in secilen_kupon_idleri else 0
            cursor.execute("""
                INSERT INTO tahminler 
                (fikstur_id, secilen_tahmin, tahmin_olasilik, kategori, guven_skoru, ai_rapor, kupon_maci, durum, sonuc)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'HAZIR', 'BEKLIYOR')
                ON DUPLICATE KEY UPDATE
                    secilen_tahmin = VALUES(secilen_tahmin),
                    tahmin_olasilik = VALUES(tahmin_olasilik),
                    kategori = VALUES(kategori),
                    guven_skoru = VALUES(guven_skoru),
                    ai_rapor = VALUES(ai_rapor),
                    kupon_maci = VALUES(kupon_maci),
                    durum = 'HAZIR'
            """, (
                t['fikstur_id'], t['secilen_tahmin'], t['tahmin_olasilik'],
                t['kategori'], t['guven_skoru'], t['ai_rapor'], kupon_mu
            ))
            kupon_etiket = "🔥 [KUPON MAÇI]" if kupon_mu else ""
            print(f"[+] {t['ev_sahibi']} vs {t['deplasman']} -> {t['secilen_tahmin']} (%{t['tahmin_olasilik']} | ~{t['tahmini_oran']} Oran | {t['kategori']}) {kupon_etiket}")

        if secilen_kupon_idleri:
            print(f"🎯 {hedef_tarih} GÜNÜN KUPONU HAZIR: {len(secilen_kupon_idleri)} Maç | Tahmini Toplam Oran: {carpim_oran:.2f}")

    # Mevcut bitmiş maçlar varsa sonuçlarını hemen güncelle
    from guncelle import tahmin_sonuclarini_veritabanina_isle
    try:
        tahmin_sonuclarini_veritabanina_isle(conn)
    except Exception:
        pass

    conn.close()

if __name__ == "__main__":
    tahmin_motorunu_calistir()
