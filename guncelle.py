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
            host=CONFIG["db"]["host"], user=CONFIG["db"]["user"],
            password=CONFIG["db"]["password"], database=CONFIG["db"]["database"],
            port=CONFIG["db"]["port"], charset='utf8mb4',
            autocommit=True, cursorclass=pymysql.cursors.DictCursor
        )
    except Exception as e:
        print(f"Veritabanı hatası: {e}")
        return None

def flt(val, default=None):
    if val is None or val == "" or val == "-":
        return default
    try:
        return float(str(val).replace("%", "").strip())
    except:
        return default

def poisson(k, lam):
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (math.exp(-lam) * (lam ** k)) / math.factorial(k)

def yari_matrisi_olustur(lam_ev, lam_dep, max_gol=4):
    m = {}
    top = 0.0
    for i in range(max_gol + 1):
        for j in range(max_gol + 1):
            p = poisson(i, lam_ev) * poisson(j, lam_dep)
            m[(i, j)] = p
            top += p
    if top > 0:
        for k in m:
            m[k] /= top
    return m

# =========================================================================
# 1. ADIM: FT MAÇLARDAN YARI BAZLI (1Y VE 2Y) PROFİLLERİ ÇIKAR
# =========================================================================
def gecmis_verileri_analiz_et(conn, referans_tarih):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM fikstur 
        WHERE durum LIKE '%%FT%%' AND tarih < %s
        ORDER BY tarih ASC, id ASC
    """, (referans_tarih,))
    ft_maclar = cursor.fetchall()

    gecmis = {}
    for m in ft_maclar:
        ev_s = flt(m.get('ev_skor'))
        dep_s = flt(m.get('dep_skor'))
        ev_iy = flt(m.get('ev_iy_skor'))
        dep_iy = flt(m.get('dep_iy_skor'))

        if ev_s is None or dep_s is None or ev_iy is None or dep_iy is None:
            continue

        ev_2y = max(0.0, ev_s - ev_iy)
        dep_2y = max(0.0, dep_s - dep_iy)

        ev_sut = flt(m.get('ev_sut'), 0)
        dep_sut = flt(m.get('dep_sut'), 0)
        temel_var = (ev_sut + dep_sut) > 0

        taraflar = [
            (m['ev_sahibi'], 'EV', ev_iy, dep_iy, ev_2y, dep_2y, ev_s, dep_s, 'ev_', 'dep_'),
            (m['deplasman'], 'DEP', dep_iy, ev_iy, dep_2y, ev_2y, dep_s, ev_s, 'dep_', 'ev_')
        ]

        for takim, saha, iy_a, iy_y, ik_a, ik_y, ms_a, ms_y, k_on, r_on in taraflar:
            if takim not in gecmis:
                gecmis[takim] = []

            gecmis[takim].append({
                'saha': saha,
                'temel_var': temel_var,
                'iy_atilan': iy_a,
                'iy_yenen': iy_y,
                'ik_atilan': ik_a,
                'ik_yenen': ik_y,
                'ms_atilan': ms_a,
                'ms_yenen': ms_y,
                'geri_dondu': 1.0 if (iy_a < iy_y and ms_a >= ms_y) else 0.0,
                'ustunluk_verdi': 1.0 if (iy_a > iy_y and ms_a <= ms_y) else 0.0,
                'iy_berabere_kazandi': 1.0 if (iy_a == iy_y and ms_a > ms_y) else 0.0,
                'iki_yari_kg': 1.0 if (iy_a > 0 and iy_y > 0 and ik_a > 0 and ik_y > 0) else 0.0,
                'iy_kg': 1.0 if (iy_a > 0 and iy_y > 0) else 0.0,
                'top_oynama': flt(m.get(f'{k_on}top_oynama'), 50.0),
                'sut': flt(m.get(f'{k_on}sut'), 12.0),
                'isabetli': flt(m.get(f'{k_on}isabetli'), 4.2),
                'yenen_isabetli': flt(m.get(f'{r_on}isabetli'), 4.2),
                'sut_yuzde': flt(m.get(f'{k_on}sut_yuzde'), 0.35),
                'korner': flt(m.get(f'{k_on}korner'), 4.8),
                'toplam_uzak': flt(m.get(f'{k_on}toplam_uzak'), 24.0)
            })

    return gecmis

def ort_hesapla(liste, anahtar, varsayilan, sadece_temel=False):
    uygun = [m[anahtar] for m in liste if (not sadece_temel or m['temel_var']) and m.get(anahtar) is not None]
    if not uygun:
        return varsayilan
    son = uygun[-10:]
    t_ag = 0.0
    t_deg = 0.0
    for idx, v in enumerate(son):
        ag = 1.25 ** idx
        t_deg += v * ag
        t_ag += ag
    return t_deg / t_ag

def yari_profili_cikar(takim, saha, gecmis):
    tum = gecmis.get(takim, [])
    saha_m = [x for x in tum if x['saha'] == saha]

    def h(anahtar, varsayilan, temel=False):
        g = ort_hesapla(tum, anahtar, varsayilan, temel)
        if len(saha_m) >= 2:
            s = ort_hesapla(saha_m, anahtar, g, temel)
            return (s * 0.58) + (g * 0.42)
        return g

    return {
        'mac_sayisi': len(tum),
        'iy_atilan': h('iy_atilan', 0.60),
        'iy_yenen': h('iy_yenen', 0.58),
        'ik_atilan': h('ik_atilan', 0.78),
        'ik_yenen': h('ik_yenen', 0.75),
        'geri_dondu': h('geri_dondu', 0.08),
        'ustunluk_verdi': h('ustunluk_verdi', 0.08),
        'iy_berabere_kazandi': h('iy_berabere_kazandi', 0.16),
        'iki_yari_kg': h('iki_yari_kg', 0.05),
        'iy_kg': h('iy_kg', 0.20),
        'top_oynama': h('top_oynama', 50.0, True),
        'sut': h('sut', 12.5, True),
        'isabetli': h('isabetli', 4.3, True),
        'yenen_isabetli': h('yenen_isabetli', 4.2, True),
        'sut_yuzde': h('sut_yuzde', 0.34, True),
        'korner': h('korner', 4.8, True),
        'toplam_uzak': h('toplam_uzak', 24.0, True)
    }

# =========================================================================
# 2. ADIM: KALİBRE EDİLMİŞ YÜKSEK ORAN & YARI ASİMETRİ MOTORU
# =========================================================================
def yuksek_oran_simule_et(ev_adi, dep_adi, gecmis):
    ev = yari_profili_cikar(ev_adi, 'EV', gecmis)
    dep = yari_profili_cikar(dep_adi, 'DEP', gecmis)
    min_mac = min(ev['mac_sayisi'], dep['mac_sayisi'])

    # 1. Yarı Gol Beklentileri
    lam_ev_1y = max(0.15, (ev['iy_atilan'] * 0.55) + (dep['iy_yenen'] * 0.45) + 0.03)
    lam_dep_1y = max(0.15, (dep['iy_atilan'] * 0.55) + (ev['iy_yenen'] * 0.45))

    # 2. Yarı Gol Beklentileri (Baskı, Şut ve Korner Çarpanlı)
    ev_baski = 1.0 + max(-0.15, min(0.30, ((ev['top_oynama'] - 50.0) * 0.005) + ((ev['sut'] - 12.5) * 0.016) + ((ev['korner'] - 4.8) * 0.015)))
    dep_baski = 1.0 + max(-0.15, min(0.30, ((dep['top_oynama'] - 50.0) * 0.005) + ((dep['sut'] - 12.5) * 0.016) + ((dep['korner'] - 4.8) * 0.015)))

    lam_ev_2y = max(0.20, ((ev['ik_atilan'] * 0.55) + (dep['ik_yenen'] * 0.45)) * ev_baski)
    lam_dep_2y = max(0.18, ((dep['ik_atilan'] * 0.55) + (ev['ik_yenen'] * 0.45)) * dep_baski)

    m1 = yari_matrisi_olustur(lam_ev_1y, lam_dep_1y, 4)
    m2 = yari_matrisi_olustur(lam_ev_2y, lam_dep_2y, 4)

    en_yakin_iy = sorted(m1.items(), key=lambda x: x[1], reverse=True)[0][0]
    en_yakin_2y = sorted(m2.items(), key=lambda x: x[1], reverse=True)[0][0]
    tahmini_ms_ev = en_yakin_iy[0] + en_yakin_2y[0]
    tahmini_ms_dep = en_yakin_iy[1] + en_yakin_2y[1]

    p = {
        'ilk yarı kg var': 0.0,
        'iy/ms x/1': 0.0,
        'iy/ms x/2': 0.0,
        'ms 1 ve kg var': 0.0,
        'ms 2 ve kg var': 0.0,
        'iki yarıda da kg var': 0.0,
        'iy/ms 1/x': 0.0,
        'iy/ms 2/x': 0.0,
        'iy/ms 1/2': 0.0,
        'iy/ms 2/1': 0.0
    }

    # YARI ASİMETRİ ÇARPANLARI (Raporda kaçan 1/X, 2/X ve 1/2 maçlarını yakalayan özel dedektör)
    # Senaryo A (2/X ve 2/1): Deplasman 1. yarı hızlı başlıyor, Ev 2. yarı ağır baskı kuruyor
    asimetri_dep_iy_ev_2y = 1.0
    if dep['iy_atilan'] >= 0.55 and ev['ik_atilan'] >= 0.85 and ev['sut'] >= 13.0:
        asimetri_dep_iy_ev_2y = 1.45 + (ev['geri_dondu'] * 1.8) + (dep['ustunluk_verdi'] * 1.8)

    # Senaryo B (1/X ve 1/2): Ev 1. yarı hızlı başlıyor ama skoru koruyamıyor, Deplasman 2. yarı açılıyor
    asimetri_ev_iy_dep_2y = 1.0
    if ev['iy_atilan'] >= 0.55 and dep['ik_atilan'] >= 0.85 and dep['sut'] >= 12.5:
        asimetri_ev_iy_dep_2y = 1.45 + (dep['geri_dondu'] * 1.8) + (ev['ustunluk_verdi'] * 1.8)

    for (iy_e, iy_d), ol1 in m1.items():
        iy_kg_var = (iy_e > 0 and iy_d > 0)
        if iy_kg_var:
            p['ilk yarı kg var'] += ol1 * (1.0 + (ev['iy_kg'] + dep['iy_kg']) * 0.6)

        for (ik_e, ik_d), ol2 in m2.items():
            ik_kg_var = (ik_e > 0 and ik_d > 0)
            ms_e = iy_e + ik_e
            ms_d = iy_d + ik_d
            baz_ol = ol1 * ol2

            if iy_e == iy_d and ms_e > ms_d:
                p['iy/ms x/1'] += baz_ol * (1.0 + ev['iy_berabere_kazandi'] * 1.2)
            if iy_e == iy_d and ms_e < ms_d:
                p['iy/ms x/2'] += baz_ol * (1.0 + dep['iy_berabere_kazandi'] * 1.2)

            if iy_e > iy_d and ms_e == ms_d:
                p['iy/ms 1/x'] += baz_ol * asimetri_ev_iy_dep_2y
            if iy_e < iy_d and ms_e == ms_d:
                p['iy/ms 2/x'] += baz_ol * asimetri_dep_iy_ev_2y
            if iy_e > iy_d and ms_e < ms_d:
                p['iy/ms 1/2'] += baz_ol * asimetri_ev_iy_dep_2y * 1.15
            if iy_e < iy_d and ms_e > ms_d:
                p['iy/ms 2/1'] += baz_ol * asimetri_dep_iy_ev_2y

            if iy_kg_var and ik_kg_var:
                kg_bonus = 1.0 + ((ev['iki_yari_kg'] + dep['iki_yari_kg']) * 2.2)
                if ev['isabetli'] >= 4.8 and dep['isabetli'] >= 4.5:
                    kg_bonus *= 1.25
                p['iki yarıda da kg var'] += baz_ol * kg_bonus

            if ms_e > ms_d and ms_e > 0 and ms_d > 0:
                p['ms 1 ve kg var'] += baz_ol
            if ms_d > ms_e and ms_e > 0 and ms_d > 0:
                p['ms 2 ve kg var'] += baz_ol

    # 60 Günlük Rapor Frekanslarına Göre Kalibre Edilmiş Market Havuzu
    market_ayarlari = {
        'ilk yarı kg var':      {'oran': 4.10,  'baz_p': 0.195, 'ag': 1.12}, # 97 kez çıktı
        'iy/ms x/1':            {'oran': 4.40,  'baz_p': 0.190, 'ag': 1.10}, # 72 kez çıktı
        'iy/ms x/2':            {'oran': 5.40,  'baz_p': 0.155, 'ag': 1.08}, # 57 kez çıktı
        'ms 1 ve kg var':       {'oran': 3.10,  'baz_p': 0.250, 'ag': 1.05},
        'ms 2 ve kg var':       {'oran': 3.75,  'baz_p': 0.205, 'ag': 1.05},
        'iki yarıda da kg var': {'oran': 11.50, 'baz_p': 0.062, 'ag': 1.15}, # 37 kez çıktı, 3 kez yakalandı!
        'iy/ms 2/x':            {'oran': 14.00, 'baz_p': 0.055, 'ag': 1.12}, # 26 kez çıktı
        'iy/ms 1/x':            {'oran': 14.00, 'baz_p': 0.055, 'ag': 1.10}, # 23 kez çıktı
        'iy/ms 1/2':            {'oran': 26.00, 'baz_p': 0.026, 'ag': 0.95}, # 13 kez çıktı
    }

    en_iyi_secim = None
    en_yuksek_puan = -1.0

    for m_adi, cfg in market_ayarlari.items():
        ol = p[m_adi]
        edge = ol / cfg['baz_p']

        # Mod 2'de +103.7 birim kazandıran skorlama + Ekstrem Sürpriz Tetikleyicisi
        if cfg['oran'] >= 11.0:
            # 11.50 - 26.00 oranlı ekstrem marketler sadece çok güçlü sinyal (edge >= 1.65) varsa öne geçer
            if edge >= 1.65:
                skor = (ol * 100.0) * (edge ** 0.85) * cfg['ag'] * 1.35
            else:
                skor = (ol * 100.0) * (edge ** 0.50) * 0.65
        else:
            # 3.10 - 5.40 oranlı ana vurgun marketleri
            skor = (ol * 100.0) * (edge ** 0.68) * cfg['ag']

        if min_mac < 3:
            skor *= 0.50

        if skor > en_yuksek_puan:
            en_yuksek_puan = skor
            en_iyi_secim = {
                'market': m_adi,
                'olasilik': round(min(65.0, ol * 100.0), 1),
                'guven': round(min(99.0, skor * 2.2), 1),
                'oran': cfg['oran']
            }

    if en_iyi_secim['guven'] >= 62.0 and min_mac >= 3:
        kategori = 'BANKO'
    elif en_iyi_secim['guven'] >= 48.0 and min_mac >= 2:
        kategori = 'IDEAL'
    else:
        kategori = 'RISKLI'

    ai_rapor = (
        f"<div class='space-y-1.5 text-[10px] text-zinc-300 leading-relaxed'>"
        f"<div class='flex justify-between border-b border-zinc-800 pb-1'>"
        f"<span class='text-zinc-500 font-bold'>tahmini devre / ms:</span>"
        f"<span class='text-amber-400 font-black'>iy: {en_yakin_iy[0]}-{en_yakin_iy[1]} • ms: {tahmini_ms_ev}-{tahmini_ms_dep}</span>"
        f"</div>"
        f"<div class='flex justify-between border-b border-zinc-800 pb-1'>"
        f"<span class='text-zinc-500 font-bold'>1. yarı gol beklentisi:</span>"
        f"<span class='text-zinc-200 font-bold'>{lam_ev_1y:.2f} vs {lam_dep_1y:.2f}</span>"
        f"</div>"
        f"<div class='flex justify-between border-b border-zinc-800 pb-1'>"
        f"<span class='text-zinc-500 font-bold'>2. yarı gol beklentisi:</span>"
        f"<span class='text-zinc-200 font-bold'>{lam_ev_2y:.2f} vs {lam_dep_2y:.2f}</span>"
        f"</div>"
        f"<div class='flex justify-between'>"
        f"<span class='text-zinc-500 font-bold'>hedef oran / değer skoru:</span>"
        f"<span class='text-emerald-400 font-bold'>@{en_iyi_secim['oran']:.2f} oran • {en_iyi_secim['guven']} puan</span>"
        f"</div>"
        f"</div>"
    )

    return {
        'secilen_tahmin': en_iyi_secim['market'],
        'tahmin_olasilik': en_iyi_secim['olasilik'],
        'kategori': kategori,
        'guven_skoru': en_iyi_secim['guven'],
        'tahmini_oran': en_iyi_secim['oran'],
        'ai_rapor': ai_rapor
    }

# =========================================================================
# 3. ADIM: GÜNÜN 3-4 MAÇLIK YÜKSEK ORAN KOMBİNİNİ SEÇ VE VERİTABANINA YAZ
# =========================================================================
def tahmin_motorunu_calistir():
    conn = db_baglantisi_kur()
    if not conn:
        return

    cursor = conn.cursor()
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
    hedef_tarihler = [(bugun + timedelta(days=i)).strftime("%Y-%m-%d") for i in [0, 1]]

    for hedef_tarih in hedef_tarihler:
        print(f"\n[*] {hedef_tarih} için yüksek oranlı kombinasyon analizi başlatıldı...")
        gecmis = gecmis_verileri_analiz_et(conn, hedef_tarih)

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
            analiz = yuksek_oran_simule_et(mac['ev_sahibi'], mac['deplasman'], gecmis)
            analiz['fikstur_id'] = mac['id']
            analiz['ev_sahibi'] = mac['ev_sahibi']
            analiz['deplasman'] = mac['deplasman']
            gunun_tahminleri.append(analiz)

        # Günün en yüksek puanlı 3 veya 4 onaylı maçını kupona al (Mod 2 Vurgun Mimarisi)
        onayli = [t for t in gunun_tahminleri if t['kategori'] in ('BANKO', 'IDEAL')]
        onayli.sort(key=lambda x: x['guven_skoru'], reverse=True)

        # Eğer çok güçlü 4. maç varsa (güven >= 70) 4 maç, yoksa 3 maç seç
        kupon_limiti = 4 if (len(onayli) >= 4 and onayli[3]['guven_skoru'] >= 70.0) else 3
        secilen_kupon_idleri = {t['fikstur_id'] for t in onayli[:kupon_limiti]}

        carpim_oran = 1.0
        for t in onayli[:kupon_limiti]:
            carpim_oran *= t['tahmini_oran']

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
            etiket = "💣 [KUPON MAÇI]" if kupon_mu else ""
            print(f"[+] {t['ev_sahibi']} vs {t['deplasman']} -> {t['secilen_tahmin']} (@{t['tahmini_oran']} | Güven: {t['guven_skoru']} | {t['kategori']}) {etiket}")

        if secilen_kupon_idleri:
            print(f"🔥 {hedef_tarih} YÜKSEK ORAN KOMBİNİ HAZIR: {len(secilen_kupon_idleri)} Maç | Toplam Oran: {carpim_oran:.2f} (Sistem 2/{len(secilen_kupon_idleri)} + Düz Kupon Önerilir)")

    from guncelle import tahmin_sonuclarini_veritabanina_isle
    try:
        tahmin_sonuclarini_veritabanina_isle(conn)
    except Exception:
        pass

    conn.close()

if __name__ == "__main__":
    tahmin_motorunu_calistir()
