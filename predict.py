#!/usr/bin/env python3
"""
Futbol Maç Tahmin Sistemi - 2.5 Üst ve KG Var Value Bahisleri
Metodoloji: Form, Hücum Baskısı, H2H, Bonuslar, Hakem Etkisi, Value Tespiti
"""

import os
import datetime as dt
from typing import Dict, List, Any, Optional, Tuple
import mysql.connector
import numpy as np
from collections import defaultdict

# ==================== KONFİGÜRASYON ====================
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306))
}

MAJOR_TOURNAMENT_IDS = {
    1, 2, 3, 72, 84, 36, 37, 3739, 33, 34, 7372, 42, 41, 8343, 810,
    4, 5397, 62, 101, 39, 40, 38, 692, 280, 127, 83, 1449,
    169352, 5071, 28, 6720, 18, 3397, 3708, 82, 3034, 3284, 6230,
    54, 64, 29, 1060, 219, 652, 144, 1339, 1340, 1341, 5, 6, 12, 13, 19, 24, 27, 30, 31, 48, 49, 50, 52, 53, 55, 79, 102, 232, 384, 
    681, 877, 1061, 1107, 1427, 10812, 16753, 19232, 34363, 51702, 52653, 58560, 
    64475, 71900, 71901, 72112, 78740, 92016, 92614, 143625
}

# Varsayılan lig değerleri (veri yoksa kullanılır)
DEFAULT_LEAGUE_STATS = {
    "avg_goals": 2.5,
    "avg_shot_on": 8.0,
    "avg_corners": 9.0,
    "btts_ratio": 0.45,
    "over25_ratio": 0.45
}

# ==================== VERİTABANI BAĞLANTISI ====================
class Database:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = mysql.connector.connect(**DB_CONFIG)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor(dictionary=True)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_league_stats(self) -> Dict[int, Dict]:
        """Lig bazlı (category_id) istatistikleri hesaplar veya cache'ten alır."""
        query = """
            SELECT 
                category_id,
                COUNT(*) as match_count,
                AVG(ft_home + ft_away) as avg_goals,
                AVG(shot_on_h + shot_on_a) as avg_shot_on,
                AVG(corn_h + corn_a) as avg_corners,
                SUM(CASE WHEN ft_home > 0 AND ft_away > 0 THEN 1 ELSE 0 END) / COUNT(*) as btts_ratio,
                SUM(CASE WHEN (ft_home + ft_away) > 2.5 THEN 1 ELSE 0 END) / COUNT(*) as over25_ratio
            FROM results_football
            WHERE status IN ('finished', 'ended')
                AND ft_home IS NOT NULL AND ft_away IS NOT NULL
                AND category_id IS NOT NULL
            GROUP BY category_id
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        stats = {}
        for row in rows:
            stats[row['category_id']] = {
                'avg_goals': float(row['avg_goals']),
                'avg_shot_on': float(row['avg_shot_on']),
                'avg_corners': float(row['avg_corners']),
                'btts_ratio': float(row['btts_ratio']),
                'over25_ratio': float(row['over25_ratio'])
            }
        return stats

    def get_team_dna(self, team_key: str) -> Dict:
        """Takım DNA'sı (tüm geçmiş ortalamalar)"""
        query = """
            SELECT 
                AVG(ft_home) as avg_goals_scored,
                AVG(shot_on_h) as avg_shot_on,
                AVG(corn_h) as avg_corners
            FROM results_football
            WHERE status IN ('finished', 'ended')
                AND (home_team = %s OR away_team = %s)
        """
        self.cursor.execute(query, (team_key, team_key))
        row = self.cursor.fetchone()
        if row and row['avg_goals_scored']:
            return {
                'avg_goals': float(row['avg_goals_scored']),
                'avg_shot_on': float(row['avg_shot_on'] or 0),
                'avg_corners': float(row['avg_corners'] or 0)
            }
        return {'avg_goals': 1.0, 'avg_shot_on': 4.0, 'avg_corners': 4.0}

    def get_team_form(self, team_key: str, home_only: bool = False, away_only: bool = False, last_n: int = 10) -> List[Dict]:
        """Takımın son N maç formu (goller, şutlar, korner, possession vb.)"""
        if home_only:
            condition = "home_team = %s"
        elif away_only:
            condition = "away_team = %s"
        else:
            condition = "(home_team = %s OR away_team = %s)"
            params = (team_key, team_key)
        
        query = f"""
            SELECT 
                ft_home, ft_away,
                shot_on_h, shot_on_a,
                corn_h, corn_a,
                poss_h, poss_a,
                status
            FROM results_football
            WHERE status IN ('finished', 'ended') AND {condition}
            ORDER BY start_utc DESC, start_time_utc DESC
            LIMIT %s
        """
        if home_only or away_only:
            self.cursor.execute(query, (team_key, last_n))
        else:
            self.cursor.execute(query, (team_key, team_key, last_n))
        return self.cursor.fetchall()

    def get_h2h_matches(self, team1_key: str, team2_key: str, last_n: int = 10) -> List[Dict]:
        """İki takım arasındaki son N maç"""
        query = """
            SELECT ft_home, ft_away
            FROM results_football
            WHERE status IN ('finished', 'ended')
                AND ((home_team = %s AND away_team = %s) OR (home_team = %s AND away_team = %s))
            ORDER BY start_utc DESC
            LIMIT %s
        """
        self.cursor.execute(query, (team1_key, team2_key, team2_key, team1_key, last_n))
        return self.cursor.fetchall()

    def get_upcoming_matches(self) -> List[Dict]:
        """Bugün ve yarın oynanacak, başlamamış major turnuva maçları"""
        tz_tr = dt.timezone(dt.timedelta(hours=3))
        today = dt.datetime.now(tz_tr).date()
        tomorrow = today + dt.timedelta(days=1)
        query = """
            SELECT 
                event_id, start_utc, start_time_utc,
                home_team, away_team,
                odds_o25, odds_btts_yes,
                tournament_id, tournament_name, category_id, category_name, country
            FROM results_football
            WHERE status IN ('notstarted', 'scheduled')
                AND start_utc IN (%s, %s)
                AND (tournament_id IN ({}) OR category_id IN ({}))
        """.format(','.join(map(str, MAJOR_TOURNAMENT_IDS)), ','.join(map(str, MAJOR_TOURNAMENT_IDS)))
        self.cursor.execute(query, (today, tomorrow))
        return self.cursor.fetchall()

    def get_referee_avg_goals(self, referee_name: str) -> Optional[float]:
        """Hakemin yönettiği maçlardaki ortalama gol sayısı (lig ortalamasına göre normalize edilmemiş)"""
        if not referee_name:
            return None
        query = """
            SELECT AVG(ft_home + ft_away) as avg_goals
            FROM results_football
            WHERE referee = %s AND status IN ('finished', 'ended')
        """
        self.cursor.execute(query, (referee_name,))
        row = self.cursor.fetchone()
        return float(row['avg_goals']) if row and row['avg_goals'] else None

# ==================== TAHMİN MOTORU ====================
class PredictionEngine:
    def __init__(self, db: Database):
        self.db = db
        self.league_stats = db.get_league_stats()
        self.team_dna_cache = {}

    def _get_team_key(self, team_name: str, category_id: int) -> str:
        """Benzersiz takım anahtarı: isim|category_id"""
        return f"{team_name}|{category_id}"

    def _normalize(self, value: float, league_avg: float, max_ratio: float = 1.5) -> float:
        """Lig ortalamasına göre normalizasyon (cap = max_ratio)"""
        if league_avg <= 0:
            return 1.0
        return min(value / league_avg, max_ratio)

    def _get_league_stats(self, category_id: int) -> Dict:
        """Lig istatistiklerini getir, yoksa varsayılanı kullan"""
        return self.league_stats.get(category_id, DEFAULT_LEAGUE_STATS.copy())

    def calculate_form_score(self, home_team_key: str, away_team_key: str, category_id: int) -> float:
        """Adım 2 – Form ve Karakter Puanı (max 45)"""
        league = self._get_league_stats(category_id)
        league_avg_goals = league['avg_goals']

        # Home takımının son 10 maç gol ortalaması (tüm maçlar)
        home_matches = self.db.get_team_form(home_team_key, last_n=10)
        away_matches = self.db.get_team_form(away_team_key, last_n=10)
        
        def avg_goals_from_matches(matches, team_key):
            goals = []
            for m in matches:
                if m['home_team'] == team_key:
                    goals.append(m['ft_home'] or 0)
                else:
                    goals.append(m['ft_away'] or 0)
            return np.mean(goals) if goals else 0
        
        home_avg = avg_goals_from_matches(home_matches, home_team_key)
        away_avg = avg_goals_from_matches(away_matches, away_team_key)
        general_avg = (home_avg + away_avg) / 2.0
        
        # Normalizasyon
        norm_general = self._normalize(general_avg, league_avg_goals)
        general_score = min(31.5, general_avg * 7 * norm_general)
        
        # Özel form (ev sahibi iç saha, deplasman dış saha)
        home_home = self.db.get_team_form(home_team_key, home_only=True, last_n=5)
        away_away = self.db.get_team_form(away_team_key, away_only=True, last_n=5)
        
        home_home_avg = avg_goals_from_matches(home_home, home_team_key)
        away_away_avg = avg_goals_from_matches(away_away, away_team_key)
        specific_avg = (home_home_avg + away_away_avg) / 2.0
        
        norm_specific = self._normalize(specific_avg, league_avg_goals)
        specific_score = min(13.5, specific_avg * 3 * norm_specific)
        
        return general_score + specific_score

    def calculate_attack_pressure(self, home_team_key: str, away_team_key: str, category_id: int) -> float:
        """Adım 3 – Hücum Baskısı ve Tempo Puanı (max 40)"""
        league = self._get_league_stats(category_id)
        
        def get_last3_stats(team_key, home_only=False, away_only=False):
            matches = self.db.get_team_form(team_key, home_only=home_only, away_only=away_only, last_n=3)
            if len(matches) < 3:
                matches = self.db.get_team_form(team_key, last_n=3)  # fallback
            shot_on, corn, shot, poss = [], [], [], []
            for m in matches:
                if m['home_team'] == team_key:
                    shot_on.append(m['shot_on_h'] or 0)
                    corn.append(m['corn_h'] or 0)
                    shot.append((m['shot_on_h'] or 0) + (m.get('shot_h',0) or 0))  # total shot approximated
                    poss.append(m['poss_h'] or 0)
                else:
                    shot_on.append(m['shot_on_a'] or 0)
                    corn.append(m['corn_a'] or 0)
                    shot.append((m['shot_on_a'] or 0) + (m.get('shot_a',0) or 0))
                    poss.append(m['poss_a'] or 0)
            # Kesici kural: son maçta isabetli şut <=1 ise poss katkısını sıfırla
            last_shot_on = shot_on[-1] if shot_on else 0
            poss_factor = 0.0 if last_shot_on <= 1 else 1.0
            return {
                'shot_on': np.mean(shot_on) if shot_on else 0,
                'corn': np.mean(corn) if corn else 0,
                'shot': np.mean(shot) if shot else 0,
                'poss': np.mean(poss) * poss_factor if poss else 0
            }
        
        home_stats = get_last3_stats(home_team_key)
        away_stats = get_last3_stats(away_team_key)
        
        norm_shot = self._normalize((home_stats['shot_on'] + away_stats['shot_on'])/2, league['avg_shot_on'])
        norm_corn = self._normalize((home_stats['corn'] + away_stats['corn'])/2, league['avg_corners'])
        norm_poss = self._normalize((home_stats['poss'] + away_stats['poss'])/2, 50.0)  # possession ort 50
        
        home_pressure = (
            home_stats['shot_on'] * 3 * norm_shot +
            home_stats['corn'] * 1.5 * norm_corn +
            home_stats['shot'] * 1 * norm_shot +
            home_stats['poss'] * 0.2 * norm_poss
        ) / 3.0
        
        away_pressure = (
            away_stats['shot_on'] * 3 * norm_shot +
            away_stats['corn'] * 1.5 * norm_corn +
            away_stats['shot'] * 1 * norm_shot +
            away_stats['poss'] * 0.2 * norm_poss
        ) / 3.0
        
        pressure = (home_pressure + away_pressure) / 2.0
        return min(40.0, pressure)

    def calculate_h2h_score(self, home_team_key: str, away_team_key: str, category_id: int) -> float:
        """Adım 4 – H2H Geçmiş Karşılaşmalar (max 15)"""
        h2h = self.db.get_h2h_matches(home_team_key, away_team_key, last_n=10)
        if not h2h:
            return 0.0
        
        total = len(h2h)
        btts_count = sum(1 for m in h2h if (m['ft_home'] or 0) > 0 and (m['ft_away'] or 0) > 0)
        over25_count = sum(1 for m in h2h if (m['ft_home'] or 0) + (m['ft_away'] or 0) > 2.5)
        
        btts_ratio = btts_count / total
        over25_ratio = over25_count / total
        
        league = self._get_league_stats(category_id)
        norm_btts = self._normalize(btts_ratio, league['btts_ratio'])
        norm_over = self._normalize(over25_ratio, league['over25_ratio'])
        
        score = ((btts_ratio * norm_btts + over25_ratio * norm_over) / 2.0) * 15
        return min(15.0, score)

    def calculate_early_goal_bonus(self, home_team_key: str, away_team_key: str, category_id: int) -> int:
        """Adım 5 – Erken Ateş Bonusu (max +5)"""
        league = self._get_league_stats(category_id)
        league_first_half_avg = league['avg_goals'] * 0.45
        
        # Takımların ilk yarı gol ortalamalarını tahmin et (toplam golün %40'ı kabul ediliyor)
        home_matches = self.db.get_team_form(home_team_key, last_n=10)
        away_matches = self.db.get_team_form(away_team_key, last_n=10)
        
        def avg_first_half(team_key, matches):
            total_goals = []
            for m in matches:
                if m['home_team'] == team_key:
                    total_goals.append(m['ft_home'] or 0)
                else:
                    total_goals.append(m['ft_away'] or 0)
            return (np.mean(total_goals) if total_goals else 0) * 0.4
        
        home_fh = avg_first_half(home_team_key, home_matches)
        away_fh = avg_first_half(away_team_key, away_matches)
        both_avg = (home_fh + away_fh) / 2.0
        
        if home_fh > league_first_half_avg and away_fh > league_first_half_avg:
            return 5
        elif both_avg > league_first_half_avg * 1.5:
            return 3
        return 0

    def calculate_second_half_reaction_bonus(self, home_team_key: str, away_team_key: str, category_id: int) -> int:
        """Adım 5 – İkinci Yarı Reaksiyonu Bonusu (max +5)"""
        league = self._get_league_stats(category_id)
        # Basitleştirilmiş: takımların genel KG eğilimine bak
        home_matches = self.db.get_team_form(home_team_key, last_n=10)
        away_matches = self.db.get_team_form(away_team_key, last_n=10)
        
        def btts_tendency(team_key, matches):
            # Takımın oynadığı maçlarda karşılıklı gol olma oranı
            btts = 0
            total = 0
            for m in matches:
                if m['home_team'] == team_key:
                    home_goal = m['ft_home'] or 0
                    away_goal = m['ft_away'] or 0
                else:
                    home_goal = m['ft_away'] or 0
                    away_goal = m['ft_home'] or 0
                if home_goal > 0 and away_goal > 0:
                    btts += 1
                total += 1
            return btts / total if total > 0 else 0
        
        home_tend = btts_tendency(home_team_key, home_matches)
        away_tend = btts_tendency(away_team_key, away_matches)
        avg_tend = (home_tend + away_tend) / 2.0
        
        if avg_tend > league['btts_ratio'] * 1.2:
            return 5
        elif avg_tend > league['btts_ratio'] * 1.1:
            return 2
        return 0

    def calculate_referee_penalty(self, referee_name: Optional[str], category_id: int) -> float:
        """Adım 6 – Hakem Etkisi (ceza çarpanı)"""
        if not referee_name:
            return 1.0
        league = self._get_league_stats(category_id)
        ref_avg = self.db.get_referee_avg_goals(referee_name)
        if ref_avg and ref_avg < league['avg_goals'] * 0.9:
            return 0.9
        return 1.0

    def predict_match(self, match: Dict) -> Dict:
        """Tek bir maç için tahmin yapar"""
        home_team = match['home_team']
        away_team = match['away_team']
        category_id = match['category_id']
        if not category_id:
            category_id = 0  # bilinmiyorsa varsayılan
        
        home_key = self._get_team_key(home_team, category_id)
        away_key = self._get_team_key(away_team, category_id)
        
        # Hesaplamalar
        form_score = self.calculate_form_score(home_key, away_key, category_id)
        pressure_score = self.calculate_attack_pressure(home_key, away_key, category_id)
        h2h_score = self.calculate_h2h_score(home_key, away_key, category_id)
        early_bonus = self.calculate_early_goal_bonus(home_key, away_key, category_id)
        second_bonus = self.calculate_second_half_reaction_bonus(home_key, away_key, category_id)
        referee_penalty = self.calculate_referee_penalty(match.get('referee'), category_id)
        
        raw_total = form_score + pressure_score + h2h_score + early_bonus + second_bonus
        net_total = raw_total * referee_penalty
        max_possible = 110.0  # 45+40+15+5+5
        model_prob = min(99.0, (net_total / max_possible) * 100)
        
        # KG için küçük düzeltme (isteğe bağlı)
        kg_prob = model_prob * 0.95
        
        # Value kontrolü
        over_odds = match.get('odds_o25')
        btts_odds = match.get('odds_btts_yes')
        
        result = {
            'match': f"{home_team} vs {away_team}",
            'date': match['start_utc'],
            'category_id': category_id,
            'model_over_prob': model_prob,
            'model_btts_prob': kg_prob,
            'over_odds': over_odds,
            'btts_odds': btts_odds,
            'form_score': form_score,
            'pressure_score': pressure_score,
            'h2h_score': h2h_score,
            'early_bonus': early_bonus,
            'second_bonus': second_bonus,
            'referee_penalty': referee_penalty,
            'net_total': net_total,
            'league_stats': self._get_league_stats(category_id)
        }
        
        # Value kararı
        if over_odds and over_odds > 0:
            book_prob_over = 100 / over_odds
            result['over_edge'] = model_prob - book_prob_over
            result['over_play'] = result['over_edge'] >= 5
        else:
            result['over_play'] = False
            result['over_edge'] = None
        
        if btts_odds and btts_odds > 0:
            book_prob_btts = 100 / btts_odds
            result['btts_edge'] = kg_prob - book_prob_btts
            result['btts_play'] = result['btts_edge'] >= 5
        else:
            result['btts_play'] = False
            result['btts_edge'] = None
        
        return result

# ==================== RAPOR OLUŞTURUCU ====================
def print_report(pred: Dict):
    print("=" * 50)
    print(f"🏆 {pred['match']} (Lig ID: {pred['category_id']}) - {pred['date']}")
    print("=" * 50)
    print(f"📊 Model Olasılık:")
    print(f"   - 2.5 Üst: %{pred['model_over_prob']:.1f}")
    print(f"   - KG Var: %{pred['model_btts_prob']:.1f}")
    print(f"\n💰 Value Karşılaştırması:")
    
    if pred['over_odds']:
        book_over = 100 / pred['over_odds']
        edge = pred['over_edge']
        status = "OYNA" if pred['over_play'] else "OYNAMA"
        print(f"   - Over 2.5: Şirket Oranı {pred['over_odds']:.2f} (%{book_over:.1f}) vs Model %{pred['model_over_prob']:.1f} → Edge %{edge:.1f} → {status}")
    else:
        print(f"   - Over 2.5: Oran mevcut değil")
    
    if pred['btts_odds']:
        book_btts = 100 / pred['btts_odds']
        edge = pred['btts_edge']
        status = "OYNA" if pred['btts_play'] else "OYNAMA"
        print(f"   - KG Var: Şirket Oranı {pred['btts_odds']:.2f} (%{book_btts:.1f}) vs Model %{pred['model_btts_prob']:.1f} → Edge %{edge:.1f} → {status}")
    else:
        print(f"   - KG Var: Oran mevcut değil")
    
    print(f"\n📈 Detaylı Puanlar:")
    print(f"   - Form Puanı: {pred['form_score']:.1f} / 45")
    print(f"   - Hücum Baskısı: {pred['pressure_score']:.1f} / 40")
    print(f"   - H2H Puanı: {pred['h2h_score']:.1f} / 15")
    print(f"   - Erken Ateş Bonusu: +{pred['early_bonus']}")
    print(f"   - İkinci Yarı Reaksiyonu: +{pred['second_bonus']}")
    print(f"   - Hakem Cezası: %{(1-pred['referee_penalty'])*100:.0f} kesinti")
    print(f"   - Toplam Net Puan: {pred['net_total']:.1f} / 110")
    
    ls = pred['league_stats']
    print(f"\n📌 Lig Normalizasyonu Kullanıldı: (category_id {pred['category_id']})")
    print(f"   - Lig ort. gol: {ls['avg_goals']:.2f}")
    print(f"   - Lig KG yüzdesi: %{ls['btts_ratio']*100:.1f}")
    print(f"   - Lig 2.5+ yüzdesi: %{ls['over25_ratio']*100:.1f}")
    print(f"\n🔍 Takım Kimliği Notu: Takımlar (isim + kategori ID) ile ayrıştırılmıştır.")
    print("")

def main():
    db = Database()
    db.connect()
    engine = PredictionEngine(db)
    
    upcoming = db.get_upcoming_matches()
    if not upcoming:
        print("Bugün veya yarın oynanacak major turnuva maçı bulunamadı.")
        return
    
    print(f"\n🔮 TAHMİN RAPORU - {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
    for match in upcoming:
        # Hakem bilgisini çek (opsiyonel)
        db.cursor.execute("SELECT referee FROM results_football WHERE event_id = %s", (match['event_id'],))
        ref_row = db.cursor.fetchone()
        match['referee'] = ref_row['referee'] if ref_row else None
        
        pred = engine.predict_match(match)
        print_report(pred)
    
    db.close()

if __name__ == "__main__":
    main()
