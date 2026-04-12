#!/usr/bin/env python3
# en_iyi_tahminleri_kaydet.py
import mysql.connector
import os

CONFIG = {
    "db": {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306))
    }
}

def get_connection():
    return mysql.connector.connect(**CONFIG["db"])

def ensure_columns():
    conn = get_connection()
    cursor = conn.cursor()
    cols = [
        ("best_market_raw", "VARCHAR(32) NULL"),
        ("best_market_tr", "VARCHAR(64) NULL"),
        ("best_prob", "FLOAT NULL"),
        ("best_odds", "FLOAT NULL"),
        ("best_value", "FLOAT NULL")
    ]
    for col, typ in cols:
        try:
            cursor.execute(f"ALTER TABLE match_predictions ADD COLUMN {col} {typ}")
        except:
            pass
    conn.close()

def select_best_bets():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT mp.event_id,
               mp.prob_ms1, mp.prob_ms0, mp.prob_ms2,
               mp.prob_o15, mp.prob_o35,
               r.odds_1x, r.odds_12, r.odds_x2,
               r.odds_o15, r.odds_u35
        FROM match_predictions mp
        LEFT JOIN results_football r ON mp.event_id = r.event_id
        WHERE mp.best_market_raw IS NULL
    """)
    rows = cursor.fetchall()
    
    update_sql = """
        UPDATE match_predictions
        SET best_market_raw = %s, best_market_tr = %s,
            best_prob = %s, best_odds = %s, best_value = %s
        WHERE event_id = %s
    """
    count = 0
    for row in rows:
        prob_1x = (row['prob_ms1'] + row['prob_ms0']) / 100.0
        prob_12 = (row['prob_ms1'] + row['prob_ms2']) / 100.0
        prob_x2 = (row['prob_ms0'] + row['prob_ms2']) / 100.0
        prob_o15 = row['prob_o15'] / 100.0
        prob_u35 = (100.0 - row['prob_o35']) / 100.0
        
        markets = {
            '1X': {'prob': prob_1x, 'odds': row['odds_1x'], 'tr': '1x (çifte şans)'},
            '12': {'prob': prob_12, 'odds': row['odds_12'], 'tr': '12 (çifte şans)'},
            'X2': {'prob': prob_x2, 'odds': row['odds_x2'], 'tr': 'x2 (çifte şans)'},
            'O15': {'prob': prob_o15, 'odds': row['odds_o15'], 'tr': '1.5 üst'},
            'U35': {'prob': prob_u35, 'odds': row['odds_u35'], 'tr': '3.5 alt'}
        }
        
        best = None
        best_value = -999
        for key, m in markets.items():
            if m['odds'] is None or m['prob'] is None:
                continue
            odds = float(m['odds'])
            prob = float(m['prob'])
            value = (prob * odds) - 1.0
            
            # YENİ FİLTRELER
            if value > 0.05 and prob > 0.55 and odds >= 1.60:
                if value > best_value:
                    best_value = value
                    best = {
                        'raw': key,
                        'tr': m['tr'],
                        'prob': round(prob * 100, 1),
                        'odds': odds,
                        'value': round(value, 3)
                    }
        
        if best:
            cursor.execute(update_sql, (best['raw'], best['tr'], best['prob'], best['odds'], best['value'], row['event_id']))
            count += 1
        else:
            cursor.execute(update_sql, ('NO_BET', 'bahis yok', 0, 0, 0, row['event_id']))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"{count} maça en iyi bahis atandı (filtre: value>0.05, prob>%55, odds≥1.60).")

def main():
    ensure_columns()
    select_best_bets()

if __name__ == "__main__":
    main()
