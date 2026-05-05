#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Safe market motoru için kısa backtest çalıştırıcı."""
import datetime as dt
import sys
from safe_market_engine import run_backtest

if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Kullanım: python3 backtest_safe_market.py YYYY-MM-DD YYYY-MM-DD")
    run_backtest(dt.date.fromisoformat(sys.argv[1]), dt.date.fromisoformat(sys.argv[2]))
