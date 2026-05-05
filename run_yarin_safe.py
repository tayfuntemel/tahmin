#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Yarın için safe market tahmini üretir."""
import datetime as dt
from zoneinfo import ZoneInfo
from safe_market_engine import run_single

if __name__ == "__main__":
    run_single(dt.datetime.now(ZoneInfo("Europe/Istanbul")).date() + dt.timedelta(days=1))
