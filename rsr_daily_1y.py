#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from typing import Optional

import pandas as pd
import matplotlib.pyplot as plt
import exchange_calendars as xcals
from dateutil.relativedelta import relativedelta

CAL_NAME = "XTKS"
BENCH = "^N225"

WEIGHTS = {"q1": 0.4, "q2": 0.2, "q3": 0.2, "y1": 0.2}


def load_close_wide(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, index_col=0)
    cols_dt = pd.to_datetime(df.columns, errors="coerce")
    df = df.loc[:, ~cols_dt.isna()]
    df.columns = pd.to_datetime(df.columns).normalize()
    return df.reindex(sorted(df.columns), axis=1)


def pick_close_on_or_before(closes: pd.Series, day: pd.Timestamp) -> Optional[float]:
    key = pd.Timestamp(day.date())
    if key in closes.index:
        return float(closes.loc[key])
    idx = closes.index[closes.index <= key]
    return float(closes.loc[idx[-1]]) if len(idx) else None


def rsr_at_day(closes: pd.Series, s0: pd.Timestamp) -> Optional[float]:
    p0  = pick_close_on_or_before(closes, s0)
    p1y = pick_close_on_or_before(closes, s0 - relativedelta(years=1))
    pq1 = pick_close_on_or_before(closes, s0 - relativedelta(months=3))
    pq2 = pick_close_on_or_before(closes, s0 - relativedelta(months=6))
    pq3 = pick_close_on_or_before(closes, s0 - relativedelta(months=9))

    vals = [p0, p1y, pq1, pq2, pq3]
    if any(v in (None, 0) for v in vals):
        return None

    return float((
        ((p0 - pq1) / pq1) * WEIGHTS["q1"]
        + ((p0 - pq2) / pq2) * WEIGHTS["q2"]
        + ((p0 - pq3) / pq3) * WEIGHTS["q3"]
        + ((p0 - p1y) / p1y) * WEIGHTS["y1"]
    ) * 100)


def calc_daily_rsr_1y(df: pd.DataFrame, ticker: str) -> pd.Series:
    closes = df.loc[ticker].dropna()
    closes.index = pd.to_datetime(closes.index).normalize()

    cal = xcals.get_calendar(CAL_NAME)
    end_day = closes.index.max()
    start_day = end_day - relativedelta(years=1)

    sessions = cal.sessions_in_range(start_day, end_day)

    values = []
    for s in sessions:
        values.append(rsr_at_day(closes, pd.Timestamp(s).normalize()))

    return pd.Series(values, index=pd.to_datetime(sessions), name=ticker).dropna()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker", help="例: 7203.T")
    ap.add_argument("--csv", default="prices_close_wide.csv")
    args = ap.parse_args()

    df = load_close_wide(args.csv)

    for t in (args.ticker, BENCH):
        if t not in df.index:
            raise SystemExit(f"ticker not found in CSV: {t}")

    s_stock = calc_daily_rsr_1y(df, args.ticker)
    s_bench = calc_daily_rsr_1y(df, BENCH)

    plt.figure(figsize=(10, 4))
    plt.plot(s_stock.index, s_stock.values, label=args.ticker)
    plt.plot(s_bench.index, s_bench.values, color="red", label=BENCH)

    plt.axhline(0, color="gray", linewidth=0.8)
    plt.title(f"Daily RSR (last 1Y)")
    plt.xlabel("Date")
    plt.ylabel("RSR Score")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()