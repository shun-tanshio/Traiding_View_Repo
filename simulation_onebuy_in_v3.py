#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
import argparse
from pathlib import Path
import pandas as pd

# ========= デフォルト設定 =========
DEFAULT_TOP_PATH = Path("top45_codes_20241230.txt")   # 参照する銘柄リスト（txt / csv）
DEFAULT_WIDE_PATH = Path("prices_close_wide.csv")     # 終値ワイドCSV
DEFAULT_SORT_BY = "pct"                               # pct / profit_pct
# ========= /デフォルト設定 =========


def _normalize_date_arg(s: str) -> pd.Timestamp:
    """YYYY_MM_DD / YYYY-MM-DD / YYYY/MM/DD → Timestamp"""
    s = str(s).strip().replace("/", "-").replace("_", "-")
    try:
        ts = pd.Timestamp(s).normalize()
    except Exception as e:
        raise ValueError(f"日付形式が不正です: {s}") from e
    if pd.isna(ts):
        raise ValueError(f"日付形式が不正です: {s}")
    return ts


def _parse_args(argv):
    p = argparse.ArgumentParser(
        prog=Path(argv[0]).name,
        description="銘柄リスト（txt/csv）を元に、各銘柄1株ずつの売買シミュレーションを行います。",
    )
    p.add_argument("buy_date", help="買った日（YYYY_MM_DD / YYYY-MM-DD / YYYY/MM/DD）")
    p.add_argument("sell_date", help="売る日（YYYY_MM_DD / YYYY-MM-DD / YYYY/MM/DD）")
    p.add_argument("--top", default=str(DEFAULT_TOP_PATH), help="参照する銘柄リスト（txt/csv）")
    p.add_argument("--wide", default=str(DEFAULT_WIDE_PATH), help="終値ワイドCSV")
    p.add_argument("--sort", default=DEFAULT_SORT_BY, choices=["pct", "profit_pct"], help="ソートキー")
    return p.parse_args(argv[1:])


def extract_codes_from_any_text(path: Path) -> list[str]:
    """txt / csv から4桁コードを抽出（TradingView形式もOK）"""
    try:
        df = pd.read_csv(path, header=None)
        vals = df.astype(str).values.ravel().tolist()
    except Exception:
        text = path.read_text(encoding="utf-8", errors="ignore")
        vals = re.split(r"[\s,]+", text)

    codes = []
    for v in vals:
        m = re.search(r"(\d{4})", str(v))
        if m:
            codes.append(m.group(1))

    # 重複除去（順序維持）
    seen, uniq = set(), []
    for c in codes:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq


def load_close_wide(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, index_col=0)
    cols_dt = pd.to_datetime(df.columns, errors="coerce")
    df = df.loc[:, ~cols_dt.isna()].copy()
    df.columns = pd.to_datetime(df.columns).normalize()
    return df.reindex(sorted(df.columns), axis=1)


def resolve_row_key(df: pd.DataFrame, code: str) -> str | None:
    idx = df.index.astype(str).tolist()
    for k in (f"{code}.T", f"TSE:{code}", code):
        if k in idx:
            return k
    for k in idx:
        if k.endswith(code):
            return k
    return None


def last_value_on_or_before(row: pd.Series, target: pd.Timestamp):
    s = row.dropna()
    s = s[s.index <= target]
    if s.empty:
        return None, None
    return float(s.iloc[-1]), pd.Timestamp(s.index[-1])


def main(argv=None) -> None:
    argv = sys.argv if argv is None else argv
    args = _parse_args(argv)

    buy_date = _normalize_date_arg(args.buy_date)
    sell_date = _normalize_date_arg(args.sell_date)
    if sell_date < buy_date:
        raise SystemExit("売る日が買う日より前です")

    codes = extract_codes_from_any_text(Path(args.top))
    if not codes:
        raise SystemExit("銘柄コードを抽出できませんでした")

    df = load_close_wide(Path(args.wide))

    ok_rows, ng_rows = [], []

    for pos, code in enumerate(codes, start=1):
        ticker = f"{code}.T"
        key = resolve_row_key(df, code)
        if key is None:
            ng_rows.append((pos, ticker, "行が見つかりません"))
            continue

        row = df.loc[key]
        buy, buy_used = last_value_on_or_before(row, buy_date)
        sell, sell_used = last_value_on_or_before(row, sell_date)

        if buy is None or sell is None or buy == 0:
            ng_rows.append((pos, ticker, "データ不足"))
            continue

        diff = sell - buy
        pct = diff / buy * 100.0

        ok_rows.append({
            "pos": pos,
            "ticker": ticker,
            "buy": buy,
            "sell": sell,
            "diff": diff,
            "pct": pct,
            "buy_used": buy_used,
            "sell_used": sell_used,
        })

    ok_rows.sort(key=lambda x: x["pct"], reverse=True)

    total_buy = sum(r["buy"] for r in ok_rows)
    total_sell = sum(r["sell"] for r in ok_rows)
    total_profit = total_sell - total_buy
    total_pct = total_profit / total_buy * 100 if total_buy else float("nan")

    print("ーーーー")
    print(f"条件: 各銘柄1株 / BUY={buy_date.date()} / SELL={sell_date.date()} / LIST={args.top}")
    print("")

    for r in ok_rows:
        print(
            f"{r['pos']:02d} | {r['ticker']} : "
            f"{r['buy']:,.2f}, {r['sell']:,.2f}, {r['diff']:,.2f}, {r['pct']:,.2f}% "
            f"(buy_used={r['buy_used'].date()}, sell_used={r['sell_used'].date()})"
        )

    if ng_rows:
        print("")
        print("---- データ不足/未取得 ----")
        for pos, ticker, reason in ng_rows:
            print(f"{pos:02d} | {ticker} : ({reason})")

    print("")
    print(f"合計（購入額） : {total_buy:,.2f} 円")
    print(f"合計（売却評価額） : {total_sell:,.2f} 円")
    print(f"合計（損益） : {total_profit:,.2f} 円")
    print(f"合計（損益率） : {total_pct:,.2f}%  (対象={len(ok_rows)}銘柄)")
    print("ーーーー")


if __name__ == "__main__":
    main()