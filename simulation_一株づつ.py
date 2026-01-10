#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from pathlib import Path
import pandas as pd

# ========= 設定 =========
TOP45_PATH = Path("top45_codes_20241230.csv")
WIDE_PATH  = Path("prices_close_wide.csv")

PAST_DATE = pd.Timestamp("2024-12-30")

OUT_PATH = Path("top45_20241230_vs_now_1share_sim_sorted.txt")

# 並べ替えキー
#   "pct"        : 価格の利率（%）でソート
#   "profit_pct" : 1株の損益率（%）でソート（pctと同じ意味）
SORT_BY = "pct"
# ========= /設定 =========


def extract_codes_from_csv(path: Path) -> list[str]:
    """
    CSV内のどこに書かれていても 4桁コードを抽出する。
    例: 7203 / TSE:7203 / 7203.T などを想定。
    """
    df = pd.read_csv(path, header=None)
    vals = df.astype(str).values.ravel().tolist()

    codes: list[str] = []
    for v in vals:
        v = v.strip()
        if not v or v.lower() == "nan":
            continue
        m = re.search(r"(\d{4})", v)
        if m:
            codes.append(m.group(1))

    # 重複除去（順序維持）
    seen = set()
    uniq: list[str] = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq


def load_close_wide(path: Path) -> pd.DataFrame:
    """
    prices_close_wide.csv を読み込み、列を日付に正規化して昇順に整列。
    余計な列があれば日付に変換できる列だけ残す。
    """
    df = pd.read_csv(path, index_col=0)

    # 日付として解釈できる列だけ残す
    cols_dt = pd.to_datetime(df.columns, errors="coerce")
    ok = ~cols_dt.isna()
    df = df.loc[:, ok].copy()

    # 日付列に正規化
    df.columns = pd.to_datetime(df.columns, errors="coerce").normalize()
    df = df.reindex(sorted(df.columns), axis=1)
    return df


def resolve_row_key(df: pd.DataFrame, code: str) -> str | None:
    """
    インデックスが '7203.T' / 'TSE:7203' / '7203' など色々でも拾えるようにする。
    """
    idx = df.index.astype(str)
    idx_set = set(idx.tolist())

    candidates = [f"{code}.T", f"TSE:{code}", code]
    for k in candidates:
        if k in idx_set:
            return k

    # 念のため suffix でも探索
    for k in idx_set:
        if k.endswith(f"{code}.T") or k == f"TSE:{code}" or k.endswith(code):
            return k

    return None


def last_value_on_or_before(row: pd.Series, target: pd.Timestamp) -> tuple[float | None, pd.Timestamp | None]:
    """
    target日付以前で、値が入っている直近の値を返す
    """
    s = row.dropna()
    if s.empty:
        return None, None
    s = s[s.index <= target]
    if s.empty:
        return None, None
    return float(s.iloc[-1]), pd.Timestamp(s.index[-1])


def last_value(row: pd.Series) -> tuple[float | None, pd.Timestamp | None]:
    """
    最新日（rowの最後に値がある日）の値を返す
    """
    s = row.dropna()
    if s.empty:
        return None, None
    return float(s.iloc[-1]), pd.Timestamp(s.index[-1])


def main() -> None:
    codes = extract_codes_from_csv(TOP45_PATH)
    if not codes:
        raise SystemExit("top45_codes_20241230.csv から4桁コードを抽出できませんでした。")

    df = load_close_wide(WIDE_PATH)

    ok_rows = []  # 計算できた銘柄
    ng_rows = []  # エラー/データ不足

    for pos, code in enumerate(codes, start=1):
        ticker = f"{code}.T"

        key = resolve_row_key(df, code)
        if key is None:
            ng_rows.append((pos, ticker, "prices_close_wideに行が見つかりません"))
            continue

        row = df.loc[key]
        past, past_used = last_value_on_or_before(row, PAST_DATE)
        now, now_used = last_value(row)

        if past is None:
            ng_rows.append((pos, ticker, f"過去データ不足: {PAST_DATE.date()}以前が空"))
            continue
        if now is None:
            ng_rows.append((pos, ticker, "最新データ不足: 行が空"))
            continue
        if past == 0:
            ng_rows.append((pos, ticker, "過去値が0のため利率計算不可"))
            continue

        diff = now - past
        pct = (diff / past) * 100.0

        # ★ 1株ずつ固定
        shares = 1.0
        cost = past
        value_now = now
        profit = diff
        profit_pct = pct

        ok_rows.append({
            "pos": pos,                 # 元CSVで何番目か
            "ticker": ticker,
            "past": past,
            "now": now,
            "diff": diff,
            "pct": pct,
            "shares": shares,
            "cost": cost,
            "value_now": value_now,
            "profit": profit,
            "profit_pct": profit_pct,
            "past_used": past_used,
            "now_used": now_used,
        })

    # ソート
    if SORT_BY == "profit_pct":
        ok_rows.sort(key=lambda x: x["profit_pct"], reverse=True)
    else:
        ok_rows.sort(key=lambda x: x["pct"], reverse=True)

    # 合計（各銘柄1株ずつ買った合計）
    total_cost = sum(r["cost"] for r in ok_rows)               # 投資元本（購入額合計）
    total_value_now = sum(r["value_now"] for r in ok_rows)     # 現在評価額合計
    total_profit = total_value_now - total_cost
    total_profit_pct = (total_profit / total_cost) * 100.0 if total_cost else float("nan")

    lines = []
    lines.append("ーーーー")
    lines.append(f"条件: 各銘柄1株ずつ購入 / SORT_BY={SORT_BY}")
    lines.append("")

    for r in ok_rows:
        lines.append(
            f"{r['pos']:02d} | {r['ticker']} : "
            f"{r['past']:,.2f}, {r['now']:,.2f}, {r['diff']:,.2f}, {r['pct']:,.2f}%"
            f" | 株数={r['shares']:.0f}, 購入={r['cost']:,.2f}, "
            f"現在評価額={r['value_now']:,.2f}, 損益={r['profit']:,.2f}, 損益率={r['profit_pct']:,.2f}%"
        )

    if ng_rows:
        lines.append("")
        lines.append("---- データ不足/未取得 ----")
        for pos, ticker, reason in ng_rows:
            lines.append(f"{pos:02d} | {ticker} : ({reason})")

    lines.append("")
    lines.append(f"合計（購入額） : {total_cost:,.2f} 円")
    lines.append(f"合計（現在評価額） : {total_value_now:,.2f} 円")
    lines.append(f"合計（損益） : {total_profit:,.2f} 円")
    lines.append(f"合計（損益率） : {total_profit_pct:,.2f}%  (対象={len(ok_rows)}銘柄)")
    lines.append("ーーーー")

    text = "\n".join(lines)
    print(text)

    # ファイルにも保存
    OUT_PATH.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()