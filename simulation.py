#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from pathlib import Path
import pandas as pd

# 入力ファイル
TOP45_PATH = Path("top45_codes_20241230.csv")
WIDE_PATH  = Path("prices_close_wide.csv")

# 比較したい日付
PAST_DATE = pd.Timestamp("2024-12-30")

# 出力ファイル（不要ならコメントアウトOK）
OUT_PATH = Path("top45_20241230_vs_now_from_prices_close_wide_sorted.txt")


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

    cols_dt = pd.to_datetime(df.columns, errors="coerce")
    ok = ~cols_dt.isna()
    df = df.loc[:, ok].copy()

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

    ok_rows = []      # 計算できた銘柄
    ng_rows = []      # エラー/データ不足

    # 元CSVでの順番を持たせる（1始まり）
    for pos, code in enumerate(codes, start=1):
        ticker = f"{code}.T"

        key = resolve_row_key(df, code)
        if key is None:
            ng_rows.append((pos, ticker, "prices_close_wideに行が見つかりません"))
            continue

        row = df.loc[key]

        past_val, _past_used = last_value_on_or_before(row, PAST_DATE)
        now_val, _now_used = last_value(row)

        if past_val is None:
            ng_rows.append((pos, ticker, f"過去データ不足: {PAST_DATE.date()}以前が空"))
            continue
        if now_val is None:
            ng_rows.append((pos, ticker, "最新データ不足: 行が空"))
            continue
        if past_val == 0:
            ng_rows.append((pos, ticker, "過去値が0のため利率計算不可"))
            continue

        diff = now_val - past_val
        pct = (diff / past_val) * 100.0

        ok_rows.append({
            "pos": pos,            # 元CSVで何番目か
            "ticker": ticker,
            "past": past_val,
            "now": now_val,
            "diff": diff,
            "pct": pct
        })

    # 利率（%）の降順でソート
    ok_rows.sort(key=lambda x: x["pct"], reverse=True)

    # 合計（計算できた銘柄のみ）
    sum_past = sum(r["past"] for r in ok_rows)
    sum_now  = sum(r["now"]  for r in ok_rows)
    sum_diff = sum(r["diff"] for r in ok_rows)
    total_pct = (sum_diff / sum_past) * 100.0 if sum_past != 0 else float("nan")

    lines = []
    lines.append("ーーーー")
    for r in ok_rows:
        # 先頭に「元CSV順」を入れる
        lines.append(f"{r['pos']:02d} | {r['ticker']} : {r['past']:,.2f}, {r['now']:,.2f}, {r['diff']:,.2f}, {r['pct']:,.2f}%")

    if ng_rows:
        lines.append("")
        lines.append("---- データ不足/未取得 ----")
        for pos, ticker, reason in ng_rows:
            lines.append(f"{pos:02d} | {ticker} : ({reason})")

    lines.append("")
    lines.append(f"合計 : {sum_past:,.2f}, {sum_now:,.2f}, {sum_diff:,.2f}, {total_pct:,.2f}%  (対象={len(ok_rows)}銘柄)")
    lines.append("ーーーー")

    text = "\n".join(lines)
    print(text)

    OUT_PATH.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()