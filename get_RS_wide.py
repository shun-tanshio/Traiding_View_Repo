#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""get_RS.py

目的
  終値ワイドCSV（例: prices_close_wide_world.csv）を参照して、
  各銘柄のRSスコア（stable_RSR）を 1日分（指定日）だけ計算し、
  **終値ワイドCSVと同じ形（行=銘柄, 列=日付）** のRSスコアCSVへ書き込み保存します。

  ＝「アップロードしたCSVの終値のところをスコアに置き換える」イメージです。
  （既存のRSスコアCSVがあれば、同じ日付列を上書き/追加して“蓄積”できます）

元にしたロジック
  /mnt/data/RSR（日経比）だけ.py の下記を踏襲:
    - 東証カレンダーでの営業日補正
    - CSVに存在する直近日へ寄せる処理
    - stable_relative_score の式（μ/σ）と WEIGHTS/EPS

出力（デフォルト）
  rs_scores_wide.csv
  - index: ticker
  - columns: 入力CSVと同じ日付列（文字列）
  - 値: rs_score（指定日列だけ埋まる。既存ファイルがあれば過去列も保持）

使い方
  # CSV最新日で計算（SIM_DATEなし）
  python get_RS.py

  # 指定日で計算（休日でもOK。直近営業日＋CSV直近日へ補正）
  python get_RS.py --sim-date 2026/02/01

  # 入力/出力/指数を明示
  python get_RS.py --input prices_close_wide_world.csv --bench 1306.T --out rs_scores_wide.csv

注意
  - BENCH_TICKER は入力CSVのindex（行）に存在する必要があります。
  - 価格<=0、データ不足の銘柄は NaN（未計算）になります。
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import exchange_calendars as xcals
from dateutil.relativedelta import relativedelta

# デフォルト（必要ならCLIで上書き）
DEFAULT_INPUT = "prices_close_wide_world.csv"
DEFAULT_CAL = "XTKS"
DEFAULT_BENCH = "1306.T"  # TOPIX代替ETF
DEFAULT_OUT = "rs_scores_wide.csv"

WEIGHTS = {
    "q1": 0.4,
    "q2": 0.2,
    "q3": 0.2,
    "y1": 0.2,
}

EPS = 1e-12  # 0除算回避


def load_close_wide_with_colmap(csv_path: str) -> Tuple[pd.DataFrame, List[str], Dict[pd.Timestamp, str]]:
    """入力CSVを読み込み、計算用に列をdatetimeへ正規化したDFと、
    “元の列名(文字列)”を保持するための情報を返す。

    Returns
      df_dt: columnsがdatetime(normalize)になった終値ワイド
      col_labels: 入力CSVの“日付列（文字列）”のみのリスト（順序維持）
      dt_to_label: datetime(normalize) -> 元の文字列列名
    """
    df_raw = pd.read_csv(csv_path, index_col=0)

    # 入力の列名（文字列）を保持
    raw_cols = list(df_raw.columns)

    cols_dt = pd.to_datetime(raw_cols, errors="coerce")
    ok = ~pd.isna(cols_dt)

    # 日付列だけ残す
    df = df_raw.loc[:, ok].copy()

    # datetime化・正規化
    cols_dt_ok = pd.to_datetime(df.columns, errors="coerce").normalize()

    # 元文字列との対応表（同一日が複数列に出ない想定）
    dt_to_label = {pd.Timestamp(dt).normalize(): lbl for dt, lbl in zip(cols_dt_ok, df.columns)}

    df.columns = cols_dt_ok
    df = df.reindex(sorted(df.columns), axis=1)

    # “入力CSVと同じ列順（文字列）”を作る（df.columnsの昇順に合わせて整列）
    col_labels = [dt_to_label[pd.Timestamp(c).normalize()] for c in df.columns]

    # indexを文字列へ寄せる（以降も一貫）
    df.index = df.index.astype(str)

    return df, col_labels, dt_to_label


def find_latest_date_with_any_data(df: pd.DataFrame) -> pd.Timestamp:
    for c in reversed(df.columns.tolist()):
        if df[c].notna().any():
            return pd.Timestamp(c)
    raise ValueError("CSV内に有効な日付データが見つかりません。")


def prev_or_same_session(cal, ymd: str) -> pd.Timestamp:
    ts = pd.Timestamp(ymd)
    start = ts - pd.Timedelta(days=40)
    end = ts
    sessions = cal.sessions_in_range(start, end)
    if len(sessions) == 0:
        raise ValueError("指定日以前の営業日が見つかりません。")
    return sessions[-1]


def align_to_csv_available_date(df: pd.DataFrame, day: pd.Timestamp) -> pd.Timestamp:
    """カレンダーで補正した営業日 day に対して、CSV側の存在日に寄せる。"""
    day = pd.Timestamp(day).normalize()

    cols = pd.Index(df.columns)
    candidates = cols[cols <= day]
    if len(candidates) == 0:
        raise ValueError("CSVに指定日以前のデータがありません。")
    return pd.Timestamp(candidates.max()).normalize()


def pick_close_on_or_before(closes: pd.Series, session_day: pd.Timestamp) -> Optional[float]:
    if closes is None or closes.empty:
        return None

    key = pd.Timestamp(session_day.date())
    if key in closes.index:
        v = closes.loc[key]
        if isinstance(v, pd.Series):
            v = v.iloc[0]
        return float(v)

    idx = closes.index[closes.index <= key]
    if len(idx) == 0:
        return None
    v2 = closes.loc[idx[-1]]
    if isinstance(v2, pd.Series):
        v2 = v2.iloc[0]
    return float(v2)


def stable_relative_score(
    p0, p1y, pq1, pq2, pq3,
    b0, b1y, bq1, bq2, bq3,
) -> Optional[float]:
    """安定して指数に勝つスコア: score = μ / (σ + EPS)

    各期間hについて
      a_h = log(p0/p_h) - log(b0/b_h)

    μ = Σ w_h a_h
    σ = sqrt( Σ w_h (a_h - μ)^2 )
    """
    vals = [p0, p1y, pq1, pq2, pq3, b0, b1y, bq1, bq2, bq3]
    if any(v is None for v in vals):
        return None

    # 価格が0以下はログが壊れるので除外
    if any(v <= 0 for v in [p0, p1y, pq1, pq2, pq3, b0, b1y, bq1, bq2, bq3]):
        return None

    a_q1 = np.log(p0 / pq1) - np.log(b0 / bq1)
    a_q2 = np.log(p0 / pq2) - np.log(b0 / bq2)
    a_q3 = np.log(p0 / pq3) - np.log(b0 / bq3)
    a_y1 = np.log(p0 / p1y) - np.log(b0 / b1y)

    a = np.array([a_q1, a_q2, a_q3, a_y1], dtype=float)
    w = np.array([WEIGHTS["q1"], WEIGHTS["q2"], WEIGHTS["q3"], WEIGHTS["y1"]], dtype=float)

    mu = float(np.sum(w * a))
    var = float(np.sum(w * (a - mu) ** 2))
    sigma = float(np.sqrt(max(var, 0.0)))

    return mu / (sigma + EPS)


def compute_base_day(df: pd.DataFrame, cal, sim_date: Optional[str]) -> pd.Timestamp:
    if sim_date is None:
        base_day = find_latest_date_with_any_data(df)
        return pd.Timestamp(base_day).normalize()

    s = prev_or_same_session(cal, sim_date)
    return align_to_csv_available_date(df, s)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=DEFAULT_INPUT, help="終値ワイドCSV")
    ap.add_argument("--bench", default=DEFAULT_BENCH, help="指数ティッカー（入力CSVのindexに必要）")
    ap.add_argument("--sim-date", default=None, help="基準日（YYYY-MM-DD or YYYY/MM/DD）。未指定ならCSV最新日")
    ap.add_argument("--calendar", default=DEFAULT_CAL, help="取引所カレンダー名（デフォルトXTKS）")
    ap.add_argument("--out", default=DEFAULT_OUT, help="出力RSスコアCSV（ワイド）")
    args = ap.parse_args()

    # 1) load
    try:
        df, col_labels, dt_to_label = load_close_wide_with_colmap(args.input)
    except Exception as e:
        print("取得失敗")
        print(f"CSV読込エラー: {args.input} / {repr(e)}")
        raise SystemExit(1)

    bench = str(args.bench)
    if bench not in df.index:
        print(f"エラー: 指数ティッカー {bench} が入力CSVに見つかりません。")
        raise SystemExit(1)

    # 2) base day
    cal = xcals.get_calendar(args.calendar)
    base_day = compute_base_day(df, cal, args.sim_date)

    # base_day を「入力CSVの元列名（文字列）」へ戻す
    if base_day not in dt_to_label:
        # 理屈上は align_to_csv_available_date で必ず入るはずだが保険
        base_label = base_day.strftime("%Y-%m-%d")
    else:
        base_label = dt_to_label[base_day]

    # 3) target dates（基準日からの相対）
    target_1y = (base_day.date() - relativedelta(years=1))
    target_q1 = (base_day.date() - relativedelta(months=3))
    target_q2 = (base_day.date() - relativedelta(months=6))
    target_q3 = (base_day.date() - relativedelta(months=9))

    s0 = prev_or_same_session(cal, base_day.strftime("%Y-%m-%d"))
    s1y = prev_or_same_session(cal, target_1y.strftime("%Y-%m-%d"))
    sq1 = prev_or_same_session(cal, target_q1.strftime("%Y-%m-%d"))
    sq2 = prev_or_same_session(cal, target_q2.strftime("%Y-%m-%d"))
    sq3 = prev_or_same_session(cal, target_q3.strftime("%Y-%m-%d"))

    # 4) bench closes
    bench_row = df.loc[bench].dropna()
    bench_closes = bench_row.copy()
    bench_closes.index = pd.to_datetime(bench_closes.index).normalize()
    bench_closes = bench_closes.sort_index().astype(float)

    b0 = pick_close_on_or_before(bench_closes, s0)
    b1y = pick_close_on_or_before(bench_closes, s1y)
    bq1 = pick_close_on_or_before(bench_closes, sq1)
    bq2 = pick_close_on_or_before(bench_closes, sq2)
    bq3 = pick_close_on_or_before(bench_closes, sq3)

    if any(v is None for v in [b0, b1y, bq1, bq2, bq3]):
        print("エラー: 指数（bench）の参照終値が揃いません（データ不足の可能性）。")
        print(f"bench={bench} base_day={base_day.strftime('%Y-%m-%d')}")
        raise SystemExit(1)

    # 5) per ticker score
    scores: Dict[str, float] = {}
    skipped: List[str] = []

    for ticker in df.index.tolist():
        if ticker == bench:
            continue

        row = df.loc[ticker].dropna()
        if row.empty:
            skipped.append(ticker)
            continue

        closes = row.copy()
        closes.index = pd.to_datetime(closes.index).normalize()
        closes = closes.sort_index().astype(float)

        p0 = pick_close_on_or_before(closes, s0)
        p1y = pick_close_on_or_before(closes, s1y)
        pq1 = pick_close_on_or_before(closes, sq1)
        pq2 = pick_close_on_or_before(closes, sq2)
        pq3 = pick_close_on_or_before(closes, sq3)

        score = stable_relative_score(p0, p1y, pq1, pq2, pq3, b0, b1y, bq1, bq2, bq3)
        if score is None:
            skipped.append(ticker)
            continue

        scores[ticker] = float(score)

    # 6) load/create wide RS score file (same shape as input)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True) if str(out_path.parent) not in [".", ""] else None

    if out_path.exists():
        rs_wide = pd.read_csv(out_path, index_col=0)
        rs_wide.index = rs_wide.index.astype(str)
    else:
        rs_wide = pd.DataFrame(index=df.index.astype(str), columns=col_labels, dtype=float)

    # 入力CSVの銘柄・日付列へ揃える（蓄積ファイルでも常に同一形に寄せる）
    rs_wide = rs_wide.reindex(index=df.index.astype(str), columns=col_labels)

    # base_label 列に書き込み
    if base_label not in rs_wide.columns:
        # 念のため（基本は入っている）
        rs_wide[base_label] = np.nan

    if scores:
        s = pd.Series(scores, name=base_label, dtype=float)
        rs_wide.loc[s.index, base_label] = s

    # 保存
    rs_wide.to_csv(out_path, encoding="utf-8")

    print("ーーーーー")
    print(f"保存しました: {out_path}")
    print(f"基準日（CSV列）: {base_label}  / base_day={base_day.strftime('%Y-%m-%d')}")
    print(f"指数（bench）: {bench}")
    print(f"参照日: s0={s0:%Y/%m/%d}, 1y={s1y:%Y/%m/%d}, q1={sq1:%Y/%m/%d}, q2={sq2:%Y/%m/%d}, q3={sq3:%Y/%m/%d}")
    print(f"計算成功: {len(scores)}銘柄 / 除外: {len(skipped)}銘柄")
    print("ーーーーー")


if __name__ == "__main__":
    main()
