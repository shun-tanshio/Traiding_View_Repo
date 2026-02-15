# RSRだけ.py (改良版)
# 説明: 指定した基準日で「安定して1306.T（TOPIX連動ETF）に勝っている」スコアを計算し、ランキング表示する。
import sys
from typing import Optional, Tuple, List

import pandas as pd
import numpy as np
import exchange_calendars as xcals
from dateutil.relativedelta import relativedelta

# 固定
CSV_PATH = "prices_close_wide_world.csv"
CAL_NAME = "XTKS"

# ★指数の代替: TOPIXが取れないので1306.Tを使用
BENCH_TICKER = "1306.T"

# ★シミュレーションしたい日（YYYY-MM-DD でも YYYY/MM/DD でも可）
# None のときは「CSV最新日」で計算
# SIM_DATE: Optional[str] = None
SIM_DATE = "2026/02/03"

WEIGHTS = {
    "q1": 0.4,
    "q2": 0.2,
    "q3": 0.2,
    "y1": 0.2,
}

EPS = 1e-12  # 0除算回避（微調整して良い範囲）

def load_close_wide(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, index_col=0)

    cols_dt = pd.to_datetime(df.columns, errors="coerce")
    ok = ~cols_dt.isna()
    df = df.loc[:, ok].copy()

    df.columns = pd.to_datetime(df.columns, errors="coerce").normalize()
    df = df.reindex(sorted(df.columns), axis=1)

    return df

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
    """
    カレンダーで補正した営業日 day に対して、
    CSVにその日列が無い/全銘柄NaN などの場合に、
    CSV側で存在する「その日以前の直近日」に寄せる。
    """
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
    b0, b1y, bq1, bq2, bq3
) -> Optional[float]:
    """
    ③ 安定して指数(1306.T)に勝つスコア

    各期間hについて
      a_h = log(1 + R_stock_h) - log(1 + R_bench_h)
          = log(p0/p_h) - log(b0/b_h)

    μ = Σ w_h a_h
    σ = sqrt( Σ w_h (a_h - μ)^2 )
    score = μ / (σ + EPS)
    """
    vals = [p0, p1y, pq1, pq2, pq3, b0, b1y, bq1, bq2, bq3]
    if any(v is None for v in vals):
        return None

    # 価格が0以下はログが壊れるので除外
    if any(v <= 0 for v in [p0, p1y, pq1, pq2, pq3, b0, b1y, bq1, bq2, bq3]):
        return None

    # 相対ログ成長（4本）
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

def main():
    try:
        df = load_close_wide(CSV_PATH)
    except Exception as e:
        print("取得失敗")
        print(f"CSV読込エラー: {CSV_PATH} / {repr(e)}")
        sys.exit(1)

    if BENCH_TICKER not in df.index.astype(str):
        print(f"エラー: 指数ティッカー {BENCH_TICKER} がCSVに見つかりません。")
        sys.exit(1)

    cal = xcals.get_calendar(CAL_NAME)

    # ★基準日を決める（SIM_DATEがあればそれを採用）
    if SIM_DATE is None:
        base_day = find_latest_date_with_any_data(df)
        base_day = pd.Timestamp(base_day).normalize()
    else:
        # 1) 指定日を東証営業日に補正
        s = prev_or_same_session(cal, SIM_DATE)
        # 2) CSVに存在する日に寄せる（列が無い場合など）
        base_day = align_to_csv_available_date(df, s)

    # 暦でターゲット日を作る（ここが“その日に立った”シミュレーション）
    target_1y = (base_day.date() - relativedelta(years=1))
    target_q1 = (base_day.date() - relativedelta(months=3))
    target_q2 = (base_day.date() - relativedelta(months=6))
    target_q3 = (base_day.date() - relativedelta(months=9))

    # 営業日に補正（その日以前の直近営業日）
    s0  = prev_or_same_session(cal, base_day.strftime("%Y-%m-%d"))
    s1y = prev_or_same_session(cal, target_1y.strftime("%Y-%m-%d"))
    sq1 = prev_or_same_session(cal, target_q1.strftime("%Y-%m-%d"))
    sq2 = prev_or_same_session(cal, target_q2.strftime("%Y-%m-%d"))
    sq3 = prev_or_same_session(cal, target_q3.strftime("%Y-%m-%d"))

    # 参照日時の表示（見やすく）
    ref_dates = [
        s0.strftime("%Y/%m/%d"),
        s1y.strftime("%Y/%m/%d"),
        sq1.strftime("%Y/%m/%d"),
        sq2.strftime("%Y/%m/%d"),
        sq3.strftime("%Y/%m/%d"),
    ]

    # ★指数(1306.T)の終値を先に取得
    bench_row = df.loc[BENCH_TICKER].dropna()
    bench_closes = bench_row.copy()
    bench_closes.index = pd.to_datetime(bench_closes.index).normalize()
    bench_closes = bench_closes.sort_index().astype(float)

    b0  = pick_close_on_or_before(bench_closes, s0)
    b1y = pick_close_on_or_before(bench_closes, s1y)
    bq1 = pick_close_on_or_before(bench_closes, sq1)
    bq2 = pick_close_on_or_before(bench_closes, sq2)
    bq3 = pick_close_on_or_before(bench_closes, sq3)

    results: List[Tuple[str, float]] = []
    skipped: List[str] = []

    for ticker in df.index.astype(str).tolist():
        if ticker == BENCH_TICKER:
            continue

        row = df.loc[ticker].dropna()
        if row.empty:
            skipped.append(ticker)
            continue

        closes = row.copy()
        closes.index = pd.to_datetime(closes.index).normalize()
        closes = closes.sort_index().astype(float)

        p0  = pick_close_on_or_before(closes, s0)
        p1y = pick_close_on_or_before(closes, s1y)
        pq1 = pick_close_on_or_before(closes, sq1)
        pq2 = pick_close_on_or_before(closes, sq2)
        pq3 = pick_close_on_or_before(closes, sq3)

        score = stable_relative_score(
            p0, p1y, pq1, pq2, pq3,
            b0, b1y, bq1, bq2, bq3
        )
        if score is None:
            skipped.append(ticker)
            continue

        results.append((ticker, score))

    results.sort(key=lambda x: x[1], reverse=True)

    print("ーーーーー")
    print("参照日時：" + ",".join(ref_dates))
    print(f"基準日（シミュレーション日）: {base_day.strftime('%Y/%m/%d')}")
    print(f"指数（TOPIX代替）: {BENCH_TICKER}")
    print()

    for i, (t, v) in enumerate(results, 1):
        print(f"{i}位：{t} : {v:.6f}(stable_RSR)")

    print("ーーーーー")
    if skipped:
        print(f"※計算できず除外：{len(skipped)}銘柄（データ不足/価格<=0/ログ不可など）")

if __name__ == "__main__":
    main()