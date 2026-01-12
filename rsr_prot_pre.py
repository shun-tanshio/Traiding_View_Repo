# 説明: 指定開始日から指定期間（HORIZON_MONTHS）先の利益率とRSRを比較して散布図を作成するスクリプト。
# 入力方法: `python rsr_prot_pre.py`（内部定数 `START_DATE`, `HORIZON_MONTHS`, `CSV_PATH` を使用）。
# 出力されるモノ: RSRスコアと将来利益率の散布図をmatplotlibで表示（標準出力は主にエラーや情報）。

from typing import List, Tuple, Optional
import pandas as pd
import exchange_calendars as xcals
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt

# ===== 入力パラメータ =====
START_DATE = "2022-01-06"   # ← はじまり日
HORIZON_MONTHS = 12         # ← 比較間隔（月） 例: 3, 6, 12

CSV_PATH = "prices_close_wide.csv"
CAL_NAME = "XTKS"

WEIGHTS = {
    "q1": 0.4,
    "q2": 0.2,
    "q3": 0.2,
    "y1": 0.2,
}

# ===== CSV =====
def load_close_wide(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, index_col=0)
    cols_dt = pd.to_datetime(df.columns, errors="coerce")
    df = df.loc[:, ~cols_dt.isna()]
    df.columns = pd.to_datetime(df.columns).normalize()
    return df.reindex(sorted(df.columns), axis=1)

# ===== カレンダー =====
def prev_or_same_session(cal, ymd: str) -> pd.Timestamp:
    ts = pd.Timestamp(ymd)
    sessions = cal.sessions_in_range(ts - pd.Timedelta(days=40), ts)
    return sessions[-1]

def align_to_csv_available_date(df: pd.DataFrame, day: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(df.columns[df.columns <= day].max()).normalize()

# ===== 価格取得 =====
def pick_close_on_or_before(closes: pd.Series, day: pd.Timestamp) -> Optional[float]:
    key = pd.Timestamp(day.date())
    if key in closes.index:
        return float(closes.loc[key])
    idx = closes.index[closes.index <= key]
    return float(closes.loc[idx[-1]]) if len(idx) else None

# ===== RSR =====
def safe_detect_number(p0, p1y, pq1, pq2, pq3) -> Optional[float]:
    vals = [p0, p1y, pq1, pq2, pq3]
    if any(v in (None, 0) for v in vals):
        return None
    return float((
        ((p0 - pq1) / pq1) * WEIGHTS["q1"]
        + ((p0 - pq2) / pq2) * WEIGHTS["q2"]
        + ((p0 - pq3) / pq3) * WEIGHTS["q3"]
        + ((p0 - p1y) / p1y) * WEIGHTS["y1"]
    ) * 100)

def profit_pct(p0, p_future) -> Optional[float]:
    if p0 in (None, 0) or p_future is None:
        return None
    return (p_future - p0) / p0 * 100

# ===== メイン =====
def calc_scores_with_profit(
    start_date: str,
    horizon_months: int,
) -> List[Tuple[str, float, Optional[float]]]:

    df = load_close_wide(CSV_PATH)
    cal = xcals.get_calendar(CAL_NAME)

    # 基準日
    s0 = align_to_csv_available_date(
        df,
        prev_or_same_session(cal, start_date)
    )

    # RSR用（過去）
    s1y = prev_or_same_session(cal, (s0 - relativedelta(years=1)).strftime("%Y-%m-%d"))
    sq1 = prev_or_same_session(cal, (s0 - relativedelta(months=3)).strftime("%Y-%m-%d"))
    sq2 = prev_or_same_session(cal, (s0 - relativedelta(months=6)).strftime("%Y-%m-%d"))
    sq3 = prev_or_same_session(cal, (s0 - relativedelta(months=9)).strftime("%Y-%m-%d"))

    # 利益率用（未来）
    s_future = align_to_csv_available_date(
        df,
        prev_or_same_session(
            cal,
            (s0 + relativedelta(months=horizon_months)).strftime("%Y-%m-%d")
        )
    )

    results = []

    for ticker in df.index.astype(str):
        closes = df.loc[ticker].dropna()
        if closes.empty:
            continue
        closes.index = pd.to_datetime(closes.index).normalize()

        p0  = pick_close_on_or_before(closes, s0)
        p1y = pick_close_on_or_before(closes, s1y)
        pq1 = pick_close_on_or_before(closes, sq1)
        pq2 = pick_close_on_or_before(closes, sq2)
        pq3 = pick_close_on_or_before(closes, sq3)

        score = safe_detect_number(p0, p1y, pq1, pq2, pq3)
        if score is None:
            continue

        p_future = pick_close_on_or_before(closes, s_future)
        pr = profit_pct(p0, p_future)

        results.append((ticker, score, pr))

    return sorted(results, key=lambda x: x[1], reverse=True)

# ===== 実行・プロット =====
if __name__ == "__main__":
    rows = calc_scores_with_profit(START_DATE, HORIZON_MONTHS)

    xs, ys = [], []
    x_n, y_n = None, None

    for t, score, pr in rows:
        if pr is None:
            continue
        if t == "^N225":
            x_n, y_n = score, pr
        else:
            xs.append(score)
            ys.append(pr)

    plt.figure(figsize=(8, 6))
    plt.scatter(xs, ys, alpha=0.4, label="Stocks")

    if x_n is not None:
        plt.scatter([x_n], [y_n], color="red", s=120, marker="*", label="^N225")

    plt.axhline(0)
    plt.xlabel("RSR Score")
    plt.ylabel(f"Profit {HORIZON_MONTHS}M (%)")
    plt.title(f"RSR vs Profit ({START_DATE} → +{HORIZON_MONTHS}M)")
    plt.legend()
    plt.grid(True)
    plt.show()