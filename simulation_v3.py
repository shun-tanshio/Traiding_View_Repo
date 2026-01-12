# 説明: `rsr_old.py` を実行して生成された銘柄リスト名を受け、`simulation_onebuy_in_v3.py` を呼び出すオーケストレータ。
# 入力方法: 同ディレクトリで `python simulation_v3.py` を実行（`rsr_old.py` が存在し正常に実行可能であること）。
# 出力されるモノ: `rsr_old.py` と `simulation_onebuy_in_v3.py` の標準出力を表示し、最後に `prices_close_wide.csv` を用いて日経平均の利益率（%）を標準出力に表示。

import subprocess
import datetime
import sys
import pandas as pd
from pathlib import Path


# 実行日（明示指定 or 今日）
date_str = "2024_12_30"
date_str_today = datetime.date.today().strftime("%Y_%m_%d")

def nikkei225_return_pct(
    csv_path: str | Path,
    buy_date: str,
    sell_date: str,
    ticker: str = "^N225",
) -> float:
    """
    日経平均 (^225) を
    buy_date に買って sell_date に売ったときの利益率（%）を返す。

    日付がCSVに無い場合は、その日以前の直近データを使用。
    """

    df = pd.read_csv(csv_path, index_col=0)

    # 日付列を正規化
    df.columns = pd.to_datetime(df.columns, errors="coerce").normalize()
    df = df.loc[:, ~df.columns.isna()].copy()
    df = df.reindex(sorted(df.columns), axis=1)

    if ticker not in df.index:
        raise ValueError(f"{ticker} がCSVに見つかりません")

    row = df.loc[ticker].dropna()
    if row.empty:
        raise ValueError(f"{ticker} のデータが空です")

    buy_date = pd.Timestamp(buy_date.replace("_", "-")).normalize()
    sell_date = pd.Timestamp(sell_date.replace("_", "-")).normalize()

    if sell_date < buy_date:
        raise ValueError("売る日が買う日より前です")

    # 買値（buy_date 以前の直近）
    buy_series = row[row.index <= buy_date]
    if buy_series.empty:
        raise ValueError("買い日のデータが見つかりません")
    buy_price = float(buy_series.iloc[-1])

    # 売値（sell_date 以前の直近）
    sell_series = row[row.index <= sell_date]
    if sell_series.empty:
        raise ValueError("売り日のデータが見つかりません")
    sell_price = float(sell_series.iloc[-1])

    return (sell_price - buy_price) / buy_price * 100.0

# 実行コマンド
cmd = [sys.executable, "rsr_old.py", date_str]

# コマンド実行（標準出力をキャプチャ）
result = subprocess.run(cmd, capture_output=True, text=True)

# エラーがあれば即表示
if result.returncode != 0:
    print("=== STDERR ===")
    print(result.stderr)
    raise RuntimeError("rsr_old.py failed")

# 標準出力（例: "2025-08-29,45"）
out = result.stdout.strip()
print("=== STDOUT ===")
print(out)

# ---- ここがポイント ----
date_out, saved_n_str = out.split(",")
saved_n = int(saved_n_str)

ymd = date_out.replace("-", "")  # "20250829"

f_name = f"top{saved_n}_tse_{ymd}.txt"

print("生成されたファイル名:")
print(f_name)

# 実行コマンド
cmd = [sys.executable, "simulation_onebuy_in_v3.py", date_str, date_str_today,"--top",f_name]

print(cmd)

# コマンド実行（標準出力をキャプチャ）
result = subprocess.run(cmd, capture_output=True, text=True)
out = result.stdout.strip()
print("=== STDOUT ===")
print(out)

pct = nikkei225_return_pct(
    "prices_close_wide.csv",
    date_str,
    date_str_today,
)

print(f"日経平均 利益率: {pct:.2f}%")