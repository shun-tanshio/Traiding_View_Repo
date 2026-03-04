"""日次で前日比ランキングを表示するスクリプト

同階層にある `prices_close_wide.csv` から直近2営業日の
終値を読み取り、

- 前日から本日の上がり％トップ10
- 前日から本日の下がり％トップ10
- 前日から本日の上がり値トップ10
- 前日から本日の下がり値トップ10

を表示する。

Usage:
    cd 小川試作品環境
    python3 daily_top10.py

出力は標準出力に出るだけの簡単なもの。
"""

from __future__ import annotations
import os
import sys
import pandas as pd

CSV = "prices_close_wide.csv"


def load_prices(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"価格ファイルが見つかりません: {path}")

    # 最初の列が銘柄コード、残りが日付
    df = pd.read_csv(path, index_col=0)
    # 日付列をソート
    df.columns = pd.to_datetime(df.columns)
    df = df.sort_index(axis=1)
    return df


def compute_top_lists(df: pd.DataFrame, n: int = 10) -> None:
    # 直近2日のみ抜き出す
    if df.shape[1] < 2:
        print("データが2日分未満です。CSV を更新してください。")
        return

    yesterday, today = df.columns[-2], df.columns[-1]
    prev = df[yesterday]
    curr = df[today]

    change = curr - prev
    pct = change / prev * 100

    # 日付表示用
    fmt = lambda d: d.strftime("%Y-%m-%d")
    print(f"データ取得日: {fmt(yesterday)} -> {fmt(today)}\n")

    def print_list(title: str, series: pd.Series):
        print(f"=== {title} ===")
        print(series.head(n).to_string())
        print()

    # 上がり％トップ10
    up_pct = pct.sort_values(ascending=False)
    print_list("上がり％トップ10", up_pct)

    # 下がり％トップ10
    down_pct = pct.sort_values(ascending=True)
    print_list("下がり％トップ10", down_pct)

    # 上がり値トップ10
    up_abs = change.sort_values(ascending=False)
    print_list("上がり値トップ10", up_abs)

    # 下がり値トップ10
    down_abs = change.sort_values(ascending=True)
    print_list("下がり値トップ10", down_abs)


if __name__ == "__main__":
    try:
        prices = load_prices(CSV)
    except Exception as e:
        print("エラー:", e)
        sys.exit(1)

    compute_top_lists(prices)
