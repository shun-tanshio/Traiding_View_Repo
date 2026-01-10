# update_missing_closes_until_today.py
# pip install yfinance pandas

from __future__ import annotations
from pathlib import Path
import pandas as pd
import yfinance as yf

CSV_PATH = Path("prices_close_wide.csv")

INTERVAL = "1d"
CHUNK_SIZE = 50

def load_csv() -> pd.DataFrame:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSVが見つかりません: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, index_col=0)
    return df

def get_latest_saved_date(columns: list[str]) -> pd.Timestamp | None:
    # 列名のうち、日付として解釈できるものだけで最大を取る
    parsed = pd.to_datetime(pd.Index(columns), errors="coerce")
    if parsed.isna().all():
        return None
    return parsed.max()

def fetch_close_range(tickers: list[str], start: str, end: str) -> tuple[pd.DataFrame, list[str]]:
    """
    期間[start, end) の日足終値を取得して close_wide で返す（縦=Ticker, 横=Date）
    return:
      new_wide: index=Ticker, columns=YYYY-MM-DD(str), values=Close
      failed: 取れなかったTicker
    """
    parts: list[pd.DataFrame] = []
    failed: list[str] = []

    for i in range(0, len(tickers), CHUNK_SIZE):
        chunk = tickers[i:i + CHUNK_SIZE]
        print(f"【取得中】{i+1}〜{min(i+CHUNK_SIZE, len(tickers))} / {len(tickers)} 銘柄（{start} 〜 {end}）")

        try:
            data = yf.download(
                tickers=chunk,
                start=start,   # inclusive
                end=end,       # exclusive（今日まで欲しければ明日をendにする）
                interval=INTERVAL,
                progress=False,
                threads=True,
            )
        except Exception as e:
            print("【株価取得エラー】yfinance取得で例外が発生しました。")
            print(f"  対象（先頭10）: {chunk[:10]}")
            print(f"  エラー内容: {repr(e)}")
            failed.extend(chunk)
            continue

        if data is None or data.empty:
            print("【注意】この範囲のデータが空でした（休場/データ欠落の可能性）。")
            continue

        try:
            close = data["Close"]
        except Exception as e:
            print("【株価取得エラー】'Close' 列の取り出しに失敗しました。")
            print(f"  エラー内容: {repr(e)}")
            failed.extend(chunk)
            continue

        # 1銘柄だとSeriesになることがある
        if isinstance(close, pd.Series):
            close = close.to_frame(name=chunk[0])

        if close.empty:
            print("【注意】Closeが空でした。")
            continue

        # 返ってきたTicker列に含まれないものを failed 扱い
        returned_cols = set(map(str, close.columns))
        missing_cols = [t for t in chunk if t not in returned_cols]
        if missing_cols:
            print("【注意】このチャンクで取得できなかったTickerがあります（yfinance側エラーの可能性）:")
            print("  " + ", ".join(missing_cols))
            failed.extend(missing_cols)

        # 日付を "YYYY-MM-DD" に統一
        close.index = pd.to_datetime(close.index).strftime("%Y-%m-%d")

        wide = close.T  # 行=Ticker, 列=Date

        # 全部NaNのTickerは失敗扱い
        all_nan = wide.isna().all(axis=1)
        failed.extend(wide.index[all_nan].tolist())
        wide = wide.loc[~all_nan]

        parts.append(wide)

    out = pd.concat(parts, axis=0) if parts else pd.DataFrame()
    out = out[~out.index.duplicated(keep="first")]
    out = out.sort_index()
    out = out.reindex(sorted(out.columns), axis=1)

    return out, sorted(set(map(str, failed)))

def main():
    df = load_csv()

    tickers = df.index.astype(str).tolist()
    print(f"【対象】銘柄数: {len(tickers)}")

    latest = get_latest_saved_date(list(df.columns))
    if latest is None:
        print("【致命的】CSVの列から日付が判定できません（列名が日付形式ではない可能性）。")
        return

    latest_str = latest.strftime("%Y-%m-%d")

    # 既存の最新日付の「翌日」から取りに行く
    start_dt = (latest + pd.Timedelta(days=1)).normalize()

    # 今日まで取りたいので end は「明日」（endはexclusive）
    today = pd.Timestamp.today().normalize()
    end_dt = today + pd.Timedelta(days=1)

    start = start_dt.strftime("%Y-%m-%d")
    end = end_dt.strftime("%Y-%m-%d")

    if start_dt > today:
        print("【完了】すでに最新です（追加する日付がありません）。")
        print(f"  CSV最新日付: {latest_str}")
        return

    print(f"【更新範囲】CSV最新日付: {latest_str} → 追加取得: {start} 〜 {today.strftime('%Y-%m-%d')}")

    new_wide, failed = fetch_close_range(tickers, start=start, end=end)

    # ★重要：yfinanceが「開始日以降データ無し」でも直前日を返すことがあるため、
    # 「CSV最新日付より後」だけを追加扱いにする
    if not new_wide.empty:
        new_cols = [c for c in new_wide.columns if c > latest_str]
        new_wide = new_wide.reindex(columns=new_cols)

    if new_wide.empty:
        print("【完了】追加できる新しい取引日がありません。")
        print(f"  CSV最新日付: {latest_str}")
        print("  休場日・週末・引け前実行などが原因の可能性があります。")
        if failed:
            print("【注意】取得できなかった可能性のあるTicker（参考）:")
            print("  " + ", ".join(failed[:50]) + (" …" if len(failed) > 50 else ""))
        return

    # 既存dfに新しい日付列を追加して、値を上書き更新
    merged = df.copy()

    # 列を拡張
    all_cols = sorted(set(merged.columns).union(new_wide.columns))
    merged = merged.reindex(columns=all_cols)

    # 値を反映（同じ日付があれば上書き）
    merged.update(new_wide)

    # 保存（小数2桁固定）
    merged.to_csv(CSV_PATH, encoding="utf-8-sig", float_format="%.2f")

    print(f"【保存】CSVを更新しました: {CSV_PATH}")
    print(f"【結果】追加取得できた日付列数: {len(new_wide.columns)}（{new_wide.columns[0]} … {new_wide.columns[-1]}）")

    if failed:
        print("【注意】取得できなかった可能性のあるTicker:")
        print("  " + ", ".join(failed[:50]) + (" …" if len(failed) > 50 else ""))

if __name__ == "__main__":
    main()