# close_wide_1y_manual_list_jp_fixed.py
# pip install yfinance pandas

from __future__ import annotations
import pandas as pd
import yfinance as yf

PERIOD = "15y"
INTERVAL = "1d"
CHUNK_SIZE = 40

# ★出力ファイル名は直書き（OUT_CSV未定義問題を回避）
OUT_CSV = "prices_close_wide.csv"

TICKERS_TEXT = """
1332.T
1605.T
1721.T
1801.T
1802.T
1803.T
1808.T
1812.T
1925.T
1928.T
1963.T
2002.T
2269.T
2282.T
2413.T
2432.T
2501.T
2502.T
2503.T
2768.T
2801.T
2802.T
2871.T
2914.T
3086.T
3092.T
3099.T
3289.T
3382.T
3401.T
3402.T
3405.T
3407.T
3436.T
3659.T
3697.T
3861.T
4004.T
4005.T
4021.T
4042.T
4043.T
4061.T
4062.T
4063.T
4151.T
4183.T
4188.T
4208.T
4307.T
4324.T
4385.T
4452.T
4502.T
4503.T
4506.T
4507.T
4519.T
4523.T
4543.T
4568.T
4578.T
4661.T
4689.T
4704.T
4751.T
4755.T
4901.T
4902.T
4911.T
5019.T
5020.T
5101.T
5108.T
5201.T
5214.T
5233.T
5301.T
5332.T
5333.T
5401.T
5406.T
5411.T
5631.T
5706.T
5711.T
5713.T
5714.T
5801.T
5802.T
5803.T
5831.T
6098.T
6103.T
6113.T
6146.T
6178.T
6273.T
6301.T
6302.T
6305.T
6326.T
6361.T
6367.T
6471.T
6472.T
6473.T
6479.T
6501.T
6503.T
6504.T
6506.T
6526.T
6532.T
6645.T
6674.T
6701.T
6702.T
6723.T
6724.T
6752.T
6753.T
6758.T
6762.T
6770.T
6841.T
6857.T
6861.T
6902.T
6920.T
6952.T
6954.T
6963.T
6971.T
6976.T
6981.T
6988.T
7004.T
7011.T
7012.T
7013.T
7186.T
7201.T
7202.T
7203.T
7205.T
7211.T
7261.T
7267.T
7269.T
7270.T
7272.T
7453.T
7731.T
7733.T
7735.T
7741.T
7751.T
7752.T
7832.T
7911.T
7912.T
7951.T
7974.T
8001.T
8002.T
8015.T
8031.T
8035.T
8053.T
8058.T
8233.T
8252.T
8253.T
8267.T
8304.T
8306.T
8308.T
8309.T
8316.T
8331.T
8354.T
8411.T
8591.T
8601.T
8604.T
8630.T
8697.T
8725.T
8750.T
8766.T
8795.T
8801.T
8802.T
8804.T
8830.T
9001.T
9005.T
9007.T
9008.T
9009.T
9020.T
9021.T
9022.T
9064.T
9101.T
9104.T
9107.T
9147.T
9201.T
9202.T
9432.T
9433.T
9434.T
9501.T
9502.T
9503.T
9531.T
9532.T
9602.T
9735.T
9766.T
9843.T
9983.T
9984.T
^N225
""".strip()

def parse_tickers(text: str) -> list[str]:
    raw = [line.strip() for line in text.splitlines() if line.strip()]
    seen = set()
    out = []
    for t in raw:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out

def fetch_close_wide_1y(tickers: list[str]) -> tuple[pd.DataFrame, list[str]]:
    parts: list[pd.DataFrame] = []
    failed: list[str] = []

    for i in range(0, len(tickers), CHUNK_SIZE):
        chunk = tickers[i:i + CHUNK_SIZE]
        print(f"【取得中】{i+1}〜{min(i+CHUNK_SIZE, len(tickers))} / {len(tickers)} 銘柄（期間={PERIOD}）")

        try:
            data = yf.download(
                tickers=chunk,
                period=PERIOD,
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
            print("【株価取得エラー】データが空でした。")
            failed.extend(chunk)
            continue

        # Closeだけ抜く
        try:
            close = data["Close"]
        except Exception as e:
            print("【株価取得エラー】'Close'列が取得できませんでした。")
            print(f"  エラー内容: {repr(e)}")
            failed.extend(chunk)
            continue

        # 1銘柄だとSeriesになることがある
        if isinstance(close, pd.Series):
            # この場合、成功していればその銘柄名で列名を付ける
            close = close.to_frame(name=chunk[0])

        # yfinanceが返した列（成功したTicker）を確認
        returned_cols = set(map(str, close.columns))
        missing_cols = [t for t in chunk if t not in returned_cols]
        if missing_cols:
            print("【注意】このチャンクで取得できなかったTickerがあります（yfinance側エラーの可能性）:")
            print("  " + ", ".join(missing_cols))
            failed.extend(missing_cols)

        # 日付列を "YYYY-MM-DD" に統一してワイド化（縦=Ticker 横=Date）
        close.index = pd.to_datetime(close.index).strftime("%Y-%m-%d")
        wide = close.T

        # 全部NaNのTickerも失敗扱い
        all_nan = wide.isna().all(axis=1)
        nan_failed = wide.index[all_nan].tolist()
        if nan_failed:
            print("【注意】終値が全て欠損のTickerがあります（取得失敗/取引停止などの可能性）:")
            print("  " + ", ".join(map(str, nan_failed)))
            failed.extend(map(str, nan_failed))
            wide = wide.loc[~all_nan]

        parts.append(wide)

    out = pd.concat(parts, axis=0) if parts else pd.DataFrame()
    out = out[~out.index.duplicated(keep="first")]
    out = out.sort_index().reindex(sorted(out.columns), axis=1)

    return out, sorted(set(failed))

def main():
    tickers = parse_tickers(TICKERS_TEXT)
    print(f"対象ティッカー数: {len(tickers)}")

    close_wide, failed = fetch_close_wide_1y(tickers)

    if close_wide.empty:
        print("【致命的】取得結果が空でした。終了します。")
        return

    # ★小数2桁で保存（表示の揺れ対策）
    close_wide.to_csv(OUT_CSV, encoding="utf-8-sig", float_format="%.2f")

    print(f"【保存】CSVを保存しました: {OUT_CSV}")
    print(f"【結果】行数（銘柄）={len(close_wide)}、列数（日付）={len(close_wide.columns)}")

    if failed:
        print("【まとめ】取得できなかった可能性のあるTicker一覧:")
        print("  " + ", ".join(failed))

if __name__ == "__main__":
    main()