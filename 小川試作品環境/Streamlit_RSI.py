import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import datetime


@st.cache_data
def load_prices(path: Path):
    df = pd.read_csv(path)

    if "Ticker" in df.columns:
        df = df.set_index("Ticker").T

    idx = pd.to_datetime(df.index, errors="coerce")
    mask = ~idx.isna()
    df = df.loc[mask].copy()
    df.index = pd.DatetimeIndex(idx[mask])
    df = df.apply(pd.to_numeric, errors="coerce").sort_index()
    return df


def normalize_series(s: pd.Series):
    return s / s.iloc[0] * 100 if not s.empty else s


def main():
    st.title("日経225 RSI・MA分析")

    # ===== 複数のCSVファイルを読み込む =====
    rsi_path = Path(__file__).resolve().parents[0] / "nikkei225_RSI.csv"
    ma_path = Path(__file__).resolve().parents[0] / "nikkei225_RSI_MA20.csv"
    close_path = Path(__file__).resolve().parents[0] / "prices_close_wide.csv"
    open_path = Path(__file__).resolve().parents[0] / "prices_open_wide.csv"
    
    df_rsi = load_prices(rsi_path)
    df_ma = load_prices(ma_path)
    df_close = load_prices(close_path)
    df_open = load_prices(open_path)

    if df_rsi.empty or df_ma.empty:
        st.error("データがありません")
        return

    # ===== 最新日付を取得 =====
    max_date = max(df_rsi.index.max(), df_ma.index.max()).date()
    latest_date = pd.Timestamp(max_date)
    
    # 昨日の日付を取得
    yesterday_date = latest_date - pd.Timedelta(days=1)
    while yesterday_date not in df_rsi.index:
        yesterday_date -= pd.Timedelta(days=1)
        if yesterday_date < df_rsi.index.min():
            st.warning("前日のデータがありません")
            yesterday_date = None
            break

    st.sidebar.caption(f"📅 表示日付: {max_date}")

    # ===== 昨日のRSI-MA20 と 今日の Close - Open =====
    if yesterday_date is not None:
        st.subheader("📊 昨日のRSI-MA20 ランキング ＆ 今日の(終値 - 始値)")
        yesterday_rsi = df_rsi.loc[yesterday_date]
        yesterday_ma = df_ma.loc[yesterday_date]
        yesterday_diff = (yesterday_rsi - yesterday_ma).dropna().sort_values(ascending=False)
        
        # 今日の Close と Open を用意（存在しなければ空Series）
        if latest_date in df_close.index and latest_date in df_open.index:
            latest_close = df_close.loc[latest_date]
            latest_open = df_open.loc[latest_date]
        else:
            latest_close = pd.Series(dtype=float)
            latest_open = pd.Series(dtype=float)

        # 昨日のランキング順で、今日の値（終値-始値）と%変化を並べる
        combined_data = []
        for ticker in yesterday_diff.index:
            close_val = latest_close.get(ticker, None)
            open_val = latest_open.get(ticker, None)
            if pd.notna(close_val) and pd.notna(open_val) and open_val != 0:
                today_diff = close_val - open_val
                today_pct = today_diff / open_val * 100
            elif pd.notna(close_val) and pd.notna(open_val):
                today_diff = close_val - open_val
                today_pct = None
            else:
                today_diff = None
                today_pct = None

            combined_data.append({
                "ティッカー": ticker,
                "昨日RSI-MA20": round(float(yesterday_diff[ticker]), 4),
                "今日(終値-始値)": round(float(today_diff), 2) if today_diff is not None else None,
                "今日(%変化)": round(float(today_pct), 2) if today_pct is not None else None,
            })

        combined_ranking = pd.DataFrame(combined_data)
        combined_ranking.index = combined_ranking.index + 1

        st.dataframe(
            combined_ranking,
            use_container_width=True,
        )

    # ===== 今日のRSI-MA20 =====
    st.subheader("📊 今日のRSI-MA20 ランキング")
    
    latest_rsi = df_rsi.loc[latest_date]
    latest_ma = df_ma.loc[latest_date]
    valid_diff = (latest_rsi - latest_ma).dropna().sort_values(ascending=False)

    ma_ranking = pd.DataFrame({
        "ティッカー": valid_diff.index,
        "RSI-MA20": valid_diff.values.round(4),
    }).reset_index(drop=True)

    ma_ranking.index = ma_ranking.index + 1

    st.dataframe(
        ma_ranking,
        use_container_width=True,
    )


if __name__ == "__main__":
    main()
