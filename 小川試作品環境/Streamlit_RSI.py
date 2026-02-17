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

    # ===== 両方のCSVファイルを読み込む =====
    rsi_path = Path(__file__).resolve().parents[0] / "nikkei225_RSI.csv"
    ma_path = Path(__file__).resolve().parents[0] / "nikkei225_RSI_MA20.csv"
    
    df_rsi = load_prices(rsi_path)
    df_ma = load_prices(ma_path)

    if df_rsi.empty or df_ma.empty:
        st.error("データがありません")
        return

    # ===== 最新日付を取得 =====
    max_date = max(df_rsi.index.max(), df_ma.index.max()).date()
    latest_date = pd.Timestamp(max_date)

    st.sidebar.caption(f"📅 表示日付: {max_date}")

    # ===== 最新日付のデータを取得 =====
    latest_rsi = df_rsi.loc[latest_date]
    latest_ma = df_ma.loc[latest_date]

    # NaNを除いたティッカーのみを対象
    valid_rsi = latest_rsi.dropna().sort_values(ascending=False)
    valid_ma = latest_ma.dropna().sort_values(ascending=False)
    
    # RSI - MA20 を計算
    valid_diff = (latest_rsi - latest_ma).dropna().sort_values(ascending=False)

    # ===== RSI順ランキング =====
    st.subheader("📊 RSI ランキング")
    
    rsi_ranking = pd.DataFrame({
        "ティッカー": valid_rsi.index,
        "RSI": valid_rsi.values.round(4),
    }).reset_index(drop=True)
    
    rsi_ranking.index = rsi_ranking.index + 1

    st.dataframe(
        rsi_ranking,
        use_container_width=True,
    )

    # ===== RSI-MA20順ランキング =====
    st.subheader("📊 RSI-MA20 ランキング")

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
