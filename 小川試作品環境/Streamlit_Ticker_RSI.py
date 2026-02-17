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

    # ===== ティッカー選択（1つだけ） =====
    tickers = sorted(df_rsi.columns.tolist())
    selected_ticker = st.sidebar.selectbox(
        "ティッカーを選択",
        tickers,
    )

    # ===== データ期間 =====
    min_date = min(df_rsi.index.min(), df_ma.index.min()).date()
    max_date = max(df_rsi.index.max(), df_ma.index.max()).date()
    st.sidebar.caption(f"📅 データ期間: {min_date} 〜 {max_date}")

    # ===== 開始日（基準日） =====
    start_base_date = st.sidebar.date_input(
        "開始日",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
    )

    # ===== 期間 =====
    period = st.sidebar.radio(
        "期間",
        ["1M", "3M", "6M", "YTD", "1Y", "MAX"],
        horizontal=True,
        index=5,
    )

    start_base_date = pd.Timestamp(start_base_date)

    # ===== 期間計算 =====
    if period == "1M":
        end_date = start_base_date + datetime.timedelta(days=30)
    elif period == "3M":
        end_date = start_base_date + datetime.timedelta(days=90)
    elif period == "6M":
        end_date = start_base_date + datetime.timedelta(days=180)
    elif period == "YTD":
        end_date = pd.Timestamp(start_base_date.year, 12, 31)
    elif period == "1Y":
        end_date = start_base_date + datetime.timedelta(days=365)
    else:  # MAX
        end_date = pd.Timestamp(max_date)

    # データ範囲ガード
    if end_date.date() > max_date:
        end_date = pd.Timestamp(max_date)

    start_date = start_base_date

    st.sidebar.caption(f"選択期間: {period} ｜ {start_date.date()} 〜 {end_date.date()}")

    # ===== プロット用データ =====
    plot_rows = []

    # RSIデータを追加
    s_rsi = df_rsi[selected_ticker].dropna()
    s_rsi_range = s_rsi.loc[start_date:end_date]

    if not s_rsi_range.empty:
        plot_rows.append(
            pd.DataFrame(
                {
                    "Date": s_rsi_range.index,
                    "Value": s_rsi_range.values,
                    "Type": "RSI",
                }
            )
        )

    # RSI+MA20データを追加
    s_ma = df_ma[selected_ticker].dropna()
    s_ma_range = s_ma.loc[start_date:end_date]

    if not s_ma_range.empty:
        plot_rows.append(
            pd.DataFrame(
                {
                    "Date": s_ma_range.index,
                    "Value": s_ma_range.values,
                    "Type": "RSI+MA20",
                }
            )
        )

    if not plot_rows:
        st.info("選択期間にデータがありません")
        return

    df_plot = pd.concat(plot_rows, ignore_index=True)

    fig = px.line(
        df_plot,
        x="Date",
        y="Value",
        color="Type",
        labels={"Value": "値"},
        title=f"{selected_ticker} - RSI vs RSI+MA20 ({start_date.date()} 〜 {end_date.date()})",
    )

    results = []

    # RSIの統計
    if not s_rsi_range.empty:
        start_value_rsi = s_rsi_range.iloc[0]
        end_value_rsi = s_rsi_range.iloc[-1]
        change_pct_rsi = (end_value_rsi / start_value_rsi - 1) * 100 if start_value_rsi != 0 else 0
        max_value_rsi = s_rsi_range.max()
        min_value_rsi = s_rsi_range.min()

        results.append(
            {
                "指標": "RSI",
                "開始値": round(start_value_rsi, 4),
                "終了値": round(end_value_rsi, 4),
                "変化率 (%)": round(change_pct_rsi, 2),
                "最大値": round(max_value_rsi, 4),
                "最小値": round(min_value_rsi, 4),
            }
        )

    # RSI+MA20の統計
    if not s_ma_range.empty:
        start_value_ma = s_ma_range.iloc[0]
        end_value_ma = s_ma_range.iloc[-1]
        change_pct_ma = (end_value_ma / start_value_ma - 1) * 100 if start_value_ma != 0 else 0
        max_value_ma = s_ma_range.max()
        min_value_ma = s_ma_range.min()

        results.append(
            {
                "指標": "RSI+MA20",
                "開始値": round(start_value_ma, 4),
                "終了値": round(end_value_ma, 4),
                "変化率 (%)": round(change_pct_ma, 2),
                "最大値": round(max_value_ma, 4),
                "最小値": round(min_value_ma, 4),
            }
        )

    # ===== グラフUI無効化 =====
    fig.update_layout(
        xaxis_fixedrange=True,
        yaxis_fixedrange=True,
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
        config={"displayModeBar": False},
    )

    st.subheader("📊 期間統計")

    df_result = pd.DataFrame(results).sort_values("変化率 (%)", ascending=False)

    st.dataframe(
        df_result,
        use_container_width=True,
        hide_index=True,
    )


if __name__ == "__main__":
    main()
