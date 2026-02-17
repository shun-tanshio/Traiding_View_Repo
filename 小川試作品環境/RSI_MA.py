import pandas as pd

# 読み込み
df = pd.read_csv("prices_close_wide.csv")

# Tickerをindexに
df = df.set_index("Ticker")

# 転置（行=日付、列=銘柄）
df = df.T

# 日付をdatetimeに
df.index = pd.to_datetime(df.index)
df = df.sort_index()

period = 14

# 差分
delta = df.diff()

gain = delta.clip(lower=0)
loss = -delta.clip(upper=0)

# Wilder平滑化
avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()

rs = avg_gain / avg_loss
rsi = 100 - (100 / (1 + rs))

# RSI MA20
rsi_ma20 = rsi.rolling(20).mean()

# ===== 元の形式に戻す =====
rsi_out = rsi.T
rsi_ma20_out = rsi_ma20.T

# 保存
rsi_out.to_csv("nikkei225_RSI.csv")
rsi_ma20_out.to_csv("nikkei225_RSI_MA20.csv")

print("完了")
