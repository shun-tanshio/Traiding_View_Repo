import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import TimeSeriesSplit

# -----------------------------
# 1. CSV読み込み & 数値型変換
# -----------------------------
tickers = ["7203.T", "6758.T", "9984.T"]
dfs = []

for t in tickers:
    df = pd.read_csv(f"stock_data/{t}_15y.csv", index_col=0)
    df.index = pd.to_datetime(df.index, format="%Y-%m-%d", errors='coerce')

    numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    df.dropna(subset=numeric_cols, inplace=True)
    df["Ticker"] = t
    dfs.append(df)

data = pd.concat(dfs)
data.sort_index(inplace=True)

# -----------------------------
# 2. 特徴量作成
# -----------------------------
data["MA20"] = data.groupby("Ticker")["Close"].transform(lambda x: x.rolling(20).mean())
data["MA20_diff"] = (data["Close"] - data["MA20"]) / data["MA20"]

def compute_RSI(series, window=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

data["RSI"] = data.groupby("Ticker")["Close"].transform(lambda x: compute_RSI(x))

# -----------------------------
# 3. 回帰ターゲット（次の日終値 - 当日始値）
# -----------------------------
data["target_profit"] = data.groupby("Ticker")["Close"].transform(lambda x: x.shift(-1)) - data["Open"]

# 特徴量にNaNが残っていたら削除
features = ["Open", "High", "Low", "Close", "Volume", "MA20", "MA20_diff", "RSI"]
data.dropna(subset=features + ["target_profit"], inplace=True)

X = data[features]
y = data["target_profit"]

# -----------------------------
# 4. 時系列分割 & 学習
# -----------------------------
tscv = TimeSeriesSplit(n_splits=5)
profit_scores = []

for train_idx, test_idx in tscv.split(X):
    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

    model = RandomForestRegressor(n_estimators=200, max_depth=5, random_state=42)
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)

    # 利益スコア：予測符号 × 実際の値
    profit_score = np.sum(np.sign(y_pred) * y_test)
    profit_scores.append(profit_score)

# -----------------------------
# 5. 過去分割の利益スコア表示
# -----------------------------
for i, s in enumerate(profit_scores):
    print(f"分割 {i+1} の利益スコア: {s:.2f}")
print(f"平均利益スコア: {np.mean(profit_scores):.2f}")

# -----------------------------
# 6. 直近数日の予測（次の日の上がる/下がる＋予測幅）
# -----------------------------
# 直近5日分を対象（全銘柄）
X_recent = X.iloc[-5:]
recent_pred = model.predict(X_recent)

# 直近予測（銘柄ごとに次の日を予測）
print("\n=== 銘柄別・次の日予測 ===")
for t in tickers:
    df_t = data[data["Ticker"] == t]
    X_recent = df_t[features].iloc[-1:]  # 各銘柄の直近1日
    pred = model.predict(X_recent)[0]  # 予測値はスカラー
    
    direction = "上昇" if pred > 0 else "下落"
    next_day = df_t.index[-1] + pd.Timedelta(days=1)  # 次の日の日付
    print(f"{t} の {next_day.date()} 予測: {direction}, 予測幅: {pred:.2f} 円")

