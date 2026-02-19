import yfinance as yf
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import accuracy_score

# ------------------------
# 1. データ取得
# ------------------------
ticker = yf.Ticker("7203.T")  # トヨタ
df = ticker.history(period="5y")

df = df[["Open", "High", "Low", "Close", "Volume"]]

# ------------------------
# 2. MA20
# ------------------------
df["MA20"] = df["Close"].rolling(window=20).mean()
df["MA20_diff"] = (df["Close"] - df["MA20"]) / df["MA20"]

# ------------------------
# 3. RSI (14日)
# ------------------------
window = 14

delta = df["Close"].diff()
gain = delta.clip(lower=0)
loss = -delta.clip(upper=0)

avg_gain = gain.rolling(window).mean()
avg_loss = loss.rolling(window).mean()

rs = avg_gain / avg_loss
df["RSI"] = 100 - (100 / (1 + rs))

# ------------------------
# 4. 1日先ターゲット（分類）
# ------------------------
df["target"] = (df["Close"].shift(-1) > df["Close"]).astype(int)

# ------------------------
# 5. 欠損削除
# ------------------------
df = df.dropna()

# ------------------------
# 6. 特徴量
# ------------------------
features = [
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "MA20",
    "MA20_diff",
    "RSI"
]

X = df[features]
y = df["target"]

# ------------------------
# 7. 時系列分割
# ------------------------
tscv = TimeSeriesSplit(n_splits=5)

accuracies = []

for train_index, test_index in tscv.split(X):
    X_train, X_test = X.iloc[train_index], X.iloc[test_index]
    y_train, y_test = y.iloc[train_index], y.iloc[test_index]

    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=5,
        random_state=42
    )

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    acc = accuracy_score(y_test, y_pred)
    accuracies.append(acc)

print("各分割の精度:", accuracies)
print("平均精度:", np.mean(accuracies))
