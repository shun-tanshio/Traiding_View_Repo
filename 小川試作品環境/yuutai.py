import os
import yfinance as yf
import pandas as pd

def get_scenario_params():
    scenario = os.getenv("SCENARIO", "stress").lower()

    configs = {
        "easy":   {"lending_rate": 2.0, "interest_rate": 1.0, "hold_days": 3},
        "normal": {"lending_rate": 3.0, "interest_rate": 1.5, "hold_days": 5},
        "hard":   {"lending_rate": 5.0, "interest_rate": 1.8, "hold_days": 7},
        "stress": {"lending_rate": 8.0, "interest_rate": 2.0, "hold_days": 10},
    }

    if scenario not in configs:
        raise ValueError("SCENARIO must be easy / normal / hard / stress")

    return scenario, configs[scenario]


def seven_yutai_10y_sim():
    scenario, params = get_scenario_params()

    ticker = "3382.T"
    shares = 100
    yutai_value = 2000

    stock = yf.Ticker(ticker)
    hist = stock.history(period="10y")[["Close"]]

    hist["Year"] = hist.index.year
    hist["Month"] = hist.index.month

    results = []

    for year in sorted(hist["Year"].unique()):
        feb = hist[(hist["Year"] == year) & (hist["Month"] == 2)]
        if len(feb) == 0:
            continue

        last_day = feb.iloc[-1]
        price = last_day["Close"]
        principal = price * shares

        lending_cost = principal * (params["lending_rate"]/100) * (params["hold_days"]/365)
        interest_cost = principal * (params["interest_rate"]/100) * (params["hold_days"]/365)
        total_cost = lending_cost + interest_cost

        profit = yutai_value - total_cost

        results.append({
            "year": year,
            "price": round(price, 2),
            "principal": round(principal, 2),
            "cost": round(total_cost, 2),
            "profit": round(profit, 2),
            "yield_%": round(profit/principal*100, 3)
        })

    df = pd.DataFrame(results)

    print(f"\n=== SCENARIO: {scenario.upper()} ===")
    print(df)
    print("\n最低利益:", df["profit"].min())
    print("平均利益:", df["profit"].mean())
    print("最低利回り:", df["yield_%"].min())

    return df


if __name__ == "__main__":
    seven_yutai_10y_sim()