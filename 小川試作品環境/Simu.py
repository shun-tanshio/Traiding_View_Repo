import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent

def load_wide(path):
	df = pd.read_csv(path, index_col=0)
	df.columns = pd.to_datetime(df.columns)
	return df

def main():
	rsi_path = BASE / 'nikkei225_RSI.csv'
	rsi_ma_path = BASE / 'nikkei225_RSI_MA20.csv'
	close_path = BASE / 'prices_close_wide.csv'
	open_path = BASE / 'prices_open_wide.csv'

	rsi = load_wide(rsi_path)
	rsi_ma = load_wide(rsi_ma_path)
	close = load_wide(close_path)
	openp = load_wide(open_path)

	# transpose so index = dates, columns = tickers
	rsi_t = rsi.T
	rsi_ma_t = rsi_ma.T
	close_t = close.T
	open_t = openp.T

	today = datetime.now().date()
	end_date = today - timedelta(days=1)
	start_date = today - timedelta(days=30)

	available_dates = sorted(d.date() for d in rsi_t.index)
	# choose dates between start_date and end_date that exist in RSI index
	dates = [d for d in available_dates if start_date <= d <= end_date]

	results = []

	price_dates = sorted(d.date() for d in close_t.index)

	for d in dates:
		try:
			r_row = rsi_t.loc[pd.Timestamp(d)]
			ma_row = rsi_ma_t.loc[pd.Timestamp(d)]
		except KeyError:
			continue

		diff = (r_row - ma_row).dropna()
		if diff.empty:
			continue
		top20 = diff.sort_values(ascending=False).head(20)
		# find next price date
		try:
			idx = price_dates.index(d)
			next_date = price_dates[idx+1]
		except (ValueError, IndexError):
			# no next day available
			continue

		# get open and close series for next_date
		next_ts = pd.Timestamp(next_date)
		if next_ts not in close_t.index or next_ts not in open_t.index:
			continue

		close_next = close_t.loc[next_ts]
		open_next = open_t.loc[next_ts]

		per_ticker = {}
		total = 0.0
		count = 0
		for ticker in top20.index:
			if ticker in close_next.index and ticker in open_next.index:
				c = close_next[ticker]
				o = open_next[ticker]
				if pd.isna(c) or pd.isna(o):
					continue
				ret = c - o
				per_ticker[ticker] = float(ret)
				total += ret
				count += 1

		results.append({
			'date': d.isoformat(),
			'n_selected': count,
			'sum_next_close_minus_open': float(total),
			'tickers': ';'.join(top20.index.tolist()),
		})

		# print per-ticker details to terminal
		print(f"\nDate: {d.isoformat()}  next_day: {next_date.isoformat()}  selected:{len(top20)}  computed:{count}  sum:{total}")
		for tk in top20.index:
			val = per_ticker.get(tk, None)
			if val is None:
				print(f"  {tk}: NA")
			else:
				print(f"  {tk}: {val:+.2f}")

	out = pd.DataFrame(results)
	out_path = BASE / 'simu_rsi_diff_top20_returns.csv'
	out.to_csv(out_path, index=False)
	print('done, saved to', out_path)

if __name__ == '__main__':
	main()

