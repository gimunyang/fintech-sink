api_key = ""
tickers = ["MSFT", "AAPL"]
start_date = datetime.date(2003, 1, 1)

import datetime, inspect, time
import concurrent.futures as f
import pandas as pd
from polygon import RESTClient

def fetch_minute(symbol, qfrom, qto):
    print(('fetch', symbol, qfrom, qto))
    hist = polygoncli.stocks_equities_aggregates(symbol, "1", "minute", qfrom, qto)
    print(hist.resultsCount, hist.queryCount)
    return hist
        
futures_list = []
with f.ThreadPoolExecutor() as executor:
    for ticker in tickers:
        check_date = start_date
        end_date = check_date.today()

        while check_date < end_date:
            qfrom = check_date.strftime('%Y-%m-%d')
            dt_to = check_date + datetime.timedelta(10)
            qto = dt_to.strftime("%Y-%m-%d")

            print(('queue', ticker, qfrom, qto))
            fut = executor.submit(fetch_minute, ticker, qfrom, qto)
            futures_list.append(fut)
            check_date = dt_to

all_results = dict()

for fut in f.as_completed(futures_list):
    hist_resp = fut.result()
    if hist_resp.ticker not in all_results:
        all_results[hist_resp.ticker] = []        
    all_results[hist_resp.ticker] = all_results[hist_resp.ticker] + hist_resp.results

all_dfs = []
for ticker, results in all_results.items():
    a_df = pd.DataFrame(results)
    a_df['ticker'] = ticker
    all_dfs.append(a_df)
    
fetched_dataframe = pd.concat(all_dfs)
fetched_dataframe['ts'] = pd.to_datetime(fetched_dataframe['t'], unit='ms', utc=True)
fetched_dataframe.set_index('ts').sort_index()
