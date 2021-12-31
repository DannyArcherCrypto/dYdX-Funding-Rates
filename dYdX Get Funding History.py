
from dydx3 import Client
from web3 import Web3
import pandas as pd
import datetime as datetime

client = Client(
    host='https://api.dydx.exchange'
)

markets = client.public.get_markets()
markets = pd.DataFrame(markets.data['markets'])

for futures_local_name in markets.columns:
    print(futures_local_name)
    future_name_local = str(futures_local_name)
    initial = '2021-02-26T00:00:00Z'
    start_time = datetime.datetime.strptime(initial, "%Y-%m-%dT%H:%M:%SZ")
    timeseries = pd.DataFrame()
    new_results = True

    today = datetime.datetime.now()

    while new_results == True:
        print(start_time)
        end_time = start_time + datetime.timedelta(hours=90)
        api_result = client.public.get_historical_funding(
            market=future_name_local,
            effective_before_or_at=start_time,
        )

        api_result = pd.DataFrame(api_result.data['historicalFunding'])
        timeseries = pd.concat([timeseries, api_result])
        timeseries = timeseries.drop_duplicates(subset=['effectiveAt'])    
        start_time = end_time

        if start_time > today:
            new_results = False
        
        timeseries.to_pickle("./Funding_Data/" + str(future_name_local) + ".pkl")
        timeseries = pd.read_pickle("./Funding_Data/" + str(future_name_local) + ".pkl")

        if start_time > today:
            new_results = False
