import os
import unittest

from datetime import datetime, timedelta, timezone

import pandas as pd
from lumibot.data_sources import AlpacaData
from lumibot.entities.asset import Asset
from lumibot.entities.bars import Bars

from alpaca.data.timeframe import TimeFrame, TimeFrameUnit


ALPACA_CONFIG = {  # Paper trading!
    # Put your own Alpaca key here:
    "API_KEY": os.environ.get("APCA_API_KEY_ID"),
    # Put your own Alpaca secret here:
    "API_SECRET": os.environ.get("APCA_API_SECRET_KEY"),
    # If you want to use real money you must change this to False
    "PAPER": True,
    # "ENDPOINT": "https://paper-api.alpaca.markets",
}


class TestAlpacaData(unittest.TestCase):

    def setUp(self):
        self.alpaca_data = AlpacaData(ALPACA_CONFIG)

    def test_init(self):
        self.assertEqual(self.alpaca_data.api_key, ALPACA_CONFIG["API_KEY"])
        self.assertEqual(self.alpaca_data.api_secret, ALPACA_CONFIG["API_SECRET"])
        self.assertEqual(self.alpaca_data.is_paper, ALPACA_CONFIG["PAPER"])

    def test_timeframe_to_timedelta(self):
        td = AlpacaData.timeframe_to_timedelta(TimeFrame.Minute)
        print(f"{td=}")
        self.assertEqual(AlpacaData.timeframe_to_timedelta(TimeFrame.Minute), timedelta(minutes=1))
        self.assertEqual(AlpacaData.timeframe_to_timedelta(TimeFrame.Hour), timedelta(hours=1))
        self.assertEqual(AlpacaData.timeframe_to_timedelta(TimeFrame.Day), timedelta(days=1))
        self.assertEqual(AlpacaData.timeframe_to_timedelta(TimeFrame.Week), timedelta(weeks=1))
        self.assertEqual(AlpacaData.timeframe_to_timedelta(TimeFrame(2, TimeFrameUnit.Minute)), timedelta(minutes=2))
        self.assertEqual(AlpacaData.timeframe_to_timedelta(TimeFrame(5, TimeFrameUnit.Hour)), timedelta(hours=5))
        # Day and Week units can only be used with amount 1

    def test_get_last_price(self):
        asset = Asset(symbol="AAPL")
        price = self.alpaca_data.get_last_price(asset)
        print(f"{price=}")
        self.assertIsNotNone(price)
        self.assertIsInstance(price, float)

    def test_get_historical_prices(self):
        asset = Asset(symbol="AAPL")
        bars = self.alpaca_data.get_historical_prices(asset, 100, "day")
        print(f"{bars=}")
        print(bars.df)
        self.assertIsNotNone(bars)
        self.assertIsInstance(bars, Bars)

    def test_get_historical_prices_with_timestep(self):
        asset = Asset(symbol="NVDA")
        bars = self.alpaca_data.get_historical_prices(asset, 100, timestep="hour")
        print(f"{bars=}")
        print(bars.df)
        self.assertIsNotNone(bars)
        self.assertIsInstance(bars, Bars)

    def test_get_historical_prices_with_start(self):
        asset = Asset(symbol="VZ")
        start_datetime = datetime(year=2022, month=12, day=1, hour=14)
        bars = self.alpaca_data.get_historical_prices(asset, 12, timestep="day", start=start_datetime)
        print(f"{bars=}")
        self.assertIsNotNone(bars)
        self.assertIsInstance(bars, Bars)
        self.assertGreater(len(bars.df), 10)

    def test_get_historical_prices_in_start_end_range(self):
        asset = Asset(symbol="T")
        start_datetime = datetime(year=2022, month=12, day=1, hour=14)
        end_datetime = datetime(year=2022, month=12, day=2, hour=18)
        bars = self.alpaca_data.get_historical_prices(asset, 12, timestep="day", start=start_datetime, end=end_datetime)
        bars

    def test_option_get_last_price(self):
        asset = Asset(symbol="SPY", asset_type="option", expiration="2025-12-19", strike=200, right="call")
        price = self.alpaca_data.get_last_price(asset)
        print(f"{price=}")
        self.assertIsNotNone(price)
        self.assertIsInstance(price, float)

    def test_option_get_historical_prices(self):
        asset = Asset(symbol="AAPL", asset_type="option", expiration="2025-12-19", strike=200, right="call")
        # Symbol should be AAPL251219C00200000
        #              Got AAPL251219C00200000
        bars = self.alpaca_data.get_historical_prices(asset, 10, "day")
        print(f"{bars=}")
        # if bars and bars.df:
        #     print(bars.df)
        self.assertIsNotNone(bars)
        self.assertIsInstance(bars, Bars)
        option_asset = Asset.symbol2asset("AAPL261218P00350000")
        print(f"{option_asset=}")
        bars = self.alpaca_data.get_historical_prices(option_asset, length=25, timestep="day", exchange=None)
        print(f"{bars=}")    
        self.assertIsNotNone(bars)
        self.assertIsInstance(bars, Bars)
        self.assertGreater(len(bars.df), 1)

    def test_get_option_chain(self):
        asset = Asset(symbol="AAPL")
        chain = self.alpaca_data.get_chains(asset)
        print(f"{chain=}")
        self.assertIsNotNone(chain)
        self.assertIsInstance(chain, dict)

    def test_get_option_chain_for_exchange(self):
        asset = Asset(symbol="IBM")
        chain = self.alpaca_data.get_chains(asset, exchange="X")
        print(f"{chain=}")
        self.assertIsNotNone(chain)
        self.assertIsInstance(chain, dict)

    def test_get_barset_from_api(self):
        asset = Asset(symbol="ETH", asset_type="crypto")
        quote = Asset(symbol="USD", asset_type="forex")
        df = self.alpaca_data.get_barset_from_api(asset, "1Min", limit=100, quote=quote)
        print(f"{df=}")
        self.assertIsNotNone(df)
        self.assertIsInstance(df, pd.DataFrame)

    # def test_pull_source_bars(self):
    #     assets = [Asset(symbol="AAPL"), Asset(symbol="GOOG")]
    #     result = self.alpaca_data._pull_source_bars(assets, 100, "day")
    #     self.assertIsNotNone(result)
    #     self.assertIsInstance(result, dict)
    #     for asset, df in result.items():
    #         self.assertIsInstance(df, pd.DataFrame)

    # def test_pull_source_symbol_bars(self):
    #     asset = Asset(symbol="AAPL")
    #     df = self.alpaca_data._pull_source_symbol_bars(asset, 100, "day")
    #     self.assertIsNotNone(df)
    #     self.assertIsInstance(df, pd.DataFrame)

    # def test_parse_source_symbol_bars(self):
    #     asset = Asset(symbol="AAPL")
    #     df = self.alpaca_data.get_barset_from_api(asset, "1Min", 100)
    #     bars = self.alpaca_data._parse_source_symbol_bars(df, asset)
    #     self.assertIsNotNone(bars)
    #     self.assertIsInstance(bars, Bars)


if __name__ == '__main__':
    unittest.main()
