import os
import unittest

import pandas as pd
from lumibot.data_sources import AlpacaData
from lumibot.entities.asset import Asset
from lumibot.entities.bars import Bars


ALPACA_CONFIG = {  # Paper trading!
    # Put your own Alpaca key here:
    "API_KEY": os.environ.get("APCA_API_KEY_ID"),
    # Put your own Alpaca secret here:
    "API_SECRET": os.environ.get("APCA_API_SECRET_KEY"),
    # If you want to use real money you must change this to False
    "PAPER": True,
}


class TestAlpacaData(unittest.TestCase):

    def setUp(self):
        self.alpaca_data = AlpacaData(ALPACA_CONFIG)

    def test_init(self):
        self.assertEqual(self.alpaca_data.api_key, ALPACA_CONFIG["API_KEY"])
        self.assertEqual(self.alpaca_data.api_secret, ALPACA_CONFIG["API_SECRET"])
        self.assertEqual(self.alpaca_data.is_paper, ALPACA_CONFIG["PAPER"])

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
    
    def test_option_get_last_price(self):
        asset = Asset(symbol="AAPL", asset_type="option", expiration="2025-12-19", strike=200, right="call")
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

    # def test_get_barset_from_api(self):
    #     asset = Asset(symbol="AAPL")
    #     df = self.alpaca_data.get_barset_from_api(asset, "1Min", limit=100)
    #     self.assertIsNotNone(df)
    #     self.assertIsInstance(df, pd.DataFrame)

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
