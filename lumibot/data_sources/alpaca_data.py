import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pandas as pd
from alpaca.data.historical import CryptoHistoricalDataClient, StockHistoricalDataClient
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import (
    CryptoBarsRequest,
    CryptoLatestQuoteRequest,
    CryptoLatestTradeRequest,
    OptionBarsRequest,
    OptionChainRequest,
    OptionLatestTradeRequest,
    OptionLatestQuoteRequest,
    StockBarsRequest,
    StockLatestTradeRequest,
    StockLatestQuoteRequest,
)
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from lumibot.entities import Asset, Bars
from lumibot.tools.helpers import create_options_symbol, parse_timestep_qty_and_unit

from .data_source import DataSource


class AlpacaData(DataSource):
    SOURCE = "ALPACA"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {
            "timestep": "minute",
            "representations": [TimeFrame.Minute, "1 Min", "1Min", "Min", "minute"],
        },
        {
            "timestep": "5 minutes",
            "representations": [
                TimeFrame(5, TimeFrameUnit.Minute), "5 Min", "5Min",
            ],
        },
        {
            "timestep": "10 minutes",
            "representations": [
                TimeFrame(10, TimeFrameUnit.Minute), "10 Min", "10Min",
            ],
        },
        {
            "timestep": "15 minutes",
            "representations": [
                TimeFrame(15, TimeFrameUnit.Minute), "15 Min", "15Min",
            ],
        },
        {
            "timestep": "30 minutes",
            "representations": [
                TimeFrame(30, TimeFrameUnit.Minute), "30 Min", "30Min",
            ],
        },
        {
            "timestep": "hour",
            "representations": [
                TimeFrame.Hour, "1 Hour", "1Hour", "Hour", "hour",
            ],
        },
        {
            "timestep": "2 hours",
            "representations": [
                TimeFrame(2, TimeFrameUnit.Hour), "2 Hour", "2Hour",
            ],
        },
        {
            "timestep": "4 hours",
            "representations": [
                TimeFrame(4, TimeFrameUnit.Hour), "4 Hour", "4Hour",
            ],
        },
        {
            "timestep": "day",
            "representations": [TimeFrame.Day, "1 Day", "1Day", "Day", "day",],
        },
    ]

    @staticmethod
    def timeframe_to_timedelta(timeframe: TimeFrame) -> timedelta:
        """Converts an alpaca TimeFrame to a timedelta.

        Args:
            timeframe_unit: The alpaca TimeFrame to convert.

        Returns:
            A timedelta representing the specified timeframe unit.
        """

        match timeframe.unit:
            case TimeFrameUnit.Minute:
                return timedelta(minutes=timeframe.amount)
            case TimeFrameUnit.Hour:
                return timedelta(hours=timeframe.amount)
            case TimeFrameUnit.Day:
                return timedelta(days=timeframe.amount)
            case TimeFrameUnit.Week:
                return timedelta(weeks=timeframe.amount)

        raise ValueError(f"Invalid timeframe unit: {timeframe.unit}")

    """Common base class for data_sources/alpaca and brokers/alpaca"""

    @staticmethod
    def _format_datetime(dt):
        return pd.Timestamp(dt).isoformat()

    def __init__(self, config, max_workers=20, chunk_size=100):
        super().__init__()
        # Alpaca authorize 200 requests per minute and per API key
        # Setting the max_workers for multithreading with a maximum
        # of 200
        self.name = "alpaca"
        self.max_workers = min(max_workers, 200)

        # When requesting data for assets for example,
        # if there is too many assets, the best thing to do would
        # be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        self._option_exchange_codes = None
        self._reverse_option_exchange_codes = None

        # Connection to alpaca REST API
        self.config = config

        if isinstance(config, dict) and "API_KEY" in config:
            self.api_key = config["API_KEY"]
        elif hasattr(config, "API_KEY"):
            self.api_key = config.API_KEY
        else:
            raise ValueError("API_KEY not found in config")

        if isinstance(config, dict) and "API_SECRET" in config:
            self.api_secret = config["API_SECRET"]
        elif hasattr(config, "API_SECRET"):
            self.api_secret = config.API_SECRET
        else:
            raise ValueError("API_SECRET not found in config")

        # Get the PAID_ACCOUNT parameter, which defaults to False
        if isinstance(config, dict) and "PAID_ACCOUNT" in config:
            self.is_paid_account = config["PAID_ACCOUNT"]
        elif hasattr(config, "PAID_ACCOUNT"):
            self.is_paid_account = config.PAID_ACCOUNT
        else:
            self.is_paid_account = False

        # If an ENDPOINT is provided, warn the user that it is not used anymore
        # Instead they should use the "PAPER" parameter, which is boolean
        if isinstance(config, dict) and "ENDPOINT" in config:
            logging.warning(
                """The ENDPOINT parameter is not used anymore for AlpacaData, please use the PAPER parameter instead.
                The 'PAPER' parameter is boolean, and defaults to True.
                The ENDPOINT parameter will be removed in a future version of lumibot."""
            )

        # Get the PAPER parameter, which defaults to True
        if isinstance(config, dict) and "PAPER" in config:
            self.is_paper = config["PAPER"]
        elif hasattr(config, "PAPER"):
            self.is_paper = config.PAPER
        else:
            self.is_paper = True

        if isinstance(config, dict) and "VERSION" in config:
            self.version = config["VERSION"]
        elif hasattr(config, "VERSION"):
            self.version = config.VERSION
        else:
            self.version = "v2"

    def get_option_exchange_codes(self):
        """_summary_

        Raises:
            ValueError: Failed to get the option exchange codes from Alpaca

        Returns:
            Exchange code dictionary as supplied by Alpaca and a reverse dictionary with the exchange names in uppercase as keys.
        """
        if self._option_exchange_codes is None:
            client = OptionHistoricalDataClient(self.api_key, self.api_secret)
            self._option_exchange_codes = client.get_option_exchange_codes()
            if (
                (self._option_exchange_codes is None)
                or not len(self._option_exchange_codes)
                or not isinstance(self._option_exchange_codes, dict)
            ):
                self._option_exchange_codes = None
                raise ValueError("Did not get the option exchange codes from Alpaca")
            self._reverse_option_exchange_codes = {
                value.upper(): key for key, value in self._option_exchange_codes.items()
            }
        return self._option_exchange_codes, self._reverse_option_exchange_codes

    def lookup_exchange_code(self, exchange):
        exchange_codes, reverse_option_exchange_codes = self.get_option_exchange_codes()
        # If exchange is already an exchange code key, then return it
        if exchange in exchange_codes:
            return exchange
        # Look for the exchange name in the reverse dictionary
        exchange_code = reverse_option_exchange_codes.get(exchange.upper(), None)
        if exchange_code is None:
            raise ValueError(f"Could not find the exchange code for {exchange}")
        return exchange_code

    def get_chains(self, asset: Asset, quote=None, exchange: str = None):
        """
        This function returns the option chains for a given stock asset.

        Parameters
        ----------
        asset : Asset
            The stock asset to get the option chains for.
        quote : Asset
            The quote asset to get the option chains for (currently not used for Alpaca)
        exchange : str
            The exchange to get the option chains for.

        Returns
        -------
        dict
            A dictionary containing the option chains for the given stock asset.
        """

        if asset.asset_type != "stock":
            raise ValueError("The asset type must be 'stock' to get option chains")

        client = OptionHistoricalDataClient(self.api_key, self.api_secret)
        params = OptionChainRequest(underlying_symbol=asset.symbol)
        alpaca_chain = client.get_option_chain(params)

        if exchange is not None:
            exchange = self.lookup_exchange_code(exchange)

        option_contracts = {
            "Multiplier": None,
            "Exchange": set(),
            "Chains": {"CALL": defaultdict(list), "PUT": defaultdict(list)},
        }

        for snapshot in alpaca_chain.values():
            asset = Asset.symbol2asset(snapshot.symbol)
            if asset.asset_type != "option":
                print(f"Skipping {snapshot} because the symbol is not an option")
                continue
            quote = snapshot.latest_quote
            if quote:
                if exchange:
                    if quote.ask_exchange != exchange or quote.bid_exchange != exchange:
                        continue
                option_contracts["Exchange"].add(quote.ask_exchange)
                option_contracts["Exchange"].add(quote.bid_exchange)
                option_contracts["Chains"][asset.right][asset.expiration.strftime('%Y-%m-%d')].append(asset.strike)

        return option_contracts

    def get_last_price(self, asset, quote=None, exchange=None, **kwargs):
        if quote is not None:
            # If the quote is not None, we use it even if the asset is a tuple
            if isinstance(asset, Asset) and asset.asset_type == "stock":
                symbol = asset.symbol
            elif isinstance(asset, tuple):
                symbol = f"{asset[0].symbol}/{quote.symbol}"
            else:
                symbol = f"{asset.symbol}/{quote.symbol}"
        elif isinstance(asset, tuple):
            symbol = f"{asset[0].symbol}/{asset[1].symbol}"
        else:
            symbol = asset.symbol

        if isinstance(asset, tuple) and asset[0].asset_type == "crypto":
            client = CryptoHistoricalDataClient()
            quote_params = CryptoLatestQuoteRequest(symbol_or_symbols=symbol)
            quote = client.get_crypto_latest_quote(quote_params)

            # Get the first item in the dictionary
            quote = quote[list(quote.keys())[0]]

            # The price is the average of the bid and ask
            price = (quote.bid_price + quote.ask_price) / 2

        elif isinstance(asset, Asset) and asset.asset_type == "crypto":
            client = CryptoHistoricalDataClient()
            quote_params = CryptoLatestQuoteRequest(symbol_or_symbols=symbol)
            quote = client.get_crypto_latest_quote(quote_params)

            # Get the first item in the dictionary
            quote = quote[list(quote.keys())[0]]

            # The price is the average of the bid and ask
            price = (quote.bid_price + quote.ask_price) / 2

        elif isinstance(asset, Asset) and asset.asset_type == "option":
            # Options
            symbol = create_options_symbol(
                asset.symbol,
                asset.expiration,
                asset.right,
                asset.strike,
            )
            client = OptionHistoricalDataClient(self.api_key, self.api_secret)
            params = OptionLatestTradeRequest(symbol_or_symbols=symbol)
            trade = client.get_option_latest_trade(params)[symbol]
            price = trade.price

        else:
            # Stocks
            client = StockHistoricalDataClient(self.api_key, self.api_secret)
            params = StockLatestTradeRequest(symbol_or_symbols=symbol)
            trade = client.get_stock_latest_trade(params)[symbol]
            price = trade.price

        return price

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, start=None, end=None, quote=None, exchange=None, include_after_hours=True
    ):
        """Get bars for a given asset"""
        if isinstance(asset, str):
            asset = Asset(symbol=asset)

        if not timestep:
            timestep = self.get_timestep()

        response = self._pull_source_symbol_bars(
            asset,
            length,
            timestep=timestep,
            timeshift=timeshift,
            start=start,
            end=end,
            quote=quote,
            exchange=exchange,
            include_after_hours=include_after_hours,
        )
        if isinstance(response, float):
            return response
        elif response is None:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote, length=length)
        return bars

    def get_quote(self, asset, quote=None, exchange=None):
        """
        This function returns the quote of an asset.
        Parameters
        ----------
        asset: Asset
            The asset to get the quote for
        quote: Asset
            The quote asset to get the quote for (currently not used for Tradier)
        exchange: str
            The exchange to get the quote for (currently not used for Tradier)

        Returns
        -------
        dict
           Quote of the asset
        """

        if asset.asset_type == "option":
            symbol = create_options_symbol(
                asset.symbol,
                asset.expiration,
                asset.right,
                asset.strike,
            )
        else:
            symbol = asset.symbol

        quotes_df = self.tradier.market.get_quotes([symbol])

        # If the dataframe is empty, return an empty dictionary
        if quotes_df is None or quotes_df.empty:
            return {}

        # Get the quote from the dataframe and convert it to a dictionary
        quote = quotes_df.iloc[0].to_dict()

        # Return the quote
        return quote

    def query_greeks(self, asset: Asset):
        """
        This function returns the greeks of an option as reported by the Tradier API.

        Parameters
        ----------
        asset : Asset
            The option asset to get the greeks for.

        Returns
        -------
        dict
            A dictionary containing the greeks of the option.
        """
        greeks = {}
        stock_symbol = asset.symbol
        expiration = asset.expiration
        option_symbol = create_options_symbol(stock_symbol, expiration, asset.right, asset.strike)
        df_chains = self.tradier.market.get_option_chains(stock_symbol, expiration, greeks=True)
        df = df_chains[df_chains["symbol"] == option_symbol]
        if df.empty:
            return {}

        for col in [x for x in df.columns if 'greeks' in x]:
            greek_name = col.replace('greeks.', '')
            greeks[greek_name] = df[col].iloc[0]
        return greeks

    def get_barset_from_api(self, asset, timeframe, limit=None, start=None, end=None, quote=None):
        """
        gets historical bar data for the given stock symbol
        and time params.

        outputs a dataframe open, high, low, close columns and
        a UTC timezone aware index.
        """
        if isinstance(asset, tuple):
            if quote is None:
                quote = asset[1]
            asset = asset[0]

        if not limit:
            limit = 1000
        # print(f"{limit=}")
        if isinstance(timeframe, str):
            timeframe = self._parse_source_timestep(timeframe)
            timeframe = self._parse_source_timestep(timeframe, reverse=True)
        is_limited_request = not start or not end
        # print(f"{timeframe=}")
        # logging.info(f"{AlpacaData.timeframe_to_timedelta(timeframe)=}")
        # print(f"{AlpacaData.timeframe_to_timedelta(timeframe)=}")
        # print(f"{is_limited_request=}")
        if not start and not end:
            end = datetime.now(timezone.utc)
        if not self.is_paid_account:
            # Alpaca free accounts get 15 minute delayed data
            end = end - timedelta(minutes=15)
        if end:
            end = self.to_default_timezone(end)
        # print(f"{end=}")
        if start:
            start = self.to_default_timezone(start)
            if not end:
                end = start + (limit * AlpacaData.timeframe_to_timedelta(timeframe))
        else:
            start = end - (limit * AlpacaData.timeframe_to_timedelta(timeframe))
        # print(f"{start=}")
        # print(f"{end=}")
        # logging.info(f"{start=}, {end=}")

        if asset.asset_type == "crypto":
            symbol = f"{asset.symbol}/{quote.symbol}"

            client = CryptoHistoricalDataClient()
            params = CryptoBarsRequest(symbol_or_symbols=symbol, timeframe=timeframe, start=start, end=end, limit=limit)
            barset = client.get_crypto_bars(params)

        elif asset.asset_type == "option":
            symbol = create_options_symbol(
                asset.symbol,
                asset.expiration,
                asset.right,
                asset.strike,
            )

            client = OptionHistoricalDataClient(self.api_key, self.api_secret)
            params = OptionBarsRequest(symbol_or_symbols=symbol, timeframe=timeframe, start=start, end=end, limit=limit)

            try:
                barset = client.get_option_bars(params)
            except Exception as e:
                logging.error(f"Could not get options data from Alpaca for {symbol} with the following error: {e}")
                return None
        else:
            symbol = asset.symbol

            client = StockHistoricalDataClient(self.api_key, self.api_secret)
            params = StockBarsRequest(symbol_or_symbols=symbol, timeframe=timeframe, start=start, end=end, limit=limit)
            # logging.info(f"StockBarsRequest: {params=}")
            # print(f"StockBarsRequest: {params=}")
            try:
                barset = client.get_stock_bars(params)
            except Exception as e:
                logging.error(f"Could not get pricing data from Alpaca for {symbol} with the following error: {e}")
                return None

        df = barset.df

        # Alpaca now returns a dataframe with a MultiIndex. We only want an index of timestamps
        df = df.reset_index(level=0, drop=True)

        if df.empty:
            logging.error(f"Could not get any pricing data from Alpaca for {symbol}, the DataFrame came back empty")
            return None

        df = df[~df.index.duplicated(keep="first")]
        df = df.iloc[-limit:]
        df = df[df.close > 0]

        if is_limited_request and (len(df) < limit):
            logging.warning(
                f"Dataframe for {symbol} has {len(df)} rows while {limit} were requested. Further data does not exist for Alpaca"
            )

        return df

    def _pull_source_bars(
        self, assets, length, timestep=MIN_TIMESTEP, timeshift=None, start=None, end=None, quote=None, include_after_hours=True
    ):
        """pull broker bars for a list assets"""
        if not self.is_paid_account and (timeshift is None and timestep == "day"):
            # Alpaca throws an error if we don't do this and don't have a data subscription because
            # they require a subscription for historical data less than 15 minutes old
            timeshift = timedelta(minutes=16)

        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)

        if timeshift:
            if end is None:
                end = datetime.now()
            end = end - timeshift

        result = {}
        for asset in assets:
            data = self.get_barset_from_api(asset, parsed_timestep, length, start=start, end=end, quote=quote)
            result[asset] = data

        return result

    def _pull_source_symbol_bars(
        self, asset, length, timestep=MIN_TIMESTEP, timeshift=None, start=None, end=None, quote=None, exchange=None, include_after_hours=True
    ):
        if exchange is not None:
            logging.warning(
                f"the exchange parameter is not implemented for AlpacaData, but {exchange} was passed as the exchange"
            )

        """pull broker bars for a given asset"""
        response = self._pull_source_bars([asset], length, timestep=timestep, timeshift=timeshift, start=start, end=end, quote=quote)
        return response[asset]

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        # TODO: Alpaca return should also include dividend yield
        response["return"] = response["close"].pct_change()
        bars = Bars(response, self.SOURCE, asset, raw=response, quote=quote)
        return bars
