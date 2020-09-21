from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread

import requests
import datetime as dt
import time, logging, math

import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL

class BlueprintBot:
    def __init__(
        self, api_key, api_secret, api_base_url="https://paper-api.alpaca.markets",
        version='v2', logfile=None, max_workers=200, chunk_size=100,
        minutes_before_closing=15, sleeptime=1, debug=False
    ):

        #Setting Logging to both console and a file if logfile is specified
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("requests").setLevel(logging.ERROR)
        self.logfile = logfile
        logger = logging.getLogger()
        if debug:
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.DEBUG)

        logFormater = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s")
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormater)
        logger.addHandler(consoleHandler)
        if logfile:
            fileHandler = logging.FileHandler(logfile, mode='w')
            fileHandler.setFormatter(logFormater)
            logger.addHandler(fileHandler)

        #Alpaca authorize 200 requests per minute and per API key
        #Setting the max_workers for multithreading to 200
        #to go full speed if needed
        self.max_workers = min(max_workers, 200)

        #When requesting data for assets for example,
        #if there is too many assets, the best thing to do would
        #be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        #Setting how many minutes before market closes
        #The bot should stop
        self.minutes_before_closing = minutes_before_closing

        #Timesleep after each on_market_open execution
        self.sleeptime = sleeptime

        #Connection to alpaca REST API
        self.alpaca = tradeapi.REST(api_key, api_secret, URL(api_base_url), version)

        #Connection to alpaca socket stream
        self.stream = tradeapi.StreamConn(api_key, api_secret, URL(api_base_url))
        self.set_streams()

        # getting the account object
        self.account = self.get_account()

    #======Builtin helper functions=========
    def get_positions(self):
        """Get the account positions"""
        positions = self.alpaca.list_positions()
        return positions

    def get_open_orders(self):
        """Get the account open orders"""
        orders = self.alpaca.list_orders(status="open")
        return orders

    def cancel_open_orders(self):
        """Cancel all the buying orders with status still open"""
        orders = self.alpaca.list_orders(status="open")
        for order in orders:
            logging.info("Market order of | %d %s %s | completed." % (int(order.qty), order.symbol, order.side))
            self.alpaca.cancel_order(order.id)

    def get_ongoing_assets(self):
        """Get the list of symbols for positions
        and open orders"""
        orders = self.get_open_orders()
        positions = self.get_positions()
        result = [o.symbol for o in orders] + [p.symbol for p in positions]
        return list(set(result))

    def is_market_open(self):
        """return True if market is open else false"""
        isOpen = self.alpaca.get_clock().is_open
        return isOpen

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        clock = self.alpaca.get_clock()
        opening_time = clock.next_open.replace(tzinfo=dt.timezone.utc).timestamp()
        curr_time = clock.timestamp.replace(tzinfo=dt.timezone.utc).timestamp()
        time_to_open = opening_time - curr_time
        return time_to_open

    def get_time_to_close(self):
        """Return the remaining time for the market to close in seconds"""
        clock = self.alpaca.get_clock()
        closing_time = clock.next_close.replace(tzinfo=dt.timezone.utc).timestamp()
        curr_time = clock.timestamp.replace(tzinfo=dt.timezone.utc).timestamp()
        time_to_close = closing_time - curr_time
        return time_to_close

    def await_market_to_open(self):
        """Executes infinite loop until market opens"""
        isOpen = self.is_market_open()
        while(not isOpen):
            time_to_open = self.get_time_to_open()
            if time_to_open > 60 * 60:
                delta = dt.timedelta(seconds=time_to_open)
                logging.info("Market will open in %s." % str(delta))
                time.sleep(60 *60)
            elif time_to_open > 60:
                logging.info("%d minutes til market open." % int(time_to_open / 60))
                time.sleep(60)
            else:
                logging.info("%d seconds til market open." % time_to_open)
                time.sleep(time_to_open)

            isOpen = self.is_market_open()

    def await_market_to_close(self):
        """Sleep until market closes"""
        isOpen = self.is_market_open()
        if isOpen:
            time_to_close = self.get_time_to_close()
            sleeptime = max(0, time_to_close)
            time.sleep(sleeptime)

    def get_account(self):
        """Get the account data from the API"""
        account = self.alpaca.get_account()
        return account

    def get_tradable_assets(self):
        """Get the list of all tradable assets from the market"""
        assets = self.alpaca.list_assets()
        assets = [asset for asset in assets if asset.tradable]
        return assets

    def get_last_price(self, symbol):
        """Takes and asset symbol and returns the last known price"""
        bars = self.alpaca.get_barset(symbol, 'minute', 1)
        last_price = bars[symbol][0].c
        return last_price

    def get_chunks(self, l, chunk_size=None):
        if chunk_size is None: chunk_size=self.chunk_size
        chunks = []
        for i in range(0, len(l), chunk_size):
            chunks.append(l[i:i + chunk_size])
        return chunks

    def get_bars(self, symbols, time_unity, length):
        bar_sets = {}
        chunks = self.get_chunks(symbols)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            tasks = []
            for chunk in chunks:
                tasks.append(executor.submit(
                    self.alpaca.get_barset, chunk, time_unity, length
                ))

            for task in as_completed(tasks):
                bar_sets.update(task.result())

        return bar_sets

    def submit_order(self, symbol, quantity, side, limit_price=None, stop_price=None):
        """Submit an order for an asset"""
        if(quantity > 0):
            try:
                order_type = 'limit' if limit_price else 'market'
                order_class = 'oto' if stop_price else None
                time_in_force = 'day'
                kwargs = {
                    'type': order_type,
                    'order_class': order_class,
                    'time_in_force': time_in_force,
                    'limit_price': limit_price,
                }
                if stop_price:
                    kwargs['stop_loss'] = {'stop_price': stop_price}

                #Remove items with None values
                kwargs = { k:v for k,v in kwargs.items() if v }
                self.alpaca.submit_order(symbol, quantity, side, **kwargs)
                return True
            except Exception as e:
                message = str(e)
                if "stop price must not be greater than base price / 1.001" in message:
                    logging.info(
                        "Order of | %d %s %s | did not go through because the share base price became lesser than the stop loss price." %
                        (quantity, symbol, side)
                    )
                    return False
                else:
                    logging.debug(
                        "Order of | %d %s %s | did not go through. The following error occured: %s" %
                        (quantity, symbol, side, e)
                    )
                    return False
        else:
            logging.info("Order of | %d %s %s | not completed" % (quantity, symbol, side))
            return True

    def submit_orders(self, orders):
        """submit orders"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            tasks = []
            for order in orders:
                symbol = order.get('symbol')
                quantity = order.get('quantity')
                side = order.get('side')

                func = lambda args, kwargs: self.submit_order(*args, **kwargs)
                args = (symbol, quantity, side)
                kwargs = {}
                if order.get('stop_price'): kwargs['stop_price'] = order.get('stop_price')
                if order.get('limit_price'): kwargs['limit_price'] = order.get('limit_price')

                tasks.append(executor.submit(
                    func, args, kwargs
                ))

    def sell_all(self, cancel_open_orders=True):
        """sell all positions"""
        orders = []
        positions = self.get_positions()
        for position in positions:
            order = {
                'symbol': position.symbol,
                'quantity': int(position.qty),
                'side': 'sell'
            }
            orders.append(order)
        self.submit_orders(orders)

        if cancel_open_orders:
            self.cancel_open_orders()

    #=======Stream Events========================

    @staticmethod
    def log_trade_event(data):
        order = data.order
        type_event = data.event
        symbol = order.get('symbol')
        side = order.get('side')
        order_quantity = order.get('qty')
        order_type = order.get('order_type').capitalize()

        # if statement on event type
        if type_event == 'fill':
            price = data.price
            filled_quantity = data.qty
            logging.info(
                "%s order of | %s %s %s | filled. %s$ per share" %
                (order_type, filled_quantity, symbol, side, price)
            )
            if order_quantity != filled_quantity:
                logging.info(
                    "Initial %s order of | %s %s %s | completed." %
                    (order_type, order_quantity, symbol, side)
                )

        elif type_event == 'partial_fill':
            price = data.price
            filled_quantity = data.qty
            logging.info(
                "%s order of | %s %s %s | partially filled." %
                (order_type, order_quantity, symbol, side)
            )
            logging.info(
                "%s order of | %s %s %s | completed. %s$ per share" %
                (order_type, filled_quantity, symbol, side, price)
            )

        elif type_event == 'new':
            logging.info(
                "New %s order of | %s %s %s | submited." %
                (order_type, order_quantity, symbol, side)
            )

        elif type_event == 'canceled':
            logging.info(
                "%s order of | %s %s %s | canceled." %
                (order_type, order_quantity, symbol, side)
            )

        else:
            logging.debug(
                "Unhandled type event %s for %s order of | %s %s %s |" %
                (type_event, order_type, order_quantity, symbol, side)
            )

    @staticmethod
    def on_trade_event(data):
        """Overload this method to trigger customized actions
        after a trade_update event is sent via Socket Streams"""
        pass

    def set_streams(self):
        """Set the asynchronous actions to be executed after
        when events are sent via socket streams"""
        @self.stream.on(r'^trade_updates$')
        async def default_on_trade_event(conn, channel, data):
            BlueprintBot.log_trade_event(data)
            BlueprintBot.on_trade_event(data)

        t = Thread(target=self.stream.run, args=[['trade_updates']])
        t.start()

    #=======Lifecycle methods====================

    def initialize(self):
        """Use this lifecycle method to initialize parameters"""
        pass

    def before_market_opens(self):
        """Lifecycle method executed before market opens
        Example: self.cancel_open_orders()"""
        pass

    def on_market_open(self):
        """Use this lifecycle method for trading.
        Will be executed indefinetly until there
        will be only self.minutes_before_closing
        minutes before market closes"""
        pass

    def before_market_closes(self):
        """Use this lifecycle method to execude code
        self.minutes_before_closing minutes before closing.
        Example: self.sell_all()"""
        pass

    def after_market_closes(self):
        """Use this lifecycle method to execute code
        after market closes. Exampling: dumping stats/reports"""
        pass

    def on_bot_crash(self):
        """Use this lifecycle method to execute code
        when an exception is raised and the bot crashes"""
        pass

    def run(self):
        """The main execution point.
        Execute the lifecycle methods"""
        try:
            logging.info("Executing the initialize lifecycle method")
            self.initialize()

            logging.info("Executing the before_market_opens lifecycle method")
            if not self.is_market_open():
                self.before_market_opens()

            self.await_market_to_open()
            time_to_close = self.get_time_to_close()
            while time_to_close > self.minutes_before_closing * 60:
                logging.info("Executing the on_market_open lifecycle method")
                self.on_market_open()
                time_to_close = self.get_time_to_close()
                sleeptime = time_to_close - 15 * 60
                sleeptime = max(min(sleeptime, 60 * self.sleeptime), 0)
                logging.info("Sleeping for %d seconds" % sleeptime)
                time.sleep(sleeptime)

            if self.is_market_open():
                logging.info("Executing the before_market_closes lifecycle method")
                self.before_market_closes()

            self.await_market_to_close()
            logging.info("Executing the after_market_closes lifecycle method")
            self.after_market_closes()
        except:
            self.on_bot_crash()
