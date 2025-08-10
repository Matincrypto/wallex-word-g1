import requests
import json
import time
import logging
import asyncio
import sqlite3
import websockets
from telegram import Bot
from telegram.constants import ParseMode
from datetime import datetime, timedelta
import pytz
from decimal import Decimal

# --- Logging Configuration ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Helper Functions ---

def load_config():
    """Loads the configuration from config.json file."""
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.critical("FATAL: config.json not found. Please create it.")
        exit()

# --- Database Management ---
DB_FILE = 'signals.db'

def setup_database():
    """Initializes the database and creates the signals table if it doesn't exist."""
    try:
        con = sqlite3.connect(DB_FILE)
        cur = con.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                wallex_price REAL NOT NULL,
                coincatch_price REAL NOT NULL,
                percentage_diff REAL NOT NULL,
                timestamp DATETIME NOT NULL
            )
        ''')
        cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_check ON signals (symbol, action, timestamp);")
        con.commit()
        con.close()
        logger.info("Database setup complete. 'signals' table is ready.")
    except Exception as e:
        logger.critical(f"FATAL: Could not set up database: {e}")
        exit()

def save_signal_to_db(symbol, action, wallex_price, coincatch_price, percentage_diff):
    """Saves a new signal to the database."""
    try:
        con = sqlite3.connect(DB_FILE)
        cur = con.cursor()
        cur.execute(
            "INSERT INTO signals (symbol, action, wallex_price, coincatch_price, percentage_diff, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
            (symbol, action, wallex_price, coincatch_price, percentage_diff, datetime.now(pytz.utc))
        )
        con.commit()
        con.close()
        logger.info(f"Successfully saved signal for {symbol} to database.")
    except Exception as e:
        logger.error(f"Error saving signal for {symbol} to DB: {e}")

def check_for_recent_signal(symbol, action, window_minutes):
    """Checks if a similar signal was sent within the defined time window."""
    try:
        con = sqlite3.connect(DB_FILE)
        cur = con.cursor()
        threshold_time = datetime.now(pytz.utc) - timedelta(minutes=window_minutes)
        cur.execute(
            "SELECT 1 FROM signals WHERE symbol = ? AND action = ? AND timestamp > ? LIMIT 1",
            (symbol, action, threshold_time)
        )
        exists = cur.fetchone()
        con.close()
        return exists is not None
    except Exception as e:
        logger.error(f"Error checking recent signals for {symbol}: {e}")
        return False

# --- WebSocket & API Functions ---

class WallexWebsocketManager:
    """Manages WebSocket connection to Wallex for real-time ask data."""
    def __init__(self, url):
        self.url = url
        self.order_books = {}
        self.connection = None
        self._is_running = False

    async def connect_and_listen(self):
        self._is_running = True
        while self._is_running:
            try:
                logger.info(f"Connecting to Wallex WebSocket at {self.url}...")
                async with websockets.connect(self.url) as ws:
                    self.connection = ws
                    logger.info("Successfully connected to Wallex WebSocket.")
                    if self.order_books:
                         await self.subscribe_to_streams(list(self.order_books.keys()))
                    await self._listen()
            except (websockets.ConnectionClosed, ConnectionRefusedError) as e:
                logger.warning(f"Wallex WebSocket connection lost: {e}. Reconnecting in 15 seconds...")
                self.connection = None
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"An unexpected error occurred with Wallex WebSocket: {e}", exc_info=True)
                self._is_running = False

    async def _listen(self):
        try:
            async for message in self.connection:
                try:
                    data = json.loads(message)
                    if isinstance(data, list) and len(data) == 2:
                        channel_name, orders_data = data
                        if "@sellDepth" in channel_name:
                            symbol = channel_name.split('@')[0]
                            if symbol not in self.order_books:
                                self.order_books[symbol] = {}
                            processed_orders = [[Decimal(order['price']), Decimal(order['quantity'])] for order in orders_data]
                            self.order_books[symbol]['asks'] = processed_orders
                except Exception as e:
                    logger.error(f"Error processing WebSocket message: {e}")
        except websockets.ConnectionClosed as e:
            logger.warning(f"Listen loop terminated as connection closed: {e}")
        except AttributeError:
             logger.warning("Listen loop aborted, connection object is not available.")

    async def subscribe_to_streams(self, symbols):
        if not self.connection:
             logger.warning("Cannot subscribe, WebSocket is not yet connected.")
             return
        logger.info(f"Subscribing to Sell-Depth for {len(symbols)} symbols...")
        for symbol in symbols:
            if symbol not in self.order_books:
                self.order_books[symbol] = {}
            subscription_message = ["subscribe", {"channel": f"{symbol}@sellDepth"}]
            try:
                await self.connection.send(json.dumps(subscription_message))
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Failed to send subscription for {symbol}@sellDepth: {e}")
                return
        logger.info("Finished sending all subscription requests.")

    def get_weighted_avg_ask_price(self, symbol, depth=5):
        order_book = self.order_books.get(symbol)
        if not order_book or not order_book.get('asks'):
            return None
        orders = order_book['asks'][:depth]
        if not orders: return None
        total_value = Decimal(0)
        total_volume = Decimal(0)
        for price, volume in orders:
            total_value += price * volume
            total_volume += volume
        return float(total_value / total_volume) if total_volume > 0 else None

    def stop(self):
        self._is_running = False

def get_coincatch_prices(config):
    """Fetches all last trade prices from CoinCatch."""
    try:
        url = config['price_sources']['coincatch']['base_url'] + config['price_sources']['coincatch']['tickers_endpoint']
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json().get('data', [])
        
        # Using the correct key 'close' to get the price
        prices = {
            ticker.get('symbol', '').replace('-', ''): float(price)
            for ticker in data
            if (price := ticker.get('close')) is not None
        }
        
        logger.info(f"Fetched {len(prices)} last trade prices from CoinCatch.")
        return {k: v for k, v in prices.items() if k and v}
    except Exception as e:
        logger.error(f"Error fetching CoinCatch prices: {e}")
        return {}


def escape_markdown(text):
    text = str(text)
    escape_chars = '_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{char}' if char in escape_chars else char for char in text)

def get_wallex_usdt_markets(config):
    try:
        url = config['price_sources']['wallex']['base_url'] + config['price_sources']['wallex']['markets_endpoint']
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json().get("result", {}).get("symbols", {})
        return {s: d for s, d in data.items() if d.get('quoteAsset') == 'USDT'}
    except Exception as e:
        logger.error(f"Error fetching Wallex markets: {e}")
        return {}

async def send_telegram_message(config, message_text):
    bot_token = config['telegram']['bot_token']
    chat_id = config['telegram']['group_chat_id']
    thread_id = config['telegram']['message_thread_id']
    if not bot_token: return
    try:
        bot = Bot(token=bot_token)
        await bot.send_message(
            chat_id=chat_id, text=message_text, parse_mode=ParseMode.MARKDOWN_V2,
            message_thread_id=thread_id, disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Error sending Telegram message: {e}")

# --- Main Analysis Logic ---
async def analyze_prices(config, ws_manager):
    logger.info("--- Starting New Cycle (Strategy: Wallex Ask vs CoinCatch Last Price) ---")
    
    coincatch_last_prices = get_coincatch_prices(config)
    if not coincatch_last_prices:
        logger.warning("Could not fetch CoinCatch prices. Skipping cycle.")
        return

    wallex_symbols = list(ws_manager.order_books.keys())
    dedup_window = config['settings']['deduplication_window_minutes']
    signal_threshold = config['settings']['price_difference_threshold']
    
    print("================== ANALYSIS ==================")
    for symbol in wallex_symbols:
        wallex_avg_ask = ws_manager.get_weighted_avg_ask_price(symbol)
        if not wallex_avg_ask:
            continue

        coincatch_last_price = coincatch_last_prices.get(symbol)
        if not coincatch_last_price:
            continue
        
        profit_pct = ((coincatch_last_price - wallex_avg_ask) / wallex_avg_ask) * 100
        print(f"📊 {symbol:<10} | Wallex Ask: {wallex_avg_ask:,.4f} $ | CoinCatch Last: {coincatch_last_price:,.4f} $ | Diff: {profit_pct:+.2f}%")
            
        if profit_pct >= signal_threshold:
            print(f"🔥🔥🔥 [BUY on Wallex] Signal for {symbol}: CoinCatch is {profit_pct:.2f}% more expensive 🔥🔥🔥")
            action = "BUY"
            if not check_for_recent_signal(symbol, action, dedup_window):
                save_signal_to_db(symbol, action, wallex_avg_ask, coincatch_last_price, profit_pct)
                
                message = (f"🟢 {escape_markdown(symbol)}*\n\n"
                            f"Enter: `${escape_markdown(f'{wallex_avg_ask:,.4f}')}`\n"
                            f"Trget: `${escape_markdown(f'{coincatch_last_price:,.4f}')}`\n"
                            f"💰 *Profit: {escape_markdown(f'{profit_pct:.2f}')}\\%*")
                await send_telegram_message(config, message)
    print("================ END OF ANALYSIS ================\n")


async def main():
    config = load_config()
    setup_database()
    ws_manager = WallexWebsocketManager(config['price_sources']['wallex']['websocket_url'])
    ws_task = asyncio.create_task(ws_manager.connect_and_listen())
    
    wallex_usdt_markets = get_wallex_usdt_markets(config)
    if not wallex_usdt_markets:
        logger.critical("Could not fetch Wallex markets to subscribe. Exiting.")
        ws_manager.stop()
        await ws_task
        return
        
    await asyncio.sleep(5)
    await ws_manager.subscribe_to_streams(list(wallex_usdt_markets.keys()))
    
    try:
        while True:
            await analyze_prices(config, ws_manager)
            wait_time = config['settings']['check_interval_seconds']
            logger.info(f"--- Cycle Complete. Waiting for {wait_time} seconds. ---")
            await asyncio.sleep(wait_time)
    except asyncio.CancelledError:
        logger.info("Main loop cancelled.")
    finally:
        logger.info("Shutting down WebSocket manager...")
        ws_manager.stop()
        await ws_task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user. Exiting.")