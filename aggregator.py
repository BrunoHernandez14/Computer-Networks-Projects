import websocket
import json
import threading
import time
import pickle
from datetime import datetime
from pathlib import Path
import requests
from textblob import TextBlob

class MarketDataAggregator:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()
        self.data_file = Path('market_data.pkl')
        self.sentiment_file = Path('sentiment_data.pkl')
        self.sentiment_data = {
            'reddit': [],
            'news': [],
            'overall_sentiment': 0,
            'last_update': None
        }
        
    def update_data(self, exchange, product, bid, ask, volume):
        """Thread-safe data update"""
        with self.lock:
            if exchange not in self.data:
                self.data[exchange] = {}
            
            spread = ask - bid
            spread_percent = (spread / bid) * 100 if bid > 0 else 0
            
            self.data[exchange][product] = {
                'bid': bid,
                'ask': ask,
                'spread': spread,
                'spread_percent': spread_percent,
                'volume': volume,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Save data to file for Streamlit to read
            self.save_data()
    
    def save_data(self):
        """Save data to pickle file"""
        try:
            with open(self.data_file, 'wb') as f:
                pickle.dump(self.data, f)
        except Exception as e:
            print(f"Error saving data: {e}")
    
    def analyze_sentiment(self, text):
        """Analyze sentiment using TextBlob"""
        try:
            analysis = TextBlob(text)
            return analysis.sentiment.polarity
        except:
            return 0
    
    def fetch_reddit_sentiment(self):
        """Fetch posts from crypto subreddits"""
        try:
            subreddits = ['cryptocurrency', 'bitcoin', 'ethereum']
            reddit_sentiments = []
            
            for subreddit in subreddits:
                url = f'https://www.reddit.com/r/{subreddit}/hot.json?limit=10'
                headers = {'User-Agent': 'CryptoSentimentBot/1.0'}
                
                print(f"  Fetching r/{subreddit}...")
                response = requests.get(url, headers=headers, timeout=10)
                print(f"  Status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    for post in data['data']['children']:
                        title = post['data']['title']
                        sentiment = self.analyze_sentiment(title)
                        
                        reddit_sentiments.append({
                            'source': f'r/{subreddit}',
                            'text': title,
                            'sentiment': sentiment,
                            'score': post['data']['score'],
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                    
                    print(f"   Fetched {len(data['data']['children'])} posts")
                
                time.sleep(2)
            
            print(f"Total Reddit posts: {len(reddit_sentiments)}")
            return reddit_sentiments
        except Exception as e:
            print(f"Error fetching Reddit data: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def fetch_crypto_news(self):
        """Fetch crypto news headlines"""
        try:
            print("  Fetching crypto news...")
            news_sentiments = []
            
            url = 'https://min-api.cryptocompare.com/data/v2/news/?lang=EN'
            
            response = requests.get(url, timeout=10)
            print(f"  News API Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                if 'Data' in data:
                    for article in data['Data'][:10]:
                        title = article.get('title', '')
                        sentiment = self.analyze_sentiment(title)
                        
                        news_sentiments.append({
                            'source': article.get('source', 'Unknown'),
                            'text': title,
                            'sentiment': sentiment,
                            'url': article.get('url', ''),
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                    
                    print(f"   Fetched {len(news_sentiments)} news articles")
            
            return news_sentiments
        except Exception as e:
            print(f" Error fetching news: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def update_sentiment_data(self):
        """Update sentiment analysis data"""
        print("\nFetching sentiment data...")
        
        reddit_data = self.fetch_reddit_sentiment()
        news_data = self.fetch_crypto_news()
        
        print(f"\nSummary:")
        print(f"  Reddit posts: {len(reddit_data)}")
        print(f"  News articles: {len(news_data)}")
        
        if reddit_data or news_data:
            all_sentiments = [item['sentiment'] for item in reddit_data + news_data]
            overall_sentiment = sum(all_sentiments) / len(all_sentiments) if all_sentiments else 0
            
            print(f"  Overall sentiment: {overall_sentiment:.3f}")
            
            with self.lock:
                self.sentiment_data = {
                    'reddit': reddit_data[-20:],
                    'news': news_data[-20:],
                    'overall_sentiment': overall_sentiment,
                    'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                try:
                    with open(self.sentiment_file, 'wb') as f:
                        pickle.dump(self.sentiment_data, f)
                    print(f" Sentiment data saved to {self.sentiment_file}")
                except Exception as e:
                    print(f" Error saving sentiment data: {e}")
        else:
            print(" No sentiment data collected")
    
    def display_data(self):
        """Display aggregated data"""
        with self.lock:
            print("\n" + "="*80)
            print(f"{'MARKET DATA AGGREGATOR':^80}")
            print(f"{'Updated: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'):^80}")
            print("="*80)
            
            for exchange, products in self.data.items():
                print(f"\n{exchange.upper()}")
                print("-" * 80)
                
                for product, data in products.items():
                    print(f"\n  Product: {product}")
                    print(f"  Best Bid: ${data['bid']:,.2f}")
                    print(f"  Best Ask: ${data['ask']:,.2f}")
                    print(f"  Spread: ${data['spread']:.2f} ({data['spread_percent']:.3f}%)")
                    print(f"  24h Volume: {data['volume']:,.2f}")
                    print(f"  Last Update: {data['timestamp']}")
            
            print("\n" + "="*80 + "\n")

# Global aggregator instance
aggregator = MarketDataAggregator()

# ============= COINBASE WEBSOCKET =============
def coinbase_on_message(ws, message):
    data = json.loads(message)
    
    if data.get('type') == 'ticker':
        product = data.get('product_id')
        best_bid = float(data.get('best_bid', 0))
        best_ask = float(data.get('best_ask', 0))
        volume_24h = float(data.get('volume_24h', 0))
        
        aggregator.update_data('coinbase', product, best_bid, best_ask, volume_24h)

def coinbase_on_error(ws, error):
    print(f"Coinbase Error: {error}")

def coinbase_on_close(ws, close_status_code, close_msg):
    print("Coinbase connection closed")

def coinbase_on_open(ws):
    print(" Connected to Coinbase")
    
    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channels": ["ticker"]
    }
    
    ws.send(json.dumps(subscribe_message))

def start_coinbase():
    ws = websocket.WebSocketApp(
        "wss://ws-feed.exchange.coinbase.com",
        on_open=coinbase_on_open,
        on_message=coinbase_on_message,
        on_error=coinbase_on_error,
        on_close=coinbase_on_close
    )
    ws.run_forever()

# ============= KRAKEN WEBSOCKET =============
def kraken_on_message(ws, message):
    data = json.loads(message)
    
    if isinstance(data, list) and len(data) >= 4:
        if isinstance(data[1], dict) and 'a' in data[1] and 'b' in data[1]:
            ticker_data = data[1]
            pair = data[3]
            
            best_ask = float(ticker_data['a'][0])
            best_bid = float(ticker_data['b'][0])
            volume_24h = float(ticker_data['v'][1])
            
            aggregator.update_data('kraken', pair, best_bid, best_ask, volume_24h)

def kraken_on_error(ws, error):
    print(f"Kraken Error: {error}")

def kraken_on_close(ws, close_status_code, close_msg):
    print("Kraken connection closed")

def kraken_on_open(ws):
    print("Connected to Kraken")
    
    subscribe_message = {
        "event": "subscribe",
        "pair": ["XBT/USD", "ETH/USD"],
        "subscription": {"name": "ticker"}
    }
    
    ws.send(json.dumps(subscribe_message))

def start_kraken():
    ws = websocket.WebSocketApp(
        "wss://ws.kraken.com",
        on_open=kraken_on_open,
        on_message=kraken_on_message,
        on_error=kraken_on_error,
        on_close=kraken_on_close
    )
    ws.run_forever()

# ============= MAIN EXECUTION =============
if __name__ == "__main__":
    print("Starting Market Data Aggregator...")
    print("Connecting to exchanges...\n")
    
    # Start Coinbase WebSocket in a thread
    coinbase_thread = threading.Thread(target=start_coinbase, daemon=True)
    coinbase_thread.start()
    
    # Start Kraken WebSocket in a thread
    kraken_thread = threading.Thread(target=start_kraken, daemon=True)
    kraken_thread.start()
    
    # Wait for connections to establish
    time.sleep(2)
    
    print("\nPress Ctrl+C to exit\n")
    
    # Fetch sentiment data immediately on startup
    print("Fetching initial sentiment data...")
    aggregator.update_sentiment_data()
    
    try:
        # Display data every 5 seconds
        counter = 0
        while True:
            time.sleep(5)
            aggregator.display_data()
            
            # Update sentiment data every 5 minutes (60 cycles)
            counter += 1
            if counter % 60 == 0:
                aggregator.update_sentiment_data()
                counter = 0
    except KeyboardInterrupt:
        print("\n\nShutting down...")