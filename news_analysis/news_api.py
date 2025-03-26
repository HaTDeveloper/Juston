"""
News API Integration Module
-------------------------
This module handles integration with external news APIs for additional news sources.
It includes functionality for:
- Fetching news from financial news APIs
- Parsing and normalizing API responses
- Caching API responses to avoid rate limits
"""

import os
import json
import time
import logging
import requests
import datetime
from typing import List, Dict, Any, Optional
from requests.exceptions import RequestException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("news_api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class NewsAPIClient:
    """Class to handle integration with external news APIs"""
    
    def __init__(self, cache_dir: str = None):
        """
        Initialize the news API client
        
        Args:
            cache_dir: Directory to store API response cache
        """
        self.api_keys = {
            'alpha_vantage': os.environ.get('ALPHA_VANTAGE_API_KEY', ''),
            'newsapi': os.environ.get('NEWS_API_KEY', ''),
            'finnhub': os.environ.get('FINNHUB_API_KEY', '')
        }
        
        self.cache_dir = cache_dir or os.path.join(os.path.dirname(__file__), 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Cache expiration in seconds
        self.cache_expiration = 3600  # 1 hour
    
    def _get_cache_path(self, api_name: str, query: str) -> str:
        """
        Get cache file path for an API request
        
        Args:
            api_name: Name of the API
            query: Query string or identifier
            
        Returns:
            Path to cache file
        """
        # Create a safe filename from the query
        safe_query = "".join(c if c.isalnum() else "_" for c in query)
        return os.path.join(self.cache_dir, f"{api_name}_{safe_query}.json")
    
    def _get_from_cache(self, api_name: str, query: str) -> Optional[Dict[str, Any]]:
        """
        Get cached API response
        
        Args:
            api_name: Name of the API
            query: Query string or identifier
            
        Returns:
            Cached response or None if not found or expired
        """
        cache_path = self._get_cache_path(api_name, query)
        
        try:
            if not os.path.exists(cache_path):
                return None
            
            # Check if cache is expired
            cache_time = os.path.getmtime(cache_path)
            if time.time() - cache_time > self.cache_expiration:
                logger.info(f"Cache expired for {api_name} query: {query}")
                return None
            
            # Read cache file
            with open(cache_path, 'r') as f:
                cached_data = json.load(f)
            
            logger.info(f"Retrieved from cache: {api_name} query: {query}")
            return cached_data
            
        except Exception as e:
            logger.error(f"Error reading cache for {api_name} query {query}: {str(e)}")
            return None
    
    def _save_to_cache(self, api_name: str, query: str, data: Dict[str, Any]) -> bool:
        """
        Save API response to cache
        
        Args:
            api_name: Name of the API
            query: Query string or identifier
            data: API response data
            
        Returns:
            True if saved successfully, False otherwise
        """
        cache_path = self._get_cache_path(api_name, query)
        
        try:
            with open(cache_path, 'w') as f:
                json.dump(data, f)
            
            logger.info(f"Saved to cache: {api_name} query: {query}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving cache for {api_name} query {query}: {str(e)}")
            return False
    
    def get_alpha_vantage_news(self, symbols: List[str] = None, topics: List[str] = None) -> List[Dict[str, Any]]:
        """
        Get news from Alpha Vantage API
        
        Args:
            symbols: List of stock symbols
            topics: List of news topics
            
        Returns:
            List of news articles
        """
        api_key = self.api_keys.get('alpha_vantage')
        if not api_key:
            logger.error("Alpha Vantage API key not found")
            return []
        
        # Create query identifier
        query_id = f"symbols={'_'.join(symbols) if symbols else 'none'}_topics={'_'.join(topics) if topics else 'none'}"
        
        # Check cache
        cached_data = self._get_from_cache('alpha_vantage', query_id)
        if cached_data:
            return cached_data.get('feed', [])
        
        # Prepare request parameters
        params = {
            'function': 'NEWS_SENTIMENT',
            'apikey': api_key
        }
        
        if symbols:
            params['tickers'] = ','.join(symbols)
        
        if topics:
            params['topics'] = ','.join(topics)
        
        try:
            logger.info(f"Fetching news from Alpha Vantage: {query_id}")
            response = requests.get('https://www.alphavantage.co/query', params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Save to cache
            self._save_to_cache('alpha_vantage', query_id, data)
            
            # Extract and normalize articles
            articles = []
            for item in data.get('feed', []):
                article = {
                    'title': item.get('title', ''),
                    'url': item.get('url', ''),
                    'published_date': datetime.datetime.fromisoformat(item.get('time_published', '').replace('T', ' ').split(' ')[0]),
                    'source': item.get('source', ''),
                    'language': 'en',  # Alpha Vantage provides English news
                    'content': item.get('summary', ''),
                    'sentiment': {
                        'compound': float(item.get('overall_sentiment_score', 0)),
                        'positive': float(item.get('ticker_sentiment', [{}])[0].get('ticker_sentiment_score', 0)) if item.get('ticker_sentiment') else 0,
                        'negative': 0,  # Not provided by Alpha Vantage
                        'neutral': 0    # Not provided by Alpha Vantage
                    },
                    'symbols': [ticker.get('ticker') for ticker in item.get('ticker_sentiment', [])]
                }
                articles.append(article)
            
            logger.info(f"Retrieved {len(articles)} articles from Alpha Vantage")
            return articles
            
        except RequestException as e:
            logger.error(f"Error fetching news from Alpha Vantage: {str(e)}")
            return []
            
        except Exception as e:
            logger.error(f"Error processing Alpha Vantage news: {str(e)}")
            return []
    
    def get_newsapi_news(self, query: str = None, sources: List[str] = None, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get news from NewsAPI
        
        Args:
            query: Search query
            sources: List of news sources
            days: Number of days to look back
            
        Returns:
            List of news articles
        """
        api_key = self.api_keys.get('newsapi')
        if not api_key:
            logger.error("NewsAPI key not found")
            return []
        
        # Create query identifier
        query_id = f"query={query or 'none'}_sources={'_'.join(sources) if sources else 'none'}_days={days}"
        
        # Check cache
        cached_data = self._get_from_cache('newsapi', query_id)
        if cached_data:
            return cached_data.get('articles', [])
        
        # Calculate date range
        from_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y-%m-%d')
        
        # Prepare request parameters
        params = {
            'apiKey': api_key,
            'from': from_date,
            'language': 'en',
            'sortBy': 'publishedAt'
        }
        
        if query:
            params['q'] = query
        else:
            # Default to Saudi stock market related terms if no query provided
            params['q'] = 'Saudi stock market OR Tadawul OR Saudi economy'
        
        if sources:
            params['sources'] = ','.join(sources)
        
        try:
            logger.info(f"Fetching news from NewsAPI: {query_id}")
            response = requests.get('https://newsapi.org/v2/everything', params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Save to cache
            self._save_to_cache('newsapi', query_id, data)
            
            # Extract and normalize articles
            articles = []
            for item in data.get('articles', []):
                # Skip articles without content
                if not item.get('content') or item.get('content') == '[Removed]':
                    continue
                
                article = {
                    'title': item.get('title', ''),
                    'url': item.get('url', ''),
                    'published_date': datetime.datetime.fromisoformat(item.get('publishedAt', '').replace('Z', '+00:00')),
                    'source': item.get('source', {}).get('name', ''),
                    'language': 'en',  # NewsAPI provides English news
                    'content': item.get('content', '') + ' ' + (item.get('description', '') or ''),
                    'symbols': []  # Will be extracted later
                }
                articles.append(article)
            
            logger.info(f"Retrieved {len(articles)} articles from NewsAPI")
            return articles
            
        except RequestException as e:
            logger.error(f"Error fetching news from NewsAPI: {str(e)}")
            return []
            
        except Exception as e:
            logger.error(f"Error processing NewsAPI news: {str(e)}")
            return []
    
    def get_finnhub_news(self, symbol: str = None, category: str = None, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get news from Finnhub API
        
        Args:
            symbol: Stock symbol
            category: News category
            days: Number of days to look back
            
        Returns:
            List of news articles
        """
        api_key = self.api_keys.get('finnhub')
        if not api_key:
            logger.error("Finnhub API key not found")
            return []
        
        # Create query identifier
        query_id = f"symbol={symbol or 'none'}_category={category or 'none'}_days={days}"
        
        # Check cache
        cached_data = self._get_from_cache('finnhub', query_id)
        if cached_data:
            return cached_data
        
        # Calculate date range
        to_date = datetime.datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y-%m-%d')
        
        # Prepare request parameters
        headers = {
            'X-Finnhub-Token': api_key
        }
        
        params = {}
        
        # Choose endpoint based on parameters
        if symbol:
            endpoint = f"https://finnhub.io/api/v1/company-news"
            params['symbol'] = symbol
            params['from'] = from_date
            params['to'] = to_date
        else:
            endpoint = f"https://finnhub.io/api/v1/news"
            params['category'] = category or 'general'
        
        try:
            logger.info(f"Fetching news from Finnhub: {query_id}")
            response = requests.get(endpoint, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Save to cache
            self._save_to_cache('finnhub', query_id, data)
            
            # Extract and normalize articles
            articles = []
            for item in data:
                # Skip articles without content
                if not item.get('summary'):
                    continue
                
                article = {
                    'title': item.get('headline', ''),
                    'url': item.get('url', ''),
                    'published_date': datetime.datetime.fromtimestamp(item.get('datetime', 0)),
                    'source': item.get('source', ''),
                    'language': 'en',  # Finnhub provides English news
                    'content': item.get('summary', ''),
                    'symbols': [item.get('related', '')] if item.get('related') else []
                }
                articles.append(article)
            
            logger.info(f"Retrieved {len(articles)} articles from Finnhub")
            return articles
            
        except RequestException as e:
            logger.error(f"Error fetching news from Finnhub: {str(e)}")
            return []
            
        except Exception as e:
            logger.error(f"Error processing Finnhub news: {str(e)}")
            return []


# Example usage
if __name__ == "__main__":
    # Initialize the news API client
    news_api = NewsAPIClient()
    
    # Get news from Alpha Vantage
    symbols = ["2222.SR", "1120.SR"]  # Aramco and Al Rajhi Bank
    alpha_vantage_news = news_api.get_alpha_vantage_news(symbols=symbols)
    print(f"Alpha Vantage news: {len(alpha_vantage_news)} articles")
    
    # Get news from NewsAPI
    query = "Saudi Arabia stock market"
    newsapi_news = news_api.get_newsapi_news(query=query, days=3)
    print(f"NewsAPI news: {len(newsapi_news)} articles")
    
    # Get news from Finnhub
    finnhub_news = news_api.get_finnhub_news(category="general", days=3)
    print(f"Finnhub news: {len(finnhub_news)} articles")
