"""
News Analysis Module for Saudi Stock Bot
---------------------------------------
This module handles collection and analysis of news related to Saudi stocks.
It includes functionality for:
- Collecting news from various sources
- Translating Arabic news to English
- Sentiment analysis of news
- Storing news and analysis results in MongoDB
"""

import os
import re
import time
import logging
import datetime
import requests
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from bs4 import BeautifulSoup
from googletrans import Translator
from textblob import TextBlob
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pymongo import MongoClient, DESCENDING
from pymongo.errors import PyMongoError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("news_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Download NLTK resources if not already downloaded
try:
    nltk.data.find('vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon')


class NewsAnalyzer:
    """Class to handle collection and analysis of news related to Saudi stocks"""
    
    def __init__(self, mongo_uri: str = None):
        """
        Initialize the news analyzer
        
        Args:
            mongo_uri: MongoDB connection URI. If None, uses environment variable or default
        """
        if mongo_uri is None:
            mongo_uri = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
        
        self.mongo_uri = mongo_uri
        self.mongo_client = None
        self.db = None
        self.news_collection = None
        self.translator = Translator()
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        
        # Initialize MongoDB connection
        self._initialize_mongo()
        
        # News sources configuration
        self.news_sources = [
            {
                'name': 'Argaam',
                'url': 'https://www.argaam.com/en/company/companies-prices',
                'language': 'en',
                'type': 'financial'
            },
            {
                'name': 'Saudi Exchange',
                'url': 'https://www.saudiexchange.sa/wps/portal/tadawul/market-participants/news',
                'language': 'en',
                'type': 'exchange'
            },
            {
                'name': 'Arab News',
                'url': 'https://www.arabnews.com/tags/saudi-stock-exchange',
                'language': 'en',
                'type': 'general'
            },
            {
                'name': 'CNBC Arabia',
                'url': 'https://www.cnbcarabia.com/market/saudi',
                'language': 'ar',
                'type': 'financial'
            },
            {
                'name': 'Aleqtisadiah',
                'url': 'https://www.aleqt.com/tags/31',
                'language': 'ar',
                'type': 'financial'
            }
        ]
    
    def _initialize_mongo(self) -> None:
        """Initialize MongoDB connection and collections"""
        try:
            self.mongo_client = MongoClient(self.mongo_uri)
            self.db = self.mongo_client['saudi_stock_news']
            self.news_collection = self.db['news']
            
            # Create indexes for efficient querying
            self.news_collection.create_index([('symbol', 1)])
            self.news_collection.create_index([('published_date', DESCENDING)])
            self.news_collection.create_index([('source', 1)])
            
            logger.info(f"MongoDB connection initialized with URI: {self.mongo_uri}")
            
        except PyMongoError as e:
            logger.error(f"Error initializing MongoDB connection: {str(e)}")
            raise
    
    def translate_text(self, text: str, source_lang: str = 'ar', target_lang: str = 'en') -> str:
        """
        Translate text from source language to target language
        
        Args:
            text: Text to translate
            source_lang: Source language code
            target_lang: Target language code
            
        Returns:
            Translated text
        """
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                if not text or text.strip() == '':
                    return ''
                
                # For short texts, use googletrans
                if len(text) < 5000:
                    translation = self.translator.translate(text, src=source_lang, dest=target_lang)
                    return translation.text
                
                # For longer texts, split and translate in chunks
                chunks = [text[i:i+4999] for i in range(0, len(text), 4999)]
                translated_chunks = []
                
                for chunk in chunks:
                    translation = self.translator.translate(chunk, src=source_lang, dest=target_lang)
                    translated_chunks.append(translation.text)
                    time.sleep(1)  # Avoid rate limiting
                
                return ' '.join(translated_chunks)
                
            except Exception as e:
                logger.error(f"Error translating text (attempt {attempt+1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying translation in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"Failed to translate text after {max_retries} attempts")
                    return text  # Return original text if translation fails
    
    def analyze_sentiment(self, text: str) -> Dict[str, float]:
        """
        Analyze sentiment of text
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with sentiment scores
        """
        try:
            if not text or text.strip() == '':
                return {
                    'compound': 0.0,
                    'positive': 0.0,
                    'negative': 0.0,
                    'neutral': 1.0
                }
            
            # Use VADER for sentiment analysis
            vader_scores = self.sentiment_analyzer.polarity_scores(text)
            
            # Use TextBlob for additional analysis
            blob = TextBlob(text)
            textblob_polarity = blob.sentiment.polarity
            textblob_subjectivity = blob.sentiment.subjectivity
            
            # Combine scores
            combined_scores = {
                'compound': vader_scores['compound'],
                'positive': vader_scores['pos'],
                'negative': vader_scores['neg'],
                'neutral': vader_scores['neu'],
                'polarity': textblob_polarity,
                'subjectivity': textblob_subjectivity
            }
            
            return combined_scores
            
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {str(e)}")
            return {
                'compound': 0.0,
                'positive': 0.0,
                'negative': 0.0,
                'neutral': 1.0,
                'polarity': 0.0,
                'subjectivity': 0.0,
                'error': str(e)
            }
    
    def fetch_news_from_source(self, source: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Fetch news from a specific source
        
        Args:
            source: Dictionary with source information
            
        Returns:
            List of news articles
        """
        max_retries = 3
        retry_delay = 2  # seconds
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching news from {source['name']}")
                response = requests.get(source['url'], headers=headers, timeout=30)
                response.raise_for_status()
                
                # Parse HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract news articles based on source-specific selectors
                articles = []
                
                # Note: In a real implementation, each source would have custom extraction logic
                # This is a simplified generic implementation
                if source['name'] == 'Argaam':
                    news_elements = soup.select('div.article-box')
                    for element in news_elements:
                        try:
                            title_elem = element.select_one('h3.title a')
                            if not title_elem:
                                continue
                                
                            title = title_elem.text.strip()
                            url = title_elem.get('href', '')
                            if url and not url.startswith('http'):
                                url = f"https://www.argaam.com{url}"
                                
                            date_elem = element.select_one('span.date')
                            date_str = date_elem.text.strip() if date_elem else ''
                            
                            # Extract article content
                            article_content = self._fetch_article_content(url, headers)
                            
                            articles.append({
                                'title': title,
                                'url': url,
                                'published_date': self._parse_date(date_str),
                                'source': source['name'],
                                'language': source['language'],
                                'content': article_content
                            })
                        except Exception as e:
                            logger.error(f"Error extracting article from {source['name']}: {str(e)}")
                
                elif source['name'] == 'Saudi Exchange':
                    news_elements = soup.select('div.news-item')
                    for element in news_elements:
                        try:
                            title_elem = element.select_one('h3.news-title a')
                            if not title_elem:
                                continue
                                
                            title = title_elem.text.strip()
                            url = title_elem.get('href', '')
                            if url and not url.startswith('http'):
                                url = f"https://www.saudiexchange.sa{url}"
                                
                            date_elem = element.select_one('span.news-date')
                            date_str = date_elem.text.strip() if date_elem else ''
                            
                            # Extract article content
                            article_content = self._fetch_article_content(url, headers)
                            
                            articles.append({
                                'title': title,
                                'url': url,
                                'published_date': self._parse_date(date_str),
                                'source': source['name'],
                                'language': source['language'],
                                'content': article_content
                            })
                        except Exception as e:
                            logger.error(f"Error extracting article from {source['name']}: {str(e)}")
                
                elif source['name'] == 'Arab News':
                    news_elements = soup.select('div.article-item')
                    for element in news_elements:
                        try:
                            title_elem = element.select_one('h3.article-title a')
                            if not title_elem:
                                continue
                                
                            title = title_elem.text.strip()
                            url = title_elem.get('href', '')
                            if url and not url.startswith('http'):
                                url = f"https://www.arabnews.com{url}"
                                
                            date_elem = element.select_one('span.article-date')
                            date_str = date_elem.text.strip() if date_elem else ''
                            
                            # Extract article content
                            article_content = self._fetch_article_content(url, headers)
                            
                            articles.append({
                                'title': title,
                                'url': url,
                                'published_date': self._parse_date(date_str),
                                'source': source['name'],
                                'language': source['language'],
                                'content': article_content
                            })
                        except Exception as e:
                            logger.error(f"Error extracting article from {source['name']}: {str(e)}")
                
                elif source['name'] == 'CNBC Arabia':
                    news_elements = soup.select('div.news-card')
                    for element in news_elements:
                        try:
                            title_elem = element.select_one('h3.card-title a')
                            if not title_elem:
                                continue
                                
                            title = title_elem.text.strip()
                            url = title_elem.get('href', '')
                            if url and not url.startswith('http'):
                                url = f"https://www.cnbcarabia.com{url}"
                                
                            date_elem = element.select_one('span.card-date')
                            date_str = date_elem.text.strip() if date_elem else ''
                            
                            # Extract article content
                            article_content = self._fetch_article_content(url, headers)
                            
                            articles.append({
                                'title': title,
                                'url': url,
                                'published_date': self._parse_date(date_str),
                                'source': source['name'],
                                'language': source['language'],
                                'content': article_content
                            })
                        except Exception as e:
                            logger.error(f"Error extracting article from {source['name']}: {str(e)}")
                
                elif source['name'] == 'Aleqtisadiah':
                    news_elements = soup.select('div.article-item')
                    for element in news_elements:
                        try:
                            title_elem = element.select_one('h2.article-title a')
                            if not title_elem:
                                continue
                                
                            title = title_elem.text.strip()
                            url = title_elem.get('href', '')
                            if url and not url.startswith('http'):
                                url = f"https://www.aleqt.com{url}"
                                
                            date_elem = element.select_one('span.article-date')
                            date_str = date_elem.text.strip() if date_elem else ''
                            
                            # Extract article content
                            article_content = self._fetch_article_content(url, headers)
                            
                            articles.append({
                                'title': title,
                                'url': url,
                                'published_date': self._parse_date(date_str),
                                'source': source['name'],
                                'language': source['language'],
                                'content': article_content
                            })
                        except Exception as e:
                            logger.error(f"Error extracting article from {source['name']}: {str(e)}")
                
                logger.info(f"Fetched {len(articles)} articles from {source['name']}")
                return articles
                
            except requests.RequestException as e:
                logger.error(f"Error fetching news from {source['name']} (attempt {attempt+1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"Failed to fetch news from {source['name']} after {max_retries} attempts")
                    return []
    
    def _fetch_article_content(self, url: str, headers: Dict[str, str]) -> str:
        """
        Fetch and extract content from an article URL
        
        Args:
            url: Article URL
            headers: HTTP headers
            
        Returns:
            Article content
        """
        try:
            if not url:
                return ""
                
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.extract()
            
            # Extract article content - this is a simplified approach
            # In a real implementation, each source would have custom extraction logic
            article_content = ""
            
            # Try common article content selectors
            content_selectors = [
                'div.article-content',
                'div.entry-content',
                'div.post-content',
                'div.content',
                'article',
                'main'
            ]
            
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    article_content = content_elem.get_text(separator=' ', strip=True)
                    break
            
            # If no content found, use body text
            if not article_content:
                article_content = soup.body.get_text(separator=' ', strip=True)
            
            # Clean up content
            article_content = re.sub(r'\s+', ' ', article_content).strip()
            
            return article_content
            
        except Exception as e:
            logger.error(f"Error fetching article content from {url}: {str(e)}")
            return ""
    
    def _parse_date(self, date_str: str) -> datetime.datetime:
        """
        Parse date string to datetime object
        
        Args:
            date_str: Date string
            
        Returns:
            Datetime object
        """
        try:
            if not date_str or date_str.strip() == '':
                return datetime.datetime.now()
            
            # Try common date formats
            date_formats = [
                '%Y-%m-%d',
                '%d-%m-%Y',
                '%b %d, %Y',
                '%d %b %Y',
                '%B %d, %Y',
                '%d %B %Y',
                '%Y/%m/%d',
                '%d/%m/%Y'
            ]
            
            for date_format in date_formats:
                try:
                    return datetime.datetime.strptime(date_str.strip(), date_format)
                except ValueError:
                    continue
            
            # If all formats fail, try to extract date using regex
            date_pattern = r'(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})'
            match = re.search(date_pattern, date_str)
            if match:
                day, month, year = match.groups()
                if len(year) == 2:
                    year = f"20{year}"
                return datetime.datetime(int(year), int(month), int(day))
            
            # If all else fails, return current date
            logger.warning(f"Could not parse date string: {date_str}")
            return datetime.datetime.now()
            
        except Exception as e:
            logger.error(f"Error parsing date string '{date_str}': {str(e)}")
            return datetime.datetime.now()
    
    def process_news_article(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a news article for sentiment analysis
        
        Args:
            article: News article data
            
        Returns:
            Processed article with sentiment analysis
        """
        try:
            # Translate if needed
            if article['language'] == 'ar':
                logger.info(f"Translating article: {article['title']}")
                article['title_en'] = self.translate_text(article['title'], 'ar', 'en')
                article['content_en'] = self.translate_text(article['content'], 'ar', 'en')
                text_for_analysis = article['title_en'] + ' ' + article['content_en']
            else:
                article['title_en'] = article['title']
                article['content_en'] = article['content']
                text_for_analysis = article['title'] + ' ' + article['content']
            
            # Analyze sentiment
            logger.info(f"Analyzing sentiment for article: {article['title']}")
            article['sentiment'] = self.analyze_sentiment(text_for_analysis)
            
            # Extract stock symbols mentioned in the article
            article['symbols'] = self._extract_stock_symbols(text_for_analysis)
            
            # Add processing timestamp
            article['processed_at'] = datetime.datetime.now()
            
            return article
            
        except Exception as e:
            logger.error(f"Error processing article: {str(e)}")
            article['error'] = str(e)
            return article
    
    def _extract_stock_symbols(self, text: str) -> List[str]:
        """
        Extract stock symbols mentioned in text
        
        Args:
            text: Text to analyze
            
        Returns:
            List of stock symbols
        """
        try:
            # This is a simplified approach
            # In a real implementation, would use more sophisticated NER and pattern matching
            
            # Pattern for Saudi stock symbols (XXXX.SR)
            symbol_pattern = r'(\d{4})\.SR'
            symbols = re.findall(symbol_pattern, text)
            
            # Add .SR suffix to symbols
            symbols = [f"{symbol}.SR" for symbol in symbols]
            
            # Common Saudi stock names and their symbols
            stock_name_mapping = {
                'aramco': '2222.SR',
                'saudi aramco': '2222.SR',
                'sabic': '2010.SR',
                'al rajhi': '1120.SR',
                'rajhi bank': '1120.SR',
                'stc': '7010.SR',
                'saudi telecom': '7010.SR',
                'samba': '1090.SR',
                'ncb': '1180.SR',
                'national commercial bank': '1180.SR',
                'maaden': '1211.SR',
                'saudi arabian mining': '1211.SR',
                'almarai': '2280.SR'
            }
            
            # Check for stock names in text
            text_lower = text.lower()
            for name, symbol in stock_name_mapping.items():
                if name in text_lower:
                    symbols.append(symbol)
            
            # Remove duplicates and sort
            symbols = sorted(list(set(symbols)))
            
            return symbols
            
        except Exception as e:
            logger.error(f"Error extracting stock symbols: {str(e)}")
            return []
    
    def store_news_in_db(self, articles: List[Dict[str, Any]]) -> int:
        """
        Store news articles in MongoDB
        
        Args:
            articles: List of news articles
            
        Returns:
            Number of articles stored
        """
        if not articles:
            return 0
        
        try:
            # Check for duplicates based on URL
            stored_count = 0
            for article in articles:
                # Check if article already exists
                existing = self.news_collection.find_one({'url': article['url']})
                if existing:
                    logger.info(f"Article already exists: {article['title']}")
                    continue
                
                # Insert article
                result = self.news_collection.insert_one(article)
                if result.inserted_id:
                    stored_count += 1
                    logger.info(f"Stored article: {article['title']}")
                else:
                    logger.warning(f"Failed to store article: {article['title']}")
            
            logger.info(f"Stored {stored_count} new articles in database")
            return stored_count
            
        except PyMongoError as e:
            logger.error(f"MongoDB error storing news: {str(e)}")
            return 0
            
        except Exception as e:
            logger.error(f"Error storing news in database: {str(e)}")
            return 0
    
    def collect_and_analyze_news(self) -> Dict[str, Any]:
        """
        Collect and analyze news from all sources
        
        Returns:
            Dictionary with results summary
        """
        results = {
            'total_articles': 0,
            'processed_articles': 0,
            'stored_articles': 0,
            'sources': {},
            'errors': []
        }
        
        try:
            all_articles = []
            
            # Collect news from each source
            for source in self.news_sources:
                try:
                    logger.info(f"Collecting news from {source['name']}")
                    articles = self.fetch_news_from_source(source)
                    results['sources'][source['name']] = len(articles)
                    results['total_articles'] += len(articles)
                    all_articles.extend(articles)
                except Exception as e:
                    error_msg = f"Error collecting news from {source['name']}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
            
            # Process each article
            processed_articles = []
            for article in all_articles:
                try:
                    processed_article = self.process_news_article(article)
                    processed_articles.append(processed_article)
                    results['processed_articles'] += 1
                except Exception as e:
                    error_msg = f"Error processing article {article.get('title', 'Unknown')}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
            
            # Store processed articles
            stored_count = self.store_news_in_db(processed_articles)
            results['stored_articles'] = stored_count
            
            logger.info(f"News collection and analysis completed: {results}")
            return results
            
        except Exception as e:
            error_msg = f"Error in news collection and analysis: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            return results
    
    def get_news_for_symbol(self, symbol: str, days: int = 7, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get recent news for a specific stock symbol
        
        Args:
            symbol: Stock symbol
            days: Number of days to look back
            limit: Maximum number of news articles to return
            
        Returns:
            List of news articles
        """
        try:
            # Calculate date range
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=days)
            
            # Query MongoDB
            query = {
                'symbols': symbol,
                'published_date': {'$gte': start_date, '$lte': end_date}
            }
            
            # Sort by published date (newest first) and limit results
            cursor = self.news_collection.find(query).sort('published_date', DESCENDING).limit(limit)
            
            # Convert cursor to list
            news_articles = list(cursor)
            
            logger.info(f"Retrieved {len(news_articles)} news articles for symbol {symbol}")
            return news_articles
            
        except PyMongoError as e:
            logger.error(f"MongoDB error retrieving news for symbol {symbol}: {str(e)}")
            return []
            
        except Exception as e:
            logger.error(f"Error retrieving news for symbol {symbol}: {str(e)}")
            return []
    
    def get_sentiment_summary(self, symbol: str, days: int = 7) -> Dict[str, Any]:
        """
        Get sentiment summary for a specific stock symbol
        
        Args:
            symbol: Stock symbol
            days: Number of days to look back
            
        Returns:
            Dictionary with sentiment summary
        """
        try:
            # Get news articles
            news_articles = self.get_news_for_symbol(symbol, days=days, limit=100)
            
            if not news_articles:
                return {
                    'symbol': symbol,
                    'period_days': days,
                    'article_count': 0,
                    'average_sentiment': {
                        'compound': 0.0,
                        'positive': 0.0,
                        'negative': 0.0,
                        'neutral': 0.0
                    },
                    'sentiment_trend': 'neutral'
                }
            
            # Calculate average sentiment
            compound_scores = []
            positive_scores = []
            negative_scores = []
            neutral_scores = []
            
            for article in news_articles:
                sentiment = article.get('sentiment', {})
                compound_scores.append(sentiment.get('compound', 0.0))
                positive_scores.append(sentiment.get('positive', 0.0))
                negative_scores.append(sentiment.get('negative', 0.0))
                neutral_scores.append(sentiment.get('neutral', 0.0))
            
            # Calculate averages
            avg_compound = sum(compound_scores) / len(compound_scores) if compound_scores else 0.0
            avg_positive = sum(positive_scores) / len(positive_scores) if positive_scores else 0.0
            avg_negative = sum(negative_scores) / len(negative_scores) if negative_scores else 0.0
            avg_neutral = sum(neutral_scores) / len(neutral_scores) if neutral_scores else 0.0
            
            # Determine sentiment trend
            if avg_compound >= 0.05:
                sentiment_trend = 'positive'
            elif avg_compound <= -0.05:
                sentiment_trend = 'negative'
            else:
                sentiment_trend = 'neutral'
            
            # Create summary
            summary = {
                'symbol': symbol,
                'period_days': days,
                'article_count': len(news_articles),
                'average_sentiment': {
                    'compound': avg_compound,
                    'positive': avg_positive,
                    'negative': avg_negative,
                    'neutral': avg_neutral
                },
                'sentiment_trend': sentiment_trend,
                'latest_articles': [
                    {
                        'title': article.get('title', ''),
                        'url': article.get('url', ''),
                        'published_date': article.get('published_date', ''),
                        'source': article.get('source', ''),
                        'sentiment': article.get('sentiment', {}).get('compound', 0.0)
                    }
                    for article in news_articles[:5]  # Include 5 latest articles
                ]
            }
            
            logger.info(f"Generated sentiment summary for symbol {symbol}: {sentiment_trend}")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating sentiment summary for symbol {symbol}: {str(e)}")
            return {
                'symbol': symbol,
                'period_days': days,
                'article_count': 0,
                'error': str(e)
            }


# Example usage
if __name__ == "__main__":
    # Initialize the news analyzer
    news_analyzer = NewsAnalyzer()
    
    # Collect and analyze news
    results = news_analyzer.collect_and_analyze_news()
    print(f"News collection results: {results}")
    
    # Get sentiment summary for a symbol
    symbol = "2222.SR"  # Aramco
    sentiment_summary = news_analyzer.get_sentiment_summary(symbol, days=7)
    print(f"Sentiment summary for {symbol}: {sentiment_summary}")
