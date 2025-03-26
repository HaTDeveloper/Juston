"""
Test Script for Saudi Stock Bot
----------------------------
This script tests the functionality of the Saudi Stock Bot components.
It performs basic validation of each module to ensure they work correctly.
"""

import os
import sys
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("test_saudi_stock_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import components
try:
    from data_collection.stock_data import StockDataCollector
    from data_collection.database import DatabaseManager
    from news_analysis.news_analyzer import NewsAnalyzer
    from analysis.golden_opportunities.scanner import GoldenOpportunitiesScanner
    from analysis.trends.analyzer import TrendAnalyzer
    from models.confidence_evaluator import ConfidenceEvaluator
    from utils.notification_system import NotificationSystem
    
    logger.info("Successfully imported all components")
except ImportError as e:
    logger.error(f"Error importing components: {str(e)}")
    sys.exit(1)


def test_data_collection():
    """Test the data collection module"""
    logger.info("Testing data collection module...")
    
    try:
        # Initialize database manager
        db_manager = DatabaseManager(use_test_db=True)
        
        # Initialize data collector
        data_collector = StockDataCollector(db_manager)
        
        # Test fetching data for a sample stock
        symbol = "1120.SR"  # Al Rajhi Bank
        data = data_collector.fetch_stock_data(symbol, period="1mo")
        
        if data is not None and not data.empty:
            logger.info(f"Successfully fetched data for {symbol}")
            logger.info(f"Data shape: {data.shape}")
            logger.info(f"Data columns: {data.columns.tolist()}")
            logger.info(f"Data sample:\n{data.head()}")
            return True
        else:
            logger.error(f"Failed to fetch data for {symbol}")
            return False
    
    except Exception as e:
        logger.error(f"Error testing data collection: {str(e)}")
        return False


def test_news_analysis():
    """Test the news analysis module"""
    logger.info("Testing news analysis module...")
    
    try:
        # Initialize database manager
        db_manager = DatabaseManager(use_test_db=True)
        
        # Initialize news analyzer
        news_analyzer = NewsAnalyzer(db_manager)
        
        # Test analyzing news for a sample stock
        symbol = "1120.SR"  # Al Rajhi Bank
        news_results = news_analyzer.analyze_stock_news(symbol)
        
        if news_results is not None:
            logger.info(f"Successfully analyzed news for {symbol}")
            logger.info(f"News sentiment score: {news_results.get('sentiment_score', 'N/A')}")
            logger.info(f"Number of news items: {len(news_results.get('news_items', []))}")
            return True
        else:
            logger.error(f"Failed to analyze news for {symbol}")
            return False
    
    except Exception as e:
        logger.error(f"Error testing news analysis: {str(e)}")
        return False


def test_golden_opportunities():
    """Test the golden opportunities module"""
    logger.info("Testing golden opportunities module...")
    
    try:
        # Initialize database manager
        db_manager = DatabaseManager(use_test_db=True)
        
        # Initialize data collector
        data_collector = StockDataCollector(db_manager)
        
        # Initialize golden opportunities scanner
        scanner = GoldenOpportunitiesScanner(data_collector)
        
        # Test scanning for opportunities for a sample stock
        symbol = "1120.SR"  # Al Rajhi Bank
        opportunities = scanner.scan_stock(symbol)
        
        if opportunities is not None:
            logger.info(f"Successfully scanned for opportunities for {symbol}")
            logger.info(f"Has opportunity: {opportunities.get('has_opportunity', False)}")
            logger.info(f"Opportunity type: {opportunities.get('opportunity_type', 'N/A')}")
            logger.info(f"Number of signals: {len(opportunities.get('signals', []))}")
            return True
        else:
            logger.error(f"Failed to scan for opportunities for {symbol}")
            return False
    
    except Exception as e:
        logger.error(f"Error testing golden opportunities: {str(e)}")
        return False


def test_trend_analysis():
    """Test the trend analysis module"""
    logger.info("Testing trend analysis module...")
    
    try:
        # Initialize database manager
        db_manager = DatabaseManager(use_test_db=True)
        
        # Initialize data collector
        data_collector = StockDataCollector(db_manager)
        
        # Initialize trend analyzer
        analyzer = TrendAnalyzer(data_collector)
        
        # Test analyzing trends for a sample stock
        symbol = "1120.SR"  # Al Rajhi Bank
        trends = analyzer.analyze_stock(symbol)
        
        if trends is not None:
            logger.info(f"Successfully analyzed trends for {symbol}")
            logger.info(f"Trend direction: {trends.get('trend_direction', 'N/A')}")
            logger.info(f"Trend strength: {trends.get('trend_strength', 'N/A')}")
            logger.info(f"Number of signals: {len(trends.get('signals', []))}")
            return True
        else:
            logger.error(f"Failed to analyze trends for {symbol}")
            return False
    
    except Exception as e:
        logger.error(f"Error testing trend analysis: {str(e)}")
        return False


def test_confidence_evaluation():
    """Test the confidence evaluation module"""
    logger.info("Testing confidence evaluation module...")
    
    try:
        # Initialize confidence evaluator
        evaluator = ConfidenceEvaluator()
        
        # Create sample data
        symbol = "1120.SR"  # Al Rajhi Bank
        
        # Sample golden opportunity results
        golden_opportunity_results = {
            'symbol': symbol,
            'latest_price': 98.75,
            'latest_date': datetime.now().date(),
            'has_opportunity': True,
            'opportunity_type': 'bullish',
            'confidence_score': 85,
            'signals': [
                {
                    'category': 'candlestick',
                    'type': 'Hammer',
                    'signal': 'bullish',
                    'strength': 8,
                    'description': 'Bullish hammer pattern detected, indicating potential reversal.'
                },
                {
                    'category': 'momentum',
                    'type': 'RSI',
                    'signal': 'bullish',
                    'strength': 7,
                    'description': 'RSI shows bullish divergence at 35.50, suggesting potential upward reversal.'
                }
            ]
        }
        
        # Sample news results
        news_results = {
            'sentiment_score': 0.6,
            'news_items': [
                {
                    'headline': 'Company announces strong quarterly results',
                    'sentiment': 'bullish',
                    'impact': 8,
                    'date': datetime.now().date()
                }
            ]
        }
        
        # Sample trend results
        trend_results = {
            'trend_direction': 'bullish',
            'trend_strength': 7,
            'signals': [
                {
                    'category': 'moving_average',
                    'type': 'bullish_crossover_MA20_MA50',
                    'signal': 'bullish',
                    'strength': 8,
                    'description': 'Bullish crossover: MA20 crossed above MA50 0 days ago, indicating potential uptrend.'
                }
            ]
        }
        
        # Test evaluating a stock
        evaluation = evaluator.evaluate_stock(
            symbol,
            golden_opportunity_results,
            news_results,
            trend_results
        )
        
        if evaluation is not None:
            logger.info(f"Successfully evaluated {symbol}")
            logger.info(f"Overall direction: {evaluation.get('overall_direction', 'N/A')}")
            logger.info(f"Confidence level: {evaluation.get('confidence_level', 'N/A')}")
            logger.info(f"Overall score: {evaluation.get('overall_score', 'N/A')}")
            logger.info(f"Recommendation action: {evaluation.get('recommendation', {}).get('action', 'N/A')}")
            return True
        else:
            logger.error(f"Failed to evaluate {symbol}")
            return False
    
    except Exception as e:
        logger.error(f"Error testing confidence evaluation: {str(e)}")
        return False


def test_notification_system():
    """Test the notification system"""
    logger.info("Testing notification system...")
    
    try:
        # Initialize notification system
        notification_system = NotificationSystem()
        
        # Test formatting a golden opportunity notification
        opportunity = {
            'symbol': '1120.SR',
            'opportunity_type': 'bullish',
            'confidence_score': 85,
            'latest_price': 98.75,
            'signals': [
                {
                    'category': 'candlestick',
                    'type': 'Hammer',
                    'signal': 'bullish',
                    'strength': 8,
                    'description': 'Bullish hammer pattern detected, indicating potential reversal.'
                }
            ],
            'support_level': 95.20,
            'resistance_level': 100.50
        }
        
        notification = notification_system.format_golden_opportunity_notification(opportunity)
        
        if notification is not None:
            logger.info("Successfully formatted golden opportunity notification")
            logger.info(f"Notification title: {notification.get('title', 'N/A')}")
            logger.info(f"Notification color: {notification.get('color', 'N/A')}")
            logger.info(f"Number of fields: {len(notification.get('fields', []))}")
            return True
        else:
            logger.error("Failed to format golden opportunity notification")
            return False
    
    except Exception as e:
        logger.error(f"Error testing notification system: {str(e)}")
        return False


def run_all_tests():
    """Run all tests and report results"""
    logger.info("Starting all tests...")
    
    tests = [
        ("Data Collection", test_data_collection),
        ("News Analysis", test_news_analysis),
        ("Golden Opportunities", test_golden_opportunities),
        ("Trend Analysis", test_trend_analysis),
        ("Confidence Evaluation", test_confidence_evaluation),
        ("Notification System", test_notification_system)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\n{'=' * 50}\nRunning test: {test_name}\n{'=' * 50}")
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            logger.error(f"Unexpected error in {test_name} test: {str(e)}")
            results.append((test_name, False))
    
    # Print summary
    logger.info("\n\n" + "=" * 50)
    logger.info("TEST SUMMARY")
    logger.info("=" * 50)
    
    all_passed = True
    for test_name, success in results:
        status = "PASSED" if success else "FAILED"
        logger.info(f"{test_name}: {status}")
        if not success:
            all_passed = False
    
    logger.info("=" * 50)
    logger.info(f"OVERALL: {'PASSED' if all_passed else 'FAILED'}")
    logger.info("=" * 50)
    
    return all_passed


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
