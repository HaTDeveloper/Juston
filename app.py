"""
Main Application Entry Point
--------------------------
This is the main entry point for the Saudi Stock Bot application.
It initializes all components and starts the web server.
"""

import os
import logging
from dotenv import load_dotenv
import threading
import schedule
import time

# Import components
from data_collection.stock_data import StockDataCollector
from data_collection.database import DatabaseManager
from news_analysis.news_analyzer import NewsAnalyzer
from analysis.golden_opportunities.scanner import GoldenOpportunitiesScanner
from analysis.trends.analyzer import TrendAnalyzer
from models.confidence_evaluator import ConfidenceEvaluator
from utils.notification_system import NotificationSystem
from ui.app import UserInterface

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("saudi_stock_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_scheduler():
    """Run the scheduler in a separate thread"""
    while True:
        schedule.run_pending()
        time.sleep(1)


def main():
    """Main function to initialize and run the application"""
    try:
        logger.info("Starting Saudi Stock Bot")
        
        # Initialize components
        logger.info("Initializing components")
        
        # Database manager
        db_manager = DatabaseManager()
        
        # Data collector
        data_collector = StockDataCollector(db_manager)
        
        # News analyzer
        news_analyzer = NewsAnalyzer(db_manager)
        
        # Golden opportunities scanner
        golden_scanner = GoldenOpportunitiesScanner(data_collector)
        
        # Trend analyzer
        trend_analyzer = TrendAnalyzer(data_collector)
        
        # Confidence evaluator
        confidence_evaluator = ConfidenceEvaluator()
        
        # Notification system with webhook from environment variable
        discord_webhook_url = os.environ.get('DISCORD_WEBHOOK_URL')
        notification_system = NotificationSystem(discord_webhook_url=discord_webhook_url)
        
        # Send startup notification if enabled
        if os.environ.get('SEND_STARTUP_NOTIFICATION', 'False').lower() == 'true':
            logger.info("Sending startup notification")
            notification_system.send_discord_notification(
                title="🚀 بوت تحليل الأسهم السعودية",
                description="تم بدء تشغيل البوت بنجاح وهو جاهز للعمل الآن.",
                color="info"
            )
        
        # Schedule data collection (every 30 minutes during market hours)
        schedule.every(30).minutes.do(data_collector.update_all_stocks)
        
        # Schedule news analysis (every hour)
        schedule.every(60).minutes.do(news_analyzer.analyze_latest_news)
        
        # Schedule daily report (after market close)
        schedule.every().day.at("17:00").do(
            notification_system.send_daily_report,
            golden_scanner.get_todays_opportunities(),
            confidence_evaluator.get_todays_recommendations()
        )
        
        # Schedule weekly report (Friday after market close)
        schedule.every().friday.at("17:30").do(
            notification_system.send_weekly_report,
            golden_scanner.get_weekly_opportunities(),
            confidence_evaluator.get_weekly_recommendations()
        )
        
        # Start scheduler in a separate thread
        scheduler_thread = threading.Thread(target=run_scheduler)
        scheduler_thread.daemon = True
        scheduler_thread.start()
        
        # Initialize user interface
        ui = UserInterface(
            data_collector=data_collector,
            golden_scanner=golden_scanner,
            trend_analyzer=trend_analyzer,
            confidence_evaluator=confidence_evaluator,
            notification_system=notification_system
        )
        
        # Get port from environment variable
        port = int(os.environ.get('PORT', 5000))
        
        # Run the web application
        logger.info(f"Starting web server on port {port}")
        ui.run(host='0.0.0.0', port=port)
        
    except Exception as e:
        logger.error(f"Error starting application: {str(e)}")


if __name__ == "__main__":
    main()
