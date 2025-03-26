"""
News Scheduler Module
-------------------
This module handles scheduling periodic news collection and analysis.
It includes functionality for:
- Setting up scheduled jobs for news collection
- Running the scheduler in the background
- Logging scheduled job execution
"""

import time
import logging
import threading
import schedule
from datetime import datetime
from typing import Callable, Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("news_scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class NewsScheduler:
    """Class to handle scheduling of news collection and analysis"""
    
    def __init__(self):
        """Initialize the scheduler"""
        self.scheduler_thread = None
        self.is_running = False
        self.last_run_times = {}
    
    def schedule_job(self, job_func: Callable, interval_minutes: int, job_name: str = None) -> bool:
        """
        Schedule a job to run at specified intervals
        
        Args:
            job_func: Function to run
            interval_minutes: Interval in minutes
            job_name: Name of the job for logging
            
        Returns:
            True if scheduled successfully, False otherwise
        """
        try:
            if job_name is None:
                job_name = job_func.__name__
            
            # Define a wrapper function that logs execution
            def job_wrapper():
                try:
                    logger.info(f"Running scheduled news job: {job_name}")
                    start_time = time.time()
                    result = job_func()
                    end_time = time.time()
                    execution_time = end_time - start_time
                    self.last_run_times[job_name] = {
                        'start_time': datetime.fromtimestamp(start_time),
                        'execution_time': execution_time,
                        'success': True
                    }
                    logger.info(f"News job {job_name} completed in {execution_time:.2f} seconds")
                    return result
                except Exception as e:
                    logger.error(f"Error in scheduled news job {job_name}: {str(e)}")
                    self.last_run_times[job_name] = {
                        'start_time': datetime.fromtimestamp(start_time),
                        'execution_time': time.time() - start_time,
                        'success': False,
                        'error': str(e)
                    }
                    return None
            
            # Schedule the job
            if interval_minutes < 60:
                # For intervals less than an hour, schedule every X minutes
                schedule.every(interval_minutes).minutes.do(job_wrapper)
                logger.info(f"Scheduled news job {job_name} to run every {interval_minutes} minutes")
            else:
                # For longer intervals, schedule at specific times
                hours = interval_minutes // 60
                schedule.every(hours).hours.do(job_wrapper)
                logger.info(f"Scheduled news job {job_name} to run every {hours} hours")
            
            return True
            
        except Exception as e:
            logger.error(f"Error scheduling news job {job_name}: {str(e)}")
            return False
    
    def start_scheduler(self) -> bool:
        """
        Start the scheduler in a background thread
        
        Returns:
            True if started successfully, False otherwise
        """
        if self.is_running:
            logger.warning("News scheduler is already running")
            return False
        
        try:
            # Define the scheduler loop
            def run_scheduler():
                logger.info("Starting news scheduler")
                self.is_running = True
                while self.is_running:
                    try:
                        schedule.run_pending()
                        time.sleep(1)
                    except Exception as e:
                        logger.error(f"Error in news scheduler loop: {str(e)}")
                        time.sleep(5)  # Wait a bit longer if there's an error
                logger.info("News scheduler stopped")
            
            # Start the scheduler in a daemon thread
            self.scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
            self.scheduler_thread.start()
            logger.info("News scheduler thread started")
            return True
            
        except Exception as e:
            logger.error(f"Error starting news scheduler: {str(e)}")
            self.is_running = False
            return False
    
    def stop_scheduler(self) -> bool:
        """
        Stop the scheduler
        
        Returns:
            True if stopped successfully, False otherwise
        """
        if not self.is_running:
            logger.warning("News scheduler is not running")
            return False
        
        try:
            self.is_running = False
            if self.scheduler_thread:
                self.scheduler_thread.join(timeout=5)
            logger.info("News scheduler stopped")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping news scheduler: {str(e)}")
            return False
    
    def get_job_status(self) -> Dict[str, Any]:
        """
        Get the status of scheduled jobs
        
        Returns:
            Dictionary with job status information
        """
        return {
            'is_running': self.is_running,
            'jobs': [str(job) for job in schedule.jobs],
            'last_run_times': self.last_run_times
        }
    
    def clear_jobs(self) -> None:
        """Clear all scheduled jobs"""
        schedule.clear()
        logger.info("All scheduled news jobs cleared")


# Example usage
if __name__ == "__main__":
    from news_analyzer import NewsAnalyzer
    
    # Initialize the analyzer and scheduler
    news_analyzer = NewsAnalyzer()
    scheduler = NewsScheduler()
    
    # Define collection function
    def collect_and_analyze_news():
        return news_analyzer.collect_and_analyze_news()
    
    # Schedule the collection job
    scheduler.schedule_job(collect_and_analyze_news, interval_minutes=60, job_name="collect_news")
    
    # Start the scheduler
    scheduler.start_scheduler()
    
    # Keep the main thread alive for demonstration
    try:
        while True:
            time.sleep(60)
            status = scheduler.get_job_status()
            print(f"News scheduler status: {status['is_running']}")
            print(f"Last run times: {status['last_run_times']}")
    except KeyboardInterrupt:
        print("Stopping news scheduler...")
        scheduler.stop_scheduler()
