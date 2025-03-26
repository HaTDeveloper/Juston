"""
Notification and Reporting System
-------------------------------
This module handles sending notifications and generating reports for the Saudi stock bot.
It includes functionality for:
- Sending Discord webhook notifications
- Generating daily and weekly reports
- Formatting recommendations for different output formats
"""

import logging
import json
import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("notification_system.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class NotificationSystem:
    """Class to handle notifications and reports for the Saudi stock bot"""
    
    def __init__(self, discord_webhook_url: str = None):
        """
        Initialize the notification system
        
        Args:
            discord_webhook_url: Discord webhook URL for sending notifications
        """
        # Get Discord webhook URL from environment variable if not provided
        self.discord_webhook_url = discord_webhook_url or os.getenv('DISCORD_WEBHOOK_URL')
        
        if not self.discord_webhook_url:
            logger.warning("Discord webhook URL not provided. Discord notifications will be disabled.")
        
        # Set up colors for different notification types
        self.colors = {
            'bullish': 0x00FF00,  # Green
            'bearish': 0xFF0000,  # Red
            'neutral': 0xFFFF00,  # Yellow
            'info': 0x0099FF,     # Blue
            'error': 0xFF00FF     # Purple
        }
        
        logger.info("Notification System initialized")
    
    def send_discord_notification(self, 
                                 title: str, 
                                 description: str, 
                                 fields: List[Dict[str, Any]] = None,
                                 color: str = 'info',
                                 thumbnail_url: str = None,
                                 image_url: str = None) -> bool:
        """
        Send notification to Discord webhook
        
        Args:
            title: Notification title
            description: Notification description
            fields: List of field dictionaries with name, value, and inline keys
            color: Color type (bullish, bearish, neutral, info, error)
            thumbnail_url: URL for thumbnail image
            image_url: URL for main image
            
        Returns:
            Boolean indicating success
        """
        try:
            if not self.discord_webhook_url:
                logger.warning("Discord webhook URL not available. Notification not sent.")
                return False
            
            # Create embed
            embed = {
                "title": title,
                "description": description,
                "color": self.colors.get(color, self.colors['info']),
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {
                    "text": "Saudi Stock Bot | Automated Analysis"
                }
            }
            
            # Add fields if provided
            if fields:
                embed["fields"] = fields
            
            # Add thumbnail if provided
            if thumbnail_url:
                embed["thumbnail"] = {"url": thumbnail_url}
            
            # Add image if provided
            if image_url:
                embed["image"] = {"url": image_url}
            
            # Create payload
            payload = {
                "username": "Saudi Stock Bot",
                "embeds": [embed]
            }
            
            # Send to Discord
            response = requests.post(
                self.discord_webhook_url,
                json=payload
            )
            
            if response.status_code == 204:
                logger.info(f"Discord notification sent successfully: {title}")
                return True
            else:
                logger.error(f"Failed to send Discord notification. Status code: {response.status_code}, Response: {response.text}")
                return False
            
        except Exception as e:
            logger.error(f"Error sending Discord notification: {str(e)}")
            return False
    
    def format_golden_opportunity_notification(self, opportunity: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format golden opportunity for notification
        
        Args:
            opportunity: Golden opportunity analysis result
            
        Returns:
            Formatted notification data
        """
        try:
            symbol = opportunity.get('symbol', 'Unknown')
            opportunity_type = opportunity.get('opportunity_type', 'neutral')
            confidence_score = opportunity.get('confidence_score', 0)
            latest_price = opportunity.get('latest_price', 0)
            
            # Create title and description
            title = f"🔔 Golden Opportunity: {symbol}"
            description = f"**{opportunity_type.upper()} opportunity detected with {confidence_score:.0f}% confidence**\n"
            description += f"Current price: {latest_price:.2f} SAR\n\n"
            
            # Add top signals
            signals = opportunity.get('signals', [])
            top_signals = sorted(signals, key=lambda x: x.get('strength', 0), reverse=True)[:3]
            
            fields = []
            for signal in top_signals:
                fields.append({
                    "name": f"{signal.get('category', 'Signal')} - {signal.get('type', 'Unknown')}",
                    "value": signal.get('description', 'No description available'),
                    "inline": False
                })
            
            # Add support and resistance levels
            support_level = opportunity.get('support_level')
            resistance_level = opportunity.get('resistance_level')
            
            if support_level and resistance_level:
                fields.append({
                    "name": "Key Levels",
                    "value": f"Support: {support_level:.2f} SAR\nResistance: {resistance_level:.2f} SAR",
                    "inline": False
                })
            
            return {
                "title": title,
                "description": description,
                "fields": fields,
                "color": opportunity_type if opportunity_type in ['bullish', 'bearish'] else 'neutral'
            }
            
        except Exception as e:
            logger.error(f"Error formatting golden opportunity notification: {str(e)}")
            return {
                "title": f"Golden Opportunity: {opportunity.get('symbol', 'Unknown')}",
                "description": "Error formatting notification",
                "fields": [],
                "color": "error"
            }
    
    def format_recommendation_notification(self, evaluation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format recommendation for notification
        
        Args:
            evaluation: Stock evaluation result
            
        Returns:
            Formatted notification data
        """
        try:
            symbol = evaluation.get('symbol', 'Unknown')
            overall_direction = evaluation.get('overall_direction', 'neutral')
            confidence_level = evaluation.get('confidence_level', 'low')
            overall_score = evaluation.get('overall_score', 0)
            latest_price = evaluation.get('latest_price', 0)
            
            # Get recommendation details
            recommendation = evaluation.get('recommendation', {})
            action = recommendation.get('action', 'Hold')
            time_horizon = recommendation.get('time_horizon', 'Unknown')
            risk_level = recommendation.get('risk_level', 'Unknown')
            supporting_points = recommendation.get('supporting_points', [])
            
            # Create title and description
            title = f"📊 Stock Recommendation: {symbol}"
            description = f"**{action.upper()}** with **{confidence_level.upper()}** confidence ({overall_score:.0f}%)\n"
            description += f"Current price: {latest_price:.2f} SAR\n"
            description += f"Time horizon: {time_horizon}\n"
            description += f"Risk level: {risk_level}\n\n"
            
            # Create fields for supporting points
            fields = []
            for i, point in enumerate(supporting_points[:5]):
                fields.append({
                    "name": f"Supporting Point #{i+1} ({point.get('type', 'analysis')})",
                    "value": point.get('description', 'No description available'),
                    "inline": False
                })
            
            # Add technical, news, and trend contributions
            technical_eval = evaluation.get('technical_evaluation', {})
            news_eval = evaluation.get('news_evaluation', {})
            trend_eval = evaluation.get('trend_evaluation', {})
            
            fields.append({
                "name": "Analysis Contributions",
                "value": (
                    f"Technical: {technical_eval.get('technical_direction', 'neutral')} ({technical_eval.get('technical_score', 0):.0f}%)\n"
                    f"News: {news_eval.get('news_direction', 'neutral')} ({news_eval.get('news_score', 0):.0f}%)\n"
                    f"Trend: {trend_eval.get('trend_direction', 'neutral')} ({trend_eval.get('trend_score', 0):.0f}%)"
                ),
                "inline": False
            })
            
            return {
                "title": title,
                "description": description,
                "fields": fields,
                "color": overall_direction if overall_direction in ['bullish', 'bearish'] else 'neutral'
            }
            
        except Exception as e:
            logger.error(f"Error formatting recommendation notification: {str(e)}")
            return {
                "title": f"Stock Recommendation: {evaluation.get('symbol', 'Unknown')}",
                "description": "Error formatting notification",
                "fields": [],
                "color": "error"
            }
    
    def send_golden_opportunity_alert(self, opportunity: Dict[str, Any]) -> bool:
        """
        Send golden opportunity alert to Discord
        
        Args:
            opportunity: Golden opportunity analysis result
            
        Returns:
            Boolean indicating success
        """
        try:
            # Format notification
            notification = self.format_golden_opportunity_notification(opportunity)
            
            # Send to Discord
            return self.send_discord_notification(
                title=notification['title'],
                description=notification['description'],
                fields=notification['fields'],
                color=notification['color']
            )
            
        except Exception as e:
            logger.error(f"Error sending golden opportunity alert: {str(e)}")
            return False
    
    def send_recommendation_alert(self, evaluation: Dict[str, Any]) -> bool:
        """
        Send recommendation alert to Discord
        
        Args:
            evaluation: Stock evaluation result
            
        Returns:
            Boolean indicating success
        """
        try:
            # Format notification
            notification = self.format_recommendation_notification(evaluation)
            
            # Send to Discord
            return self.send_discord_notification(
                title=notification['title'],
                description=notification['description'],
                fields=notification['fields'],
                color=notification['color']
            )
            
        except Exception as e:
            logger.error(f"Error sending recommendation alert: {str(e)}")
            return False
    
    def generate_daily_report(self, 
                             opportunities: List[Dict[str, Any]],
                             recommendations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate daily report
        
        Args:
            opportunities: List of golden opportunities
            recommendations: List of stock recommendations
            
        Returns:
            Report data
        """
        try:
            # Count opportunities by type
            bullish_opportunities = [o for o in opportunities if o.get('opportunity_type') == 'bullish']
            bearish_opportunities = [o for o in opportunities if o.get('opportunity_type') == 'bearish']
            
            # Count recommendations by action
            buy_recommendations = [r for r in recommendations if 'Buy' in r.get('recommendation', {}).get('action', '')]
            sell_recommendations = [r for r in recommendations if 'Sell' in r.get('recommendation', {}).get('action', '')]
            hold_recommendations = [r for r in recommendations if 'Hold' in r.get('recommendation', {}).get('action', '')]
            
            # Create report title and description
            today = datetime.now().strftime('%Y-%m-%d')
            title = f"📈 Daily Market Report: {today}"
            description = f"**Summary of today's analysis**\n\n"
            description += f"Total opportunities: {len(opportunities)} ({len(bullish_opportunities)} bullish, {len(bearish_opportunities)} bearish)\n"
            description += f"Recommendations: {len(buy_recommendations)} Buy, {len(sell_recommendations)} Sell, {len(hold_recommendations)} Hold\n\n"
            
            # Create fields for top opportunities
            fields = []
            
            # Add top bullish opportunities
            if bullish_opportunities:
                top_bullish = sorted(bullish_opportunities, key=lambda x: x.get('confidence_score', 0), reverse=True)[:3]
                bullish_text = ""
                for opp in top_bullish:
                    symbol = opp.get('symbol', 'Unknown')
                    confidence = opp.get('confidence_score', 0)
                    price = opp.get('latest_price', 0)
                    bullish_text += f"**{symbol}**: {confidence:.0f}% confidence, Price: {price:.2f} SAR\n"
                
                fields.append({
                    "name": "🟢 Top Bullish Opportunities",
                    "value": bullish_text or "None found",
                    "inline": False
                })
            
            # Add top bearish opportunities
            if bearish_opportunities:
                top_bearish = sorted(bearish_opportunities, key=lambda x: x.get('confidence_score', 0), reverse=True)[:3]
                bearish_text = ""
                for opp in top_bearish:
                    symbol = opp.get('symbol', 'Unknown')
                    confidence = opp.get('confidence_score', 0)
                    price = opp.get('latest_price', 0)
                    bearish_text += f"**{symbol}**: {confidence:.0f}% confidence, Price: {price:.2f} SAR\n"
                
                fields.append({
                    "name": "🔴 Top Bearish Opportunities",
                    "value": bearish_text or "None found",
                    "inline": False
                })
            
            # Add top buy recommendations
            if buy_recommendations:
                top_buys = sorted(buy_recommendations, key=lambda x: x.get('overall_score', 0), reverse=True)[:3]
                buys_text = ""
                for rec in top_buys:
                    symbol = rec.get('symbol', 'Unknown')
                    confidence = rec.get('overall_score', 0)
                    price = rec.get('latest_price', 0)
                    buys_text += f"**{symbol}**: {confidence:.0f}% confidence, Price: {price:.2f} SAR\n"
                
                fields.append({
                    "name": "💰 Top Buy Recommendations",
                    "value": buys_text or "None found",
                    "inline": False
                })
            
            # Add top sell recommendations
            if sell_recommendations:
                top_sells = sorted(sell_recommendations, key=lambda x: x.get('overall_score', 0), reverse=True)[:3]
                sells_text = ""
                for rec in top_sells:
                    symbol = rec.get('symbol', 'Unknown')
                    confidence = rec.get('overall_score', 0)
                    price = rec.get('latest_price', 0)
                    sells_text += f"**{symbol}**: {confidence:.0f}% confidence, Price: {price:.2f} SAR\n"
                
                fields.append({
                    "name": "📉 Top Sell Recommendations",
                    "value": sells_text or "None found",
                    "inline": False
                })
            
            return {
                "title": title,
                "description": description,
                "fields": fields,
                "color": "info"
            }
            
        except Exception as e:
            logger.error(f"Error generating daily report: {str(e)}")
            return {
                "title": f"Daily Market Report: {datetime.now().strftime('%Y-%m-%d')}",
                "description": "Error generating report",
                "fields": [],
                "color": "error"
            }
    
    def send_daily_report(self, 
                         opportunities: List[Dict[str, Any]],
                         recommendations: List[Dict[str, Any]]) -> bool:
        """
        Send daily report to Discord
        
        Args:
            opportunities: List of golden opportunities
            recommendations: List of stock recommendations
            
        Returns:
            Boolean indicating success
        """
        try:
            # Generate report
            report = self.generate_daily_report(opportunities, recommendations)
            
            # Send to Discord
            return self.send_discord_notification(
                title=report['title'],
                description=report['description'],
                fields=report['fields'],
                color=report['color']
            )
            
        except Exception as e:
            logger.error(f"Error sending daily report: {str(e)}")
            return False
    
    def generate_weekly_report(self, 
                              weekly_opportunities: List[Dict[str, Any]],
                              weekly_recommendations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate weekly report
        
        Args:
            weekly_opportunities: List of golden opportunities for the week
            weekly_recommendations: List of stock recommendations for the week
            
        Returns:
            Report data
        """
        try:
            # Group opportunities by day
            opportunities_by_day = {}
            for opp in weekly_opportunities:
                date_str = opp.get('latest_date', datetime.now().date()).strftime('%Y-%m-%d')
                if date_str not in opportunities_by_day:
                    opportunities_by_day[date_str] = []
                opportunities_by_day[date_str].append(opp)
            
            # Count opportunities by type
            bullish_opportunities = [o for o in weekly_opportunities if o.get('opportunity_type') == 'bullish']
            bearish_opportunities = [o for o in weekly_opportunities if o.get('opportunity_type') == 'bearish']
            
            # Count recommendations by action
            buy_recommendations = [r for r in weekly_recommendations if 'Buy' in r.get('recommendation', {}).get('action', '')]
            sell_recommendations = [r for r in weekly_recommendations if 'Sell' in r.get('recommendation', {}).get('action', '')]
            
            # Create report title and description
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            title = f"📊 Weekly Market Report: {start_date} to {end_date}"
            description = f"**Summary of this week's analysis**\n\n"
            description += f"Total opportunities: {len(weekly_opportunities)} ({len(bullish_opportunities)} bullish, {len(bearish_opportunities)} bearish)\n"
            description += f"Recommendations: {len(buy_recommendations)} Buy, {len(sell_recommendations)} Sell\n\n"
            
            # Create fields for weekly summary
            fields = []
            
            # Add daily opportunity counts
            daily_counts = {}
            for date_str, opps in opportunities_by_day.items():
                bullish = len([o for o in opps if o.get('opportunity_type') == 'bullish'])
                bearish = len([o for o in opps if o.get('opportunity_type') == 'bearish'])
                daily_counts[date_str] = (bullish, bearish)
            
            if daily_counts:
                daily_text = ""
                for date_str, (bullish, bearish) in sorted(daily_counts.items()):
                    daily_text += f"**{date_str}**: {bullish} bullish, {bearish} bearish\n"
                
                fields.append({
                    "name": "📅 Daily Opportunity Counts",
                    "value": daily_text or "No data available",
                    "inline": False
                })
            
            # Add top performing stocks (those with most bullish signals)
            stock_counts = {}
            for opp in bullish_opportunities:
                symbol = opp.get('symbol', 'Unknown')
                if symbol not in stock_counts:
                    stock_counts[symbol] = 0
                stock_counts[symbol] += 1
            
            if stock_counts:
                top_stocks = sorted(stock_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                stocks_text = ""
                for symbol, count in top_stocks:
                    stocks_text += f"**{symbol}**: {count} bullish signals\n"
                
                fields.append({
                    "name": "🌟 Top Performing Stocks This Week",
                    "value": stocks_text or "No data available",
                    "inline": False
                })
            
            # Add worst performing stocks (those with most bearish signals)
            stock_counts = {}
            for opp in bearish_opportunities:
                symbol = opp.get('symbol', 'Unknown')
                if symbol not in stock_counts:
                    stock_counts[symbol] = 0
                stock_counts[symbol] += 1
            
            if stock_counts:
                bottom_stocks = sorted(stock_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                stocks_text = ""
                for symbol, count in bottom_stocks:
                    stocks_text += f"**{symbol}**: {count} bearish signals\n"
                
                fields.append({
                    "name": "⚠️ Worst Performing Stocks This Week",
                    "value": stocks_text or "No data available",
                    "inline": False
                })
            
            # Add top buy recommendations for the week
            if buy_recommendations:
                top_buys = sorted(buy_recommendations, key=lambda x: x.get('overall_score', 0), reverse=True)[:5]
                buys_text = ""
                for rec in top_buys:
                    symbol = rec.get('symbol', 'Unknown')
                    confidence = rec.get('overall_score', 0)
                    buys_text += f"**{symbol}**: {confidence:.0f}% confidence\n"
                
                fields.append({
                    "name": "💰 Top Buy Recommendations This Week",
                    "value": buys_text or "None found",
                    "inline": False
                })
            
            return {
                "title": title,
                "description": description,
                "fields": fields,
                "color": "info"
            }
            
        except Exception as e:
            logger.error(f"Error generating weekly report: {str(e)}")
            return {
                "title": f"Weekly Market Report",
                "description": "Error generating report",
                "fields": [],
                "color": "error"
            }
    
    def send_weekly_report(self, 
                          weekly_opportunities: List[Dict[str, Any]],
                          weekly_recommendations: List[Dict[str, Any]]) -> bool:
        """
        Send weekly report to Discord
        
        Args:
            weekly_opportunities: List of golden opportunities for the week
            weekly_recommendations: List of stock recommendations for the week
            
        Returns:
            Boolean indicating success
        """
        try:
            # Generate report
            report = self.generate_weekly_report(weekly_opportunities, weekly_recommendations)
            
            # Send to Discord
            return self.send_discord_notification(
                title=report['title'],
                description=report['description'],
                fields=report['fields'],
                color=report['color']
            )
            
        except Exception as e:
            logger.error(f"Error sending weekly report: {str(e)}")
            return False
    
    def generate_chart_image(self, 
                           symbol: str, 
                           df: pd.DataFrame, 
                           save_path: str) -> Optional[str]:
        """
        Generate chart image for a stock
        
        Args:
            symbol: Stock symbol
            df: DataFrame with OHLC data
            save_path: Path to save the chart image
            
        Returns:
            Path to saved image or None if failed
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # Create figure with two subplots (price and volume)
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
            
            # Plot price
            ax1.plot(df.index, df['close'], label='Close Price')
            
            # Add moving averages
            if len(df) >= 20:
                df['MA20'] = df['close'].rolling(window=20).mean()
                ax1.plot(df.index, df['MA20'], label='20-day MA', linestyle='--')
            
            if len(df) >= 50:
                df['MA50'] = df['close'].rolling(window=50).mean()
                ax1.plot(df.index, df['MA50'], label='50-day MA', linestyle='-.')
            
            # Set title and labels
            ax1.set_title(f'{symbol} Price Chart', fontsize=16)
            ax1.set_ylabel('Price (SAR)', fontsize=12)
            ax1.grid(True, alpha=0.3)
            ax1.legend()
            
            # Plot volume
            ax2.bar(df.index, df['volume'], color='blue', alpha=0.5)
            ax2.set_ylabel('Volume', fontsize=12)
            ax2.set_xlabel('Date', fontsize=12)
            ax2.grid(True, alpha=0.3)
            
            # Adjust layout and save
            plt.tight_layout()
            plt.savefig(save_path)
            plt.close()
            
            logger.info(f"Chart image generated for {symbol} at {save_path}")
            return save_path
            
        except Exception as e:
            logger.error(f"Error generating chart image for {symbol}: {str(e)}")
            return None


# Example usage
if __name__ == "__main__":
    # Initialize notification system
    notification_system = NotificationSystem()
    
    # Example golden opportunity
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
    
    # Send golden opportunity alert
    notification_system.send_golden_opportunity_alert(opportunity)
    
    # Example recommendation
    evaluation = {
        'symbol': '1120.SR',
        'overall_direction': 'bullish',
        'confidence_level': 'high',
        'overall_score': 82,
        'latest_price': 98.75,
        'recommendation': {
            'action': 'Buy',
            'time_horizon': 'Medium-term (1-3 months)',
            'risk_level': 'Medium',
            'supporting_points': [
                {
                    'type': 'technical',
                    'description': 'Bullish hammer pattern detected, indicating potential reversal.',
                    'strength': 8
                }
            ]
        },
        'technical_evaluation': {
            'technical_direction': 'bullish',
            'technical_score': 85
        },
        'news_evaluation': {
            'news_direction': 'bullish',
            'news_score': 75
        },
        'trend_evaluation': {
            'trend_direction': 'bullish',
            'trend_score': 80
        }
    }
    
    # Send recommendation alert
    notification_system.send_recommendation_alert(evaluation)
