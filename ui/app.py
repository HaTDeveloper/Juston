"""
User Interface Module
------------------
This module provides a Flask-based web interface for the Saudi stock bot.
It includes functionality for:
- Displaying stock analysis and recommendations
- Searching for specific stocks
- Viewing historical analysis
- Configuring notification preferences
"""

import logging
import os
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, redirect, url_for
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ui.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class UserInterface:
    """Class to handle the web interface for the Saudi stock bot"""
    
    def __init__(self, 
                 data_collector=None, 
                 golden_scanner=None, 
                 trend_analyzer=None, 
                 confidence_evaluator=None,
                 notification_system=None):
        """
        Initialize the user interface
        
        Args:
            data_collector: StockDataCollector instance
            golden_scanner: GoldenOpportunitiesScanner instance
            trend_analyzer: TrendAnalyzer instance
            confidence_evaluator: ConfidenceEvaluator instance
            notification_system: NotificationSystem instance
        """
        self.data_collector = data_collector
        self.golden_scanner = golden_scanner
        self.trend_analyzer = trend_analyzer
        self.confidence_evaluator = confidence_evaluator
        self.notification_system = notification_system
        
        # Create Flask app
        self.app = Flask(__name__, 
                         template_folder='templates',
                         static_folder='static')
        
        # Register routes
        self._register_routes()
        
        logger.info("User Interface initialized")
    
    def _register_routes(self):
        """Register Flask routes"""
        
        @self.app.route('/')
        def index():
            """Home page"""
            return render_template('index.html')
        
        @self.app.route('/dashboard')
        def dashboard():
            """Dashboard page"""
            # Get latest opportunities and recommendations
            opportunities = self._get_latest_opportunities()
            recommendations = self._get_latest_recommendations()
            
            return render_template(
                'dashboard.html',
                opportunities=opportunities,
                recommendations=recommendations
            )
        
        @self.app.route('/stock/<symbol>')
        def stock_detail(symbol):
            """Stock detail page"""
            # Get stock data
            stock_data = self._get_stock_data(symbol)
            
            # Get latest analysis
            opportunity = self._get_stock_opportunity(symbol)
            recommendation = self._get_stock_recommendation(symbol)
            
            return render_template(
                'stock_detail.html',
                symbol=symbol,
                stock_data=stock_data,
                opportunity=opportunity,
                recommendation=recommendation
            )
        
        @self.app.route('/opportunities')
        def opportunities():
            """Opportunities page"""
            # Get all opportunities
            all_opportunities = self._get_all_opportunities()
            
            return render_template(
                'opportunities.html',
                opportunities=all_opportunities
            )
        
        @self.app.route('/recommendations')
        def recommendations():
            """Recommendations page"""
            # Get all recommendations
            all_recommendations = self._get_all_recommendations()
            
            return render_template(
                'recommendations.html',
                recommendations=all_recommendations
            )
        
        @self.app.route('/search')
        def search():
            """Search page"""
            query = request.args.get('q', '')
            results = []
            
            if query:
                results = self._search_stocks(query)
            
            return render_template(
                'search.html',
                query=query,
                results=results
            )
        
        @self.app.route('/settings')
        def settings():
            """Settings page"""
            return render_template('settings.html')
        
        @self.app.route('/api/analyze', methods=['POST'])
        def api_analyze():
            """API endpoint to analyze a stock"""
            data = request.json
            symbol = data.get('symbol')
            
            if not symbol:
                return jsonify({'error': 'Symbol is required'}), 400
            
            # Run analysis
            result = self._analyze_stock(symbol)
            
            return jsonify(result)
        
        @self.app.route('/api/opportunities', methods=['GET'])
        def api_opportunities():
            """API endpoint to get opportunities"""
            opportunities = self._get_latest_opportunities()
            return jsonify(opportunities)
        
        @self.app.route('/api/recommendations', methods=['GET'])
        def api_recommendations():
            """API endpoint to get recommendations"""
            recommendations = self._get_latest_recommendations()
            return jsonify(recommendations)
        
        @self.app.route('/api/market_summary', methods=['GET'])
        def api_market_summary():
            """API endpoint to get market summary"""
            summary = self._get_market_summary()
            return jsonify(summary)
        
        @self.app.route('/api/settings', methods=['POST'])
        def api_settings():
            """API endpoint to update settings"""
            data = request.json
            success = self._update_settings(data)
            
            if success:
                return jsonify({'status': 'success'})
            else:
                return jsonify({'status': 'error'}), 500
    
    def _get_latest_opportunities(self) -> List[Dict[str, Any]]:
        """
        Get latest golden opportunities
        
        Returns:
            List of opportunity dictionaries
        """
        try:
            # In a real implementation, this would fetch from a database
            # For now, return sample data
            return [
                {
                    'symbol': '1120.SR',
                    'opportunity_type': 'bullish',
                    'confidence_score': 85,
                    'latest_price': 98.75,
                    'latest_date': datetime.now().date(),
                    'key_signals': [
                        'Bullish hammer pattern',
                        'RSI oversold',
                        'Volume spike'
                    ]
                },
                {
                    'symbol': '2222.SR',
                    'opportunity_type': 'bearish',
                    'confidence_score': 78,
                    'latest_price': 32.40,
                    'latest_date': datetime.now().date(),
                    'key_signals': [
                        'Bearish engulfing pattern',
                        'MACD bearish crossover',
                        'Resistance rejection'
                    ]
                }
            ]
        except Exception as e:
            logger.error(f"Error getting latest opportunities: {str(e)}")
            return []
    
    def _get_latest_recommendations(self) -> List[Dict[str, Any]]:
        """
        Get latest stock recommendations
        
        Returns:
            List of recommendation dictionaries
        """
        try:
            # In a real implementation, this would fetch from a database
            # For now, return sample data
            return [
                {
                    'symbol': '1120.SR',
                    'action': 'Buy',
                    'confidence_level': 'high',
                    'overall_score': 82,
                    'latest_price': 98.75,
                    'latest_date': datetime.now().date(),
                    'time_horizon': 'Medium-term (1-3 months)',
                    'risk_level': 'Medium'
                },
                {
                    'symbol': '2222.SR',
                    'action': 'Sell',
                    'confidence_level': 'medium',
                    'overall_score': 68,
                    'latest_price': 32.40,
                    'latest_date': datetime.now().date(),
                    'time_horizon': 'Short-term (1-4 weeks)',
                    'risk_level': 'Medium'
                },
                {
                    'symbol': '7010.SR',
                    'action': 'Hold',
                    'confidence_level': 'medium',
                    'overall_score': 55,
                    'latest_price': 105.20,
                    'latest_date': datetime.now().date(),
                    'time_horizon': 'Short-term (1-4 weeks)',
                    'risk_level': 'Low'
                }
            ]
        except Exception as e:
            logger.error(f"Error getting latest recommendations: {str(e)}")
            return []
    
    def _get_stock_data(self, symbol: str) -> Dict[str, Any]:
        """
        Get stock data for a symbol
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary with stock data
        """
        try:
            # In a real implementation, this would use the data_collector
            # For now, return sample data
            return {
                'symbol': symbol,
                'name': 'Sample Stock',
                'sector': 'Banking',
                'latest_price': 98.75,
                'change': 1.25,
                'change_percent': 1.28,
                'volume': 1234567,
                'market_cap': 123456789000,
                'pe_ratio': 15.6,
                'dividend_yield': 3.2,
                'price_history': [
                    {'date': '2025-03-20', 'close': 95.50},
                    {'date': '2025-03-21', 'close': 96.25},
                    {'date': '2025-03-22', 'close': 97.00},
                    {'date': '2025-03-23', 'close': 96.75},
                    {'date': '2025-03-24', 'close': 97.50},
                    {'date': '2025-03-25', 'close': 97.50},
                    {'date': '2025-03-26', 'close': 98.75}
                ]
            }
        except Exception as e:
            logger.error(f"Error getting stock data for {symbol}: {str(e)}")
            return {'symbol': symbol, 'error': str(e)}
    
    def _get_stock_opportunity(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get latest golden opportunity for a stock
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Opportunity dictionary or None
        """
        try:
            # In a real implementation, this would fetch from a database
            # For now, return sample data if symbol matches
            if symbol == '1120.SR':
                return {
                    'symbol': symbol,
                    'opportunity_type': 'bullish',
                    'confidence_score': 85,
                    'latest_price': 98.75,
                    'latest_date': datetime.now().date(),
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
                        },
                        {
                            'category': 'volume',
                            'type': 'high_volume_bullish',
                            'signal': 'bullish',
                            'strength': 8,
                            'description': 'Bullish volume spike: 2.50x average volume with 1.28% price increase.'
                        }
                    ],
                    'support_level': 95.20,
                    'resistance_level': 100.50
                }
            elif symbol == '2222.SR':
                return {
                    'symbol': symbol,
                    'opportunity_type': 'bearish',
                    'confidence_score': 78,
                    'latest_price': 32.40,
                    'latest_date': datetime.now().date(),
                    'signals': [
                        {
                            'category': 'candlestick',
                            'type': 'Engulfing',
                            'signal': 'bearish',
                            'strength': 8,
                            'description': 'Bearish engulfing pattern detected, indicating potential reversal.'
                        },
                        {
                            'category': 'momentum',
                            'type': 'MACD',
                            'signal': 'bearish',
                            'strength': 7,
                            'description': 'MACD crossed below signal line, generating a bearish signal.'
                        }
                    ],
                    'support_level': 31.00,
                    'resistance_level': 33.50
                }
            return None
        except Exception as e:
            logger.error(f"Error getting opportunity for {symbol}: {str(e)}")
            return None
    
    def _get_stock_recommendation(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get latest recommendation for a stock
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Recommendation dictionary or None
        """
        try:
            # In a real implementation, this would fetch from a database
            # For now, return sample data if symbol matches
            if symbol == '1120.SR':
                return {
                    'symbol': symbol,
                    'overall_direction': 'bullish',
                    'confidence_level': 'high',
                    'overall_score': 82,
                    'latest_price': 98.75,
                    'latest_date': datetime.now().date(),
                    'recommendation': {
                        'action': 'Buy',
                        'time_horizon': 'Medium-term (1-3 months)',
                        'risk_level': 'Medium',
                        'supporting_points': [
                            {
                                'type': 'technical',
                                'description': 'Bullish hammer pattern detected, indicating potential reversal.',
                                'strength': 8
                            },
                            {
                                'type': 'news',
                                'description': 'Positive quarterly earnings report exceeding analyst expectations.',
                                'strength': 7
                            },
                            {
                                'type': 'trend',
                                'description': 'Price crossed above 50-day moving average, indicating potential uptrend.',
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
            elif symbol == '2222.SR':
                return {
                    'symbol': symbol,
                    'overall_direction': 'bearish',
                    'confidence_level': 'medium',
                    'overall_score': 68,
                    'latest_price': 32.40,
                    'latest_date': datetime.now().date(),
                    'recommendation': {
                        'action': 'Sell',
                        'time_horizon': 'Short-term (1-4 weeks)',
                        'risk_level': 'Medium',
                        'supporting_points': [
                            {
                                'type': 'technical',
                                'description': 'Bearish engulfing pattern detected, indicating potential reversal.',
                                'strength': 8
                            },
                            {
                                'type': 'news',
                                'description': 'Negative industry outlook affecting future growth prospects.',
                                'strength': 6
                            }
                        ]
                    },
                    'technical_evaluation': {
                        'technical_direction': 'bearish',
                        'technical_score': 78
                    },
                    'news_evaluation': {
                        'news_direction': 'bearish',
                        'news_score': 65
                    },
                    'trend_evaluation': {
                        'trend_direction': 'neutral',
                        'trend_score': 55
                    }
                }
            return None
        except Exception as e:
            logger.error(f"Error getting recommendation for {symbol}: {str(e)}")
            return None
    
    def _get_all_opportunities(self) -> List[Dict[str, Any]]:
        """
        Get all golden opportunities
        
        Returns:
            List of opportunity dictionaries
        """
        try:
            # In a real implementation, this would fetch from a database
            # For now, return sample data
            return self._get_latest_opportunities() + [
                {
                    'symbol': '7010.SR',
                    'opportunity_type': 'bullish',
                    'confidence_score': 72,
                    'latest_price': 105.20,
                    'latest_date': datetime.now().date(),
                    'key_signals': [
                        'Golden cross',
                        'Support bounce',
                        'Positive volume trend'
                    ]
                }
            ]
        except Exception as e:
            logger.error(f"Error getting all opportunities: {str(e)}")
            return []
    
    def _get_all_recommendations(self) -> List[Dict[str, Any]]:
        """
        Get all stock recommendations
        
        Returns:
            List of recommendation dictionaries
        """
        try:
            # In a real implementation, this would fetch from a database
            # For now, return sample data
            return self._get_latest_recommendations()
        except Exception as e:
            logger.error(f"Error getting all recommendations: {str(e)}")
            return []
    
    def _search_stocks(self, query: str) -> List[Dict[str, Any]]:
        """
        Search for stocks by symbol or name
        
        Args:
            query: Search query
            
        Returns:
            List of matching stock dictionaries
        """
        try:
            # In a real implementation, this would search a database
            # For now, return sample data
            sample_stocks = [
                {'symbol': '1120.SR', 'name': 'Al Rajhi Bank', 'sector': 'Banking'},
                {'symbol': '2222.SR', 'name': 'Saudi Aramco', 'sector': 'Energy'},
                {'symbol': '7010.SR', 'name': 'STC', 'sector': 'Telecommunications'}
            ]
            
            # Filter by query
            query = query.lower()
            results = [
                stock for stock in sample_stocks
                if query in stock['symbol'].lower() or query in stock['name'].lower()
            ]
            
            return results
        except Exception as e:
            logger.error(f"Error searching stocks for '{query}': {str(e)}")
            return []
    
    def _analyze_stock(self, symbol: str) -> Dict[str, Any]:
        """
        Run analysis on a stock
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Analysis results
        """
        try:
            # In a real implementation, this would use the analysis components
            # For now, return sample data
            opportunity = self._get_stock_opportunity(symbol)
            recommendation = self._get_stock_recommendation(symbol)
            
            return {
                'symbol': symbol,
                'analysis_time': datetime.now().isoformat(),
                'opportunity': opportunity,
                'recommendation': recommendation
            }
        except Exception as e:
            logger.error(f"Error analyzing stock {symbol}: {str(e)}")
            return {'symbol': symbol, 'error': str(e)}
    
    def _get_market_summary(self) -> Dict[str, Any]:
        """
        Get market summary
        
        Returns:
            Market summary dictionary
        """
        try:
            # In a real implementation, this would calculate from actual data
            # For now, return sample data
            return {
                'date': datetime.now().date().isoformat(),
                'index_value': 12345.67,
                'index_change': 23.45,
                'index_change_percent': 0.19,
                'advancing_stocks': 87,
                'declining_stocks': 65,
                'unchanged_stocks': 23,
                'total_volume': 1234567890,
                'total_value': 9876543210,
                'top_sectors': [
                    {'name': 'Banking', 'change_percent': 0.75},
                    {'name': 'Energy', 'change_percent': 0.32},
                    {'name': 'Telecommunications', 'change_percent': -0.18}
                ],
                'top_gainers': [
                    {'symbol': '1234.SR', 'name': 'Stock A', 'change_percent': 4.56},
                    {'symbol': '5678.SR', 'name': 'Stock B', 'change_percent': 3.21}
                ],
                'top_losers': [
                    {'symbol': '9012.SR', 'name': 'Stock C', 'change_percent': -3.45},
                    {'symbol': '3456.SR', 'name': 'Stock D', 'change_percent': -2.67}
                ]
            }
        except Exception as e:
            logger.error(f"Error getting market summary: {str(e)}")
            return {'error': str(e)}
    
    def _update_settings(self, settings: Dict[str, Any]) -> bool:
        """
        Update user settings
        
        Args:
            settings: Settings dictionary
            
        Returns:
            Boolean indicating success
        """
        try:
            # In a real implementation, this would update settings in a database
            # For now, just log the settings
            logger.info(f"Updating settings: {settings}")
            return True
        except Exception as e:
            logger.error(f"Error updating settings: {str(e)}")
            return False
    
    def run(self, host: str = '0.0.0.0', port: int = None, debug: bool = False):
        """
        Run the Flask application
        
        Args:
            host: Host to run on
            port: Port to run on (uses PORT environment variable if not specified)
            debug: Whether to run in debug mode
        """
        # Get port from environment variable if not specified
        if port is None:
            port = int(os.environ.get('PORT', 5000))
        
        logger.info(f"Starting web interface on {host}:{port}")
        self.app.run(host=host, port=port, debug=debug)


# Example usage
if __name__ == "__main__":
    # Initialize user interface
    ui = UserInterface()
    
    # Run the application
    ui.run(debug=True)
