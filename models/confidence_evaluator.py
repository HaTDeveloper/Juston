"""
Confidence Evaluation System
--------------------------
This module combines results from different analysis modules to provide
overall recommendations with confidence scores.

It includes functionality for:
- Combining technical analysis signals
- Integrating news sentiment analysis
- Calculating overall confidence scores
- Generating investment recommendations
"""

import logging
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("confidence_evaluation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ConfidenceEvaluator:
    """Class to evaluate confidence in trading signals and generate recommendations"""
    
    def __init__(self, 
                 technical_weight: float = 0.6, 
                 news_weight: float = 0.2, 
                 trend_weight: float = 0.2):
        """
        Initialize the confidence evaluator
        
        Args:
            technical_weight: Weight for technical analysis (0-1)
            news_weight: Weight for news sentiment analysis (0-1)
            trend_weight: Weight for trend analysis (0-1)
        """
        # Ensure weights sum to 1
        total_weight = technical_weight + news_weight + trend_weight
        self.technical_weight = technical_weight / total_weight
        self.news_weight = news_weight / total_weight
        self.trend_weight = trend_weight / total_weight
        
        # Configuration parameters
        self.high_confidence_threshold = 75  # Confidence score above this is considered high
        self.medium_confidence_threshold = 60  # Confidence score above this is considered medium
        
        logger.info(f"Confidence Evaluator initialized with weights: Technical={self.technical_weight}, News={self.news_weight}, Trend={self.trend_weight}")
    
    def evaluate_technical_signals(self, golden_opportunity_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate technical analysis signals from golden opportunities analysis
        
        Args:
            golden_opportunity_results: Results from golden opportunities analysis
            
        Returns:
            Dictionary with technical evaluation results
        """
        try:
            if not golden_opportunity_results or 'signals' not in golden_opportunity_results:
                logger.warning("No golden opportunity results to evaluate")
                return {
                    'technical_score': 0,
                    'technical_direction': 'neutral',
                    'key_signals': []
                }
            
            signals = golden_opportunity_results.get('signals', [])
            
            if not signals:
                logger.warning("No signals in golden opportunity results")
                return {
                    'technical_score': 0,
                    'technical_direction': 'neutral',
                    'key_signals': []
                }
            
            # Count bullish and bearish signals
            bullish_signals = [s for s in signals if s.get('signal') == 'bullish']
            bearish_signals = [s for s in signals if s.get('signal') == 'bearish']
            
            # Calculate weighted scores
            bullish_score = sum(s.get('strength', 5) for s in bullish_signals)
            bearish_score = sum(s.get('strength', 5) for s in bearish_signals)
            
            # Determine technical direction
            if bullish_score > bearish_score:
                technical_direction = 'bullish'
                technical_score = min(100, (bullish_score / (bullish_score + bearish_score + 1)) * 100)
            elif bearish_score > bullish_score:
                technical_direction = 'bearish'
                technical_score = min(100, (bearish_score / (bullish_score + bearish_score + 1)) * 100)
            else:
                technical_direction = 'neutral'
                technical_score = 50
            
            # Get key signals (top 3 by strength)
            sorted_signals = sorted(signals, key=lambda x: x.get('strength', 0), reverse=True)
            key_signals = sorted_signals[:3]
            
            return {
                'technical_score': technical_score,
                'technical_direction': technical_direction,
                'bullish_score': bullish_score,
                'bearish_score': bearish_score,
                'key_signals': key_signals
            }
            
        except Exception as e:
            logger.error(f"Error evaluating technical signals: {str(e)}")
            return {
                'technical_score': 0,
                'technical_direction': 'neutral',
                'key_signals': [],
                'error': str(e)
            }
    
    def evaluate_news_sentiment(self, news_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate news sentiment analysis results
        
        Args:
            news_results: Results from news sentiment analysis
            
        Returns:
            Dictionary with news sentiment evaluation results
        """
        try:
            if not news_results or 'sentiment_score' not in news_results:
                logger.warning("No news sentiment results to evaluate")
                return {
                    'news_score': 50,
                    'news_direction': 'neutral',
                    'key_news': []
                }
            
            # Get sentiment score (assumed to be -1 to 1)
            sentiment_score = news_results.get('sentiment_score', 0)
            
            # Convert to 0-100 scale
            news_score = (sentiment_score + 1) * 50
            
            # Determine news direction
            if news_score > 60:
                news_direction = 'bullish'
            elif news_score < 40:
                news_direction = 'bearish'
            else:
                news_direction = 'neutral'
            
            # Get key news (top 3 by relevance or impact)
            news_items = news_results.get('news_items', [])
            sorted_news = sorted(news_items, key=lambda x: x.get('impact', 0), reverse=True)
            key_news = sorted_news[:3]
            
            return {
                'news_score': news_score,
                'news_direction': news_direction,
                'sentiment_score': sentiment_score,
                'key_news': key_news
            }
            
        except Exception as e:
            logger.error(f"Error evaluating news sentiment: {str(e)}")
            return {
                'news_score': 50,
                'news_direction': 'neutral',
                'key_news': [],
                'error': str(e)
            }
    
    def evaluate_trend_analysis(self, trend_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate trend analysis results
        
        Args:
            trend_results: Results from trend analysis
            
        Returns:
            Dictionary with trend evaluation results
        """
        try:
            if not trend_results or 'trend_direction' not in trend_results:
                logger.warning("No trend analysis results to evaluate")
                return {
                    'trend_score': 50,
                    'trend_direction': 'neutral',
                    'key_trend_signals': []
                }
            
            # Get trend direction and strength
            trend_direction = trend_results.get('trend_direction', 'neutral')
            trend_strength = trend_results.get('trend_strength', 5)
            
            # Convert to 0-100 scale
            if trend_direction == 'bullish':
                trend_score = 50 + (trend_strength * 5)
            elif trend_direction == 'bearish':
                trend_score = 50 - (trend_strength * 5)
            else:
                trend_score = 50
            
            # Ensure score is within 0-100 range
            trend_score = max(0, min(100, trend_score))
            
            # Get key trend signals (top 3 by strength)
            trend_signals = trend_results.get('signals', [])
            sorted_signals = sorted(trend_signals, key=lambda x: x.get('strength', 0), reverse=True)
            key_trend_signals = sorted_signals[:3]
            
            return {
                'trend_score': trend_score,
                'trend_direction': trend_direction,
                'trend_strength': trend_strength,
                'key_trend_signals': key_trend_signals
            }
            
        except Exception as e:
            logger.error(f"Error evaluating trend analysis: {str(e)}")
            return {
                'trend_score': 50,
                'trend_direction': 'neutral',
                'key_trend_signals': [],
                'error': str(e)
            }
    
    def calculate_overall_confidence(self, 
                                    technical_eval: Dict[str, Any],
                                    news_eval: Dict[str, Any],
                                    trend_eval: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate overall confidence score and recommendation
        
        Args:
            technical_eval: Technical evaluation results
            news_eval: News sentiment evaluation results
            trend_eval: Trend evaluation results
            
        Returns:
            Dictionary with overall confidence and recommendation
        """
        try:
            # Get scores
            technical_score = technical_eval.get('technical_score', 50)
            news_score = news_eval.get('news_score', 50)
            trend_score = trend_eval.get('trend_score', 50)
            
            # Get directions
            technical_direction = technical_eval.get('technical_direction', 'neutral')
            news_direction = news_eval.get('news_direction', 'neutral')
            trend_direction = trend_eval.get('trend_direction', 'neutral')
            
            # Calculate weighted score
            weighted_score = (
                technical_score * self.technical_weight +
                news_score * self.news_weight +
                trend_score * self.trend_weight
            )
            
            # Determine overall direction
            directions = [technical_direction, news_direction, trend_direction]
            weights = [self.technical_weight, self.news_weight, self.trend_weight]
            
            bullish_weight = sum(weights[i] for i, d in enumerate(directions) if d == 'bullish')
            bearish_weight = sum(weights[i] for i, d in enumerate(directions) if d == 'bearish')
            
            if bullish_weight > bearish_weight:
                overall_direction = 'bullish'
            elif bearish_weight > bullish_weight:
                overall_direction = 'bearish'
            else:
                overall_direction = 'neutral'
            
            # Determine confidence level
            if weighted_score >= self.high_confidence_threshold:
                confidence_level = 'high'
            elif weighted_score >= self.medium_confidence_threshold:
                confidence_level = 'medium'
            else:
                confidence_level = 'low'
            
            # Generate recommendation
            recommendation = self._generate_recommendation(
                overall_direction, 
                confidence_level, 
                weighted_score,
                technical_eval,
                news_eval,
                trend_eval
            )
            
            return {
                'overall_score': weighted_score,
                'overall_direction': overall_direction,
                'confidence_level': confidence_level,
                'technical_contribution': technical_score * self.technical_weight,
                'news_contribution': news_score * self.news_weight,
                'trend_contribution': trend_score * self.trend_weight,
                'recommendation': recommendation
            }
            
        except Exception as e:
            logger.error(f"Error calculating overall confidence: {str(e)}")
            return {
                'overall_score': 50,
                'overall_direction': 'neutral',
                'confidence_level': 'low',
                'recommendation': 'Hold - Insufficient data for recommendation',
                'error': str(e)
            }
    
    def _generate_recommendation(self, 
                               direction: str, 
                               confidence_level: str, 
                               score: float,
                               technical_eval: Dict[str, Any],
                               news_eval: Dict[str, Any],
                               trend_eval: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate detailed recommendation based on analysis
        
        Args:
            direction: Overall direction (bullish/bearish/neutral)
            confidence_level: Confidence level (high/medium/low)
            score: Overall confidence score
            technical_eval: Technical evaluation results
            news_eval: News sentiment evaluation results
            trend_eval: Trend evaluation results
            
        Returns:
            Dictionary with recommendation details
        """
        # Base recommendation
        if direction == 'bullish':
            action = 'Buy' if confidence_level in ['high', 'medium'] else 'Hold with bullish bias'
        elif direction == 'bearish':
            action = 'Sell' if confidence_level in ['high', 'medium'] else 'Hold with bearish bias'
        else:
            action = 'Hold'
        
        # Get key signals from each analysis
        key_technical = technical_eval.get('key_signals', [])
        key_news = news_eval.get('key_news', [])
        key_trend = trend_eval.get('key_trend_signals', [])
        
        # Create supporting points
        supporting_points = []
        
        # Add technical points
        for signal in key_technical:
            if signal.get('signal') == direction or (direction == 'neutral' and signal.get('strength', 0) > 7):
                supporting_points.append({
                    'type': 'technical',
                    'description': signal.get('description', ''),
                    'strength': signal.get('strength', 5)
                })
        
        # Add news points
        for news in key_news:
            if news.get('sentiment') == direction or (direction == 'neutral' and news.get('impact', 0) > 7):
                supporting_points.append({
                    'type': 'news',
                    'description': news.get('headline', ''),
                    'strength': news.get('impact', 5)
                })
        
        # Add trend points
        for signal in key_trend:
            if signal.get('signal') == direction or (direction == 'neutral' and signal.get('strength', 0) > 7):
                supporting_points.append({
                    'type': 'trend',
                    'description': signal.get('description', ''),
                    'strength': signal.get('strength', 5)
                })
        
        # Sort supporting points by strength
        supporting_points.sort(key=lambda x: x.get('strength', 0), reverse=True)
        
        # Generate time horizon based on signals
        if any('long' in s.get('type', '') for s in key_trend):
            time_horizon = 'Long-term (3-6 months)'
        elif any('intermediate' in s.get('type', '') for s in key_technical):
            time_horizon = 'Medium-term (1-3 months)'
        else:
            time_horizon = 'Short-term (1-4 weeks)'
        
        # Generate risk level
        if confidence_level == 'high':
            risk_level = 'Low to Medium'
        elif confidence_level == 'medium':
            risk_level = 'Medium'
        else:
            risk_level = 'High'
        
        # Create recommendation object
        recommendation = {
            'action': action,
            'confidence_score': score,
            'confidence_level': confidence_level,
            'direction': direction,
            'time_horizon': time_horizon,
            'risk_level': risk_level,
            'supporting_points': supporting_points[:5]  # Top 5 supporting points
        }
        
        return recommendation
    
    def evaluate_stock(self, 
                      symbol: str,
                      golden_opportunity_results: Dict[str, Any],
                      news_results: Dict[str, Any],
                      trend_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate a stock and generate overall recommendation
        
        Args:
            symbol: Stock symbol
            golden_opportunity_results: Results from golden opportunities analysis
            news_results: Results from news sentiment analysis
            trend_results: Results from trend analysis
            
        Returns:
            Dictionary with evaluation results and recommendation
        """
        try:
            logger.info(f"Evaluating confidence for {symbol}")
            
            # Evaluate each component
            technical_eval = self.evaluate_technical_signals(golden_opportunity_results)
            news_eval = self.evaluate_news_sentiment(news_results)
            trend_eval = self.evaluate_trend_analysis(trend_results)
            
            # Calculate overall confidence
            overall_result = self.calculate_overall_confidence(
                technical_eval,
                news_eval,
                trend_eval
            )
            
            # Get latest price and date
            latest_price = golden_opportunity_results.get('latest_price') or trend_results.get('latest_price')
            latest_date = golden_opportunity_results.get('latest_date') or trend_results.get('latest_date')
            
            # Create final result
            result = {
                'symbol': symbol,
                'evaluation_time': datetime.now(),
                'latest_price': latest_price,
                'latest_date': latest_date,
                'overall_score': overall_result.get('overall_score'),
                'overall_direction': overall_result.get('overall_direction'),
                'confidence_level': overall_result.get('confidence_level'),
                'recommendation': overall_result.get('recommendation'),
                'technical_evaluation': technical_eval,
                'news_evaluation': news_eval,
                'trend_evaluation': trend_eval
            }
            
            logger.info(f"Completed confidence evaluation for {symbol}: {result['overall_direction']} with {result['confidence_level']} confidence")
            return result
            
        except Exception as e:
            logger.error(f"Error evaluating stock {symbol}: {str(e)}")
            return {
                'symbol': symbol,
                'evaluation_time': datetime.now(),
                'overall_score': 50,
                'overall_direction': 'neutral',
                'confidence_level': 'low',
                'recommendation': {
                    'action': 'Hold - Error in evaluation',
                    'confidence_score': 0,
                    'confidence_level': 'low',
                    'supporting_points': []
                },
                'error': str(e)
            }


# Example usage
if __name__ == "__main__":
    # Sample data
    golden_opportunity_results = {
        'symbol': 'SAMPLE',
        'latest_price': 150.25,
        'latest_date': datetime.now().date(),
        'has_opportunity': True,
        'opportunity_type': 'bullish',
        'confidence_score': 75,
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
    
    # Initialize evaluator
    evaluator = ConfidenceEvaluator()
    
    # Evaluate stock
    result = evaluator.evaluate_stock('SAMPLE', golden_opportunity_results, news_results, trend_results)
    
    # Print results
    print(f"Overall direction: {result['overall_direction']}")
    print(f"Confidence level: {result['confidence_level']}")
    print(f"Overall score: {result['overall_score']}")
    print(f"Recommendation: {result['recommendation']['action']}")
    print("\nSupporting points:")
    for point in result['recommendation']['supporting_points']:
        print(f"- {point['type']}: {point['description']}")
