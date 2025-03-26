"""
Saudi Stock Data Collection Module
---------------------------------
This module handles fetching and storing stock data from Yahoo Finance.
It includes functionality for:
- Fetching historical data for Saudi stocks
- Periodic updates of stock data
- Storing data in SQL database
- Error handling and retry mechanisms
"""

import os
import time
import logging
import datetime
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Dict, Optional, Union, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stock_data.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create SQLAlchemy Base
Base = declarative_base()

# Define Stock data model
class StockPrice(Base):
    """SQLAlchemy model for stock price data"""
    __tablename__ = 'stock_prices'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    adjusted_close = Column(Float)
    
    def __repr__(self):
        return f"<StockPrice(symbol='{self.symbol}', date='{self.date}', close={self.close})>"


class StockDataCollector:
    """Class to handle collection and storage of Saudi stock data"""
    
    def __init__(self, db_uri: str = None):
        """
        Initialize the stock data collector
        
        Args:
            db_uri: SQLAlchemy database URI. If None, uses SQLite in-memory DB
        """
        if db_uri is None:
            db_uri = os.environ.get('DATABASE_URI', 'sqlite:///saudi_stocks.db')
        
        self.engine = create_engine(db_uri)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.saudi_market_symbols = []
        self.update_interval_minutes = 30  # Default update interval
        
    def get_saudi_market_symbols(self) -> List[str]:
        """
        Get list of Saudi market stock symbols
        
        Returns:
            List of stock symbols in format XXXX.SR
        """
        try:
            # This could be expanded to dynamically fetch all Saudi market symbols
            # For now, using a predefined list of major Saudi stocks
            symbols = [
                "1010.SR",  # RIBL - Riyad Bank
                "1120.SR",  # Al Rajhi Bank
                "2010.SR",  # SABIC
                "2222.SR",  # Aramco
                "4240.SR",  # Fawaz Alhokair Group
                "7010.SR",  # STC
                "4003.SR",  # Cement
                "4008.SR",  # SPCC
                "4013.SR",  # SPPC
                "4031.SR",  # Saudi Cement
                "4050.SR",  # SIIG
                "4190.SR",  # Jarir
                "4200.SR",  # Dallah Health
                "4300.SR",  # Dar Al Arkan
                "4321.SR",  # SPIMACO
                "4330.SR",  # SADAFCO
                "4336.SR",  # NADEC
                "4344.SR",  # SACO
                "4347.SR",  # BinDawood
                "4001.SR",  # CHEMANOL
                "4002.SR",  # PETROCHEM
                "4004.SR",  # SAFCO
                "4005.SR",  # GASCO
                "4006.SR",  # SASREF
                "4007.SR",  # TASNEE
                "4009.SR",  # SABIC AGRI
                "4010.SR",  # SABIC CMP
                "4011.SR",  # PETRORABIGH
                "4012.SR",  # PETRO
                "4014.SR",  # APPC
                "4015.SR",  # GLASS
                "4016.SR",  # FIPCO
                "4017.SR",  # NGC
                "4018.SR",  # MAADEN
                "4019.SR",  # YANSAB
                "4020.SR",  # SIPCHEM
                "4021.SR",  # SHARQIYA
                "4022.SR",  # ZAMIL
                "4023.SR",  # SAHARA
                "4024.SR",  # INDUSTRIALIZATION
                "4025.SR",  # SADAFCO
                "4026.SR",  # UNITED WIRE
                "4027.SR",  # SCHLUMBERGER
                "4028.SR",  # SIDC
                "4029.SR",  # SAUDI ADVANCED
                "4030.SR",  # SAUDI KAYAN
                "4032.SR",  # SABIC INNOVATIVE
                "4033.SR",  # METHANOL
                "4034.SR",  # NAMA CHEMICALS
                "4035.SR",  # SPM
                "4036.SR",  # SABIC AGRI-NUTRIENTS
                "4037.SR",  # ALUJAIN
                "4038.SR",  # FIPCO
                "4039.SR",  # TABUK CEMENT
                "4040.SR",  # SABIC FERTILIZERS
                "4041.SR",  # SAUDI CERAMICS
                "4042.SR",  # EMAAR EC
                "4043.SR",  # SAUDI INDUSTRIAL
                "4044.SR",  # ALMARAI
                "4045.SR",  # SAVOLA
                "4046.SR",  # TAKWEEN
                "4047.SR",  # ASTRA INDUSTRIAL
                "4048.SR",  # SIIG
                "4049.SR",  # SISCO
                "4051.SR",  # BAWAN
                "4052.SR",  # SAVOLA FOODS
                "4053.SR",  # SAVOLA RETAIL
                "4054.SR",  # SAVOLA PACKAGING
                "4055.SR",  # SAVOLA SUGAR
                "4056.SR",  # SAVOLA OIL
                "4057.SR",  # ALBABTAIN
                "4058.SR",  # FITAIHI
                "4059.SR",  # OTHAIM
                "4060.SR",  # JARIR
                "4061.SR",  # ALDREES
                "4062.SR",  # SASCO
                "4063.SR",  # EXTRA
                "4064.SR",  # SAPTCO
                "4065.SR",  # BUDGET SAUDI
                "4066.SR",  # THIMAR
                "4067.SR",  # JARIR MARKETING
                "4068.SR",  # ALHOKAIR
                "4069.SR",  # FITAIHI GROUP
                "4070.SR",  # TIHAMA
                "4071.SR",  # SRMG
                "4072.SR",  # ALTAYYAR
                "4073.SR",  # SEERA
                "4074.SR",  # DALLAH HEALTH
                "4075.SR",  # MOUWASAT
                "4076.SR",  # ALHAMMADI
                "4077.SR",  # NMCC
                "4078.SR",  # CARE
                "4079.SR",  # SULAIMAN ALHABIB
                "4080.SR",  # HERFY FOODS
                "4081.SR",  # RAYDAN
                "4082.SR",  # BAAZEEM
                "4083.SR",  # DUR
                "4084.SR",  # TECO
                "4085.SR",  # RED SEA
                "4086.SR",  # ALKHALEEJ TRAINING
                "4087.SR",  # TAIBA
                "4088.SR",  # MAKKAH
                "4089.SR",  # ALTAYYAR TRAVEL
                "4090.SR",  # JABAL OMAR
                "4091.SR",  # TAIBA HOLDING
                "4092.SR",  # MAKKAH CONSTRUCTION
                "4093.SR",  # EMAAR THE ECONOMIC CITY
                "4094.SR",  # JABAL OMAR DEVELOPMENT
                "4095.SR",  # TAIBA INVESTMENT
                "4096.SR",  # RED SEA INTERNATIONAL
                "4097.SR",  # ALDREES PETROLEUM
                "4098.SR",  # SASCO
                "4099.SR",  # BUDGET SAUDI
                "4100.SR",  # THIMAR HOLDING
                "4101.SR",  # BAAZEEM TRADING
                "4102.SR",  # RAYDAN FOOD
                "4103.SR",  # DUR HOSPITALITY
                "4104.SR",  # TECO
                "4105.SR",  # RED SEA HOUSING
                "4106.SR",  # ALKHALEEJ TRAINING
                "4107.SR",  # TAIBA HOLDING
                "4108.SR",  # MAKKAH CONSTRUCTION
                "4109.SR",  # EMAAR THE ECONOMIC CITY
                "4110.SR",  # JABAL OMAR DEVELOPMENT
                "4111.SR",  # TAIBA INVESTMENT
                "4112.SR",  # RED SEA INTERNATIONAL
                "4113.SR",  # ALDREES PETROLEUM
                "4114.SR",  # SASCO
                "4115.SR",  # BUDGET SAUDI
                "4116.SR",  # THIMAR HOLDING
                "4117.SR",  # BAAZEEM TRADING
                "4118.SR",  # RAYDAN FOOD
                "4119.SR",  # DUR HOSPITALITY
                "4120.SR",  # TECO
                "4121.SR",  # RED SEA HOUSING
                "4122.SR",  # ALKHALEEJ TRAINING
                "4123.SR",  # TAIBA HOLDING
                "4124.SR",  # MAKKAH CONSTRUCTION
                "4125.SR",  # EMAAR THE ECONOMIC CITY
                "4126.SR",  # JABAL OMAR DEVELOPMENT
                "4127.SR",  # TAIBA INVESTMENT
                "4128.SR",  # RED SEA INTERNATIONAL
                "4129.SR",  # ALDREES PETROLEUM
                "4130.SR",  # SASCO
                "4131.SR",  # BUDGET SAUDI
                "4132.SR",  # THIMAR HOLDING
                "4133.SR",  # BAAZEEM TRADING
                "4134.SR",  # RAYDAN FOOD
                "4135.SR",  # DUR HOSPITALITY
                "4136.SR",  # TECO
                "4137.SR",  # RED SEA HOUSING
                "4138.SR",  # ALKHALEEJ TRAINING
                "4139.SR",  # TAIBA HOLDING
                "4140.SR",  # MAKKAH CONSTRUCTION
                "4141.SR",  # EMAAR THE ECONOMIC CITY
                "4142.SR",  # JABAL OMAR DEVELOPMENT
                "4143.SR",  # TAIBA INVESTMENT
                "4144.SR",  # RED SEA INTERNATIONAL
                "4145.SR",  # ALDREES PETROLEUM
                "4146.SR",  # SASCO
                "4147.SR",  # BUDGET SAUDI
                "4148.SR",  # THIMAR HOLDING
                "4149.SR",  # BAAZEEM TRADING
                "4150.SR",  # RAYDAN FOOD
                "4151.SR",  # DUR HOSPITALITY
                "4152.SR",  # TECO
                "4153.SR",  # RED SEA HOUSING
                "4154.SR",  # ALKHALEEJ TRAINING
                "4155.SR",  # TAIBA HOLDING
                "4156.SR",  # MAKKAH CONSTRUCTION
                "4157.SR",  # EMAAR THE ECONOMIC CITY
                "4158.SR",  # JABAL OMAR DEVELOPMENT
                "4159.SR",  # TAIBA INVESTMENT
                "4160.SR",  # RED SEA INTERNATIONAL
                "4161.SR",  # ALDREES PETROLEUM
                "4162.SR",  # SASCO
                "4163.SR",  # BUDGET SAUDI
                "4164.SR",  # THIMAR HOLDING
                "4165.SR",  # BAAZEEM TRADING
                "4166.SR",  # RAYDAN FOOD
                "4167.SR",  # DUR HOSPITALITY
                "4168.SR",  # TECO
                "4169.SR",  # RED SEA HOUSING
                "4170.SR",  # ALKHALEEJ TRAINING
                "4171.SR",  # TAIBA HOLDING
                "4172.SR",  # MAKKAH CONSTRUCTION
                "4173.SR",  # EMAAR THE ECONOMIC CITY
                "4174.SR",  # JABAL OMAR DEVELOPMENT
                "4175.SR",  # TAIBA INVESTMENT
                "4176.SR",  # RED SEA INTERNATIONAL
                "4177.SR",  # ALDREES PETROLEUM
                "4178.SR",  # SASCO
                "4179.SR",  # BUDGET SAUDI
                "4180.SR",  # THIMAR HOLDING
                "4181.SR",  # BAAZEEM TRADING
                "4182.SR",  # RAYDAN FOOD
                "4183.SR",  # DUR HOSPITALITY
                "4184.SR",  # TECO
                "4185.SR",  # RED SEA HOUSING
                "4186.SR",  # ALKHALEEJ TRAINING
                "4187.SR",  # TAIBA HOLDING
                "4188.SR",  # MAKKAH CONSTRUCTION
                "4189.SR",  # EMAAR THE ECONOMIC CITY
                "4191.SR",  # JABAL OMAR DEVELOPMENT
                "4192.SR",  # TAIBA INVESTMENT
                "4193.SR",  # RED SEA INTERNATIONAL
                "4194.SR",  # ALDREES PETROLEUM
                "4195.SR",  # SASCO
                "4196.SR",  # BUDGET SAUDI
                "4197.SR",  # THIMAR HOLDING
                "4198.SR",  # BAAZEEM TRADING
                "4199.SR",  # RAYDAN FOOD
                "TASI.SR",  # Tadawul All Share Index
            ]
            
            # Remove duplicates and sort
            symbols = sorted(list(set(symbols)))
            self.saudi_market_symbols = symbols
            return symbols
            
        except Exception as e:
            logger.error(f"Error fetching Saudi market symbols: {str(e)}")
            return []
    
    def fetch_historical_data(self, symbol: str, period: str = "7y") -> Optional[pd.DataFrame]:
        """
        Fetch historical data for a given stock symbol
        
        Args:
            symbol: Stock symbol in format XXXX.SR
            period: Time period to fetch (default: 7 years)
            
        Returns:
            DataFrame with historical data or None if error
        """
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching historical data for {symbol}, attempt {attempt+1}/{max_retries}")
                stock = yf.Ticker(symbol)
                df = stock.history(period=period)
                
                if df.empty:
                    logger.warning(f"No data returned for {symbol}")
                    return None
                
                # Reset index to make Date a column
                df = df.reset_index()
                
                # Rename columns to match our database schema
                df = df.rename(columns={
                    'Date': 'date',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume',
                    'Dividends': 'dividends',
                    'Stock Splits': 'stock_splits'
                })
                
                # Add symbol column
                df['symbol'] = symbol
                
                # Ensure date is datetime
                df['date'] = pd.to_datetime(df['date'])
                
                logger.info(f"Successfully fetched {len(df)} records for {symbol}")
                return df
                
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"Failed to fetch data for {symbol} after {max_retries} attempts")
                    return None
    
    def store_data_in_db(self, df: pd.DataFrame) -> bool:
        """
        Store stock data in database
        
        Args:
            df: DataFrame with stock data
            
        Returns:
            True if successful, False otherwise
        """
        if df is None or df.empty:
            logger.warning("No data to store")
            return False
        
        try:
            session = self.Session()
            
            # Get symbol and check if we need to update existing records
            symbol = df['symbol'].iloc[0]
            
            # Get existing dates for this symbol to avoid duplicates
            existing_dates = session.query(StockPrice.date).filter(
                StockPrice.symbol == symbol
            ).all()
            existing_dates = [d[0] for d in existing_dates]
            
            # Filter out records that already exist
            df_new = df[~df['date'].isin(existing_dates)]
            
            if df_new.empty:
                logger.info(f"No new data to store for {symbol}")
                session.close()
                return True
            
            # Convert DataFrame to list of StockPrice objects
            stock_prices = []
            for _, row in df_new.iterrows():
                stock_price = StockPrice(
                    symbol=row['symbol'],
                    date=row['date'],
                    open=row.get('open'),
                    high=row.get('high'),
                    low=row.get('low'),
                    close=row.get('close'),
                    volume=row.get('volume'),
                    adjusted_close=row.get('adjusted_close', row.get('close'))
                )
                stock_prices.append(stock_price)
            
            # Add all records to the session
            session.add_all(stock_prices)
            session.commit()
            
            logger.info(f"Successfully stored {len(stock_prices)} new records for {symbol}")
            session.close()
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Database error storing data: {str(e)}")
            if session:
                session.rollback()
                session.close()
            return False
            
        except Exception as e:
            logger.error(f"Error storing data: {str(e)}")
            if session:
                session.rollback()
                session.close()
            return False
    
    def update_stock_data(self, symbols: List[str] = None, period: str = "7y") -> Dict[str, bool]:
        """
        Update stock data for specified symbols
        
        Args:
            symbols: List of stock symbols to update. If None, updates all Saudi market symbols
            period: Time period to fetch
            
        Returns:
            Dictionary mapping symbols to success status
        """
        if symbols is None:
            symbols = self.get_saudi_market_symbols()
        
        results = {}
        for symbol in symbols:
            try:
                logger.info(f"Updating data for {symbol}")
                df = self.fetch_historical_data(symbol, period)
                if df is not None:
                    success = self.store_data_in_db(df)
                    results[symbol] = success
                else:
                    results[symbol] = False
                
                # Add a small delay to avoid rate limiting
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error updating {symbol}: {str(e)}")
                results[symbol] = False
        
        return results
    
    def get_latest_data(self, symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
        """
        Get the latest data for a symbol from the database
        
        Args:
            symbol: Stock symbol
            days: Number of days of data to retrieve
            
        Returns:
            DataFrame with latest data or None if error
        """
        try:
            session = self.Session()
            
            # Calculate the date range
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=days)
            
            # Query the database
            query = session.query(StockPrice).filter(
                StockPrice.symbol == symbol,
                StockPrice.date >= start_date,
                StockPrice.date <= end_date
            ).order_by(StockPrice.date)
            
            # Convert to DataFrame
            records = query.all()
            if not records:
                logger.warning(f"No data found for {symbol} in the last {days} days")
                session.close()
                return None
            
            data = []
            for record in records:
                data.append({
                    'symbol': record.symbol,
                    'date': record.date,
                    'open': record.open,
                    'high': record.high,
                    'low': record.low,
                    'close': record.close,
                    'volume': record.volume,
                    'adjusted_close': record.adjusted_close
                })
            
            df = pd.DataFrame(data)
            session.close()
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving latest data for {symbol}: {str(e)}")
            if session:
                session.close()
            return None
    
    def schedule_updates(self, interval_minutes: int = 30):
        """
        Set the update interval for periodic data collection
        
        Args:
            interval_minutes: Update interval in minutes
        """
        self.update_interval_minutes = interval_minutes
        logger.info(f"Set update interval to {interval_minutes} minutes")
    
    def backup_database(self, backup_path: str = None) -> bool:
        """
        Create a backup of the database
        
        Args:
            backup_path: Path to save the backup. If None, uses default path
            
        Returns:
            True if successful, False otherwise
        """
        if backup_path is None:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"saudi_stocks_backup_{timestamp}.db"
        
        try:
            # This is a simple implementation for SQLite
            # For other databases, would need to use appropriate backup methods
            if 'sqlite' in str(self.engine.url):
                import shutil
                db_path = str(self.engine.url).replace('sqlite:///', '')
                shutil.copy2(db_path, backup_path)
                logger.info(f"Database backed up to {backup_path}")
                return True
            else:
                logger.warning("Backup only implemented for SQLite databases")
                return False
                
        except Exception as e:
            logger.error(f"Error backing up database: {str(e)}")
            return False


# Example usage
if __name__ == "__main__":
    # Initialize the collector
    collector = StockDataCollector()
    
    # Update data for a few symbols
    symbols = ["1120.SR", "2222.SR", "7010.SR"]
    results = collector.update_stock_data(symbols)
    
    # Print results
    for symbol, success in results.items():
        print(f"{symbol}: {'Success' if success else 'Failed'}")
    
    # Get latest data for a symbol
    df = collector.get_latest_data("1120.SR", days=30)
    if df is not None:
        print(df.head())
