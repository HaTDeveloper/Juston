"""
README for Saudi Stock Bot
========================

A comprehensive analysis and recommendation system for Saudi stocks.

Features
--------
- Data collection from Yahoo Finance API
- News analysis with sentiment evaluation
- Golden opportunity detection (short-term trading signals)
- Trend analysis for medium to long-term investments
- Confidence evaluation system combining multiple signals
- Notification system with Discord integration
- User-friendly web interface

Requirements
-----------
- Python 3.10+
- See requirements.txt for all dependencies

Setup
-----
1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up environment variables (see .env.example)
4. Run the application: `python app.py`

Environment Variables
-------------------
- PORT: Port for the web server (default: 5000)
- DISCORD_WEBHOOK_URL: URL for Discord notifications

Deployment on Render
------------------
This application is configured for deployment on Render platform:
1. Connect your GitHub repository to Render
2. Create a new Web Service
3. Use the following settings:
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `python app.py`
4. Add the DISCORD_WEBHOOK_URL environment variable
5. Deploy the application

Project Structure
---------------
- data_collection/: Stock data collection and database management
- news_analysis/: News gathering and sentiment analysis
- analysis/: Technical analysis modules
  - golden_opportunities/: Short-term trading signals
  - trends/: Medium to long-term trend analysis
- models/: Confidence evaluation system
- utils/: Utility functions and notification system
- ui/: Web interface
- config/: Configuration files
- tests/: Test scripts

Testing
------
Run tests with: `python -m tests.test_system`

License
-------
For educational purposes only. Not financial advice.

Disclaimer
---------
This bot is for educational and research purposes only. Always consult a financial advisor before making investment decisions.
"""
