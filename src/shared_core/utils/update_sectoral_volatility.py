#!/opt/homebrew/bin/python3.13
"""
Update Sectoral Volatility Data for Baseline Understanding
Fetches sectoral indices (not constituents) and calculates their overall volatility for baseline market regime analysis

USAGE:
    - Used by: Market regime analysis, baseline volatility understanding
    - Called from: Production scheduler, manual execution
    - Saves to: shared_core/config/sector_volatility.json
    - Dependencies: requests, json, datetime, pandas (optional for CSV parsing)

PURPOSE:
    - Updates overall volatility for 9 NSE sectoral indices (the indices themselves, not constituents)
    - Fetches historical index data from NSE website
    - Calculates 20-day and 55-day volatility for each index
    - Provides baseline volatility understanding for market regime analysis
    - Covers: NIFTY 50, NIFTY BANK, NIFTY AUTO, NIFTY IT, NIFTY PHARMA, NIFTY FMCG, NIFTY METAL, NIFTY ENERGY, NIFTY REALTY

NSE SECTORAL INDICES (Index Symbols):
    - NIFTY 50: NIFTY 50 (broad market index)
    - NIFTY BANK: NIFTY BANK (banking sector index)
    - NIFTY AUTO: NIFTY AUTO (automotive sector index)
    - NIFTY IT: NIFTY IT (IT sector index)
    - NIFTY PHARMA: NIFTY PHARMA (pharmaceutical sector index)
    - NIFTY FMCG: NIFTY FMCG (FMCG sector index)
    - NIFTY METAL: NIFTY METAL (metals sector index)
    - NIFTY ENERGY: NIFTY ENERGY (energy sector index)
    - NIFTY REALTY: NIFTY REALTY (realty sector index)

SCHEDULING:
    - WEEKLY EXECUTION: Run every Sunday for weekly sector updates
    - Recommended time: 6:00 PM IST (after market close)
    - Manual execution: python -m shared_core.utils.update_sectoral_volatility
    - Production scheduler: Integrated with weekly data refresh
    - Market calendar aware: Skips holidays

CREATED: October 31, 2025
UPDATED: November 6, 2025 - Initial implementation for NSE sectoral indices
UPDATED: January 2025 - Moved to shared_core for centralized access
UPDATED: January 2025 - Changed to fetch from NSE website instead of Zerodha API
"""

import sys
from pathlib import Path
import json
from datetime import datetime, timedelta
import time
import math
import requests
from typing import Dict, List, Optional, Any

# Add project root to path for config imports
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

def get_nse_index_symbols() -> Dict[str, str]:
    """
    Get NSE sectoral index symbols mapping
    
    RETURNS:
        dict: Mapping of index name to NSE symbol
    """
    return {
        'NIFTY_50': 'NIFTY 50',
        'NIFTY_BANK': 'NIFTY BANK',
        'NIFTY_AUTO': 'NIFTY AUTO',
        'NIFTY_IT': 'NIFTY IT',
        'NIFTY_PHARMA': 'NIFTY PHARMA',
        'NIFTY_FMCG': 'NIFTY FMCG',
        'NIFTY_METAL': 'NIFTY METAL',
        'NIFTY_ENERGY': 'NIFTY ENERGY',
        'NIFTY_REALTY': 'NIFTY REALTY'
    }

def get_nse_session() -> requests.Session:
    """
    Create a requests session with proper headers for NSE website
    
    RETURNS:
        requests.Session: Configured session
    """
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.nseindia.com/',
    })
    return session

def fetch_index_historical_data_nse(session: requests.Session, index_symbol: str, days_back: int = 60) -> List[Dict[str, Any]]:
    """
    Fetch historical data for an index from NSE website
    
    ARGS:
        session: requests.Session with proper headers
        index_symbol: NSE index symbol (e.g., 'NIFTY 50')
        days_back: Number of days of historical data to fetch
        
    RETURNS:
        list: List of historical OHLC data points
    """
    try:
        # Calculate date range
        to_date = datetime.now()
        from_date = to_date - timedelta(days=days_back)
        
        # First, visit the main page to get cookies (NSE requires this)
        try:
            session.get('https://www.nseindia.com/', timeout=10)
            time.sleep(1)  # Small delay to ensure cookies are set
        except Exception as e:
            print(f"⚠️ Warning: Could not set NSE cookies: {e}")
        
        # NSE API endpoint for historical index data
        # Format: https://www.nseindia.com/api/historical/indicesHistory
        base_url = "https://www.nseindia.com/api/historical/indicesHistory"
        
        # NSE uses specific parameter format
        # Map index symbols to NSE index codes
        index_code_map = {
            'NIFTY 50': 'NIFTY 50',
            'NIFTY BANK': 'NIFTY BANK',
            'NIFTY AUTO': 'NIFTY AUTO',
            'NIFTY IT': 'NIFTY IT',
            'NIFTY PHARMA': 'NIFTY PHARMA',
            'NIFTY FMCG': 'NIFTY FMCG',
            'NIFTY METAL': 'NIFTY METAL',
            'NIFTY ENERGY': 'NIFTY ENERGY',
            'NIFTY REALTY': 'NIFTY REALTY'
        }
        
        index_code = index_code_map.get(index_symbol, index_symbol)
        
        # Prepare parameters (NSE API format)
        params = {
            'indexType': index_code,
            'from': from_date.strftime('%d-%m-%Y'),
            'to': to_date.strftime('%d-%m-%Y'),
        }
        
        # Fetch historical data
        response = session.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Parse NSE response format
        historical_data = []
        
        # NSE response can have different structures
        if isinstance(data, dict):
            # Check for 'data' key
            records = data.get('data', [])
            if not records and 'data' in data:
                # Sometimes data is nested differently
                if isinstance(data['data'], dict):
                    records = data['data'].get('data', [])
        elif isinstance(data, list):
            records = data
        else:
            records = []
        
        for record in records:
            # NSE format variations:
            # Format 1: {"CH_TIMESTAMP": "01-Jan-2025", "CH_OPENING_PRICE": 25500.0, ...}
            # Format 2: {"TIMESTAMP": "01-Jan-2025", "OPEN": 25500.0, ...}
            try:
                # Try different date field names
                date_str = record.get('CH_TIMESTAMP') or record.get('TIMESTAMP') or record.get('Date') or record.get('date', '')
                if not date_str:
                    continue
                
                # Parse date (NSE format: "01-Jan-2025" or "2025-01-01")
                try:
                    parsed_date = datetime.strptime(date_str, '%d-%b-%Y')
                except ValueError:
                    try:
                        parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
                    except ValueError:
                        try:
                            parsed_date = datetime.strptime(date_str, '%d/%m/%Y')
                        except ValueError:
                            continue  # Skip if date can't be parsed
                
                # Try different field name variations
                open_price = float(record.get('CH_OPENING_PRICE') or record.get('OPEN') or record.get('open') or 0)
                high_price = float(record.get('CH_TRADE_HIGH_PRICE') or record.get('HIGH') or record.get('high') or 0)
                low_price = float(record.get('CH_TRADE_LOW_PRICE') or record.get('LOW') or record.get('low') or 0)
                close_price = float(record.get('CH_CLOSING_PRICE') or record.get('CLOSE') or record.get('close') or 0)
                volume = float(record.get('CH_TOT_TRADED_QTY') or record.get('VOLUME') or record.get('volume') or 0)
                
                if close_price > 0:  # Only add valid records
                    historical_data.append({
                        'date': parsed_date,
                        'open': open_price,
                        'high': high_price,
                        'low': low_price,
                        'close': close_price,
                        'volume': volume,
                    })
            except (ValueError, KeyError, TypeError) as e:
                continue  # Skip invalid records
        
        # Sort by date (oldest first)
        historical_data.sort(key=lambda x: x['date'])
        
        return historical_data
        
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Error fetching NSE data for {index_symbol}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response status: {e.response.status_code}")
            print(f"   Response text: {e.response.text[:200]}")
        return []
    except Exception as e:
        print(f"⚠️ Error parsing NSE data for {index_symbol}: {e}")
        import traceback
        traceback.print_exc()
        return []

def calculate_index_volatility(historical_data: List[Dict[str, Any]], window_days: int = 20) -> float:
    """
    Calculate volatility for an index using historical price data
    
    ARGS:
        historical_data: List of historical OHLC data (from NSE API)
        window_days: Number of days for volatility calculation window
        
    RETURNS:
        float: Annualized volatility percentage
    """
    try:
        if not historical_data or len(historical_data) < window_days:
            return 0.0
        
        # Extract closing prices from historical data
        closes = [day['close'] for day in historical_data if day.get('close') and day['close'] > 0]
        
        if len(closes) < window_days:
            return 0.0
        
        # Use last window_days for calculation
        recent_closes = closes[-window_days:]
        
        # Calculate daily returns
        returns = []
        for i in range(1, len(recent_closes)):
            if recent_closes[i-1] > 0:
                daily_return = (recent_closes[i] - recent_closes[i-1]) / recent_closes[i-1]
                returns.append(daily_return)
        
        if len(returns) < 2:
            return 0.0
        
        # Calculate standard deviation of returns
        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)
        std_dev = math.sqrt(variance)
        
        # Annualize volatility (assuming 252 trading days)
        annualized_volatility = std_dev * math.sqrt(252) * 100
        
        return round(annualized_volatility, 2)
        
    except Exception as e:
        print(f"⚠️ Error calculating volatility: {e}")
        return 0.0

def update_sectoral_volatility():
    """
    Update sectoral index volatility data from NSE website
    
    PROCESS:
        1. Initialize NSE website session
        2. Get index symbols for sectoral indices
        3. Fetch historical data for each index from NSE
        4. Calculate 20-day and 55-day volatility for each index
        5. Update shared_core/config/sector_volatility.json with index volatility data
        6. Maintain metadata and structure
        
    RETURNS:
        dict: Updated sectoral volatility data with index-level volatility
    """
    print("🚀 UPDATING SECTORAL INDEX VOLATILITY DATA")
    print("=" * 70)
    print("Fetching 9 NSE sectoral indices for baseline volatility understanding")
    print("Calculating overall index volatility (not individual constituents)")
    print("Data source: NSE website (www.nseindia.com)")
    print()

    # Initialize NSE session
    try:
        session = get_nse_session()
        print("✅ Initialized NSE website session")
    except Exception as e:
        print(f"❌ Failed to initialize NSE session: {e}")
        return {}

    # Load existing sector_volatility.json from shared_core
    shared_core_path = Path(__file__).resolve().parent.parent
    sector_file = shared_core_path / 'config' / 'sector_volatility.json'
    
    existing_data = {}
    if sector_file.exists():
        try:
            with open(sector_file, 'r') as f:
                existing_data = json.load(f)
            print(f"✅ Loaded existing sector_volatility.json from {sector_file}")
        except Exception as e:
            print(f"⚠️ Error loading existing data: {e}")
            existing_data = {}
    else:
        print(f"ℹ️ No existing sector_volatility.json found at {sector_file}, creating new one")
        existing_data = {}

    # Get index symbols
    print("\n🔍 Fetching index data from NSE website...")
    index_symbols = get_nse_index_symbols()
    
    if not index_symbols:
        print("❌ No index symbols found, using existing data")
        return existing_data

    # Fetch historical data and calculate volatility for each index
    print("\n📊 Calculating index volatility...")
    index_volatility = {}
    
    for index_name, nse_symbol in index_symbols.items():
        print(f"  📈 Processing {index_name} ({nse_symbol})...", end=" ")
        
        try:
            # Fetch 60 days of historical data (for both 20d and 55d calculations)
            historical_data = fetch_index_historical_data_nse(session, nse_symbol, days_back=60)
            
            if not historical_data:
                print("❌ No data")
                continue
            
            # Calculate 20-day and 55-day volatility
            vol_20d = calculate_index_volatility(historical_data, window_days=20)
            vol_55d = calculate_index_volatility(historical_data, window_days=55)
            
            # Get current price (latest close)
            current_price = historical_data[-1]['close'] if historical_data else 0.0
            
            index_volatility[index_name] = {
                'symbol': index_name,
                'nse_symbol': nse_symbol,
                'current_price': round(current_price, 2),
                'volatility_20d_pct': vol_20d,
                'volatility_55d_pct': vol_55d,
                'data_points': len(historical_data),
                'last_updated': datetime.now().isoformat()
            }
            
            print(f"✅ Vol 20d: {vol_20d}%, Vol 55d: {vol_55d}%")
            
            # Rate limiting to avoid overwhelming NSE servers
            time.sleep(2)  # NSE may rate limit, so wait 2 seconds between requests
            
        except Exception as e:
            print(f"❌ Error: {e}")
            continue

    # Preserve existing sectors data if it exists, but update metadata
    existing_sectors = existing_data.get('sectors', {})
    
    # Update metadata with index volatility data
    updated_data = {
        'metadata': {
            'generated_at': datetime.now().isoformat(),
            'source': 'NSE website (www.nseindia.com)',
            'window_days': 20,
            'sectors': len(index_volatility),
            'formula': 'volatility = sqrt(252) * std_dev(returns)',
            'data_type': 'index_volatility',
            'description': 'Overall volatility for sectoral indices (baseline understanding)',
            'straddle_patterns': {
                'description': 'Straddle strategy patterns for NIFTY/BANKNIFTY options',
                'underlying_symbols': ['NIFTY', 'BANKNIFTY'],
                'strike_selection': 'atm',
                'expiry_preference': 'weekly',
                'confidence_threshold': 0.85,
                'telegram_routing': True
            }
        },
        'index_volatility': index_volatility,  # New: Index-level volatility
        'sectors': existing_sectors  # Preserve existing constituent data if any
    }

    # Save updated data
    try:
        with open(sector_file, 'w') as f:
            json.dump(updated_data, f, indent=2)
        print(f"\n💾 Saved updated sectoral volatility data to {sector_file}")
    except Exception as e:
        print(f"❌ Failed to save sectoral volatility data: {e}")
        return existing_data

    # Summary
    print(f"\n📈 SECTORAL INDEX VOLATILITY UPDATE SUMMARY")
    print("=" * 70)
    print(f"Total indices updated: {len(index_volatility)}")
    print(f"Data source: NSE website (www.nseindia.com)")
    print(f"Use case: Baseline volatility understanding for market regime analysis")
    print(f"Data type: Index-level volatility (not individual constituents)")
    
    # Show index breakdown
    if index_volatility:
        print(f"\n📊 INDEX VOLATILITY BREAKDOWN:")
        for index_name, data in index_volatility.items():
            print(f"  • {index_name}:")
            print(f"    Price: {data['current_price']}")
            print(f"    20d Vol: {data['volatility_20d_pct']}%")
            print(f"    55d Vol: {data['volatility_55d_pct']}%")

    print(f"\n✅ SECTORAL INDEX VOLATILITY UPDATE COMPLETED")
    print(f"🎯 Ready for baseline volatility analysis")
    print(f"📁 Output: shared_core/config/sector_volatility.json")
    print(f"🔄 Recommended: Weekly execution (Sundays at 6:00 PM IST)")
    
    return updated_data

def main():
    """Main execution function"""
    print("🔄 Starting sectoral index volatility update...")
    result = update_sectoral_volatility()
    
    if result:
        print("\n🎉 Sectoral index volatility update completed successfully!")
        print("📊 Ready for baseline volatility analysis")
        return True
    else:
        print("\n❌ Sectoral index volatility update failed!")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

