#!/opt/homebrew/bin/python3.13
"""
Quick Debug: Test Symbol Parsing and Data Preservation
"""

import sys
import os
import logging

# Add project root to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))  # Go up two levels: intraday_trading -> aion_algo_trading
sys.path.insert(0, project_root)
sys.path.insert(0, script_dir)  # Also add intraday_trading directory for relative imports

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("QuickDebug")

def test_symbol_parsing():
    """Test if unified symbol parsing is working"""
    logger.info("🧪 TESTING SYMBOL PARSING")
    
    try:
        from intraday_trading.intraday_scanner.data_pipeline import DataPipeline
        
        # Create minimal pipeline instance
        pipeline = DataPipeline.__new__(DataPipeline)
        pipeline.logger = logger
        pipeline._symbol_cache = {}
        
        # Test symbols that appear in your logs
        test_symbols = [
            "NFONESTLEIND27JANFUT",  # Future from your log
            "NSEICICIBANK",           # Equity from your log  
            "NFO:RELIANCE28OCT202123000CE",  # Option
            "RELIANCE",               # Basic equity
        ]
        
        for symbol_input in test_symbols:
            tick_data = {"symbol": symbol_input, "tradingsymbol": symbol_input}
            
            # Test unified symbol parser
            if hasattr(pipeline, '_get_unified_symbol'):
                unified_result = pipeline._get_unified_symbol(tick_data)
                logger.info(f"✅ _get_unified_symbol('{symbol_input}') -> '{unified_result}'")
            else:
                logger.error(f"❌ _get_unified_symbol method not found!")
                
            # Test legacy symbol parser
            if hasattr(pipeline, '_extract_symbol'):
                legacy_result = pipeline._extract_symbol(tick_data)
                logger.info(f"✅ _extract_symbol('{symbol_input}') -> '{legacy_result}'")
                
            # Test symbol type detection
            if hasattr(pipeline, '_is_option_symbol'):
                is_option = pipeline._is_option_symbol(symbol_input)
                logger.info(f"✅ _is_option_symbol('{symbol_input}') -> {is_option}")
                
            if hasattr(pipeline, '_is_derivative'):
                is_derivative = pipeline._is_derivative(symbol_input)
                logger.info(f"✅ _is_derivative('{symbol_input}') -> {is_derivative}")
                
            logger.info("---")
            
    except Exception as e:
        logger.error(f"❌ Symbol parsing test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())

def test_redis_data_preservation():
    """Test if Redis data is being properly preserved"""
    logger.info("🧪 TESTING REDIS DATA PRESERVATION")
    
    try:
        from intraday_trading.intraday_scanner.data_pipeline import DataPipeline
        from unittest.mock import Mock, patch
        
        # Create pipeline with mock Redis
        pipeline = DataPipeline.__new__(DataPipeline)
        pipeline.logger = logger
        pipeline.redis_client = Mock()
        pipeline.realtime_client = Mock()
        
        # Mock Redis data that should be preserved
        mock_redis_data = {
            "volume_ratio": 2.5,
            "price_change_pct": 1.2,
            "rsi": 68.5,
            "delta": 0.0,  # This should NOT be preserved for equities
            "gamma": 0.0   # This should NOT be preserved for equities
        }
        
        # Test tick data for equity symbol
        equity_tick = {
            "symbol": "NSEICICIBANK",
            "tradingsymbol": "NSEICICIBANK",
            "last_price": 1393.1,
            "volume_ratio": None,  # Missing - should come from Redis
            "exchange_timestamp": "2024-01-15T10:30:00.000Z"
        }
        
        # Test if unified methods exist and work
        if hasattr(pipeline, '_get_complete_redis_data'):
            with patch.object(pipeline, '_get_complete_redis_data', return_value=mock_redis_data):
                redis_data = pipeline._get_complete_redis_data("NSEICICIBANK")
                logger.info(f"✅ _get_complete_redis_data returned: {len(redis_data)} fields")
                
                # Test smart merge
                if hasattr(pipeline, '_smart_merge_redis_data'):
                    cleaned = equity_tick.copy()
                    pipeline._smart_merge_redis_data(cleaned, redis_data, equity_tick)
                    
                    # Check results
                    volume_preserved = cleaned.get('volume_ratio') == 2.5
                    greeks_preserved = any(greek in cleaned for greek in ['delta', 'gamma', 'theta', 'vega', 'rho'])
                    
                    logger.info(f"✅ Volume ratio preserved: {volume_preserved} (value: {cleaned.get('volume_ratio')})")
                    logger.info(f"✅ Greeks preserved for equity: {greeks_preserved} (should be False)")
                    
                    if greeks_preserved:
                        logger.error("❌ CRITICAL: Greeks are being preserved for equity symbol!")
                        for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                            if greek in cleaned:
                                logger.error(f"   ❌ {greek} = {cleaned[greek]} (should not exist for equity)")
                    
        else:
            logger.error("❌ _get_complete_redis_data method not found!")
            
    except Exception as e:
        logger.error(f"❌ Redis preservation test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())

def check_method_overrides():
    """Check which methods are actually being called"""
    logger.info("🧪 CHECKING METHOD OVERRIDES")
    
    try:
        from intraday_trading.intraday_scanner.data_pipeline import DataPipeline
        
        pipeline = DataPipeline.__new__(DataPipeline)
        
        # Check if unified methods exist
        unified_methods = ['_get_unified_symbol', '_get_complete_redis_data', '_smart_merge_redis_data']
        legacy_methods = ['_extract_symbol', '_fetch_all_tick_details_from_redis', '_get_indicators_from_redis']
        
        logger.info("📋 METHOD AVAILABILITY:")
        for method in unified_methods:
            exists = hasattr(pipeline, method)
            status = "✅ EXISTS" if exists else "❌ MISSING"
            logger.info(f"   {method}: {status}")
            
        for method in legacy_methods:
            exists = hasattr(pipeline, method)
            status = "✅ EXISTS" if exists else "⚠️  MISSING (expected)"
            logger.info(f"   {method}: {status}")
            
        # Check which cleaning method is used
        if hasattr(pipeline, '_clean_tick_data_core_enhanced'):
            logger.info("✅ Using ENHANCED cleaning method")
        elif hasattr(pipeline, '_clean_tick_data_core'):
            logger.info("⚠️  Using LEGACY cleaning method (should be enhanced)")
        else:
            logger.error("❌ No cleaning method found!")
            
    except Exception as e:
        logger.error(f"❌ Method override check failed: {e}")

def check_actual_processing_flow():
    """Simulate the actual processing flow to see what's called"""
    logger.info("🧪 SIMULATING ACTUAL PROCESSING FLOW")
    
    try:
        from intraday_trading.intraday_scanner.data_pipeline import DataPipeline
        from unittest.mock import Mock, patch, call
        
        pipeline = DataPipeline.__new__(DataPipeline)
        pipeline.logger = logger
        
        # Track method calls
        method_calls = []
        
        def track_call(method_name):
            def wrapper(*args, **kwargs):
                method_calls.append(method_name)
                logger.info(f"📞 METHOD CALLED: {method_name}")
                return "NSE:TEST" if "symbol" in method_name else {}
            return wrapper
        
        # Mock the methods to track calls (only patch methods that exist)
        patch_dict = {
            '_get_unified_symbol': track_call('_get_unified_symbol'),
            '_get_complete_redis_data': track_call('_get_complete_redis_data'),
            '_clean_tick_data_core_enhanced': track_call('_clean_tick_data_core_enhanced'),
        }
        
        # Add legacy methods only if they exist
        if hasattr(pipeline, '_extract_symbol'):
            patch_dict['_extract_symbol'] = track_call('_extract_symbol')
        if hasattr(pipeline, '_extract_symbol_fast'):
            patch_dict['_extract_symbol_fast'] = track_call('_extract_symbol_fast')
        if hasattr(pipeline, '_fetch_all_tick_details_from_redis'):
            patch_dict['_fetch_all_tick_details_from_redis'] = track_call('_fetch_all_tick_details_from_redis')
        if hasattr(pipeline, '_clean_tick_data_core'):
            patch_dict['_clean_tick_data_core'] = track_call('_clean_tick_data_core')
        
        with patch.multiple(pipeline, **patch_dict):
            
            # Simulate processing a tick
            test_tick = {
                "symbol": "NSEICICIBANK",
                "last_price": 1393.1,
                "exchange_timestamp": "2024-01-15T10:30:00.000Z"
            }
            
            # This should trigger the symbol parsing and cleaning flow
            if hasattr(pipeline, '_clean_tick_data'):
                try:
                    result = pipeline._clean_tick_data(test_tick)
                    logger.info(f"✅ _clean_tick_data completed, returned: {type(result)}")
                except Exception as e:
                    logger.error(f"❌ _clean_tick_data failed: {e}")
            
            # Analyze method calls
            logger.info("📊 METHOD CALL ANALYSIS:")
            unified_calls = [m for m in method_calls if 'unified' in m.lower() or 'enhanced' in m.lower()]
            legacy_calls = [m for m in method_calls if m in ['_extract_symbol', '_extract_symbol_fast', '_fetch_all_tick_details_from_redis', '_clean_tick_data_core']]
            
            logger.info(f"   Unified methods called: {len(unified_calls)}")
            logger.info(f"   Legacy methods called: {len(legacy_calls)}")
            
            if legacy_calls and not unified_calls:
                logger.error("❌ CRITICAL: Only legacy methods are being called!")
            elif legacy_calls and unified_calls:
                logger.warning("⚠️  Mixed: Both legacy and unified methods called")
            elif unified_calls and not legacy_calls:
                logger.info("✅ SUCCESS: Only unified methods called")
                
    except Exception as e:
        logger.error(f"❌ Processing flow test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    logger.info("🚀 STARTING QUICK DEBUG")
    logger.info("=" * 50)
    
    test_symbol_parsing()
    logger.info("=" * 50)
    
    test_redis_data_preservation() 
    logger.info("=" * 50)
    
    check_method_overrides()
    logger.info("=" * 50)
    
    check_actual_processing_flow()
    logger.info("=" * 50)
    
    logger.info("🎉 QUICK DEBUG COMPLETED")