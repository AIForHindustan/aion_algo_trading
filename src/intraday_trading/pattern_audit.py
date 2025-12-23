# pattern_audit.py
"""
Comprehensive pattern detection audit
"""

import json
import time
from datetime import datetime

from shared_core.redis_clients.redis_client import get_redis_client
from patterns.singleton_factory import PatternDetectorSingletonFactory
def audit_pattern_detection():
    """Audit why patterns aren't triggering"""
    
    # ✅ FIXED: Use UnifiedRedisManager for centralized connection management
    from shared_core.redis_clients import UnifiedRedisManager, DatabaseAwareKeyBuilder
    r = get_redis_client(process_name="pattern_audit", db=1)
    
    print("🎯 PATTERN DETECTION AUDIT")
    print("=" * 60)
    
    # 1. Check if pattern detector is receiving data
    print("\n1. 📊 PATTERN DETECTOR INPUT CHECK:")
    
    # ✅ CENTRALIZED: Use key builder for stream key
    stream_key = DatabaseAwareKeyBuilder.live_processed_stream()
    messages = r.xrevrange(stream_key, count=5)
    
    if messages:
        print(f"   ✅ Stream has {len(messages)} recent messages")
        for msg_id, msg_data in messages:
            # Try to parse the message
            data_field = msg_data.get(b'data', msg_data.get('data', b'{}'))
            if isinstance(data_field, bytes):
                try:
                    tick_data = json.loads(data_field.decode('utf-8'))
                    symbol = tick_data.get('symbol', 'UNKNOWN')
                    print(f"   📨 {symbol}: {tick_data.get('last_price', 'N/A')}")
                except:
                    print(f"   ❌ Could not parse message {msg_id}")
    else:
        print("   ❌ No messages in stream")
    
    # 2. Check pattern detector output
    print("\n2. 🔍 PATTERN DETECTOR OUTPUT CHECK:")
    
    # Check alert stream
    alert_stream = 'alerts:stream'
    alert_count = r.xlen(alert_stream)
    print(f"   Alerts stream: {alert_count} messages")
    
    # Check recent alerts
    alerts = r.xrevrange(alert_stream, count=5)
    print(f"   Recent alerts ({len(alerts)}):")
    for alert_id, alert_data in alerts:
        # Try to parse alert data (could be JSON in 'data' field or direct fields)
        pattern = 'UNKNOWN'
        symbol = 'UNKNOWN'
        confidence = 'N/A'
        
        # Check for 'data' field (JSON format)
        data_field = alert_data.get(b'data', alert_data.get('data'))
        if data_field:
            try:
                if isinstance(data_field, bytes):
                    alert_json = json.loads(data_field.decode('utf-8'))
                else:
                    alert_json = json.loads(data_field)
                pattern = alert_json.get('pattern', alert_json.get('pattern_type', 'UNKNOWN'))
                symbol = alert_json.get('symbol', 'UNKNOWN')
                confidence = alert_json.get('confidence', 'N/A')
            except:
                # Try direct fields
                pattern = alert_data.get(b'pattern', alert_data.get('pattern', b'UNKNOWN'))
                symbol = alert_data.get(b'symbol', alert_data.get('symbol', b'UNKNOWN'))
                if isinstance(pattern, bytes):
                    pattern = pattern.decode('utf-8')
                if isinstance(symbol, bytes):
                    symbol = symbol.decode('utf-8')
        else:
            # Direct fields
            pattern = alert_data.get(b'pattern', alert_data.get('pattern', b'UNKNOWN'))
            symbol = alert_data.get(b'symbol', alert_data.get('symbol', b'UNKNOWN'))
            if isinstance(pattern, bytes):
                pattern = pattern.decode('utf-8')
            if isinstance(symbol, bytes):
                symbol = symbol.decode('utf-8')
        
        conf_str = f" ({confidence:.1%})" if isinstance(confidence, (int, float)) else f" ({confidence})"
        print(f"   🚨 {symbol}: {pattern}{conf_str}")
    
    # 3. Check if calculations are happening
    print("\n3. 🧮 CALCULATIONS CHECK:")
    
    # Check if indicators are being stored
    indicator_keys = r.keys("indicators:*")
    print(f"   Stored indicators: {len(indicator_keys)}")
    
    # Sample some indicators
    for key in indicator_keys[:3]:
        key_str = key.decode('utf-8') if isinstance(key, bytes) else key
        value = r.get(key)
        if value:
            try:
                data = json.loads(value)
                print(f"   📈 {key_str}: {data.get('value', 'N/A')}")
            except:
                print(f"   📈 {key_str}: {value}")
    
    # 4. Check delta_analyzer specifically
    print("\n4. Δ DELTA_ANALYZER CHECK:")
    
    delta_keys = r.keys("*delta*")
    print(f"   Delta-related keys: {len(delta_keys)}")
    for key in delta_keys:
        key_str = key.decode('utf-8') if isinstance(key, bytes) else key
        print(f"   🔍 {key_str}")
    
    # 5. Check if pattern mathematics is being called
    print("\n5. 🧩 PATTERN MATHEMATICS CHECK:")
    
    # Look for pattern calculation evidence
    pattern_keys = r.keys("*pattern*")
    print(f"   Pattern-related keys: {len(pattern_keys)}")
    
    # 6. Check scanner_main health
    print("\n6. 🏥 SCANNER MAIN HEALTH:")
    
    # Check if main loop is running
    health_key = 'scanner:health'
    health = r.get(health_key)
    if health:
        print(f"   ✅ Scanner health: {health.decode('utf-8')}")
    else:
        print("   ❌ No scanner health data")
    
    # 7. Check if data is flowing to pattern engine
    print("\n7. 📡 DATA FLOW TO PATTERN ENGINE:")
    
    # Check the last processed tick
    last_tick_key = 'last_processed_tick'
    last_tick = r.get(last_tick_key)
    if last_tick:
        print(f"   ✅ Last processed tick: {last_tick.decode('utf-8')}")
    else:
        print("   ❌ No recent tick processing")

def test_pattern_detection_with_sample_data():
    """Test pattern detection with sample data"""
    print("\n" + "=" * 60)
    print("🧪 TESTING PATTERN DETECTION")
    print("=" * 60)
    
    # Create a sample tick that should trigger patterns
    sample_tick = {
        'symbol': 'RELIANCE',
        'last_price': 2850.50,
        'timestamp_ms': int(time.time() * 1000),
        'volume': 150000,
        'high': 2860.00,
        'low': 2840.00, 
        'open': 2845.00,
        'close': 2850.50,
        'exchange_timestamp_ms': int(time.time() * 1000)
    }
    
    print(f"📨 Sample tick: {sample_tick['symbol']} at ₹{sample_tick['last_price']}")
    
    # Try to manually trigger pattern detection
    try:
        from intraday_trading.intraday_scanner.data_pipeline import DataPipeline
        from shared_core.redis_clients import UnifiedRedisManager
        
        # ✅ FIXED: Use UnifiedRedisManager for centralized connection management
        redis_client = get_redis_client(process_name="pattern_audit_test", db=1)
        pipeline = DataPipeline(redis_client=redis_client)
        
        print("🔍 Testing pattern detection with sample tick...")
        # Process the tick through the pipeline
        pipeline._process_market_tick_fast(sample_tick)
        print("✅ Pattern processing completed")
        
    except Exception as e:
        print(f"❌ Pattern test failed: {e}")
        import traceback
        traceback.print_exc()

def check_delta_analyzer_wiring():
    """Specifically check why delta_analyzer isn't initiating"""
    print("\n" + "=" * 60)
    print("Δ DELTA_ANALYZER WIRING CHECK")
    print("=" * 60)
    
    # 1. Check if delta_analyzer pattern exists in pattern detector
    try:
        from intraday_trading.patterns.pattern_detector import BasePatternDetector
        from shared_core.redis_clients import UnifiedRedisManager
        
        # ✅ FIXED: Use UnifiedRedisManager for centralized connection management
        redis_client = get_redis_client(process_name="pattern_audit_delta_check", db=1)
        
        # Try BasePatternDetector
        print("📋 PATTERN DETECTOR CHECK:")
        try:
            detector = BasePatternDetector(config={}, redis_client=redis_client)
            print(f"   ✅ BasePatternDetector created: {type(detector).__name__}")
            
            # Check for delta_analyzer
            if hasattr(detector, 'delta_analyzer'):
                print("   ✅ delta_analyzer attribute exists")
                delta_analyzer = detector.delta_analyzer
                if delta_analyzer:
                    print(f"   📊 Delta analyzer type: {type(delta_analyzer).__name__}")
                    print("   ✅ Delta analyzer is initialized")
                else:
                    print("   ⚠️ Delta analyzer is None (not initialized)")
            else:
                print("   ❌ delta_analyzer attribute missing")
        except Exception as e:
            print(f"   ⚠️ BasePatternDetector check failed: {e}")
        
        # Try PatternDetectorSingletonFactory.get_instance(what scanner actually uses, caller="pattern_audit.py")
        try:
            pattern_detector = PatternDetectorSingletonFactory.get_instance(redis_client=redis_client, caller="pattern_audit.py")
            print(f"   ✅ PatternDetector created: {type(pattern_detector).__name__}")
            
            if hasattr(pattern_detector, 'delta_analyzer'):
                print("   ✅ PatternDetector has delta_analyzer")
                if pattern_detector.delta_analyzer:
                    print("   ✅ Delta analyzer is initialized in PatternDetector")
                else:
                    print("   ⚠️ Delta analyzer is None in PatternDetector")
            else:
                print("   ❌ PatternDetector missing delta_analyzer")
        except Exception as e:
            print(f"   ⚠️ PatternDetector check failed: {e}")
        
    except Exception as e:
        print(f"❌ PatternDetector check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    audit_pattern_detection()
    test_pattern_detection_with_sample_data() 
    check_delta_analyzer_wiring()

def audit_registry_patterns():
    """
    Audit the 7 core patterns from the registry to ensure they can be instantiated.
    Handles the known path discrepancy for advanced patterns.
    """
    print("\n" + "=" * 60)
    print("📜 PATTERN REGISTRY AUDIT")
    print("=" * 60)

    import os
    import sys
    import importlib.util

    # 1. Load Registry
    registry_path = os.path.join(os.path.dirname(__file__), 'patterns/data/pattern_registry_config.json')
    print(f"📂 Loading registry from: {registry_path}")
    
    try:
        with open(registry_path, 'r') as f:
            registry = json.load(f)
        print("   ✅ Registry loaded successfully")
    except Exception as e:
        print(f"   ❌ Failed to load registry: {e}")
        return

    # 2. Setup Redis Client for detectors
    from shared_core.redis_clients import UnifiedRedisManager
    redis_client = get_redis_client(process_name="pattern_audit_registry", db=1)

    # 3. Iterate Patterns
    configs = registry.get('pattern_configs', {})
    print(f"\n🔍 Auditing {len(configs)} patterns:")
    
    success_count = 0
    
    for pattern_name, config in configs.items():
        if not config.get('enabled', False):
            print(f"   ⚪ {pattern_name}: Disabled (Skipping)")
            continue
            
        print(f"   👉 Checking {pattern_name}...")
        
        # Resolve source file
        source_file = config.get('source_file', '')
        
        # fix: advanced_pattern_detectors.py -> advanced.py
        if source_file.endswith('advanced_pattern_detectors.py'):
            source_file = source_file.replace('advanced_pattern_detectors.py', 'advanced.py')
            print(f"      CORRECTION: Re-routing to {source_file}")
            
        full_path = os.path.join(os.path.dirname(__file__), source_file)
        
        if not os.path.exists(full_path):
            print(f"      ❌ Source file not found: {full_path}")
            continue

        # Dynamic Import
        try:
            # Convert file path to module path for proper relative import support
            # Source file: patterns/advanced_pattern_detectors.py -> advanced.py
            # Module path: intraday_trading.patterns.advanced
            
            # Remove extension
            clean_source = source_file.replace('.py', '')
            # Replace slashes with dots
            module_path = "intraday_trading." + clean_source.replace('/', '.')
            
            print(f"      📦 Importing module: {module_path}")
            
            module = importlib.import_module(module_path)
            
            class_name = config.get('detector_class')
            if hasattr(module, class_name):
                cls = getattr(module, class_name)
                # Instantiate
                try:
                    import inspect
                    # Smart instantiation based on signature
                    sig = inspect.signature(cls.__init__)
                    params = sig.parameters
                    
                    kwargs = {}
                    if 'redis_client' in params:
                        kwargs['redis_client'] = redis_client
                    if 'config' in params:
                        kwargs['config'] = config
                        
                    instance = cls(**kwargs)
                    print(f"      ✅ Instantiated {class_name}")
                    success_count += 1
                    
                    # Interface check
                    if hasattr(instance, 'detect'):
                         print("      ✅ Has 'detect' method")
                    elif hasattr(instance, 'generate_iron_condor_signal'):
                         print("      ✅ Has 'generate_iron_condor_signal' method (Strategy Interface)")
                    else:
                         print("      ⚠️ Missing 'detect' method (or known alternate)")
                         # List public methods
                         methods = [func for func in dir(instance) if callable(getattr(instance, func)) and not func.startswith("_")]
                         print(f"         Available methods: {methods}")
                         
                except Exception as e:
                    print(f"      ❌ Instantiation failed: {e}")
            else:
                print(f"      ❌ Class {class_name} not found in module")

        except Exception as e:
             print(f"      ❌ Import/Load failed: {e}")
             import traceback
             traceback.print_exc()

    print(f"\n🏁 Registry Audit Complete: {success_count}/{len(configs)} patterns instantiated")


if __name__ == "__main__":
    audit_pattern_detection()
    test_pattern_detection_with_sample_data() 
    check_delta_analyzer_wiring()
    audit_registry_patterns()
