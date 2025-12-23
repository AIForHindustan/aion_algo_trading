# test_edge_integration.py
import redis
import time
from shared_core.calculation_service import CalculationService
# ✅ FIXED: Import from write_first_cache (UnderlyingPriceService is alias to UniversalSymbolParser)
from shared_core.write_first_cache import UnderlyingPriceService

def test_edge_calculations():
    redis_client = redis.Redis(host='localhost', port=6379, db=1)
    
    # Create services\n    from shared_core.write_first_cache import create_price_service\n    price_service = create_price_service()  # Singleton, no redis_client needed
    calc_service = CalculationService(
        redis_client=redis_client
    )
    
    # Test tick
    test_tick = {
        'symbol': 'NIFTY25DEC25000CE',
        'last_price': 150.5,
        'volume': 1000,
        'open_interest': 50000,
        'delta': 0.45,
        'gamma': 0.02,
        'theta': -5.5,
        'vega': 12.3,
        'iv': 0.18,
        'underlying_price': 25150.25,
        'timestamp': time.time()
    }
    
    # Test edge calculation
    edge_indicators = calc_service.calculate_edge_indicators(test_tick)
    print(f"Edge indicators: {list(edge_indicators.keys())}")
    
    # Test full pipeline
    result = calc_service.execute_pipeline(test_tick, price_service)
    edge_fields = [k for k in result.keys() if k.startswith('edge_')]
    print(f"Pipeline added {len(edge_fields)} edge fields")
    
    return edge_fields

if __name__ == "__main__":
    edge_fields = test_edge_calculations()
    print(f"✅ Edge integration test: {len(edge_fields)} edge fields computed")