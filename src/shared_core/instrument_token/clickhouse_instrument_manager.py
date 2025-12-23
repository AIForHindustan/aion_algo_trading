"""
ClickHouse Instrument Manager
Manages instrument data in ClickHouse for analytics using centralized instrument registry
"""
from typing import List, Dict, Any, Optional
import logging

# Import from centralized registry (same directory)
from .instrument_registry import UnifiedInstrumentRegistry, get_unified_registry

logger = logging.getLogger(__name__)


class ClickHouseInstrumentManager:
    """Manages instrument data in ClickHouse for analytics"""
    
    def __init__(self, unified_registry: Optional[UnifiedInstrumentRegistry] = None, clickhouse_client=None):
        """
        Initialize ClickHouse Instrument Manager
        
        Args:
            unified_registry: UnifiedInstrumentRegistry instance. If None, uses get_unified_registry() singleton
            clickhouse_client: ClickHouse client instance
        """
        # Use centralized registry - single source of truth
        if unified_registry is None:
            self.unified_registry = get_unified_registry()
            logger.info("✅ Using centralized instrument registry from shared_core/instrument_token")
        else:
            self.unified_registry = unified_registry
            logger.info("✅ Using provided instrument registry")
        
        self.clickhouse = clickhouse_client
        
        if not self.clickhouse:
            raise ValueError("clickhouse_client is required")
    
    def create_instrument_tables(self):
        """Create instrument metadata tables in ClickHouse"""
        self.clickhouse.execute("""
        CREATE TABLE IF NOT EXISTS instrument_metadata (
            unified_id String,
            name String,
            sector String,
            industry String,
            market_cap Nullable(Float64),
            is_index Boolean,
            underlying Nullable(String),
            created_date Date DEFAULT today()
        ) ENGINE = MergeTree()
        ORDER BY (sector, industry, unified_id)
        """)
        
        self.clickhouse.execute("""
        CREATE TABLE IF NOT EXISTS broker_instruments (
            unified_id String,
            broker String,
            broker_token String,
            tradingsymbol String,
            exchange String,
            instrument_type String,
            lot_size Int32,
            tick_size Float64,
            expiry Nullable(String),
            strike Nullable(Float64)
        ) ENGINE = MergeTree()
        ORDER BY (broker, exchange, unified_id)
        """)
    
    def sync_instruments_to_clickhouse(self):
        """
        Sync unified registry to ClickHouse
        
        This method syncs all instruments from the centralized registry to ClickHouse tables.
        Uses the single source of truth from shared_core/instrument_token/instrument_registry.py
        """
        logger.info("🔄 Syncing instruments from centralized registry to ClickHouse...")
        
        # Ensure tables exist
        self.create_instrument_tables()
        
        # Sync instrument metadata
        metadata_rows = []
        for unified_id, instrument in self.unified_registry._unified_instruments.items():
            metadata_rows.append({
                'unified_id': unified_id,
                'name': instrument.name,
                'sector': instrument.sector,
                'industry': instrument.industry,
                'market_cap': instrument.market_cap,
                'is_index': instrument.is_index,
                'underlying': instrument.underlying
            })
        
        if metadata_rows:
            # Use appropriate insert method based on client type
            if hasattr(self.clickhouse, 'insert'):
                self.clickhouse.insert('instrument_metadata', metadata_rows)
            elif hasattr(self.clickhouse, 'execute'):
                # For clickhouse-driver, use execute with INSERT
                self._insert_rows_driver('instrument_metadata', metadata_rows)
            else:
                logger.error("❌ ClickHouse client doesn't support insert or execute")
                return False
            
            logger.info(f"✅ Synced {len(metadata_rows)} instrument metadata records")
        
        # Sync broker instruments
        broker_rows = []
        for unified_id, instrument in self.unified_registry._unified_instruments.items():
            for broker_name, broker_inst in instrument.broker_instruments.items():
                broker_rows.append({
                    'unified_id': unified_id,
                    'broker': broker_name,
                    'broker_token': broker_inst.broker_token,
                    'tradingsymbol': broker_inst.tradingsymbol,
                    'exchange': broker_inst.exchange,
                    'instrument_type': broker_inst.instrument_type,
                    'lot_size': broker_inst.lot_size,
                    'tick_size': broker_inst.tick_size,
                    'expiry': broker_inst.expiry,
                    'strike': broker_inst.strike
                })
        
        if broker_rows:
            if hasattr(self.clickhouse, 'insert'):
                self.clickhouse.insert('broker_instruments', broker_rows)
            elif hasattr(self.clickhouse, 'execute'):
                self._insert_rows_driver('broker_instruments', broker_rows)
            else:
                logger.error("❌ ClickHouse client doesn't support insert or execute")
                return False
            
            logger.info(f"✅ Synced {len(broker_rows)} broker instrument records")
        
        logger.info("🎉 Instrument sync completed successfully")
        return True
    
    def _insert_rows_driver(self, table: str, rows: List[Dict[str, Any]]):
        """
        Insert rows using clickhouse-driver execute method
        
        Args:
            table: Table name
            rows: List of dictionaries with row data
        """
        if not rows:
            return
        
        # Get column names from first row
        columns = list(rows[0].keys())
        columns_str = ', '.join(columns)
        
        # Build INSERT query - clickhouse-driver uses VALUES format
        query = f"INSERT INTO {table} ({columns_str}) VALUES"
        
        # Prepare data as list of tuples
        data = [tuple(row.get(col) for col in columns) for row in rows]
        
        # Execute with data
        self.clickhouse.execute(query, data)
    
    def get_correlation_data(self, unified_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get instrument data for correlation analysis
        
        Args:
            unified_ids: List of unified IDs (e.g., ['NSE:RELIANCE', 'NSE:TCS', 'NSE:INFY'])
        
        Returns:
            List of dictionaries with instrument metadata for correlation analysis
        """
        if not unified_ids:
            logger.warning("No unified_ids provided for correlation analysis")
            return []
        
        # Build query with proper escaping
        unified_ids_str = ",".join([f"'{uid}'" for uid in unified_ids])
        
        query = f"""
        SELECT 
            im.unified_id,
            im.sector,
            im.industry,
            im.is_index,
            bi.exchange,
            bi.instrument_type,
            bi.broker_token,
            bi.tradingsymbol
        FROM instrument_metadata im
        JOIN broker_instruments bi ON im.unified_id = bi.unified_id
        WHERE im.unified_id IN ({unified_ids_str})
        AND bi.broker = 'zerodha'  -- Use Zerodha for market data
        """
        
        try:
            if hasattr(self.clickhouse, 'execute'):
                result = self.clickhouse.execute(query)
                # Convert to list of dicts if needed
                if result and isinstance(result[0], (list, tuple)):
                    columns = ['unified_id', 'sector', 'industry', 'is_index', 'exchange', 
                              'instrument_type', 'broker_token', 'tradingsymbol']
                    return [dict(zip(columns, row)) for row in result]
                return result
            else:
                logger.error("❌ ClickHouse client doesn't support execute")
                return []
        except Exception as e:
            logger.error(f"❌ Error getting correlation data: {e}")
            return []