"""
Parquet file ingester for tick data with comprehensive metadata enrichment.

This module consolidates metadata enrichment functionality, including:
- Basic instrument metadata (symbol, exchange, segment, instrument_type)
- Sector classification via pattern matching (20+ sectors)
- Bond metadata extraction (state, maturity, coupon) for SDL/GOI bonds
- JSON metadata file enrichment (consolidated from enrich_metadata_comprehensive.py)
- Optional JSON to Parquet conversion

Consolidated from enrich_metadata_comprehensive.py (November 2025)
- Sector classification: Pattern matching on symbol/name for sector assignment
- Bond enrichment: Extracts state, maturity year, coupon rate for bonds
- JSON enrichment: Enriches JSON metadata lookup files with sector/bond data
- Parquet conversion: Optionally converts enriched JSON to parquet format

Usage:
    # Enrich JSON metadata file
    from parquet_ingester import enrich_json_metadata_file
    result = enrich_json_metadata_file(
        Path('zerodha_tokens_metadata.json'),
        output_file=Path('zerodha_tokens_metadata_enriched.json'),
        convert_to_parquet=True
    )
    
    # Enrich parquet files with metadata
    from parquet_ingester import ingest_parquet_file
    result = ingest_parquet_file(Path('tick_data.parquet'), 'db.duckdb', token_cache)
"""

import json
import logging
import re
import threading
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import duckdb
from token_cache import TokenCacheManager
from singleton_db import DatabaseConnectionManager
from binary_to_parquet.production_binary_converter import ensure_production_schema

logger = logging.getLogger(__name__)

# Semaphore to limit concurrent DuckDB connections (DuckDB has connection limits)
_db_connection_semaphore = threading.Semaphore(4)  # Max 4 concurrent DB connections

# Cache sector and state mappings (created once, reused)
_sector_mapping_cache = None
_state_mapping_cache = None


def _get_sector_mapping() -> Dict[str, str]:
    """Get or create sector mapping (cached for performance)"""
    global _sector_mapping_cache
    if _sector_mapping_cache is None:
        _sector_mapping_cache = {
            # Banking & Financial Services
            'BANK': 'Banking & Financial Services', 'FINANCIAL': 'Banking & Financial Services',
            'FINANCE': 'Banking & Financial Services', 'CREDIT': 'Banking & Financial Services',
            'CAPITAL': 'Banking & Financial Services', 'INVESTMENT': 'Banking & Financial Services',
            'INSURANCE': 'Banking & Financial Services', 'MUTUAL': 'Banking & Financial Services',
            'HDFC': 'Banking & Financial Services', 'ICICI': 'Banking & Financial Services',
            'SBI': 'Banking & Financial Services', 'AXIS': 'Banking & Financial Services',
            'KOTAK': 'Banking & Financial Services', 'INDUS': 'Banking & Financial Services',
            'FEDERAL': 'Banking & Financial Services', 'BANDHAN': 'Banking & Financial Services',
            'IDFC': 'Banking & Financial Services', 'YES': 'Banking & Financial Services',
            'RBL': 'Banking & Financial Services', 'CANARA': 'Banking & Financial Services',
            'UNION': 'Banking & Financial Services', 'PNB': 'Banking & Financial Services',
            'BOI': 'Banking & Financial Services', 'BOB': 'Banking & Financial Services',
            'CENTRAL': 'Banking & Financial Services',
            # Pharmaceuticals
            'PHARMA': 'Pharmaceuticals', 'DRUG': 'Pharmaceuticals', 'MEDICAL': 'Pharmaceuticals',
            'HEALTH': 'Pharmaceuticals', 'CIPLA': 'Pharmaceuticals', 'SUN': 'Pharmaceuticals',
            'DR': 'Pharmaceuticals', 'REDDY': 'Pharmaceuticals', 'AURO': 'Pharmaceuticals',
            'BIOCON': 'Pharmaceuticals', 'DIVI': 'Pharmaceuticals', 'GLENMARK': 'Pharmaceuticals',
            'LUPIN': 'Pharmaceuticals', 'TORRENT': 'Pharmaceuticals', 'ALKEM': 'Pharmaceuticals',
            'MANKIND': 'Pharmaceuticals', 'AJANTA': 'Pharmaceuticals', 'NATCO': 'Pharmaceuticals',
            # Information Technology
            'TECH': 'Information Technology', 'SOFTWARE': 'Information Technology',
            'IT': 'Information Technology', 'INFOSYS': 'Information Technology',
            'TCS': 'Information Technology', 'WIPRO': 'Information Technology',
            'HCL': 'Information Technology', 'TECHM': 'Information Technology',
            'COFORGE': 'Information Technology', 'LTTS': 'Information Technology',
            'PERSISTENT': 'Information Technology', 'MPHASIS': 'Information Technology',
            # Automotive
            'AUTO': 'Automotive', 'MOTOR': 'Automotive', 'VEHICLE': 'Automotive',
            'MARUTI': 'Automotive', 'TATA': 'Automotive', 'MAHINDRA': 'Automotive',
            'BAJAJ': 'Automotive', 'HERO': 'Automotive', 'TVS': 'Automotive',
            'EICHER': 'Automotive', 'ASHOK': 'Automotive', 'MRF': 'Automotive',
            # Metals & Mining
            'METAL': 'Metals & Mining', 'STEEL': 'Metals & Mining', 'IRON': 'Metals & Mining',
            'COPPER': 'Metals & Mining', 'ALUMINIUM': 'Metals & Mining', 'ZINC': 'Metals & Mining',
            'MINING': 'Metals & Mining', 'HINDALCO': 'Metals & Mining', 'VEDANTA': 'Metals & Mining',
            'JSW': 'Metals & Mining', 'SAIL': 'Metals & Mining', 'NMDC': 'Metals & Mining',
            # Energy & Power
            'ENERGY': 'Energy & Power', 'POWER': 'Energy & Power', 'ELECTRIC': 'Energy & Power',
            'ONGC': 'Energy & Power', 'GAIL': 'Energy & Power', 'BPCL': 'Energy & Power',
            'HPCL': 'Energy & Power', 'IOC': 'Energy & Power', 'RELIANCE': 'Energy & Power',
            'ADANI': 'Energy & Power', 'TATAPOWER': 'Energy & Power',
            # FMCG
            'FMCG': 'FMCG', 'CONSUMER': 'FMCG', 'FOOD': 'FMCG', 'BEVERAGE': 'FMCG',
            'NESTLE': 'FMCG', 'ITC': 'FMCG', 'HUL': 'FMCG', 'DABUR': 'FMCG',
            'MARICO': 'FMCG', 'BRITANNIA': 'FMCG', 'TITAN': 'FMCG',
            # Real Estate
            'REALTY': 'Real Estate', 'REAL': 'Real Estate', 'ESTATE': 'Real Estate',
            'PROPERTY': 'Real Estate', 'CONSTRUCTION': 'Real Estate', 'DLF': 'Real Estate',
            'SOBHA': 'Real Estate', 'BRIGADE': 'Real Estate', 'PRESTIGE': 'Real Estate',
            # Infrastructure
            'INFRA': 'Infrastructure', 'INFRASTRUCTURE': 'Infrastructure',
            'ENGINEERING': 'Infrastructure', 'LARSEN': 'Infrastructure', 'L&T': 'Infrastructure',
            'GMR': 'Infrastructure', 'POWERGRID': 'Infrastructure', 'BHEL': 'Infrastructure',
            # Media & Entertainment
            'MEDIA': 'Media & Entertainment', 'ENTERTAINMENT': 'Media & Entertainment',
            'ZEE': 'Media & Entertainment', 'NETWORK': 'Media & Entertainment',
            'PVR': 'Media & Entertainment', 'INOX': 'Media & Entertainment',
            # Public Sector
            'PSE': 'Public Sector', 'PSU': 'Public Sector', 'PUBLIC': 'Public Sector',
            'GOVERNMENT': 'Public Sector', 'BHARAT': 'Public Sector', 'INDIA': 'Public Sector',
            'HINDUSTAN': 'Public Sector', 'NATIONAL': 'Public Sector',
            # Telecommunications
            'TELECOM': 'Telecommunications', 'COMMUNICATION': 'Telecommunications',
            'AIRTEL': 'Telecommunications', 'RELIANCE': 'Telecommunications',
            'JIO': 'Telecommunications', 'VODAFONE': 'Telecommunications',
            # Chemicals
            'CHEMICAL': 'Chemicals', 'CHEM': 'Chemicals', 'FERTILIZER': 'Chemicals',
            'PAINT': 'Chemicals', 'POLYMER': 'Chemicals', 'PLASTIC': 'Chemicals',
            # Textiles
            'TEXTILE': 'Textiles', 'COTTON': 'Textiles', 'FABRIC': 'Textiles',
            'RAYMOND': 'Textiles', 'ARVIND': 'Textiles', 'WELSPUN': 'Textiles',
            # Cement
            'CEMENT': 'Cement', 'CONCRETE': 'Cement', 'ULTRATECH': 'Cement',
            'SHREE': 'Cement', 'GRASIM': 'Cement', 'AMBUJA': 'Cement', 'ACC': 'Cement',
            # Healthcare
            'HEALTHCARE': 'Healthcare', 'HOSPITAL': 'Healthcare', 'MEDICAL': 'Healthcare',
            'FORTIS': 'Healthcare', 'APOLLO': 'Healthcare', 'MAX': 'Healthcare',
            # Retail
            'RETAIL': 'Retail', 'STORE': 'Retail', 'MALL': 'Retail',
            'TRENT': 'Retail', 'LIFESTYLE': 'Retail',
            # E-commerce
            'E-COMMERCE': 'E-commerce', 'ECOMMERCE': 'E-commerce', 'ONLINE': 'E-commerce',
            'FLIPKART': 'E-commerce', 'AMAZON': 'E-commerce', 'PAYTM': 'E-commerce',
            # Fintech
            'FINTECH': 'Fintech', 'PAYMENT': 'Fintech', 'WALLET': 'Fintech',
            'UPI': 'Fintech', 'PHONEPE': 'Fintech',
        }
    return _sector_mapping_cache


def _get_state_mapping() -> Dict[str, str]:
    """Get or create state mapping for SDL bonds (cached)"""
    global _state_mapping_cache
    if _state_mapping_cache is None:
        _state_mapping_cache = {
            'AP': 'Andhra Pradesh', 'AS': 'Assam', 'BR': 'Bihar', 'GA': 'Goa',
            'GJ': 'Gujarat', 'HR': 'Haryana', 'HP': 'Himachal Pradesh',
            'JK': 'Jammu & Kashmir', 'KA': 'Karnataka', 'KL': 'Kerala',
            'MP': 'Madhya Pradesh', 'MH': 'Maharashtra', 'MN': 'Manipur',
            'ML': 'Meghalaya', 'MZ': 'Mizoram', 'NL': 'Nagaland', 'OD': 'Odisha',
            'PB': 'Punjab', 'RJ': 'Rajasthan', 'SK': 'Sikkim', 'TN': 'Tamil Nadu',
            'TS': 'Telangana', 'TR': 'Tripura', 'UP': 'Uttar Pradesh',
            'UK': 'Uttarakhand', 'WB': 'West Bengal', 'DL': 'Delhi',
            'CH': 'Chandigarh', 'PY': 'Puducherry', 'AN': 'Andaman & Nicobar',
            'LD': 'Lakshadweep', 'JH': 'Jharkhand', 'CT': 'Chhattisgarh',
            'AR': 'Arunachal Pradesh', 'OR': 'Odisha'
        }
    return _state_mapping_cache


def _classify_sector(symbol: str, name: str) -> Optional[str]:
    """Classify sector based on symbol/name pattern matching"""
    if pd.isna(symbol):
        symbol = ''
    if pd.isna(name):
        name = ''
    
    symbol_upper = str(symbol).upper()
    name_upper = str(name).upper()
    
    sector_mapping = _get_sector_mapping()
    for pattern, sector in sector_mapping.items():
        if pattern in symbol_upper or pattern in name_upper:
            return sector
    return None


def _extract_bond_metadata(symbol: str, name: str) -> Dict[str, Any]:
    """Extract bond metadata (state, maturity, coupon) for SDL/GOI bonds"""
    bond_data = {}
    
    if pd.isna(symbol):
        symbol = ''
    if pd.isna(name):
        name = ''
    
    symbol_str = str(symbol)
    name_str = str(name)
    
    # Extract state from symbol (2-letter state code)
    state_match = re.search(r'^\d+([A-Z]{2})\d+', symbol_str)
    if state_match:
        state_code = state_match.group(1)
        state_mapping = _get_state_mapping()
        bond_data['bond_state'] = state_mapping.get(state_code, state_code)
    
    # Extract maturity year
    year_match = re.search(r'(\d{2})-SG$', symbol_str)
    if year_match:
        year = int('20' + year_match.group(1))
        bond_data['bond_maturity_year'] = str(year)
    
    # Extract coupon rate
    rate_match = re.search(r'(\d+\.?\d*)%', name_str)
    if rate_match:
        bond_data['bond_coupon_rate'] = rate_match.group(1) + '%'
    
    return bond_data


def enrich_parquet_with_metadata(df: pd.DataFrame, token_cache: TokenCacheManager) -> pd.DataFrame:
    """Enrich parquet DataFrame with metadata from token_lookup.json."""
    enriched_df = df.copy()
    
    # Add metadata columns if they don't exist
    metadata_columns = [
        'token_symbol', 'token_exchange', 'token_segment', 'token_instrument_type',
        'token_expiry', 'token_strike_price', 'token_option_type',
        'token_lot_size', 'token_tick_size', 'token_is_expired',
        'token_sector', 'token_asset_class', 'token_sub_category',
        # Sector and bond enrichment columns
        'sector_classified', 'bond_state', 'bond_maturity_year', 'bond_coupon_rate'
    ]
    
    for col in metadata_columns:
        if col not in enriched_df.columns:
            enriched_df[col] = None
    
    # Enrich each row with token metadata
    def enrich_row(row):
        token = row.get('instrument_token')
        if not token or pd.isna(token):
            return row
        
        try:
            token = int(token)
            metadata = token_cache.get_instrument_info(token)
            
            if metadata:
                # Update metadata fields
                row['token_symbol'] = metadata.get('symbol') or row.get('symbol')
                row['token_exchange'] = metadata.get('exchange') or row.get('exchange')
                row['token_segment'] = metadata.get('segment') or row.get('segment')
                row['token_instrument_type'] = metadata.get('instrument_type') or row.get('instrument_type')
                
                # Additional metadata
                if metadata.get('expiry'):
                    row['token_expiry'] = metadata['expiry']
                if metadata.get('strike_price'):
                    row['token_strike_price'] = metadata['strike_price']
                if metadata.get('option_type'):
                    row['token_option_type'] = metadata['option_type']
                if metadata.get('lot_size'):
                    row['token_lot_size'] = metadata['lot_size']
                if metadata.get('tick_size'):
                    row['token_tick_size'] = metadata['tick_size']
                if metadata.get('is_expired') is not None:
                    row['token_is_expired'] = bool(metadata['is_expired'])
                
                # Extended metadata (if available in token_lookup)
                if 'sector' in metadata:
                    row['token_sector'] = metadata['sector']
                if 'asset_class' in metadata:
                    row['token_asset_class'] = metadata['asset_class']
                if 'sub_category' in metadata:
                    row['token_sub_category'] = metadata['sub_category']
                
                # Sector classification (pattern matching on symbol/name)
                symbol = row.get('token_symbol') or row.get('symbol') or ''
                name = metadata.get('name', '') or ''
                if symbol or name:
                    classified_sector = _classify_sector(symbol, name)
                    if classified_sector and (not row.get('token_sector') or row.get('token_sector') in ['Unknown', 'Other', 'Missing', None, '']):
                        row['sector_classified'] = classified_sector
                        # Also update token_sector if missing
                        if not row.get('token_sector'):
                            row['token_sector'] = classified_sector
                
                # Bond metadata extraction (for SDL/GOI bonds)
                if 'SDL' in name or 'GOI' in name:
                    bond_data = _extract_bond_metadata(symbol, name)
                    if bond_data:
                        row.update(bond_data)
        except (ValueError, TypeError) as e:
        
            return row
    
    # Apply enrichment
    enriched_df = enriched_df.apply(enrich_row, axis=1)
    
    return enriched_df


def map_to_duckdb_schema(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Map parquet DataFrame columns to DuckDB schema format."""
    mapped_df = pd.DataFrame()
    
    # Core instrument identifiers - check for various possible column names
    if 'instrument_token' in df.columns:
        mapped_df['instrument_token'] = pd.to_numeric(df['instrument_token'], errors='coerce').fillna(0).astype('int64')
    elif 'token' in df.columns:
        mapped_df['instrument_token'] = pd.to_numeric(df['token'], errors='coerce').fillna(0).astype('int64')
    elif 'instrument_id' in df.columns:
        mapped_df['instrument_token'] = pd.to_numeric(df['instrument_id'], errors='coerce').fillna(0).astype('int64')
    else:
        # No instrument_token found - return empty DataFrame
        logger.warning(f"No instrument_token column found in {source_file}. Columns: {list(df.columns)}")
        return pd.DataFrame()
    
    # Symbol and exchange (prefer token_* enriched columns)
    def get_column(df, *col_names, default=None):
        for col in col_names:
            if col in df.columns:
                return df[col]
        return pd.Series([default] * len(df), index=df.index)
    
    mapped_df['symbol'] = get_column(df, 'token_symbol', 'symbol', 'tradingsymbol').fillna('')
    mapped_df['exchange'] = get_column(df, 'token_exchange', 'exchange').fillna('')
    mapped_df['segment'] = get_column(df, 'token_segment', 'segment').fillna('')
    mapped_df['instrument_type'] = get_column(df, 'token_instrument_type', 'instrument_type').fillna('')
    
    # Normalize expiry dates to date objects or YYYY-MM-DD strings
    expiry_col = get_column(df, 'token_expiry', 'expiry')
    def normalize_expiry_date(expiry_val):
        if pd.isna(expiry_val) or expiry_val is None:
            return None
        if isinstance(expiry_val, date):
            return expiry_val
        if isinstance(expiry_val, datetime):
            return expiry_val.date()
        if isinstance(expiry_val, str):
            try:
                # Try multiple date formats
                # Format 1: YYYY-MM-DD
                try:
                    return datetime.strptime(expiry_val[:10], "%Y-%m-%d").date()
                except ValueError:
                    # Format 2: DD-MMM-YYYY (e.g., "29-Dec-2025")
                    try:
                        return datetime.strptime(expiry_val, "%d-%b-%Y").date()
                    except ValueError:
                        # Format 3: DD/MM/YYYY
                        try:
                            return datetime.strptime(expiry_val, "%d/%m/%Y").date()
                        except ValueError:
                            # Format 4: YYYYMMDD
                            try:
                                if len(expiry_val) >= 8 and expiry_val[:8].isdigit():
                                    return datetime.strptime(expiry_val[:8], "%Y%m%d").date()
                                else:
                                    return None
                            except ValueError:
                                return None
            except Exception:
                return None
        return None
    mapped_df['expiry'] = expiry_col.apply(normalize_expiry_date)
    mapped_df['strike_price'] = pd.to_numeric(get_column(df, 'token_strike_price', 'strike_price', default=None), errors='coerce')
    mapped_df['option_type'] = get_column(df, 'token_option_type', 'option_type')
    mapped_df['lot_size'] = pd.to_numeric(get_column(df, 'token_lot_size', 'lot_size', default=None), errors='coerce')
    mapped_df['tick_size'] = pd.to_numeric(get_column(df, 'token_tick_size', 'tick_size', default=None), errors='coerce')
    is_expired_col = get_column(df, 'token_is_expired', 'is_expired', default=False)
    mapped_df['is_expired'] = is_expired_col.astype(bool) if is_expired_col.dtype == 'object' else is_expired_col.fillna(False)
    
    # Timestamps - normalize to datetime
    if 'exchange_timestamp' in df.columns:
        mapped_df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'], errors='coerce')
        # Ensure UTC timezone for calculations
        if mapped_df['exchange_timestamp'].dt.tz is None:
             mapped_df['exchange_timestamp'] = mapped_df['exchange_timestamp'].dt.tz_localize('UTC')
        else:
             mapped_df['exchange_timestamp'] = mapped_df['exchange_timestamp'].dt.tz_convert('UTC')

        # Calculate nanoseconds (convert datetime64 to int64 nanoseconds)
        mapped_df['exchange_timestamp_ns'] = (
            (mapped_df['exchange_timestamp'] - pd.Timestamp('1970-01-01', tz='UTC')).dt.total_seconds() * 1_000_000_000
        ).fillna(0).astype('int64')
    elif 'exchange_timestamp_epoch' in df.columns:
        # Convert epoch to datetime
        mapped_df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp_epoch'], unit='s', errors='coerce')
        mapped_df['exchange_timestamp_ns'] = (df['exchange_timestamp_epoch'] * 1_000_000_000).astype('int64').fillna(0)
    else:
        mapped_df['exchange_timestamp'] = pd.NaT
        mapped_df['exchange_timestamp_ns'] = 0
    
    if 'last_trade_time' in df.columns:
        mapped_df['last_traded_timestamp'] = pd.to_datetime(df['last_trade_time'], errors='coerce')
        # Ensure UTC
        if mapped_df['last_traded_timestamp'].dt.tz is None:
            mapped_df['last_traded_timestamp'] = mapped_df['last_traded_timestamp'].dt.tz_localize('UTC')
        else:
            mapped_df['last_traded_timestamp'] = mapped_df['last_traded_timestamp'].dt.tz_convert('UTC')

        mapped_df['last_traded_timestamp_ns'] = (
            (mapped_df['last_traded_timestamp'] - pd.Timestamp('1970-01-01', tz='UTC')).dt.total_seconds() * 1_000_000_000
        ).fillna(0).astype('int64')
    else:
        mapped_df['last_traded_timestamp'] = pd.NaT
        mapped_df['last_traded_timestamp_ns'] = 0
    
    mapped_df['timestamp'] = pd.Timestamp.utcnow()
    
    # Price data
    if 'last_price' in df.columns:
        mapped_df['last_price'] = pd.to_numeric(df['last_price'], errors='coerce').fillna(0.0)
    elif 'last_traded_price' in df.columns:
        mapped_df['last_price'] = pd.to_numeric(df['last_traded_price'], errors='coerce').fillna(0.0)
    else:
        mapped_df['last_price'] = 0.0
    
    # OHLC handling
    if 'ohlc' in df.columns and df['ohlc'].notna().any():
        # OHLC might be a dict or nested structure
        ohlc_values = df['ohlc'].apply(lambda x: x if isinstance(x, dict) else {})
        mapped_df['open_price'] = pd.Series(pd.to_numeric([x.get('o', 0) for x in ohlc_values], errors='coerce'), index=df.index).fillna(0.0)
        mapped_df['high_price'] = pd.Series(pd.to_numeric([x.get('h', 0) for x in ohlc_values], errors='coerce'), index=df.index).fillna(0.0)
        mapped_df['low_price'] = pd.Series(pd.to_numeric([x.get('l', 0) for x in ohlc_values], errors='coerce'), index=df.index).fillna(0.0)
        mapped_df['close_price'] = pd.Series(pd.to_numeric([x.get('c', 0) for x in ohlc_values], errors='coerce'), index=df.index).fillna(0.0)
    else:
        mapped_df['open_price'] = 0.0
        mapped_df['high_price'] = 0.0
        mapped_df['low_price'] = 0.0
        mapped_df['close_price'] = 0.0
    
    avg_price_col = 'average_traded_price' if 'average_traded_price' in df.columns else 'average_price'
    if avg_price_col in df.columns:
        mapped_df['average_traded_price'] = pd.to_numeric(df[avg_price_col], errors='coerce').fillna(0.0)
    else:
        mapped_df['average_traded_price'] = 0.0
    
    # Volume and quantity
    if 'volume' in df.columns:
        mapped_df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype('int64')
    elif 'volume_traded' in df.columns:
        mapped_df['volume'] = pd.to_numeric(df['volume_traded'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['volume'] = 0
    
    if 'last_traded_quantity' in df.columns:
        mapped_df['last_traded_quantity'] = pd.to_numeric(df['last_traded_quantity'], errors='coerce').fillna(0).astype('int64')
    elif 'zerodha_last_traded_quantity' in df.columns:
        mapped_df['last_traded_quantity'] = pd.to_numeric(df['zerodha_last_traded_quantity'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['last_traded_quantity'] = 0
    
    if 'total_buy_quantity' in df.columns:
        mapped_df['total_buy_quantity'] = pd.to_numeric(df['total_buy_quantity'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['total_buy_quantity'] = 0
    
    if 'total_sell_quantity' in df.columns:
        mapped_df['total_sell_quantity'] = pd.to_numeric(df['total_sell_quantity'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['total_sell_quantity'] = 0
    
    # Open interest
    if 'oi' in df.columns:
        mapped_df['open_interest'] = pd.to_numeric(df['oi'], errors='coerce').fillna(0).astype('int64')
    elif 'open_interest' in df.columns:
        mapped_df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['open_interest'] = 0
    
    if 'oi_day_high' in df.columns:
        mapped_df['oi_day_high'] = pd.to_numeric(df['oi_day_high'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['oi_day_high'] = 0
    
    if 'oi_day_low' in df.columns:
        mapped_df['oi_day_low'] = pd.to_numeric(df['oi_day_low'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['oi_day_low'] = 0
    
    # Market depth - extract from depth column if available
    for level in range(1, 6):
        mapped_df[f'bid_{level}_price'] = None
        mapped_df[f'bid_{level}_quantity'] = 0
        mapped_df[f'bid_{level}_orders'] = 0
        mapped_df[f'ask_{level}_price'] = None
        mapped_df[f'ask_{level}_quantity'] = 0
        mapped_df[f'ask_{level}_orders'] = 0
    
    # Extract best bid/ask
    if 'best_bid_price' in df.columns:
        mapped_df['bid_1_price'] = pd.to_numeric(df['best_bid_price'], errors='coerce')
        if 'best_bid_quantity' in df.columns:
            mapped_df['bid_1_quantity'] = pd.to_numeric(df['best_bid_quantity'], errors='coerce').fillna(0).astype('int64')
        else:
            mapped_df['bid_1_quantity'] = 0
        if 'best_bid_orders' in df.columns:
            mapped_df['bid_1_orders'] = pd.to_numeric(df['best_bid_orders'], errors='coerce').fillna(0).astype('int32')
        else:
            mapped_df['bid_1_orders'] = 0
    
    if 'best_ask_price' in df.columns:
        mapped_df['ask_1_price'] = pd.to_numeric(df['best_ask_price'], errors='coerce')
        if 'best_ask_quantity' in df.columns:
            mapped_df['ask_1_quantity'] = pd.to_numeric(df['best_ask_quantity'], errors='coerce').fillna(0).astype('int64')
        else:
            mapped_df['ask_1_quantity'] = 0
        if 'best_ask_orders' in df.columns:
            mapped_df['ask_1_orders'] = pd.to_numeric(df['best_ask_orders'], errors='coerce').fillna(0).astype('int32')
        else:
            mapped_df['ask_1_orders'] = 0
    
    # Metadata fields
    mapped_df['packet_type'] = df['mode'] if 'mode' in df.columns else 'full'
    mapped_df['data_quality'] = 'complete'
    mapped_df['session_type'] = 'regular'  # Could be enhanced with session detection
    mapped_df['source_file'] = source_file
    mapped_df['processing_batch'] = 'parquet_ingestion_enriched'
    
    return mapped_df


def ingest_parquet_file(file_path: Path, db_path: str, token_cache: Optional[TokenCacheManager] = None, enrich: bool = True) -> Dict[str, Any]:
    """Ingest a parquet file into DuckDB with optional metadata enrichment."""
    try:
        # Load parquet file - try multiple methods
        df = None
        read_error = None
        
        # Method 1: Try pandas (standard)
        try:
            df = pd.read_parquet(file_path)
        except Exception as e1:
            # Method 2: Try pandas with different engine
            try:
                df = pd.read_parquet(file_path, engine='pyarrow')
            except Exception as e2:
                # Method 3: Try fastparquet engine
                try:
                    df = pd.read_parquet(file_path, engine='fastparquet')
                except Exception as e3:
                    # Method 4: Try pyarrow directly
                    try:
                        import pyarrow.parquet as pq
                        table = pq.read_table(file_path)
                        df = table.to_pandas()
                    except Exception as e4:
                        # All methods failed - skip this file
                        logger.warning(f"⚠️  Cannot read parquet file: {file_path.name} - tried 4 methods, all failed")
                        return {
                            "success": False,
                            "error": f"Cannot read file with any method: {str(e1)[:100]}",
                            "row_count": 0,
                            "original_rows": 0
                        }
        
        original_rows = len(df)
        
        # Normalize column names to lowercase
        df.columns = df.columns.astype(str).str.lower()
        
        # Enrich with metadata if requested
        if enrich and token_cache:
            df = enrich_parquet_with_metadata(df, token_cache)
        
        # Map to DuckDB schema
        mapped_df = map_to_duckdb_schema(df, str(file_path))
        
        # Ensure required columns exist
        required_cols = [
            'instrument_token', 'symbol', 'exchange_timestamp', 'exchange_timestamp_ns',
            'last_price', 'volume'
        ]
        
        missing_cols = [col for col in required_cols if col not in mapped_df.columns]
        if missing_cols:
            logger.warning(f"Missing required columns in {file_path}: {missing_cols}")
            return {
                "success": False,
                "error": f"Missing required columns: {missing_cols}",
                "row_count": 0,
                "original_rows": original_rows
            }
        
        # Filter rows with valid instrument_token (skip if no instrument_token column)
        if 'instrument_token' in mapped_df.columns:
            mapped_df = mapped_df[mapped_df['instrument_token'] > 0].copy()
        else:
            logger.warning(f"No instrument_token column in mapped_df for {file_path}")
            return {
                "success": False,
                "error": "No instrument_token column after mapping",
                "row_count": 0,
                "original_rows": original_rows
            }
        
        # ✅ Filter out rows with UNKNOWN symbols - these lack proper metadata
        if 'symbol' in mapped_df.columns:
            before_filter = len(mapped_df)
            # Filter out: NULL, empty, or UNKNOWN_* symbols
            mapped_df = mapped_df[
                mapped_df['symbol'].notna() & 
                (mapped_df['symbol'] != '') & 
                (~mapped_df['symbol'].astype(str).str.startswith('UNKNOWN', na=False))
            ].copy()
            filtered_out = before_filter - len(mapped_df)
            if filtered_out > 0:
                logger.warning(f"Filtered out {filtered_out:,} rows with UNKNOWN/missing symbols from {file_path.name}")
        
        valid_rows = len(mapped_df)
        
        # Return data if no DB path provided
        # This prevents InvalidInputException when called with db_path=None
        if db_path is None:
            return {
                "row_count": valid_rows,
                "original_rows": original_rows,
                "columns": list(mapped_df.columns),
                "success": True,
                "dataframe": mapped_df
            }

        # Insert into DuckDB
        # Create a new connection per thread (DuckDB connections are not thread-safe)
        conn = duckdb.connect(db_path)
        try:
            ensure_production_schema(conn)
            
            # Convert to Arrow table for efficient insertion
            arrow_table = pa.Table.from_pandas(mapped_df)
            
            # Register as view and insert (sanitize view name - no dots allowed)
            view_name = f"temp_parquet_{int(datetime.now().timestamp() * 1000000)}"
            conn.register(view_name, arrow_table)
            
            try:
                # Get column list
                columns = ', '.join(mapped_df.columns)
                conn.execute(f"""
                    INSERT OR REPLACE INTO tick_data_corrected ({columns})
                    SELECT {columns} FROM {view_name}
                """)
                
                logger.info(f"Ingested {valid_rows}/{original_rows} rows from {file_path.name}")
            finally:
                try:
                    conn.unregister(view_name)
                except Exception:
                    pass  # Ignore errors when unregistering
        finally:
            conn.close()
        
        return {
            "row_count": valid_rows,
            "original_rows": original_rows,
            "columns": list(mapped_df.columns),
            "success": True
        }
    except Exception as e:
        logger.error(f"Error ingesting {file_path}: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }


def enrich_metadata_comprehensive(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich metadata dictionary in-memory with sector classification and bond metadata.
    Compatible with enrich_metadata_comprehensive.py API for backward compatibility.
    
    Args:
        metadata: Dictionary of token -> metadata dict
    
    Returns:
        Enriched metadata dictionary (modified in-place and returned)
    """
    print('🔍 CREATING COMPREHENSIVE DATA ENRICHMENT SYSTEM')
    print('=' * 70)
    
    enriched_count = 0
    sector_stats = defaultdict(int)
    bond_stats = defaultdict(int)
    
    for token_str, data in metadata.items():
        try:
            symbol = data.get('symbol', '') or data.get('tradingsymbol', '')
            name = data.get('name', '')
            
            current_sector = data.get('sector', 'Missing')
            
            # Skip if already has a valid sector (not Unknown, Other, or Missing)
            if current_sector not in ['Unknown', 'Other', 'Missing', None, '']:
                continue
            
            # Enrich sector data via pattern matching
            classified_sector = _classify_sector(symbol, name)
            if classified_sector:
                data['sector'] = classified_sector
                enriched_count += 1
                sector_stats[classified_sector] += 1
            
            # Enrich bond data (for SDL/GOI bonds)
            if 'SDL' in name or 'GOI' in name:
                bond_data = _extract_bond_metadata(symbol, name)
                if bond_data:
                    # Map bond_data keys to match JSON structure
                    if 'bond_state' in bond_data:
                        data['state'] = bond_data['bond_state']
                    if 'bond_maturity_year' in bond_data:
                        data['maturity_year'] = bond_data['bond_maturity_year']
                    if 'bond_coupon_rate' in bond_data:
                        data['coupon_rate'] = bond_data['bond_coupon_rate']
                    bond_stats['SDL' if 'SDL' in name else 'GOI'] += 1
            
        except Exception as e:
            continue
    
    print(f'Enriched {enriched_count} tokens with sector data')
    
    # Analyze results
    print(f'\n📊 SECTOR ENRICHMENT RESULTS:')
    for sector, count in sorted(sector_stats.items(), key=lambda x: x[1], reverse=True):
        print(f'  {sector}: {count:,}')
    
    print(f'\n📊 BOND ENRICHMENT RESULTS:')
    for bond_type, count in sorted(bond_stats.items(), key=lambda x: x[1], reverse=True):
        print(f'  {bond_type}: {count:,}')
    
    return metadata


def save_enriched_metadata(metadata: Dict[str, Any], file_path: str):
    """
    Save enriched metadata to JSON file.
    Compatible with enrich_metadata_comprehensive.py API for backward compatibility.
    """
    with open(file_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f'\n💾 Enriched metadata saved to: {file_path}')


def generate_final_statistics(metadata: Dict[str, Any]):
    """
    Generate and print final enrichment statistics.
    Compatible with enrich_metadata_comprehensive.py API for backward compatibility.
    """
    print(f'\n📊 FINAL ENRICHMENT STATISTICS:')
    print('=' * 50)
    
    asset_classes = defaultdict(int)
    sectors = defaultdict(int)
    bond_types = defaultdict(int)
    states = defaultdict(int)
    
    for token, data in metadata.items():
        if 'asset_class' in data:
            asset_classes[data['asset_class']] += 1
        if 'sector' in data:
            sectors[data['sector']] += 1
        if 'bond_type' in data:
            bond_types[data['bond_type']] += 1
        if 'state' in data:
            states[data['state']] += 1
    
    print(f'Asset Classes:')
    for asset_class, count in sorted(asset_classes.items()):
        print(f'  {asset_class}: {count:,}')
    
    print(f'\nTop Sectors:')
    for sector, count in sorted(sectors.items(), key=lambda x: x[1], reverse=True)[:15]:
        print(f'  {sector}: {count:,}')
    
    print(f'\nBond Types:')
    for bond_type, count in sorted(bond_types.items()):
        print(f'  {bond_type}: {count:,}')
    
    print(f'\nTop States (for SDL bonds):')
    for state, count in sorted(states.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f'  {state}: {count:,}')
    
    print(f'\n✅ ENRICHMENT COMPLETE!')
    print(f'Total tokens processed: {len(metadata):,}')
    print(f'Tokens with sector data: {sum(1 for data in metadata.values() if data.get("sector") and data.get("sector") != "Unknown"):,}')
    print(f'Tokens with bond data: {sum(1 for data in metadata.values() if "bond_type" in data):,}')
    print(f'Tokens with state data: {sum(1 for data in metadata.values() if "state" in data):,}')


def enrich_json_metadata_file(
    input_file: Path,
    output_file: Optional[Path] = None,
    convert_to_parquet: bool = False,
    parquet_output: Optional[Path] = None
) -> Dict[str, Any]:
    """
    Enrich JSON metadata file with sector classification and bond metadata.
    Consolidates functionality from enrich_metadata_comprehensive.py
    
    Args:
        input_file: Path to input JSON metadata file (token -> metadata dict)
        output_file: Path to save enriched JSON (if None, overwrites input)
        convert_to_parquet: Whether to also convert enriched JSON to parquet
        parquet_output: Path for parquet output (if convert_to_parquet is True)
    
    Returns:
        Dict with enrichment statistics
    """
    try:
        logger.info(f"📋 Loading JSON metadata from: {input_file}")
        
        # Load JSON metadata
        with open(input_file, 'r') as f:
            metadata = json.load(f)
        
        logger.info(f"✅ Loaded {len(metadata):,} tokens from JSON file")
        
        # Enrich metadata (using the in-memory function)
        enrich_metadata_comprehensive(metadata)
        
        # Save enriched JSON
        output_path = output_file or input_file
        with open(output_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"💾 Saved enriched metadata to: {output_path}")
        
        # Calculate stats for return value
        enriched_count = sum(1 for data in metadata.values() 
                           if data.get('sector') and data.get('sector') not in ['Unknown', 'Other', 'Missing', None, ''])
        sector_stats = defaultdict(int)
        for data in metadata.values():
            sector = data.get('sector')
            if sector and sector not in ['Unknown', 'Other', 'Missing', None, '']:
                sector_stats[sector] += 1
        
        result = {
            "success": True,
            "tokens_processed": len(metadata),
            "tokens_enriched": enriched_count,
            "sector_stats": dict(sector_stats),
            "output_file": str(output_path)
        }
        
        # Optionally convert to parquet
        if convert_to_parquet:
            parquet_path = Path(parquet_output) if parquet_output else (output_path.with_suffix('.parquet'))
            logger.info(f"📦 Converting enriched JSON to parquet: {parquet_path}")
            
            # Convert metadata dict to DataFrame
            records = []
            for token_str, data in metadata.items():
                try:
                    record = {
                        'instrument_token': int(token_str),
                        **data
                    }
                    records.append(record)
                except (ValueError, TypeError):
                    continue
            
            if records:
                df = pd.DataFrame(records)
                df.to_parquet(parquet_path, index=False, engine='pyarrow')
                logger.info(f"✅ Converted to parquet: {len(df):,} rows")
                result['parquet_file'] = str(parquet_path)
                result['parquet_rows'] = len(df)
        
        return result
        
    except Exception as e:
        logger.error(f"Error enriching JSON metadata file: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }


def ingest_jsonl_file(file_path: Path, db_path: str) -> Dict[str, Any]:
    """Ingest a JSONL file into DuckDB."""
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        records = []
        for line in lines:
            records.append(json.loads(line.strip()))
        
        df = pd.DataFrame(records)
        
        # Convert to Arrow table
        table = pa.Table.from_pandas(df)
        
        return {
            "row_count": len(df),
            "columns": list(df.columns),
            "arrow_table": table,
            "success": True
        }
    except Exception as e:
        logger.error(f"Error ingesting {file_path}: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def main():
    """
    CLI entry point for JSON metadata enrichment.
    Usage: python parquet_ingester.py <input_json> [--output <output_json>] [--to-parquet] [--parquet-output <parquet_file>]
    """
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(
        description="Enrich JSON metadata files with sector classification and bond metadata"
    )
    parser.add_argument(
        'input_file',
        type=str,
        help='Path to input JSON metadata file (token -> metadata dict)'
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        default=None,
        help='Path to output enriched JSON file (default: overwrites input)'
    )
    parser.add_argument(
        '--to-parquet',
        action='store_true',
        help='Also convert enriched JSON to parquet format'
    )
    parser.add_argument(
        '--parquet-output',
        type=str,
        default=None,
        help='Path for parquet output file (default: <output_json>.parquet)'
    )
    
    args = parser.parse_args()
    
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"❌ Input file not found: {input_path}")
        sys.exit(1)
    
    output_path = Path(args.output) if args.output else None
    parquet_path = Path(args.parquet_output) if args.parquet_output else None
    
    print("🚀 ENRICHING JSON METADATA FILE")
    print("=" * 70)
    print(f"Input: {input_path}")
    if output_path:
        print(f"Output: {output_path}")
    if args.to_parquet:
        print(f"Parquet: {parquet_path or (output_path or input_path).with_suffix('.parquet')}")
    print()
    
    result = enrich_json_metadata_file(
        input_file=input_path,
        output_file=output_path,
        convert_to_parquet=args.to_parquet,
        parquet_output=parquet_path
    )
    
    if result.get('success'):
        print("\n✅ ENRICHMENT COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print(f"Tokens processed: {result.get('tokens_processed', 0):,}")
        print(f"Tokens enriched: {result.get('tokens_enriched', 0):,}")
        print(f"Output file: {result.get('output_file')}")
        if result.get('parquet_file'):
            print(f"Parquet file: {result.get('parquet_file')} ({result.get('parquet_rows', 0):,} rows)")
        sys.exit(0)
    else:
        print(f"\n❌ ENRICHMENT FAILED: {result.get('error', 'Unknown error')}")
        sys.exit(1)


if __name__ == '__main__':
    main()

