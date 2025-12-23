#!/opt/homebrew/bin/python3.13
"""
BRUTE FORCE TICK EXTRACTION - Last resort for severely corrupted parquet files
Extracts tick-like patterns from binary data when standard parsers fail
"""

import sys
from pathlib import Path
import struct
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from collections import Counter
from typing import List, Dict, Optional, Tuple

def improved_brute_force_tick_extraction(file_path: str) -> pd.DataFrame:
    """Enhanced brute force with better record size detection"""
    file_path_obj = Path(file_path)
    
    # Extract file date from filename for validation
    file_date = None
    date_match = re.search(r'(\d{8})', file_path_obj.stem)
    if date_match:
        try:
            file_date = datetime.strptime(date_match.group(1), '%Y%m%d')
            print(f"📅 File date from filename: {file_date.date()}")
        except:
            pass
    
    with open(file_path, 'rb') as f:
        raw_data = f.read()
    
    print(f"📊 File size: {len(raw_data):,} bytes")
    
    # Extract potential timestamps
    timestamps = extract_timestamps(raw_data, file_date)
    print(f"📍 Found {len(timestamps)} potential timestamps")
    
    # Extract numeric data
    numeric_data = extract_numeric_patterns(raw_data)
    print(f"💰 Found {len(numeric_data)} numeric patterns")
    
    # Better record size detection
    record_size = detect_tick_record_size(raw_data, timestamps, numeric_data)
    print(f"🔍 Detected record size: {record_size} bytes")
    
    if record_size and record_size >= 32:  # Reasonable minimum for tick data
        ticks = extract_structured_records(raw_data, record_size, file_date)
    else:
        print("⚠️ Using heuristic grouping")
        ticks = heuristic_record_grouping(timestamps, numeric_data, file_date or datetime(2025, 11, 7))
    
    if not ticks:
        return pd.DataFrame()
    
    df = pd.DataFrame(ticks)
    print(f"✅ Extracted {len(df)} records")
    if len(df) > 0:
        print(f"📋 Columns: {list(df.columns)}")
        print(f"\n📄 Sample data:")
        print(df.head(5))
    
    return df

def detect_tick_record_size(raw_data: bytes, timestamps: List[Tuple], numeric_data: List[Tuple]) -> Optional[int]:
    """Better record size detection for tick data using multiple methods"""
    candidate_sizes = []
    
    # Common tick data record sizes based on binary packet types: 184, 44, 32, 8 bytes
    # Also include common multiples and variations
    common_sizes = [8, 32, 40, 44, 48, 56, 64, 72, 80, 88, 96, 104, 112, 120, 128, 136, 144, 152, 160, 168, 176, 184]
    
    # Method 1: Analyze gaps between timestamps (most reliable)
    if len(timestamps) >= 2:
        timestamp_positions = sorted([pos for (_, _, pos) in timestamps])
        timestamp_gaps = [timestamp_positions[i+1] - timestamp_positions[i] 
                         for i in range(len(timestamp_positions)-1)]
        
        if timestamp_gaps:
            gap_counts = Counter(timestamp_gaps)
            # Get gaps that appear multiple times and are in reasonable range
            for gap, count in gap_counts.most_common(5):
                if 8 <= gap <= 200 and count >= 2:
                    candidate_sizes.append(gap)
                    print(f"   📍 Timestamp gap method: {gap} bytes (appears {count} times)")
    
    # Method 2: Look for regular patterns in numeric data positions
    if numeric_data:
        num_positions = sorted([pos for (_, _, pos) in numeric_data])
        if len(num_positions) > 10:
            # Look for the most common interval between numeric values
            intervals = []
            for i in range(min(1000, len(num_positions)-1)):
                interval = num_positions[i+1] - num_positions[i]
                if 8 <= interval <= 200:  # Reasonable range
                    intervals.append(interval)
            
            if intervals:
                interval_counts = Counter(intervals)
                for interval, count in interval_counts.most_common(3):
                    if count >= 5:  # Must appear at least 5 times
                        candidate_sizes.append(interval)
                        print(f"   📍 Numeric pattern method: {interval} bytes (appears {count} times)")
    
    # Method 3: Try common sizes and see which one aligns best with data
    all_candidates = list(set(common_sizes + candidate_sizes))
    best_size = None
    best_alignment = 0
    
    for size in all_candidates:
        alignment_score = calculate_alignment_score(raw_data, size, timestamps, numeric_data)
        if alignment_score > best_alignment:
            best_alignment = alignment_score
            best_size = size
        if alignment_score > 0.1:
            print(f"   📊 Size {size} bytes: alignment score {alignment_score:.3f}")
    
    if best_size and best_alignment > 0.1:
        print(f"   ✅ Best record size: {best_size} bytes (score: {best_alignment:.3f})")
        return best_size
    
    return None

def calculate_alignment_score(raw_data, record_size, timestamps, numeric_data):
    """Calculate how well a record size aligns with the data"""
    if record_size <= 0 or record_size > len(raw_data):
        return 0
    
    score = 0
    total_items = len(timestamps) + len(numeric_data)
    
    if total_items == 0:
        return 0
    
    # Check if timestamps align with record boundaries
    for _, _, pos in timestamps:
        if pos % record_size == 0:
            score += 2  # Strong alignment for timestamps
    
    # Check if numeric data aligns
    for _, _, pos in numeric_data:
        if pos % record_size in [8, 16, 24]:  # Common offsets for numeric fields
            score += 1
    
    return score / total_items

def extract_timestamps(raw_data: bytes, file_date: Optional[datetime] = None) -> List[Tuple]:
    """Extract timestamps using multiple strategies, validated against file date"""
    timestamps = []
    
    # Calculate valid timestamp range based on file date
    if file_date:
        file_ts_ms = int(file_date.timestamp() * 1000)
        min_ts_ms = file_ts_ms - 86400000  # 1 day before
        max_ts_ms = file_ts_ms + 86400000  # 1 day after
        min_ts_s = file_ts_ms // 1000 - 86400
        max_ts_s = file_ts_ms // 1000 + 86400
    else:
        # Fallback range: Nov 2024 to Nov 2025
        min_ts_ms = 1730000000000
        max_ts_ms = 1900000000000
        min_ts_s = 1730000000
        max_ts_s = 1900000000
    
    # Strategy 1: Look for 8-byte integers (Unix timestamps in milliseconds)
    for i in range(0, len(raw_data) - 8, 4):
        try:
            ts = struct.unpack('<Q', raw_data[i:i+8])[0]
            if min_ts_ms <= ts <= max_ts_ms:
                timestamps.append(('milliseconds', ts, i))
        except:
            pass
    
    # Strategy 2: Look for 4-byte integers (Unix timestamps in seconds)
    for i in range(0, len(raw_data) - 4, 4):
        try:
            ts = struct.unpack('<I', raw_data[i:i+4])[0]
            if min_ts_s <= ts <= max_ts_s:
                timestamps.append(('seconds', ts, i))
        except:
            pass
    
    return timestamps

def extract_numeric_patterns(raw_data: bytes) -> List[Tuple]:
    """Extract prices, quantities, volumes"""
    numeric_data = []
    
    # Look for double precision floats (prices)
    for i in range(0, len(raw_data) - 8, 8):
        try:
            value = struct.unpack('<d', raw_data[i:i+8])[0]
            if _is_reasonable_value(value):
                numeric_data.append(('double', value, i))
        except:
            pass
    
    # Look for single precision floats
    for i in range(0, len(raw_data) - 4, 4):
        try:
            value = struct.unpack('<f', raw_data[i:i+4])[0]
            if _is_reasonable_value(value):
                numeric_data.append(('float', value, i))
        except:
            pass
    
    # Look for integers (quantities, volumes)
    for i in range(0, len(raw_data) - 4, 4):
        try:
            value = struct.unpack('<i', raw_data[i:i+4])[0]
            if 0 < value < 10000000:  # Reasonable for quantities
                numeric_data.append(('integer', value, i))
        except:
            pass
    
    return numeric_data

def _is_reasonable_value(value) -> bool:
    """Filter out unreasonable numeric values"""
    if isinstance(value, float):
        return not (np.isnan(value) or np.isinf(value)) and 0 < abs(value) < 1e10
    return True

def extract_structured_records(raw_data: bytes, record_size: int, file_date: Optional[datetime] = None) -> List[Dict]:
    """Extract records using detected record size"""
    records = []
    
    # Try to find footer position (PAR1 marker)
    footer_pos = None
    for i in range(len(raw_data) - 100, max(0, len(raw_data) - 50000), -1):
        if raw_data[i:i+4] == b'PAR1':
            footer_pos = i
            break
    
    # Remove footer if found, otherwise use full data
    if footer_pos:
        data_without_footer = raw_data[:footer_pos]
        print(f"   📍 Found footer at position {footer_pos}, using {len(data_without_footer):,} bytes")
    else:
        data_without_footer = raw_data
    
    # Extract records with detected size
    for i in range(0, len(data_without_footer) - record_size, record_size):
        record = data_without_footer[i:i + record_size]
        
        try:
            # Try to parse as a structured tick record
            tick = parse_tick_record(record, record_size, file_date)
            if tick and is_valid_tick(tick, file_date):
                tick['byte_offset'] = i
                records.append(tick)
        except:
            continue
    
    return records

def parse_tick_record(record: bytes, record_size: int, file_date: Optional[datetime] = None) -> Optional[Dict]:
    """Parse a single tick record with flexible structure detection"""
    
    # Common tick data structures based on binary packet sizes: 184, 44, 32, 8 bytes
    # Try multiple structures in order of likelihood
    structures = [
        # Structure 1: 32 bytes - Price first (no timestamp at start) - MOST COMMON IN CORRUPTED FILES
        {'price': (8, 8, 'd'), 'quantity': (16, 4, 'I'), 'volume': (20, 4, 'I'), 'instrument_token': (24, 4, 'I')},
        
        # Structure 2: 32 bytes - LTP packet (timestamp + price + quantity + volume)
        {'timestamp': (0, 8, 'Q'), 'price': (8, 8, 'd'), 'quantity': (16, 4, 'I'), 'volume': (20, 4, 'I'), 'instrument_token': (24, 4, 'I')},
        
        # Structure 3: 32 bytes - Simple (price + quantity + volume, no timestamp)
        {'price': (0, 8, 'd'), 'quantity': (8, 4, 'I'), 'volume': (12, 4, 'I'), 'instrument_token': (16, 4, 'I')},
        
        # Structure 4: 44 bytes - Quote packet (timestamp + bid/ask + quantities)
        {'timestamp': (0, 8, 'Q'), 'bid_price': (8, 8, 'd'), 'ask_price': (16, 8, 'd'), 'bid_quantity': (24, 4, 'I'), 'ask_quantity': (28, 4, 'I'), 'instrument_token': (32, 4, 'I')},
        
        # Structure 5: 64 bytes - Extended tick data
        {'timestamp': (0, 8, 'Q'), 'price': (8, 8, 'd'), 'quantity': (16, 4, 'I'), 'volume': (20, 8, 'Q'), 'instrument_token': (28, 4, 'I'), 'high': (32, 8, 'd'), 'low': (40, 8, 'd'), 'open': (48, 8, 'd'), 'close': (56, 8, 'd')},
        
        # Structure 6: 184 bytes - Full depth packet (timestamp + price + depth levels)
        {'timestamp': (0, 8, 'Q'), 'price': (8, 8, 'd'), 'quantity': (16, 4, 'I'), 'instrument_token': (20, 4, 'I'), 'bid_1_price': (24, 8, 'd'), 'bid_1_quantity': (32, 4, 'I')},
        
        # Structure 7: Simple 32 bytes - timestamp(8) + price(8) + quantity(8) + volume(8)
        {'timestamp': (0, 8, 'Q'), 'price': (8, 8, 'd'), 'quantity': (16, 8, 'Q'), 'volume': (24, 8, 'Q')},
        
        # Structure 8: Alternative ordering - price first, timestamp later
        {'price': (0, 8, 'd'), 'quantity': (8, 4, 'I'), 'timestamp': (12, 8, 'Q'), 'volume': (20, 4, 'I')},
    ]
    
    # Filter structures that fit the record size
    valid_structures = [s for s in structures if all(offset + size <= len(record) for offset, size, _ in s.values())]
    
    for structure in valid_structures:
        try:
            tick_data = {}
            valid_fields = 0
            
            for field, (offset, size, fmt) in structure.items():
                if offset + size <= len(record):
                    value = struct.unpack('<' + fmt, record[offset:offset+size])[0]
                    
                    # Validate field values
                    if field == 'timestamp':
                        # Validate timestamp range
                        if file_date:
                            file_ts_ms = int(file_date.timestamp() * 1000)
                            if abs(value - file_ts_ms) < 86400000:  # Within 1 day
                                tick_data[field] = datetime.fromtimestamp(value / 1000.0)
                                valid_fields += 1
                        elif 1730000000000 <= value <= 1900000000000:  # Nov 2024 - Nov 2025
                            tick_data[field] = datetime.fromtimestamp(value / 1000.0)
                            valid_fields += 1
                    elif field in ('price', 'bid_price', 'ask_price', 'high', 'low', 'open', 'close'):
                        if 0.001 < value < 1000000 and not np.isnan(value):
                            tick_data[field] = value
                            valid_fields += 1
                    elif field in ('quantity', 'volume', 'bid_quantity', 'ask_quantity', 'bid_1_quantity'):
                        if 0 < value < 100000000:
                            tick_data[field] = int(value)
                            valid_fields += 1
                    elif field == 'instrument_token':
                        if 1000 < value < 1e9:
                            tick_data[field] = int(value)
                            valid_fields += 1
            
            # If we found at least 2 valid fields (preferably timestamp + price), use this structure
            if valid_fields >= 2 and ('timestamp' in tick_data or 'price' in tick_data):
                # If no timestamp found but we have file_date, use it as fallback
                if 'timestamp' not in tick_data and file_date:
                    tick_data['timestamp'] = file_date
                return tick_data
                
        except:
            continue
    
    return None

def is_valid_tick(tick: Dict, file_date: Optional[datetime] = None) -> bool:
    """Validate that extracted data looks like real tick data"""
    required_fields = 0
    
    if 'timestamp' in tick:
        # Check if timestamp is reasonable (around file date)
        if file_date:
            if abs((tick['timestamp'] - file_date).total_seconds()) < 86400:  # Within 1 day
                required_fields += 1
        else:
            # No file date, just check it's not too far in past/future
            now = datetime.now()
            if abs((tick['timestamp'] - now).days) < 365:
                required_fields += 1
    
    if 'price' in tick and 0.001 < tick['price'] < 100000:
        required_fields += 1
    elif 'bid_price' in tick and 0.001 < tick['bid_price'] < 100000:
        required_fields += 1
    
    if 'quantity' in tick and tick['quantity'] > 0:
        required_fields += 1
    elif 'volume' in tick and tick['volume'] > 0:
        required_fields += 1
    
    return required_fields >= 2

def heuristic_record_grouping(timestamps, numeric_data, file_date):
    """Fallback method when record size detection fails"""
    records = []
    
    # Group data points that are close together (likely same record)
    all_data = sorted(timestamps + numeric_data, key=lambda x: x[2])  # Sort by position
    
    i = 0
    while i < len(all_data):
        current_batch = [all_data[i]]
        current_pos = all_data[i][2]
        
        # Group items within 64 bytes (reasonable record size)
        j = i + 1
        while j < len(all_data) and all_data[j][2] - current_pos < 64:
            current_batch.append(all_data[j])
            j += 1
        
        # Try to create a record from this batch
        record = create_record_from_batch(current_batch, file_date)
        if record:
            records.append(record)
        
        i = j
    
    return records

def create_record_from_batch(data_batch: List[Tuple], file_date: Optional[datetime] = None) -> Optional[Dict]:
    """Create a record from grouped data points"""
    record = {}
    
    for data_type, value, pos in data_batch:
        if data_type == 'milliseconds':
            try:
                ts = datetime.fromtimestamp(value / 1000.0)
                # Validate against file date
                if file_date:
                    if abs((ts - file_date).total_seconds()) < 86400:
                        record['timestamp'] = ts
                else:
                    record['timestamp'] = ts
            except:
                pass
        elif data_type == 'seconds':
            try:
                ts = datetime.fromtimestamp(value)
                if file_date:
                    if abs((ts - file_date).total_seconds()) < 86400:
                        record['timestamp'] = ts
                else:
                    record['timestamp'] = ts
            except:
                pass
        elif data_type == 'double' and 0.001 < value < 100000:
            if 'price' not in record:
                record['price'] = value
            elif 'bid_price' not in record:
                record['bid_price'] = value
            else:
                record['ask_price'] = value
        elif data_type == 'float' and 0.001 < value < 100000:
            if 'price' not in record:
                record['price'] = value
        elif data_type == 'integer' and value > 0:
            if 'quantity' not in record:
                record['quantity'] = value
            elif 'volume' not in record:
                record['volume'] = value
            elif 'instrument_token' not in record and 1000 < value < 1e9:
                record['instrument_token'] = value
    
    # Validate we have at least a timestamp or price
    if 'timestamp' in record or 'price' in record:
        return record
    
    return None

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python brute_force_tick_extraction.py <parquet_file> [output_file]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        df = improved_brute_force_tick_extraction(input_file)
        
        if df.empty:
            print(f"\n❌ No data extracted from {Path(input_file).name}")
            sys.exit(1)
        
        if output_file:
            df.to_parquet(output_file, index=False)
            print(f"\n💾 Saved to: {output_file}")
        else:
            csv_file = Path(input_file).with_suffix('.extracted.csv')
            df.to_csv(csv_file, index=False)
            print(f"\n💾 Saved to: {csv_file}")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()