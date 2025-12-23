#!/opt/homebrew/bin/python3.13
"""
PARQUET TO BINARY CONVERTER
Converts parquet files back to original Zerodha WebSocket binary format

Binary Format Structure:
- Header: 2 bytes (number of packets, big-endian)
- For each packet:
  - 2 bytes: packet length (big-endian)
  - N bytes: packet data (184, 44, 32, or 8 bytes)
"""

import sys
import struct
from pathlib import Path
from typing import List, Optional
import polars as pl
import pandas as pd

def reconstruct_binary_packet(row: dict, packet_type: str = "auto") -> Optional[bytes]:
    """
    Reconstruct a binary packet from a parquet row.
    
    Packet types:
    - 184 bytes: Full quote packet (market depth)
    - 44 bytes: Quote packet (bid/ask)
    - 32 bytes: Index/LTP packet
    - 8 bytes: LTP packet (minimal)
    """
    try:
        # Auto-detect packet type based on available fields
        if packet_type == "auto":
            has_depth = any(f'bid_{i}_price' in row for i in range(1, 6))
            has_ohlc = all(f in row for f in ['open_price', 'high_price', 'low_price', 'close_price'])
            
            if has_depth:
                packet_type = "184"  # Full quote with depth
            elif 'bid_1_price' in row or 'ask_1_price' in row:
                packet_type = "44"  # Quote packet
            elif 'last_price' in row and 'volume' in row:
                packet_type = "32"  # Index/LTP packet
            else:
                packet_type = "8"  # Minimal LTP
        
        # Reconstruct based on packet type
        if packet_type == "184":
            return _reconstruct_full_quote_packet(row)
        elif packet_type == "44":
            return _reconstruct_quote_packet(row)
        elif packet_type == "32":
            return _reconstruct_index_packet(row)
        elif packet_type == "8":
            return _reconstruct_ltp_packet(row)
        else:
            return None
            
    except Exception as e:
        print(f"Error reconstructing packet: {e}")
        return None

def _reconstruct_full_quote_packet(row: dict) -> bytes:
    """Reconstruct 184-byte full quote packet"""
    packet = bytearray(184)
    
    # Instrument token (4 bytes, big-endian, signed)
    instrument_token = int(row.get('instrument_token', 0))
    packet[0:4] = struct.pack(">i", instrument_token)
    
    # Fields (4 bytes each, big-endian, unsigned)
    # Position 4-64: Various fields (15 fields = 60 bytes)
    # Note: Prices are stored in paise (multiply by 100) or in base units
    fields = [
        int(row.get('last_price', 0) * 100) if row.get('last_price') else 0,  # Price in paise
        int(row.get('last_traded_quantity', row.get('quantity', 0))),
        int(row.get('average_traded_price', row.get('average_price', 0)) * 100) if row.get('average_traded_price') or row.get('average_price') else 0,
        int(row.get('volume', 0)),
        int(row.get('total_buy_quantity', row.get('buy_quantity', 0))),
        int(row.get('total_sell_quantity', row.get('sell_quantity', 0))),
        int(row.get('open_price', 0) * 100) if row.get('open_price') else 0,
        int(row.get('high_price', 0) * 100) if row.get('high_price') else 0,
        int(row.get('low_price', 0) * 100) if row.get('low_price') else 0,
        int(row.get('close_price', 0) * 100) if row.get('close_price') else 0,
        int(row.get('last_traded_timestamp_ns', 0) / 1e6) if row.get('last_traded_timestamp_ns') else 0,  # Convert ns to ms
        int(row.get('open_interest', 0)),
        int(row.get('oi_day_high', 0)),
        int(row.get('oi_day_low', 0)),
        int(row.get('exchange_timestamp_ns', 0) / 1e6) if row.get('exchange_timestamp_ns') else 0,  # Convert ns to ms
    ]
    
    # Pack fields (each 4 bytes, unsigned int, big-endian)
    for i, value in enumerate(fields):
        offset = 4 + (i * 4)
        if offset + 4 <= 64:
            packet[offset:offset+4] = struct.pack(">I", max(0, int(value)))
    
    # Market depth (10 entries, 12 bytes each: quantity(4) + price(4) + orders(2))
    depth_start = 64
    for i in range(1, 6):  # Bid side (1-5)
        bid_price = int(row.get(f'bid_{i}_price', 0) * 100)
        bid_quantity = int(row.get(f'bid_{i}_quantity', 0))
        bid_orders = int(row.get(f'bid_{i}_orders', 0))
        offset = depth_start + (i - 1) * 12
        packet[offset:offset+4] = struct.pack(">I", bid_quantity)
        packet[offset+4:offset+8] = struct.pack(">I", bid_price)
        packet[offset+8:offset+10] = struct.pack(">H", bid_orders)
    
    for i in range(1, 6):  # Ask side (1-5)
        ask_price = int(row.get(f'ask_{i}_price', 0) * 100)
        ask_quantity = int(row.get(f'ask_{i}_quantity', 0))
        ask_orders = int(row.get(f'ask_{i}_orders', 0))
        offset = depth_start + (i + 4) * 12
        packet[offset:offset+4] = struct.pack(">I", ask_quantity)
        packet[offset+4:offset+8] = struct.pack(">I", ask_price)
        packet[offset+8:offset+10] = struct.pack(">H", ask_orders)
    
    return bytes(packet)

def _reconstruct_quote_packet(row: dict) -> bytes:
    """Reconstruct 44-byte quote packet"""
    packet = bytearray(44)
    
    # Instrument token (4 bytes)
    instrument_token = int(row.get('instrument_token', 0))
    packet[0:4] = struct.pack(">i", instrument_token)
    
    # Best bid/ask prices and quantities
    bid_price = int(row.get('bid_1_price', row.get('last_price', 0)) * 100)
    ask_price = int(row.get('ask_1_price', row.get('last_price', 0)) * 100)
    bid_quantity = int(row.get('bid_1_quantity', 0))
    ask_quantity = int(row.get('ask_1_quantity', 0))
    
    packet[4:8] = struct.pack(">I", bid_price)
    packet[8:12] = struct.pack(">I", ask_price)
    packet[12:16] = struct.pack(">I", bid_quantity)
    packet[16:20] = struct.pack(">I", ask_quantity)
    
    # Timestamp
    timestamp_ms = int(row.get('exchange_timestamp_ns', 0) / 1e6)
    packet[20:28] = struct.pack(">Q", timestamp_ms)
    
    return bytes(packet)

def _reconstruct_index_packet(row: dict) -> bytes:
    """Reconstruct 32-byte index/LTP packet"""
    packet = bytearray(32)
    
    # Instrument token (4 bytes)
    instrument_token = int(row.get('instrument_token', 0))
    packet[0:4] = struct.pack(">i", instrument_token)
    
    # Last price (8 bytes, double)
    last_price = float(row.get('last_price', 0))
    packet[4:12] = struct.pack(">d", last_price)
    
    # Quantity (4 bytes)
    quantity = int(row.get('last_traded_quantity', row.get('quantity', 0)))
    packet[12:16] = struct.pack(">I", quantity)
    
    # Volume (4 bytes)
    volume = int(row.get('volume', 0))
    packet[16:20] = struct.pack(">I", volume)
    
    # Timestamp (8 bytes)
    timestamp_ms = int(row.get('exchange_timestamp_ns', 0) / 1e6)
    if timestamp_ms == 0:
        timestamp_ms = int(row.get('timestamp', 0) * 1000) if 'timestamp' in row else 0
    packet[20:28] = struct.pack(">Q", timestamp_ms)
    
    return bytes(packet)

def _reconstruct_ltp_packet(row: dict) -> bytes:
    """Reconstruct 8-byte LTP packet (minimal)"""
    packet = bytearray(8)
    
    # Instrument token (4 bytes)
    instrument_token = int(row.get('instrument_token', 0))
    packet[0:4] = struct.pack(">i", instrument_token)
    
    # Last price (4 bytes, in paise)
    last_price = int(float(row.get('last_price', 0)) * 100)
    packet[4:8] = struct.pack(">I", last_price)
    
    return bytes(packet)

def convert_parquet_to_binary(parquet_path: Path, output_path: Optional[Path] = None, 
                              packet_type: str = "auto") -> bool:
    """
    Convert a parquet file back to binary format.
    
    Args:
        parquet_path: Path to input parquet file
        output_path: Path to output binary file (default: same name with .bin extension)
        packet_type: Packet type ("184", "44", "32", "8", or "auto")
    """
    try:
        # Read parquet file
        print(f"📖 Reading parquet file: {parquet_path}")
        df = pl.read_parquet(parquet_path)
        
        if df.is_empty():
            print("❌ Parquet file is empty")
            return False
        
        print(f"📊 Found {len(df)} rows")
        
        # Convert to pandas for easier row iteration
        df_pandas = df.to_pandas()
        
        # Reconstruct binary packets
        packets: List[bytes] = []
        failed = 0
        
        for idx, row in df_pandas.iterrows():
            packet = reconstruct_binary_packet(row.to_dict(), packet_type)
            if packet:
                packets.append(packet)
            else:
                failed += 1
                if failed <= 5:  # Show first 5 failures
                    print(f"⚠️ Failed to reconstruct packet for row {idx}")
        
        if not packets:
            print("❌ No packets could be reconstructed")
            return False
        
        print(f"✅ Reconstructed {len(packets)} packets ({failed} failed)")
        
        # Write binary file in Zerodha format
        if output_path is None:
            output_path = parquet_path.with_suffix('.bin')
        
        print(f"💾 Writing binary file: {output_path}")
        
        with open(output_path, 'wb') as f:
            # Write header: number of packets (2 bytes, big-endian)
            f.write(struct.pack(">H", len(packets)))
            
            # Write each packet: length (2 bytes) + data
            for packet in packets:
                packet_length = len(packet)
                f.write(struct.pack(">H", packet_length))
                f.write(packet)
        
        print(f"✅ Successfully wrote {len(packets)} packets to {output_path}")
        print(f"📊 File size: {output_path.stat().st_size:,} bytes")
        
        return True
        
    except Exception as e:
        print(f"❌ Error converting parquet to binary: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python parquet_to_binary.py <parquet_file> [output_file] [packet_type]")
        print("\nPacket types:")
        print("  auto - Auto-detect (default)")
        print("  184  - Full quote packet (184 bytes)")
        print("  44   - Quote packet (44 bytes)")
        print("  32   - Index/LTP packet (32 bytes)")
        print("  8    - Minimal LTP packet (8 bytes)")
        sys.exit(1)
    
    parquet_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    packet_type = sys.argv[3] if len(sys.argv) > 3 else "auto"
    
    if not parquet_path.exists():
        print(f"❌ File not found: {parquet_path}")
        sys.exit(1)
    
    success = convert_parquet_to_binary(parquet_path, output_path, packet_type)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()

