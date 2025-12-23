# shared_core/instrument_token/optimized_loaders.py
class FocusZerodhaAdapter(ZerodhaAdapter):
    """Optimized Zerodha adapter for intraday focus"""
    
    def __init__(self, csv_path: str, consolidated_json_path: Optional[str] = None, 
                 focus_symbols: Optional[Set[str]] = None):
        super().__init__(csv_path, consolidated_json_path)
        self.focus_symbols = focus_symbols or set()
    
    def load_instruments(self) -> List[BrokerInstrument]:
        """Load only focus instruments"""
        instruments = []
        
        # Load from CSV with filtering
        if self.csv_path.exists():
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if self._should_include(row):
                        try:
                            instrument = self._create_instrument_from_row(row)
                            instruments.append(instrument)
                        except Exception as e:
                            self.logger.debug(f"Skipping row: {e}")
                            continue
        
        return instruments
    
    def _should_include(self, row: Dict) -> bool:
        """Check if instrument should be included in focus registry"""
        symbol = row.get('tradingsymbol', '')
        
        # Skip expired instruments
        expiry = row.get('expiry')
        if expiry and expiry != 'NULL' and self._is_expired(expiry):
            return False
        
        # Check if symbol is in focus set
        clean_symbol = self._clean_symbol(symbol)
        return clean_symbol in self.focus_symbols

class FocusAngelOneAdapter(AngelOneAdapter):
    """Optimized Angel One adapter for intraday focus"""
    
    def __init__(self, json_path: str, focus_symbols: Optional[Set[str]] = None):
        super().__init__(json_path)
        self.focus_symbols = focus_symbols or set()
    
    def load_instruments(self) -> List[BrokerInstrument]:
        """Load only focus instruments"""
        instruments = []
        
        if self.json_path.exists():
            with open(self.json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            for item in data:
                if self._should_include(item):
                    try:
                        instrument = self._create_instrument_from_item(item)
                        instruments.append(instrument)
                    except Exception as e:
                        self.logger.debug(f"Skipping item: {e}")
                        continue
        
        return instruments
    
    def _should_include(self, item: Dict) -> bool:
        """Check if instrument should be included"""
        symbol = item.get('symbol', '')
        
        # Skip if no symbol
        if not symbol:
            return False
        
        # Skip expired options
        expiry = item.get('expiry')
        if expiry and self._is_expired(expiry):
            return False
        
        # Check focus symbols
        clean_symbol = self._clean_angel_symbol(symbol)
        return clean_symbol in self.focus_symbols