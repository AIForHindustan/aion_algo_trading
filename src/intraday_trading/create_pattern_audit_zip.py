#!/opt/homebrew/bin/python3.13
"""
Create ZIP file of all enabled pattern scripts and their dependencies
for audit purposes.
"""

import json
import os
import zipfile
from pathlib import Path
from typing import Set, List

# Project root
PROJECT_ROOT = Path(__file__).parent
PATTERNS_DIR = PROJECT_ROOT / "patterns"

def load_pattern_registry() -> dict:
    """Load pattern registry configuration."""
    config_path = PATTERNS_DIR / "data" / "pattern_registry_config.json"
    with open(config_path, 'r') as f:
        return json.load(f)

def get_enabled_patterns(config: dict) -> List[str]:
    """Extract all enabled patterns from config."""
    enabled = []
    pattern_configs = config.get("pattern_configs", {})
    for pattern_name, pattern_config in pattern_configs.items():
        if pattern_config.get("enabled", False):
            enabled.append(pattern_name)
    return enabled

def get_pattern_files(pattern_name: str) -> Set[Path]:
    """Get pattern implementation files for a given pattern name."""
    files = set()
    
    # Pattern name to file mapping
    pattern_file_map = {
        "volume_spike": ["pattern_detector.py"],
        "volume_breakout": ["pattern_detector.py"],
        "volume_price_divergence": ["pattern_detector.py"],
        "momentum_continuation": ["pattern_detector.py"],
        "breakout_pattern": ["pattern_detector.py"],
        "reversal_pattern": ["pattern_detector.py"],
        "spring_pattern": ["pattern_detector.py"],
        "coil_pattern": ["pattern_detector.py"],
        "hidden_accumulation": ["pattern_detector.py"],
        "kow_signal_straddle": ["kow_signal_straddle.py"],
        "ict_iron_condor": ["ict/ict_iron_condor.py"],
    }
    
    if pattern_name in pattern_file_map:
        for filename in pattern_file_map[pattern_name]:
            filepath = PATTERNS_DIR / filename
            if filepath.exists():
                files.add(filepath)
    
    return files

def get_helper_files() -> Set[Path]:
    """Get all helper files that patterns depend on."""
    helpers = set()
    
    # Core helper files
    core_helpers = [
        "base_detector.py",
        "pattern_mathematics.py",
        "volume_thresholds.py",
        "__init__.py",
    ]
    
    for helper in core_helpers:
        filepath = PATTERNS_DIR / helper
        if filepath.exists():
            helpers.add(filepath)
    
    # Utils directory
    utils_dir = PATTERNS_DIR / "utils"
    if utils_dir.exists():
        for filepath in utils_dir.glob("*.py"):
            if filepath.name != "__pycache__":
                helpers.add(filepath)
    
    # ICT directory (for ict_iron_condor)
    ict_dir = PATTERNS_DIR / "ict"
    if ict_dir.exists():
        ict_helpers = [
            "__init__.py",
            "killzone.py",
            "fvg.py",
            "liquidity.py",
            "ote.py",
            "premium_discount.py",
            "pattern_detector.py",
        ]
        for helper in ict_helpers:
            filepath = ict_dir / helper
            if filepath.exists():
                helpers.add(filepath)
    
    # Pattern registry config
    config_file = PATTERNS_DIR / "data" / "pattern_registry_config.json"
    if config_file.exists():
        helpers.add(config_file)
    
    return helpers

def collect_all_files(enabled_patterns: List[str]) -> Set[Path]:
    """Collect all pattern files and helper files."""
    all_files = set()
    
    # Get pattern files
    for pattern_name in enabled_patterns:
        pattern_files = get_pattern_files(pattern_name)
        all_files.update(pattern_files)
    
    # Get helper files
    helper_files = get_helper_files()
    all_files.update(helper_files)
    
    return all_files

def create_zip_file(files: Set[Path], output_path: Path):
    """Create ZIP file with all collected files."""
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for filepath in sorted(files):
            # Calculate relative path from patterns directory
            try:
                arcname = filepath.relative_to(PROJECT_ROOT)
            except ValueError:
                # If file is outside project root, use filename only
                arcname = filepath.name
            
            zipf.write(filepath, arcname)
            print(f"Added: {arcname}")

def main():
    """Main function to create audit ZIP file."""
    print("=" * 60)
    print("Pattern Audit ZIP Creator")
    print("=" * 60)
    
    # Load config
    print("\n1. Loading pattern registry config...")
    config = load_pattern_registry()
    
    # Get enabled patterns
    print("\n2. Identifying enabled patterns...")
    enabled_patterns = get_enabled_patterns(config)
    print(f"   Found {len(enabled_patterns)} enabled patterns:")
    for pattern in enabled_patterns:
        print(f"   - {pattern}")
    
    # Collect all files
    print("\n3. Collecting pattern files and dependencies...")
    all_files = collect_all_files(enabled_patterns)
    print(f"   Collected {len(all_files)} files")
    
    # Create ZIP
    print("\n4. Creating ZIP file...")
    output_path = PROJECT_ROOT / "pattern_scripts_audit.zip"
    create_zip_file(all_files, output_path)
    
    print(f"\n✅ ZIP file created: {output_path}")
    print(f"   Total files: {len(all_files)}")
    print(f"   File size: {output_path.stat().st_size / 1024:.2f} KB")

if __name__ == "__main__":
    main()

