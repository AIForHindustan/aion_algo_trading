#!/usr/bin/env python3
"""
Remove security shim files after verifying no other imports depend on them
"""
from pathlib import Path
import shutil

def check_security_module_usage():
    """Check if any other files still import from the legacy security module."""
    legacy_imports = []
    
    for py_file in Path(".").rglob("*.py"):
        if py_file.name in ['remove_legacy.py', 'targeted_audit.py']:
            continue
        with open(py_file, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')
            
        for i, line in enumerate(lines, 1):
            if ('import security' in line or 'from security import' in line) and 'aion_trading_dashboard.security' not in line:
                legacy_imports.append({
                    'file': str(py_file),
                    'line': i,
                    'content': line.strip()
                })
    
    return legacy_imports

def backup_and_remove_shim():
    """Backup and remove the security shim directory."""
    security_dir = Path("security")
    
    if not security_dir.exists():
        print("ℹ️  Security directory not found")
        return False
    
    # Check what's in the security directory
    print("Security directory contents:")
    for item in security_dir.iterdir():
        print(f"  - {item.name}")
    
    # Create backup
    backup_dir = Path(".shim_backup")
    backup_dir.mkdir(exist_ok=True)
    
    shutil.copytree(security_dir, backup_dir / "security", dirs_exist_ok=True)
    print(f"✓ Backup created at: {backup_dir / 'security'}")
    
    # Verify secure_storage.py uses correct import
    secure_storage = Path("backend/broker/secure_storage.py")
    if secure_storage.exists():
        with open(secure_storage, 'r') as f:
            content = f.read()
            if 'from aion_trading_dashboard.security.credential_manager import get_credential_manager' in content:
                print("✓ secure_storage.py uses correct import")
                
                # Remove the shim
                shutil.rmtree(security_dir)
                print("✓ Removed security shim directory")
                
                # Test import still works
                test_import()
                return True
            else:
                print("❌ secure_storage.py still uses legacy import")
                return False
    
    return False

def test_import():
    """Test that imports still work after removing shim."""
    test_script = '''
import sys
sys.path.insert(0, '.')

try:
    from aion_trading_dashboard.security.credential_manager import get_credential_manager
    print("✅ Import from aion_trading_dashboard.security works")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

try:
    # This should fail now
    import security
    print("❌ Legacy security import still works (unexpected)")
except ImportError:
    print("✅ Legacy security import correctly fails")
'''
    
    import subprocess
    result = subprocess.run(['python', '-c', test_script], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    return result.returncode == 0

def main():
    """Main removal procedure."""
    print("="*60)
    print("SECURITY SHIM REMOVAL")
    print("="*60)
    
    # Check for other legacy imports
    print("\nChecking for other security module usage...")
    legacy_imports = check_security_module_usage()
    
    if legacy_imports:
        print(f"❌ Found {len(legacy_imports)} files still using legacy security imports:")
        for imp in legacy_imports:
            print(f"  {imp['file']}:{imp['line']} - {imp['content']}")
        print("\n⚠️  Cannot remove shim until these are fixed.")
        return False
    
    print("✓ No other files use legacy security imports")
    
    # Get confirmation
    response = input("\nProceed with removing security shim? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("Removal cancelled.")
        return False
    
    # Remove shim
    if backup_and_remove_shim():
        print("\n" + "="*60)
        print("✅ SECURITY SHIM SUCCESSFULLY REMOVED")
        print("="*60)
        return True
    else:
        print("\n❌ Shim removal failed")
        return False

if __name__ == "__main__":
    main()