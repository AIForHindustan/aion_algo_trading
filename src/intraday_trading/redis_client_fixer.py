"""
Script to find and fix all direct redis.Redis() calls
"""
import os
import re
from pathlib import Path

def find_direct_redis_calls(root_dir):
    """Find all files using redis.Redis() directly"""
    pattern = r'redis\.Redis\('
    violations = []
    
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.py'):
                path = Path(root) / file
                with open(path, 'r') as f:
                    content = f.read()
                    if re.search(pattern, content):
                        violations.append(str(path))
    
    return violations

def fix_file(file_path):
    """Replace redis.Redis() with get_redis_client()"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Replace imports
    if 'import redis' in content and 'from shared_core.redis_clients.redis_client import get_redis_client' not in content:
        content = content.replace('import redis', 'import redis\nfrom shared_core.redis_clients.redis_client import get_redis_client')
    
    # Replace calls
    content = re.sub(
        r'redis\.Redis\(([^)]*)\)',
        r'get_redis_client(\1)',
        content
    )
    
    with open(file_path, 'w') as f:
        f.write(content)
    
    print(f"✅ Fixed: {file_path}")

# Run fix
root = Path("intraday_trading")
violations = find_direct_redis_calls(root)
print(f"Found {len(violations)} violations:")
for v in violations:
    print(f"  - {v}")
    fix_file(v)