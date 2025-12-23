#!/bin/bash
# fix_redis_markers.sh
echo "Fixing redis_files markers..."

# Create backup
timestamp=$(date +%Y%m%d_%H%M%S)
backup_dir=".backup_${timestamp}"
mkdir -p "$backup_dir"

# Fix sitecustomize.py files
# Adjusted to find from parent directory
find .. -name "sitecustomize.py" -type f | grep -v ".venv" | grep -v "site-packages" | while read file; do
    # Backup
    # Flatten path for backup name
    backup_name=$(echo $file | sed 's/\.\.\///' | sed 's|/|_|g')
    cp "$file" "$backup_dir/$backup_name"
    
    # Remove redis_files from markers
    # Using python to do the replacement to handle potential differences in sed across platforms (BSD vs GNU)
    python3 -c "
import sys
try:
    with open('$file', 'r') as f:
        content = f.read()
    if 'markers = {"redis_files", "intraday_trading", "alerts"}' in content:
        new_content = content.replace('markers = {"redis_files", "intraday_trading", "alerts"}', 'markers = {"intraday_trading", "alerts"}')
        with open('$file', 'w') as f:
            f.write(new_content)
        print(f'✓ Updated $file')
    else:
        print(f'ℹ️  No change needed for $file')
except Exception as e:
    print(f'❌ Error updating $file: {e}')
"
done

# Fix optimized_main.py comment
# Adjusted path to match actual location
optimized_main_path="../aion_trading_dashboard/backend/optimized_main.py"
if [ -f "$optimized_main_path" ]; then
    cp "$optimized_main_path" "$backup_dir/backend_optimized_main.py"
    
    python3 -c "
import sys
file_path = '$optimized_main_path'
try:
    with open(file_path, 'r') as f:
        content = f.read()
    old_str = 'redis_files is now replaced by shared_core/redis_clients'
    new_str = 'shared_core/redis_clients is required (redis_files was deprecated)'
    if old_str in content:
        new_content = content.replace(old_str, new_str)
        with open(file_path, 'w') as f:
            f.write(new_content)
        print('✓ Updated backend/optimized_main.py comment')
    else:
        print('ℹ️  Comment already updated or not found in optimized_main.py')
except Exception as e:
    print(f'❌ Error updating optimized_main.py: {e}')
"
fi

echo "Backups saved to: $backup_dir"
echo "✅ Redis markers updated"
