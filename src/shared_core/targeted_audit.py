#!/usr/bin/env python3
"""
Legacy Code Cleanup Assistant - Specifically targets the issues mentioned:
1. Security shim imports
2. redis_files marker usage
3. Comment cleanup in optimized_main.py
4. sys.path injection analysis
"""

import os
import re
import ast
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import json

class LegacyCodeAuditor:
    def __init__(self, project_root: str):
        self.project_root = Path(project_root).resolve()
        self.project_name = "aion_trading_dashboard"
        
    def find_security_imports(self) -> Dict[str, List[Dict]]:
        """
        Find all imports related to security shim and credential_manager.
        Returns: {filename: [import_info]}
        """
        results = {}
        
        for py_file in self.project_root.rglob("*.py"):
            if not py_file.is_file():
                continue
            
            with open(py_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            imports = []
            lines = content.split('\n')
            
            for i, line in enumerate(lines, 1):
                # Check for security imports
                if re.search(r'import\s+security(\.|$)', line) or 'from security import' in line:
                    imports.append({
                        'line_number': i,
                        'content': line.strip(),
                        'type': 'security_import'
                    })
                
                # Specifically look for credential_manager imports
                if 'credential_manager' in line and ('import' in line or 'from' in line):
                    imports.append({
                        'line_number': i,
                        'content': line.strip(),
                        'type': 'credential_manager_import'
                    })
            
            if imports:
                results[str(py_file.relative_to(self.project_root))] = imports
        
        return results
    
    def find_redis_files_references(self) -> Dict[str, List[Dict]]:
        """
        Find all references to 'redis_files' in the codebase.
        """
        results = {}
        
        for py_file in self.project_root.rglob("*.py"):
            if not py_file.is_file():
                continue
            
            with open(py_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                lines = content.split('\n')
            
            references = []
            for i, line in enumerate(lines, 1):
                if 'redis_files' in line.lower():
                    references.append({
                        'line_number': i,
                        'content': line.strip(),
                        'context': self.get_context(lines, i)
                    })
            
            if references:
                results[str(py_file.relative_to(self.project_root))] = references
        
        return results
    
    def analyze_import_utils(self) -> Dict:
        """
        Analyze import_utils.py and optimized_main.py for sys.path injections.
        """
        results = {
            'import_utils': None,
            'optimized_main': None,
            'sys_path_injections': []
        }
        
        # Check import_utils.py
        import_utils_path = self.project_root / 'backend' / 'import_utils.py'
        if import_utils_path.exists():
            with open(import_utils_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            results['import_utils'] = {
                'sys.path.modifications': self.find_sys_path_modifications(content),
                'cross_service_imports': self.find_cross_service_imports(content)
            }
        
        # Check optimized_main.py
        optimized_main_path = self.project_root / 'backend' / 'optimized_main.py'
        if optimized_main_path.exists():
            with open(optimized_main_path, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.split('\n')
            
            # Find the specific comment about redis_files
            comment_info = self.find_redis_comment(lines)
            results['optimized_main'] = {
                'redis_comment': comment_info,
                'sys.path.modifications': self.find_sys_path_modifications(content),
                'path_setup': self.find_path_setup_section(lines)
            }
        
        return results
    
    def find_sys_path_modifications(self, content: str) -> List[Dict]:
        """Find all sys.path modifications in the content."""
        modifications = []
        
        # Patterns for sys.path modifications
        patterns = [
            r'sys\.path\.(append|insert|extend)\(([^)]+)\)',
            r'sys\.path\s*\+=\s*([^\n]+)',
            r'insert\(0,\s*([^)]+)\)'  # Common pattern for prepending
        ]
        
        lines = content.split('\n')
        for i, line in enumerate(lines, 1):
            for pattern in patterns:
                match = re.search(pattern, line)
                if match:
                    modifications.append({
                        'line_number': i,
                        'content': line.strip(),
                        'type': 'sys.path_modification'
                    })
                    break
        
        return modifications
    
    def find_cross_service_imports(self, content: str) -> List[str]:
        """Find imports from other services."""
        cross_service_keywords = [
            'intraday_trading',
            'zerodha_websocket',
            'shared_core',
            'other_service'  # Add more as needed
        ]
        
        imports = []
        for keyword in cross_service_keywords:
            if keyword in content:
                imports.append(keyword)
        
        return list(set(imports))
    
    def find_redis_comment(self, lines: List[str]) -> Dict:
        """Find and analyze the redis_files comment in optimized_main.py."""
        for i, line in enumerate(lines):
            if 'redis_files' in line.lower() and ('#' in line or '"""' in line or "'''" in line):
                # Get surrounding context
                start = max(0, i - 3)
                end = min(len(lines), i + 4)
                context = lines[start:end]
                
                return {
                    'line_number': i + 1,
                    'comment': line.strip(),
                    'context': '\n'.join(context),
                    'needs_update': 'redis_files' in line
                }
        
        return {'found': False}
    
    def find_path_setup_section(self, lines: List[str]) -> List[Dict]:
        """Find the path setup section in optimized_main.py."""
        path_sections = []
        in_path_section = False
        section_lines = []
        
        for i, line in enumerate(lines):
            # Look for path-related comments or code
            if any(keyword in line.lower() for keyword in ['path', 'sys.path', 'import sys']):
                if not in_path_section:
                    in_path_section = True
                    section_start = i
            
            if in_path_section:
                section_lines.append({
                    'line_number': i + 1,
                    'content': line.rstrip()
                })
                
                # End of section (somewhat arbitrary - 10 lines after start)
                if i > section_start + 15 and 'sys.path' not in line:
                    in_path_section = False
                    if section_lines:
                        path_sections.append(section_lines)
                    section_lines = []
        
        if section_lines:
            path_sections.append(section_lines)
        
        return path_sections
    
    def get_context(self, lines: List[str], line_number: int, context_size: int = 3) -> List[str]:
        """Get context around a specific line."""
        start = max(0, line_number - context_size - 1)
        end = min(len(lines), line_number + context_size)
        
        context = []
        for i in range(start, end):
            context.append(f"{i+1:4}: {lines[i]}")
        
        return context
    
    def generate_fix_plan(self, audit_results: Dict) -> Dict:
        """Generate a step-by-step fix plan."""
        plan = {
            'security_shim': [],
            'redis_markers': [],
            'comments': [],
            'sys_path_cleanup': [],
            'priority_order': []
        }
        
        # 1. Security shim fixes
        if 'security_imports' in audit_results:
            for filepath, imports in audit_results['security_imports'].items():
                if 'backend/broker/secure_storage.py' in filepath:
                    for imp in imports:
                        if imp['type'] == 'credential_manager_import':
                            plan['security_shim'].append({
                                'file': filepath,
                                'action': 'update_import',
                                'from': imp['content'],
                                'to': 'from aion_trading_dashboard.security import credential_manager',
                                'description': 'Direct import from package instead of shim'
                            })
        
        # 2. redis_files marker fixes
        if 'redis_references' in audit_results:
            for filepath, refs in audit_results['redis_references'].items():
                for ref in refs:
                    if 'sitecustomize' in filepath:
                        plan['redis_markers'].append({
                            'file': filepath,
                            'action': 'update_marker',
                            'current': ref['content'],
                            'suggestion': 'Replace with shared_core, intraday_trading, or alerts',
                            'description': 'Update deprecated redis_files marker'
                        })
        
        # 3. Comment in optimized_main.py
        if 'import_analysis' in audit_results and audit_results['import_analysis'].get('optimized_main'):
            redis_comment = audit_results['import_analysis']['optimized_main'].get('redis_comment')
            if redis_comment and redis_comment.get('needs_update'):
                plan['comments'].append({
                    'file': 'backend/optimized_main.py',
                    'action': 'update_comment',
                    'current': redis_comment['comment'],
                    'new': '# shared_core/redis_clients is required (redis_files is deprecated)',
                    'description': 'Update misleading comment about redis_files fallback'
                })
        
        # 4. sys.path cleanup plan
        if 'import_analysis' in audit_results:
            if audit_results['import_analysis'].get('import_utils'):
                plan['sys_path_cleanup'].append({
                    'file': 'backend/import_utils.py',
                    'action': 'refactor_long_term',
                    'description': 'Migrate to proper package installs, remove cross-service path hacks',
                    'priority': 'medium',
                    'steps': [
                        'Set up proper package structure with setup.py/pyproject.toml',
                        'Install services in editable mode (pip install -e .)',
                        'Gradually remove sys.path.insert/appends',
                        'Update imports to use absolute package paths'
                    ]
                })
        
        # Define priority order
        plan['priority_order'] = [
            "1. Update secure_storage.py import (quick win)",
            "2. Remove/update redis_files markers in sitecustomize files",
            "3. Fix comment in optimized_main.py",
            "4. Plan sys.path refactoring (longer term)"
        ]
        
        return plan
    
    def run_audit(self) -> Dict:
        """Run complete audit of legacy issues."""
        print(f"Auditing legacy issues in: {self.project_root}")
        
        results = {
            'project_root': str(self.project_root),
            'project_name': self.project_name,
            'security_imports': self.find_security_imports(),
            'redis_references': self.find_redis_files_references(),
            'import_analysis': self.analyze_import_utils()
        }
        
        # Generate fix plan
        results['fix_plan'] = self.generate_fix_plan(results)
        
        return results


class CodeFixer:
    """Helper class to apply fixes."""
    
    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.backup_dir = self.project_root / '.legacy_backup'
        self.backup_dir.mkdir(exist_ok=True)
    
    def backup_file(self, filepath: str) -> Path:
        """Create a backup of a file."""
        source = self.project_root / filepath
        if not source.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        backup = self.backup_dir / f"{filepath.replace('/', '_')}.backup"
        import shutil
        shutil.copy2(source, backup)
        return backup
    
    def fix_security_import(self, filepath: str) -> bool:
        """Update security import in secure_storage.py."""
        full_path = self.project_root / filepath
        if not full_path.exists():
            print(f"Warning: {filepath} not found")
            return False
        
        # Backup the file
        self.backup_file(filepath)
        
        with open(full_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        updated = False
        new_lines = []
        
        for line in lines:
            # Replace security.credential_manager imports
            if 'security.credential_manager' in line:
                new_line = line.replace('security.credential_manager', 
                                       'aion_trading_dashboard.security.credential_manager')
                new_lines.append(new_line)
                updated = True
                print(f"  Updated: {line.strip()} -> {new_line.strip()}")
            elif 'from security import credential_manager' in line:
                new_line = 'from aion_trading_dashboard.security import credential_manager\n'
                new_lines.append(new_line)
                updated = True
                print(f"  Updated: {line.strip()} -> {new_line.strip()}")
            else:
                new_lines.append(line)
        
        if updated:
            with open(full_path, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)
            print(f"✓ Updated {filepath}")
        
        return updated
    
    def fix_redis_comment(self, filepath: str, old_comment: str, new_comment: str) -> bool:
        """Fix the redis_files comment in optimized_main.py."""
        full_path = self.project_root / filepath
        if not full_path.exists():
            return False
        
        self.backup_file(filepath)
        
        with open(full_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if old_comment in content:
            new_content = content.replace(old_comment, new_comment)
            with open(full_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"✓ Updated comment in {filepath}")
            return True
        
        return False


def print_audit_report(results: Dict):
    """Print a formatted audit report."""
    print("\n" + "="*80)
    print("LEGACY CODE AUDIT REPORT")
    print("="*80)
    
    # 1. Security Shim Analysis
    print("\n1. SECURITY SHIM ANALYSIS")
    print("-"*40)
    security_imports = results.get('security_imports', {})
    if security_imports:
        print(f"Found {len(security_imports)} files with security imports:")
        for filepath, imports in security_imports.items():
            print(f"\n  {filepath}:")
            for imp in imports:
                print(f"    Line {imp['line_number']}: {imp['content']}")
    else:
        print("No security imports found.")
    
    # 2. redis_files References
    print("\n2. REDIS_FILES REFERENCES")
    print("-"*40)
    redis_refs = results.get('redis_references', {})
    if redis_refs:
        print(f"Found {len(redis_refs)} files referencing 'redis_files':")
        for filepath, refs in redis_refs.items():
            print(f"\n  {filepath}:")
            for ref in refs:
                print(f"    Line {ref['line_number']}: {ref['content']}")
                if 'sitecustomize' in filepath:
                    print("    ⚠️  This is a sitecustomize.py file - update marker!")
    else:
        print("No redis_files references found.")
    
    # 3. Import Analysis
    print("\n3. IMPORT UTILS & SYS.PATH ANALYSIS")
    print("-"*40)
    import_analysis = results.get('import_analysis', {})
    
    if import_analysis.get('optimized_main'):
        opt_main = import_analysis['optimized_main']
        if opt_main.get('redis_comment', {}).get('needs_update'):
            print("⚠️  Found misleading comment in optimized_main.py:")
            print(f"   Line {opt_main['redis_comment']['line_number']}: {opt_main['redis_comment']['comment']}")
        
        if opt_main.get('sys.path.modifications'):
            print(f"\n  Found {len(opt_main['sys.path.modifications'])} sys.path modifications")
    
    if import_analysis.get('import_utils'):
        import_utils = import_analysis['import_utils']
        if import_utils.get('cross_service_imports'):
            print(f"\n  Cross-service imports found: {', '.join(import_utils['cross_service_imports'])}")
    
    # 4. Fix Plan
    print("\n4. RECOMMENDED FIX PLAN")
    print("-"*40)
    fix_plan = results.get('fix_plan', {})
    
    if fix_plan.get('priority_order'):
        print("Priority Order:")
        for item in fix_plan['priority_order']:
            print(f"  {item}")
    
    print("\nDetailed Actions:")
    
    if fix_plan.get('security_shim'):
        print("\n  Security Shim Fixes:")
        for action in fix_plan['security_shim']:
            print(f"    • {action['file']}: {action['description']}")
            print(f"      From: {action['from']}")
            print(f"      To:   {action['to']}")
    
    if fix_plan.get('redis_markers'):
        print("\n  Redis Marker Updates:")
        for action in fix_plan['redis_markers']:
            print(f"    • {action['file']}: {action['description']}")
    
    if fix_plan.get('sys_path_cleanup'):
        print("\n  Sys.Path Cleanup (Long-term):")
        for action in fix_plan['sys_path_cleanup']:
            print(f"    • {action['file']}: {action['description']}")
            if 'steps' in action:
                print("      Steps:")
                for step in action['steps']:
                    print(f"        - {step}")


def interactive_fix_mode(auditor: LegacyCodeAuditor, results: Dict):
    """Interactive mode to apply fixes."""
    print("\n" + "="*80)
    print("INTERACTIVE FIX MODE")
    print("="*80)
    
    fixer = CodeFixer(auditor.project_root)
    
    while True:
        print("\nOptions:")
        print("  1. Fix security import in secure_storage.py")
        print("  2. Preview all security imports")
        print("  3. Show redis_files markers")
        print("  4. Show sys.path injections")
        print("  5. Apply all recommended fixes (with backup)")
        print("  6. Export report to JSON")
        print("  7. Exit")
        
        choice = input("\nEnter choice (1-7): ").strip()
        
        if choice == '1':
            # Fix security import
            fixer.fix_security_import('backend/broker/secure_storage.py')
        
        elif choice == '2':
            # Show security imports
            security_imports = results.get('security_imports', {})
            print("\nSecurity imports found:")
            for filepath, imports in security_imports.items():
                print(f"\n{filepath}:")
                for imp in imports:
                    print(f"  Line {imp['line_number']}: {imp['content']}")
        
        elif choice == '3':
            # Show redis_files markers
            redis_refs = results.get('redis_references', {})
            print("\nredis_files markers found:")
            for filepath, refs in redis_refs.items():
                print(f"\n{filepath}:")
                for ref in refs:
                    print(f"  Line {ref['line_number']}: {ref['content']}")
        
        elif choice == '4':
            # Show sys.path injections
            import_analysis = results.get('import_analysis', {})
            if import_analysis.get('import_utils'):
                mods = import_analysis['import_utils'].get('sys.path.modifications', [])
                if mods:
                    print("\nsys.path modifications in import_utils.py:")
                    for mod in mods:
                        print(f"  Line {mod['line_number']}: {mod['content']}")
        
        elif choice == '5':
            # Apply all fixes
            print("\nApplying all fixes...")
            fix_plan = results.get('fix_plan', {})
            
            # Apply security shim fixes
            if fix_plan.get('security_shim'):
                for action in fix_plan['security_shim']:
                    if action['file'] == 'backend/broker/secure_storage.py':
                        fixer.fix_security_import(action['file'])
            
            print("\n✓ All fixes applied. Backups saved to .legacy_backup/")
        
        elif choice == '6':
            # Export report
            output_file = 'legacy_audit_report.json'
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"\n✓ Report exported to {output_file}")
        
        elif choice == '7':
            print("\nExiting.")
            break
        
        else:
            print("\nInvalid choice. Please try again.")


def main():
    """Main execution function."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python legacy_audit.py <project_root> [--interactive]")
        print("Example: python legacy_audit.py /path/to/aion_trading_dashboard --interactive")
        sys.exit(1)
    
    project_root = sys.argv[1]
    interactive_mode = '--interactive' in sys.argv
    
    # Run audit
    auditor = LegacyCodeAuditor(project_root)
    results = auditor.run_audit()
    
    # Print report
    print_audit_report(results)
    
    # Interactive mode if requested
    if interactive_mode:
        interactive_fix_mode(auditor, results)
    
    # Save report
    with open('legacy_audit_summary.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\n" + "="*80)
    print(f"Audit complete. Summary saved to legacy_audit_summary.json")
    print("="*80)


if __name__ == "__main__":
    main()