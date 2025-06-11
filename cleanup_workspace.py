#!/usr/bin/env python3
"""
Workspace Cleanup Script
Removes unnecessary test files and intermediate versions, keeping only the essential tools.
"""

import os
import glob
from pathlib import Path

def cleanup_workspace():
    """Clean up workspace keeping only essential files"""
    
    print("🧹 SQL Query Visualizer - Workspace Cleanup")
    print("=" * 60)
    
    # Files to keep (essential tools and documentation)
    keep_files = {
        # Main tools
        'sql_query_visualizer.py',           # Basic visualizer
        'advanced_sql_visualizer.py',        # Advanced visualizer  
        'final_join_visualizer.py',          # NEW: Join-focused visualizer
        'test_visualizer.py',                # Test suite
        'demo_comprehensive.py',             # Demo framework
        'join_comparison_demo.py',           # Join comparison demo
        
        # Documentation and config
        'README.md',
        'requirements.txt',
        
        # Sample queries
        'sample_queries.sql',
        'simple_test.sql',
        'join_test_query.sql',
        
        # Demo queries
        'demo_ecommerce_analytics.sql',
        'demo_financial_portfolio.sql', 
        'demo_healthcare_analytics.sql',
        
        # Keep one good example of each type
        'improved_join_layout.svg',
        'improved_join_layout.png',
        'final_join_test.svg',
        'final_join_test.png',
        'demo_advanced_ecommerce.svg',
        'demo_advanced_ecommerce.png'
    }
    
    # Get all files in the directory
    all_files = []
    for pattern in ['*.py', '*.sql', '*.svg', '*.png', '*.md', '*.txt']:
        all_files.extend(glob.glob(f'/app/{pattern}'))
    
    # Separate files to keep and remove
    files_to_remove = []
    files_to_keep = []
    
    for file_path in all_files:
        file_name = os.path.basename(file_path)
        if file_name in keep_files:
            files_to_keep.append(file_path)
        else:
            files_to_remove.append(file_path)
    
    # Show what will be kept
    print(f"\n📋 Files to KEEP ({len(files_to_keep)}):")
    for category, files in group_files_by_category(files_to_keep).items():
        print(f"\n  {category}:")
        for file in sorted(files):
            print(f"    ✅ {os.path.basename(file)}")
    
    # Show what will be removed
    print(f"\n🗑️  Files to REMOVE ({len(files_to_remove)}):")
    for category, files in group_files_by_category(files_to_remove).items():
        print(f"\n  {category}:")
        for file in sorted(files):
            print(f"    ❌ {os.path.basename(file)}")
    
    # Confirm before deletion
    if files_to_remove:
        print(f"\n⚠️  This will delete {len(files_to_remove)} files.")
        response = input("Proceed with cleanup? (y/N): ").strip().lower()
        
        if response == 'y':
            # Remove files
            removed_count = 0
            for file_path in files_to_remove:
                try:
                    os.remove(file_path)
                    removed_count += 1
                except Exception as e:
                    print(f"❌ Error removing {file_path}: {e}")
            
            print(f"\n✅ Cleanup complete! Removed {removed_count} files.")
        else:
            print("\n❌ Cleanup cancelled.")
    else:
        print("\n✅ Workspace is already clean!")
    
    # Show final summary
    print(f"\n📊 Final Workspace Summary:")
    remaining_files = []
    for pattern in ['*.py', '*.sql', '*.svg', '*.png', '*.md', '*.txt']:
        remaining_files.extend(glob.glob(f'/app/{pattern}'))
    
    final_groups = group_files_by_category(remaining_files)
    for category, files in final_groups.items():
        print(f"  {category}: {len(files)} files")

def group_files_by_category(file_paths):
    """Group files by category for better display"""
    categories = {
        'Python Tools': [],
        'Documentation': [],
        'SQL Queries': [],
        'Generated Diagrams': [],
        'Configuration': []
    }
    
    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        
        if file_name.endswith('.py'):
            categories['Python Tools'].append(file_path)
        elif file_name.endswith(('.md', '.txt')):
            if file_name == 'requirements.txt':
                categories['Configuration'].append(file_path)
            else:
                categories['Documentation'].append(file_path)
        elif file_name.endswith('.sql'):
            categories['SQL Queries'].append(file_path)
        elif file_name.endswith(('.svg', '.png')):
            categories['Generated Diagrams'].append(file_path)
    
    # Remove empty categories
    return {k: v for k, v in categories.items() if v}

if __name__ == '__main__':
    cleanup_workspace()