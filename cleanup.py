#!/usr/bin/env python3
"""
Workspace Cleanup - Remove unnecessary files
"""

import os
import glob

# Files to keep
keep_files = {
    'sql_query_visualizer.py',
    'advanced_sql_visualizer.py', 
    'final_join_visualizer.py',
    'test_visualizer.py',
    'demo_comprehensive.py',
    'join_comparison_demo.py',
    'README.md',
    'requirements.txt',
    'sample_queries.sql',
    'simple_test.sql', 
    'join_test_query.sql',
    'demo_ecommerce_analytics.sql',
    'demo_financial_portfolio.sql',
    'demo_healthcare_analytics.sql',
    'improved_join_layout.svg',
    'improved_join_layout.png'
}

print("🧹 Cleaning up workspace...")

# Get all files
all_files = []
for pattern in ['*.py', '*.sql', '*.svg', '*.png']:
    all_files.extend(glob.glob(f'/app/{pattern}'))

# Remove files not in keep list
removed = 0
for file_path in all_files:
    file_name = os.path.basename(file_path)
    if file_name not in keep_files:
        try:
            os.remove(file_path)
            print(f"❌ Removed: {file_name}")
            removed += 1
        except:
            pass

print(f"✅ Cleanup complete! Removed {removed} files.")

# Show remaining files
remaining = glob.glob('/app/*.py') + glob.glob('/app/*.sql') + glob.glob('/app/*.svg') + glob.glob('/app/*.png')
print(f"📁 Remaining files: {len(remaining)}")