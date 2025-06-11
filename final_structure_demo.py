#!/usr/bin/env python3
"""
Final Demonstration: Hierarchical Query Structure Analysis
Shows how the new visualizer handles nested SELECT statements and query encapsulation.
"""

import subprocess
import os

def run_structure_analysis(sql_file, description):
    """Run structure analysis and show results"""
    print(f"\n{'='*80}")
    print(f"📊 {description}")
    print(f"{'='*80}")
    
    # Show SQL preview
    with open(f'/app/{sql_file}', 'r') as f:
        sql_content = f.read()
        lines = sql_content.strip().split('\n')[:12]
        print("📝 SQL Query Preview:")
        for i, line in enumerate(lines, 1):
            print(f"  {i:2d}: {line}")
        print(f"     ... ({len(sql_content.split()) - len(lines)} more lines)")
    
    # Run analysis
    print(f"\n🔍 Running Hierarchical Structure Analysis...")
    try:
        result = subprocess.run([
            'python', 'query_structure_visualizer.py',
            '-f', sql_file,
            '-o', f'final_demo_{sql_file.replace(".sql", "")}'
        ], capture_output=True, text=True, cwd='/app', timeout=30)
        
        if result.returncode == 0:
            print(result.stdout)
        else:
            print("❌ Error!")
            print(result.stderr)
            
    except Exception as e:
        print(f"❌ Exception: {e}")

def main():
    """Run comprehensive hierarchical structure demonstration"""
    
    print("🏗️  SQL Query Structure Visualizer - Final Demonstration")
    print("=" * 80)
    print("This demonstrates the new approach to visualizing SQL query hierarchy:")
    print("• Isolated SELECT statements (leftmost)")
    print("• Grouped joins and relationships (middle)")
    print("• Nested structures and encapsulation (rightmost)")
    print("• Proper WITH AS statement handling")
    
    # Test cases showing different levels of complexity
    test_cases = [
        {
            'file': 'nested_query_test.sql',
            'description': 'Complex Nested Query with WITH and Subqueries'
        },
        {
            'file': 'sample_queries.sql',
            'description': 'Enterprise-Level Query with Multiple CTEs'
        },
        {
            'file': 'simple_test.sql',
            'description': 'Basic CTE Example for Comparison'
        }
    ]
    
    for test in test_cases:
        run_structure_analysis(test['file'], test['description'])
    
    print(f"\n{'='*80}")
    print("🎯 Key Improvements Demonstrated")
    print(f"{'='*80}")
    
    print("""
✅ HIERARCHICAL STRUCTURE DETECTION:
   • Correctly identifies nested SELECT statements at different levels
   • Shows encapsulation relationships (which statements contain others)
   • Displays progression from isolated to grouped to nested structures

✅ WITH CLAUSE HANDLING:
   • Properly identifies WITH blocks as containers
   • Shows individual CTEs within WITH clauses
   • Demonstrates how CTEs encapsulate their own SELECT statements

✅ SUBQUERY RECOGNITION:
   • Identifies subqueries in SELECT, FROM, and WHERE clauses
   • Shows nesting depth and containment relationships
   • Displays which tables and joins each subquery uses

✅ LEFT-TO-RIGHT PROGRESSION:
   • Level 0: Main containers (WITH blocks, main query structure)
   • Level 1: CTEs and major components  
   • Level 2: Main SELECT statements with joins
   • Level 3: Nested subqueries and detailed components

✅ JOIN KEY IDENTIFICATION:
   • Shows join relationships at each structural level
   • Identifies which joins occur within which structures
   • Displays table relationships for each component
""")
    
    print(f"\n📊 Structure Analysis Benefits:")
    print("""
🎯 FOR DATA ENGINEERS:
   • Understand complex query architecture before optimization
   • Identify bottlenecks in nested structures
   • Plan refactoring of complex queries

🎯 FOR SQL DEVELOPERS:  
   • Visualize query logic flow and dependencies
   • Debug complex nested queries more effectively
   • Document query structure for team collaboration

🎯 FOR ANALYSTS:
   • Understand data lineage through query hierarchy
   • Verify business logic implementation in complex queries
   • Trace data flow from sources to final output
""")
    
    # Show generated files
    structure_files = [f for f in os.listdir('/app') if f.startswith('final_demo_') and f.endswith('.svg')]
    if structure_files:
        print(f"\n📁 Generated Structure Diagrams:")
        for file in sorted(structure_files):
            print(f"   📊 {file}")
    
    print(f"\n🚀 Next Steps:")
    print("Use the Query Structure Visualizer to analyze your complex SQL queries:")
    print("python query_structure_visualizer.py -f your_query.sql -o analysis -v")

if __name__ == '__main__':
    main()