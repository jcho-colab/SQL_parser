#!/usr/bin/env python3
"""
Final Layout Demonstration
Shows the improved vertical table layout and left-to-right level arrangement.
"""

import subprocess
import os

def run_final_demo():
    """Demonstrate the improved join visualizer"""
    
    print("🎨 SQL Join Visualizer - Final Layout Demonstration")
    print("=" * 70)
    print("Showcasing improved vertical table layout and left-to-right level arrangement")
    
    test_cases = [
        {
            'file': 'join_test_query.sql',
            'output': 'final_demo_multiple_joins',
            'description': 'Multiple Join Types (INNER, LEFT, RIGHT)'
        },
        {
            'file': 'simple_test.sql', 
            'output': 'final_demo_cte_joins',
            'description': 'CTE with Multiple Joins'
        }
    ]
    
    for test in test_cases:
        print(f"\n{'='*50}")
        print(f"📊 {test['description']}")
        print(f"{'='*50}")
        
        # Show SQL snippet
        with open(f"/app/{test['file']}", 'r') as f:
            lines = f.read().strip().split('\n')[:8]
            print("SQL Query (first 8 lines):")
            for i, line in enumerate(lines, 1):
                print(f"  {i:2d}: {line}")
            print("     ...")
        
        # Run visualizer
        print(f"\n🚀 Generating diagram...")
        try:
            result = subprocess.run([
                'python', 'final_join_visualizer.py',
                '-f', test['file'],
                '-o', test['output'],
                '-v'
            ], capture_output=True, text=True, cwd='/app', timeout=30)
            
            if result.returncode == 0:
                print("✅ Success!")
                # Extract key metrics
                lines = result.stdout.split('\n')
                for line in lines:
                    if 'Tables:' in line or 'Joins:' in line or 'Join Types:' in line:
                        print(f"   {line.strip()}")
            else:
                print("❌ Error!")
                print(result.stderr)
                
        except Exception as e:
            print(f"❌ Error: {e}")
    
    print(f"\n{'='*70}")
    print("🎉 Layout Improvements Summary")
    print(f"{'='*70}")
    
    print("""
✅ VERTICAL TABLE LAYOUT:
   • Tables now display vertically instead of horizontally
   • Join columns are clearly separated with bullet points
   • More readable table structure with clear sections

✅ LEFT-TO-RIGHT LEVEL ARRANGEMENT:
   • Tables are arranged by their position in the join chain
   • Starting tables appear on the left
   • Joined tables flow naturally from left to right
   • Proper ranking ensures clear data flow visualization

✅ ENHANCED READABILITY:
   • 📋 Table names with clear headers
   • 🔑 Join columns prominently marked
   • Clean separation of information
   • Better use of vertical space

✅ IMPROVED JOIN VISUALIZATION:
   • Color-coded join types for easy identification
   • Clear join conditions displayed on edges
   • Proper join chain representation
   • Professional styling and layout
""")
    
    # Show generated files
    svg_files = [f for f in os.listdir('/app') if f.startswith('final_demo_') and f.endswith('.svg')]
    print(f"📁 Generated Demonstration Files:")
    for file in sorted(svg_files):
        print(f"   📊 {file}")
        png_file = file.replace('.svg', '.png')
        if os.path.exists(f'/app/{png_file}'):
            print(f"   🖼️  {png_file}")
    
    print(f"\n🎯 Usage Recommendation:")
    print("Use final_join_visualizer.py for join analysis:")
    print("python final_join_visualizer.py -f your_query.sql -o analysis -v")

if __name__ == '__main__':
    run_final_demo()