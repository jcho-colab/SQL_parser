#!/usr/bin/env python3
"""
Final Join-Focused SQL Visualizer
Robust version that correctly parses and visualizes SQL joins with focus on join columns.
"""

import re
import graphviz
import click
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional
from enum import Enum


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"


@dataclass
class Table:
    name: str
    alias: str
    join_columns: List[str] = field(default_factory=list)


@dataclass
class Join:
    left_table: str
    right_table: str
    join_type: JoinType
    condition: str
    left_column: str = ""
    right_column: str = ""


class RobustJoinParser:
    """Robust parser specifically designed for join analysis"""
    
    def __init__(self):
        self.tables: Dict[str, Table] = {}
        self.joins: List[Join] = []
    
    def parse_query(self, sql: str) -> Dict[str, any]:
        """Parse SQL query focusing on join relationships"""
        # Reset state
        self.tables = {}
        self.joins = []
        
        # Clean and prepare SQL
        sql = self._clean_sql(sql)
        
        # Extract FROM clause
        from_table = self._extract_from_table(sql)
        if from_table:
            table_name, alias = from_table
            self.tables[alias] = Table(name=table_name, alias=alias)
        
        # Extract joins using multiple strategies
        joins_data = self._extract_joins_comprehensive(sql)
        
        # Process joins sequentially
        self._process_joins(joins_data)
        
        return {
            'tables': self.tables,
            'joins': self.joins,
            'summary': {
                'table_count': len(self.tables),
                'join_count': len(self.joins),
                'join_types': {jt.value: len([j for j in self.joins if j.join_type == jt]) for jt in JoinType}
            }
        }
    
    def _clean_sql(self, sql: str) -> str:
        """Clean and normalize SQL"""
        # Remove comments
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        
        # Normalize whitespace
        sql = re.sub(r'\s+', ' ', sql.strip())
        
        return sql
    
    def _extract_from_table(self, sql: str) -> Optional[Tuple[str, str]]:
        """Extract the main table from FROM clause"""
        pattern = r'FROM\s+([\w\.]+)(?:\s+(?:AS\s+)?(\w+))?'
        match = re.search(pattern, sql, re.IGNORECASE)
        
        if match:
            table_name = match.group(1)
            alias = match.group(2) or table_name
            return table_name, alias
        
        return None
    
    def _extract_joins_comprehensive(self, sql: str) -> List[Tuple[str, str, str, str]]:
        """Extract joins using comprehensive pattern matching"""
        joins = []
        
        # Strategy 1: Individual join extraction
        # Find all JOIN clauses with their complete ON conditions
        join_clauses = self._split_join_clauses(sql)
        
        for clause in join_clauses:
            join_data = self._parse_single_join_clause(clause)
            if join_data:
                joins.append(join_data)
        
        return joins
    
    def _split_join_clauses(self, sql: str) -> List[str]:
        """Split SQL into individual join clauses"""
        print(f"DEBUG: Full SQL after cleaning: {sql}")  # Debug
        
        # Find the FROM clause and everything after it
        from_match = re.search(r'FROM\s+[\w\.\s]+', sql, re.IGNORECASE)
        if not from_match:
            print("DEBUG: No FROM clause found")  # Debug
            return []
        
        # Get the part after FROM
        from_end = from_match.end()
        after_from = sql[from_end:]
        print(f"DEBUG: After FROM: {after_from}")  # Debug
        
        # Split by JOIN keywords, keeping the keyword with the clause
        join_pattern = r'((?:INNER\s+|LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?|FULL\s+(?:OUTER\s+)?|CROSS\s+)?JOIN\s+[^WHERE|GROUP|ORDER|HAVING|LIMIT]+?)(?=\s+(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN|WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|$)'
        
        join_matches = re.findall(join_pattern, after_from, re.IGNORECASE | re.DOTALL)
        print(f"DEBUG: Found {len(join_matches)} join matches: {join_matches}")  # Debug
        
        # If the complex pattern doesn't work, try a simpler approach
        if not join_matches:
            print("DEBUG: Trying simpler pattern...")  # Debug
            # Look for individual JOIN clauses
            simple_pattern = r'((?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+\w+(?:\s+\w+)?\s+ON\s+[^JOIN]+?)(?=\s*(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN|WHERE|$)'
            join_matches = re.findall(simple_pattern, after_from, re.IGNORECASE | re.DOTALL)
            print(f"DEBUG: Simple pattern found {len(join_matches)} matches: {join_matches}")  # Debug
        
        return [match.strip() for match in join_matches if match.strip()]
    
    def _parse_single_join_clause(self, clause: str) -> Optional[Tuple[str, str, str, str]]:
        """Parse a single join clause"""
        # Pattern to extract join type, table, alias, and condition
        pattern = r'((?:INNER\s+|LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?|FULL\s+(?:OUTER\s+)?|CROSS\s+)?JOIN)\s+([\w\.]+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(.+)'
        
        match = re.search(pattern, clause, re.IGNORECASE | re.DOTALL)
        
        if match:
            join_type = match.group(1).strip()
            table_name = match.group(2)
            alias = match.group(3) or table_name
            condition = match.group(4).strip()
            
            return join_type, table_name, alias, condition
        
        return None
    
    def _process_joins(self, joins_data: List[Tuple[str, str, str, str]]):
        """Process extracted joins and build relationships"""
        # Get the list of table aliases in order
        table_order = list(self.tables.keys())
        
        for join_type_str, table_name, alias, condition in joins_data:
            # Add table if not exists
            if alias not in self.tables:
                self.tables[alias] = Table(name=table_name, alias=alias)
            
            # Determine join type
            join_type = self._parse_join_type(join_type_str)
            
            # Find the previous table in the chain
            prev_table = table_order[-1] if table_order else None
            
            if prev_table:
                # Extract join columns from condition
                left_col, right_col = self._extract_join_columns(condition, prev_table, alias)
                
                # Create join object
                join = Join(
                    left_table=prev_table,
                    right_table=alias,
                    join_type=join_type,
                    condition=condition,
                    left_column=left_col,
                    right_column=right_col
                )
                
                self.joins.append(join)
                
                # Update table join columns
                if left_col and left_col not in self.tables[prev_table].join_columns:
                    self.tables[prev_table].join_columns.append(left_col)
                if right_col and right_col not in self.tables[alias].join_columns:
                    self.tables[alias].join_columns.append(right_col)
            
            # Add current table to order if not already there
            if alias not in table_order:
                table_order.append(alias)
    
    def _parse_join_type(self, join_type_str: str) -> JoinType:
        """Parse join type from string"""
        join_type_str = join_type_str.upper()
        
        if 'LEFT' in join_type_str:
            return JoinType.LEFT
        elif 'RIGHT' in join_type_str:
            return JoinType.RIGHT
        elif 'FULL' in join_type_str:
            return JoinType.FULL
        elif 'CROSS' in join_type_str:
            return JoinType.CROSS
        else:
            return JoinType.INNER
    
    def _extract_join_columns(self, condition: str, left_table: str, right_table: str) -> Tuple[str, str]:
        """Extract join columns from condition"""
        # Look for pattern: table.column = table.column
        pattern = r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
        match = re.search(pattern, condition)
        
        if match:
            table1_alias, col1, table2_alias, col2 = match.groups()
            
            # Determine which is left and which is right
            if table1_alias == left_table:
                return col1, col2
            elif table2_alias == left_table:
                return col2, col1
            else:
                # Fallback: return first as left, second as right
                return col1, col2
        
        return "", ""


class JoinFocusedVisualizer:
    """Visualizer that emphasizes join relationships and columns"""
    
    def __init__(self):
        self.join_colors = {
            JoinType.INNER: '#1976D2',
            JoinType.LEFT: '#4CAF50',
            JoinType.RIGHT: '#FF9800',
            JoinType.FULL: '#9C27B0',
            JoinType.CROSS: '#F44336'
        }
    
    def generate_diagram(self, data: Dict, output_path: str):
        """Generate join-focused diagram"""
        tables = data['tables']
        joins = data['joins']
        summary = data['summary']
        
        dot = graphviz.Digraph(comment='SQL Join Analysis')
        dot.attr(rankdir='LR')
        dot.attr('node', shape='record', style='filled', fillcolor='lightblue', fontname='Arial')
        dot.attr('edge', fontname='Arial', fontsize='10')
        dot.attr('graph', splines='ortho', nodesep='0.8', ranksep='1.2')
        
        # Add title
        title = f'SQL Join Analysis\\n{summary["table_count"]} Tables, {summary["join_count"]} Joins'
        join_type_summary = ", ".join([f'{k}: {v}' for k, v in summary["join_types"].items() if v > 0])
        if join_type_summary:
            title += f'\\n{join_type_summary}'
        
        dot.attr(label=title, labelloc='top', fontsize='14')
        
        # Add nodes (tables) with join columns highlighted
        for table_key, table in tables.items():
            label = self._create_table_label(table)
            dot.node(table_key, label=label)
        
        # Add edges (joins) with detailed information
        for join in joins:
            self._add_join_edge(dot, join)
        
        # Render
        try:
            dot.render(output_path, format='svg', cleanup=True)
            dot.render(output_path, format='png', cleanup=True)
            print(f"✅ Join-focused diagram saved: {output_path}.svg and {output_path}.png")
        except Exception as e:
            print(f"❌ Error generating diagram: {e}")
    
    def _create_table_label(self, table: Table) -> str:
        """Create table label emphasizing join columns"""
        parts = []
        
        # Table header
        if table.alias != table.name:
            header = f"{table.alias}\\n({table.name})"
        else:
            header = table.name
        parts.append(f"<header>{header}")
        
        # Join columns section (most important)
        if table.join_columns:
            join_section = "JOIN COLUMNS"
            parts.append(join_section)
            
            for col in table.join_columns:
                parts.append(f"<{col}>🔑 {col}")
        
        # Format as record
        if len(parts) == 1:
            return parts[0]
        else:
            return "{" + " | ".join(parts) + "}"
    
    def _add_join_edge(self, dot, join: Join):
        """Add join edge with detailed information"""
        color = self.join_colors.get(join.join_type, '#000000')
        
        # Create edge label
        label_parts = [f"{join.join_type.value} JOIN"]
        
        if join.left_column and join.right_column:
            label_parts.append(f"{join.left_column} = {join.right_column}")
        elif join.condition:
            # Simplified condition display
            simplified = join.condition.replace(' ', '')
            if len(simplified) > 30:
                simplified = simplified[:27] + "..."
            label_parts.append(simplified)
        
        label = "\\n".join(label_parts)
        
        # Style edge based on join type
        style = 'bold' if join.join_type == JoinType.INNER else 'solid'
        penwidth = '3' if join.join_type == JoinType.INNER else '2'
        
        dot.edge(join.left_table, join.right_table,
                label=label, color=color, style=style, penwidth=penwidth,
                arrowsize='1.0')


@click.command()
@click.option('--sql-file', '-f', type=click.Path(exists=True), help='Path to SQL file')
@click.option('--sql', '-s', type=str, help='SQL query string')
@click.option('--output', '-o', default='final_join_diagram', help='Output file name')
@click.option('--verbose', '-v', is_flag=True, help='Show detailed information')
def main(sql_file, sql, output, verbose):
    """Final Join-Focused SQL Visualizer"""
    
    if not sql_file and not sql:
        click.echo("Error: Must provide either --sql-file or --sql")
        return
    
    # Read SQL
    sql_content = ""
    if sql_file:
        with open(sql_file, 'r') as f:
            sql_content = f.read()
    else:
        sql_content = sql
    
    # Parse with robust parser
    parser = RobustJoinParser()
    data = parser.parse_query(sql_content)
    
    # Generate visualization
    visualizer = JoinFocusedVisualizer()
    visualizer.generate_diagram(data, output)
    
    # Print summary
    summary = data['summary']
    tables = data['tables']
    joins = data['joins']
    
    click.echo(f"\n📊 Join Analysis Results:")
    click.echo(f"   Tables: {summary['table_count']}")
    click.echo(f"   Joins: {summary['join_count']}")
    
    # Show join type breakdown
    join_types = {k: v for k, v in summary['join_types'].items() if v > 0}
    if join_types:
        click.echo("   Join Types:")
        for join_type, count in join_types.items():
            click.echo(f"     {join_type}: {count}")
    
    if verbose:
        click.echo(f"\n📋 Tables with Join Columns:")
        for table_key, table in tables.items():
            click.echo(f"   {table.name} (alias: {table.alias})")
            if table.join_columns:
                click.echo(f"     Join columns: {', '.join(table.join_columns)}")
            else:
                click.echo("     No join columns")
        
        click.echo(f"\n🔗 Join Details:")
        for i, join in enumerate(joins, 1):
            click.echo(f"   {i}. {join.left_table} {join.join_type.value} JOIN {join.right_table}")
            if join.left_column and join.right_column:
                click.echo(f"      ON {join.left_column} = {join.right_column}")
            else:
                click.echo(f"      ON {join.condition}")


if __name__ == '__main__':
    main()