#!/usr/bin/env python3
"""
Join-Focused SQL Visualizer
Simplified version that specifically focuses on detecting and visualizing joins correctly.
"""

import sqlglot
from sqlglot import exp
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Optional, Any
import graphviz
import click
import re
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
    is_cte: bool = False


@dataclass
class Join:
    left_table: str
    right_table: str
    join_type: JoinType
    conditions: List[str] = field(default_factory=list)


class SimpleJoinParser:
    """Simplified parser focused specifically on join detection"""
    
    def __init__(self):
        self.tables: Dict[str, Table] = {}
        self.joins: List[Join] = []
        self.table_aliases: Dict[str, str] = {}
    
    def parse_query(self, sql: str) -> Dict[str, Any]:
        """Parse SQL focusing on join relationships"""
        # Reset state
        self.tables = {}
        self.joins = []
        self.table_aliases = {}
        
        try:
            # Try regex parsing first (more reliable for our use case)
            self._extract_with_regex(sql)
        except Exception as e:
            print(f"Regex parsing failed: {e}")
        
        # If no results, try SQLGlot as backup
        if not self.tables and not self.joins:
            try:
                parsed = sqlglot.parse_one(sql, read='')
                self._extract_with_sqlglot(parsed)
            except Exception as e:
                print(f"SQLGlot parsing also failed: {e}")
        
        return {
            'tables': self.tables,
            'joins': self.joins,
            'table_aliases': self.table_aliases
        }
    
    def _extract_with_sqlglot(self, parsed):
        """Extract using SQLGlot AST"""
        # Find all SELECT statements
        selects = [parsed] if isinstance(parsed, exp.Select) else parsed.find_all(exp.Select)
        
        for select in selects:
            # Extract tables from FROM clause
            if hasattr(select, 'from_') and select.from_:
                self._process_from_clause(select.from_)
            
            # Extract joins
            if hasattr(select, 'joins') and select.joins:
                self._process_joins(select.joins, select.from_)
    
    def _process_from_clause(self, from_clause):
        """Process FROM clause to extract base table"""
        if hasattr(from_clause, 'this'):
            table_expr = from_clause.this
            table_name, alias = self._extract_table_info(table_expr)
            if table_name:
                self.tables[alias or table_name] = Table(
                    name=table_name,
                    alias=alias or table_name
                )
                if alias:
                    self.table_aliases[alias] = table_name
    
    def _process_joins(self, joins, from_clause):
        """Process JOIN clauses"""
        # Get the base table from FROM
        base_table = None
        if hasattr(from_clause, 'this'):
            base_name, base_alias = self._extract_table_info(from_clause.this)
            base_table = base_alias or base_name
        
        current_table = base_table
        
        for join in joins:
            # Extract joined table
            joined_name, joined_alias = self._extract_table_info(join.this)
            joined_table = joined_alias or joined_name
            
            if joined_name:
                # Add table if not exists
                if joined_table not in self.tables:
                    self.tables[joined_table] = Table(
                        name=joined_name,
                        alias=joined_alias or joined_name
                    )
                    if joined_alias:
                        self.table_aliases[joined_alias] = joined_name
                
                # Extract join type
                join_type = self._extract_join_type(join)
                
                # Extract join conditions
                conditions, join_columns = self._extract_join_conditions(join)
                
                # Create join relationship
                if current_table:
                    join_obj = Join(
                        left_table=current_table,
                        right_table=joined_table,
                        join_type=join_type,
                        conditions=conditions
                    )
                    self.joins.append(join_obj)
                    
                    # Update join columns
                    self._update_join_columns(current_table, joined_table, join_columns)
                
                # For chain joins
                current_table = joined_table
    
    def _extract_table_info(self, table_expr) -> Tuple[Optional[str], Optional[str]]:
        """Extract table name and alias from table expression"""
        table_name = None
        alias = None
        
        try:
            if hasattr(table_expr, 'name'):
                table_name = str(table_expr.name)
            
            if hasattr(table_expr, 'alias') and table_expr.alias:
                alias = str(table_expr.alias)
        except Exception:
            pass
        
        return table_name, alias
    
    def _extract_join_type(self, join) -> JoinType:
        """Extract join type from join clause"""
        if hasattr(join, 'kind') and join.kind:
            kind_str = str(join.kind).upper()
            
            if 'LEFT' in kind_str:
                return JoinType.LEFT
            elif 'RIGHT' in kind_str:
                return JoinType.RIGHT
            elif 'FULL' in kind_str:
                return JoinType.FULL
            elif 'CROSS' in kind_str:
                return JoinType.CROSS
        
        return JoinType.INNER
    
    def _extract_join_conditions(self, join) -> Tuple[List[str], Dict[str, List[str]]]:
        """Extract join conditions and involved columns"""
        conditions = []
        join_columns = {}
        
        if hasattr(join, 'on') and join.on:
            try:
                condition_str = str(join.on)
                conditions.append(condition_str)
                
                # Extract column references using regex
                column_pattern = r'(\w+)\.(\w+)'
                matches = re.findall(column_pattern, condition_str)
                
                for table_alias, column in matches:
                    if table_alias not in join_columns:
                        join_columns[table_alias] = []
                    if column not in join_columns[table_alias]:
                        join_columns[table_alias].append(column)
                        
            except Exception:
                pass
        
        return conditions, join_columns
    
    def _update_join_columns(self, left_table: str, right_table: str, join_columns: Dict[str, List[str]]):
        """Update tables with their join columns"""
        for table_alias, columns in join_columns.items():
            # Resolve alias to table identifier
            table_key = table_alias
            if table_key in self.tables:
                for col in columns:
                    if col not in self.tables[table_key].join_columns:
                        self.tables[table_key].join_columns.append(col)
    
    def _extract_with_regex(self, sql: str):
        """Regex-based extraction for reliable join detection"""
        # Clean up SQL - remove comments and normalize whitespace
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)  # Remove comments
        sql = re.sub(r'\s+', ' ', sql.strip())  # Normalize whitespace
        
        print(f"Processing SQL: {sql[:100]}...")  # Debug
        
        # Extract FROM clause with table and alias
        from_pattern = r'FROM\s+([\w\.]+)(?:\s+AS\s+(\w+)|\s+(\w+))?'
        from_match = re.search(from_pattern, sql, re.IGNORECASE)
        
        if from_match:
            table_name = from_match.group(1)
            alias = from_match.group(2) or from_match.group(3) or table_name
            
            print(f"Found FROM table: {table_name} as {alias}")  # Debug
            
            self.tables[alias] = Table(name=table_name, alias=alias)
            if alias != table_name:
                self.table_aliases[alias] = table_name
        
        # Extract JOINs - improved pattern
        join_pattern = r'(INNER\s+JOIN|LEFT\s+(?:OUTER\s+)?JOIN|RIGHT\s+(?:OUTER\s+)?JOIN|FULL\s+(?:OUTER\s+)?JOIN|CROSS\s+JOIN|JOIN)\s+([\w\.]+)(?:\s+AS\s+(\w+)|\s+(\w+))?\s+ON\s+([^WHERE|GROUP|ORDER|HAVING|INNER|LEFT|RIGHT|FULL|CROSS|JOIN]+)'
        
        joins = re.findall(join_pattern, sql, re.IGNORECASE)
        
        print(f"Found {len(joins)} joins")  # Debug
        
        # Process joins in order
        table_order = [list(self.tables.keys())[0]] if self.tables else []
        
        for join_type_str, table_name, alias1, alias2, condition in joins:
            alias = alias1 or alias2 or table_name
            
            print(f"Processing join: {join_type_str} {table_name} as {alias} ON {condition}")  # Debug
            
            # Add table if not exists
            if alias not in self.tables:
                self.tables[alias] = Table(name=table_name, alias=alias)
                if alias != table_name:
                    self.table_aliases[alias] = table_name
            
            # Determine join type
            join_type = JoinType.INNER
            join_upper = join_type_str.upper()
            if 'LEFT' in join_upper:
                join_type = JoinType.LEFT
            elif 'RIGHT' in join_upper:
                join_type = JoinType.RIGHT
            elif 'FULL' in join_upper:
                join_type = JoinType.FULL
            elif 'CROSS' in join_upper:
                join_type = JoinType.CROSS
            
            # Find the previous table to join with
            prev_table = table_order[-1] if table_order else None
            
            if prev_table:
                join_obj = Join(
                    left_table=prev_table,
                    right_table=alias,
                    join_type=join_type,
                    conditions=[condition.strip()]
                )
                self.joins.append(join_obj)
                
                print(f"Created join: {prev_table} -> {alias}")  # Debug
                
                # Extract join columns
                self._extract_join_columns_from_condition(condition.strip(), prev_table, alias)
            
            # Add current table to order
            if alias not in table_order:
                table_order.append(alias)
        
        print(f"Final tables: {list(self.tables.keys())}")  # Debug
        print(f"Final joins: {len(self.joins)}")  # Debug
    
    def _extract_join_columns_from_condition(self, condition: str, left_table: str, right_table: str):
        """Extract join columns from condition string"""
        # Pattern for table.column references
        column_pattern = r'(\w+)\.(\w+)'
        matches = re.findall(column_pattern, condition)
        
        for table_alias, column in matches:
            if table_alias in self.tables:
                if column not in self.tables[table_alias].join_columns:
                    self.tables[table_alias].join_columns.append(column)


class SimpleJoinVisualizer:
    """Simple visualizer for join relationships"""
    
    def __init__(self):
        self.join_colors = {
            JoinType.INNER: '#1976D2',
            JoinType.LEFT: '#4CAF50',
            JoinType.RIGHT: '#FF9800',
            JoinType.FULL: '#9C27B0',
            JoinType.CROSS: '#F44336'
        }
    
    def generate_diagram(self, data: Dict[str, Any], output_path: str):
        """Generate simple join diagram"""
        tables = data['tables']
        joins = data['joins']
        
        dot = graphviz.Digraph(comment='SQL Join Diagram')
        dot.attr(rankdir='LR')
        dot.attr('node', shape='record', style='filled', fillcolor='lightblue')
        dot.attr('edge', fontsize='10')
        
        # Add title
        dot.attr(label=f'SQL Join Analysis\\n{len(tables)} Tables, {len(joins)} Joins', 
                labelloc='top', fontsize='14')
        
        # Add nodes (tables)
        for table_key, table in tables.items():
            label = self._create_table_label(table)
            dot.node(table_key, label=label)
        
        # Add edges (joins)
        for join in joins:
            color = self.join_colors.get(join.join_type, '#000000')
            label = f"{join.join_type.value}\\n" + "\\n".join(join.conditions)
            
            dot.edge(join.left_table, join.right_table, 
                    label=label, color=color, penwidth='2')
        
        # Render
        try:
            dot.render(output_path, format='svg', cleanup=True)
            dot.render(output_path, format='png', cleanup=True)
            print(f"✅ Diagram saved: {output_path}.svg and {output_path}.png")
        except Exception as e:
            print(f"❌ Error generating diagram: {e}")
    
    def _create_table_label(self, table: Table) -> str:
        """Create label for table node showing join columns prominently"""
        parts = []
        
        # Table name
        if table.alias != table.name:
            parts.append(f"<title>{table.alias}\\n({table.name})")
        else:
            parts.append(f"<title>{table.name}")
        
        # Join columns (highlighted)
        if table.join_columns:
            join_cols = " | ".join([f"<{col}>{col}" for col in table.join_columns])
            parts.append(f"JOIN KEYS|{join_cols}")
        
        return "{" + " | ".join(parts) + "}"


@click.command()
@click.option('--sql-file', '-f', type=click.Path(exists=True), help='Path to SQL file')
@click.option('--sql', '-s', type=str, help='SQL query string')
@click.option('--output', '-o', default='join_focused_diagram', help='Output file name')
@click.option('--verbose', '-v', is_flag=True, help='Show detailed information')
def main(sql_file, sql, output, verbose):
    """Simple Join-Focused SQL Visualizer"""
    
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
    
    # Parse
    parser = SimpleJoinParser()
    data = parser.parse_query(sql_content)
    
    # Generate diagram
    visualizer = SimpleJoinVisualizer()
    visualizer.generate_diagram(data, output)
    
    # Print summary
    tables = data['tables']
    joins = data['joins']
    
    click.echo(f"\n📊 Join Analysis Summary:")
    click.echo(f"   Tables: {len(tables)}")
    click.echo(f"   Joins: {len(joins)}")
    
    if verbose:
        click.echo(f"\n📋 Tables:")
        for table_key, table in tables.items():
            click.echo(f"   {table.name} (as {table.alias})")
            if table.join_columns:
                click.echo(f"     Join columns: {', '.join(table.join_columns)}")
        
        click.echo(f"\n🔗 Joins:")
        for join in joins:
            click.echo(f"   {join.left_table} {join.join_type.value} {join.right_table}")
            for condition in join.conditions:
                click.echo(f"     ON {condition}")


if __name__ == '__main__':
    main()