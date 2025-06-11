#!/usr/bin/env python3
"""
Enhanced SQL Query Visualizer with Focus on Join Analysis
This version specifically addresses join detection and visualization issues.
"""

import sqlglot
from sqlglot import exp
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Optional, Any
import networkx as nx
import graphviz
import click
import json
from pathlib import Path
import re
from enum import Enum
from collections import defaultdict


class NodeType(Enum):
    """Types of nodes in the query graph"""
    TABLE = "table"
    CTE = "cte"
    SUBQUERY = "subquery"
    DERIVED_TABLE = "derived_table"


class JoinType(Enum):
    """Types of joins"""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"
    IMPLICIT = "IMPLICIT"  # FROM table1, table2 with WHERE conditions


@dataclass
class JoinCondition:
    """Represents a single join condition"""
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    operator: str = "="
    
    def __str__(self):
        return f"{self.left_table}.{self.left_column} {self.operator} {self.right_table}.{self.right_column}"


@dataclass
class QueryNode:
    """Represents a node in the query graph (table, CTE, etc.)"""
    name: str
    node_type: NodeType
    alias: str = ""
    join_columns: List[str] = field(default_factory=list)  # Only columns used in joins
    all_columns: List[str] = field(default_factory=list)   # All available columns
    level: int = 0
    parent_cte: str = ""
    schema: str = ""
    
    def __post_init__(self):
        if not self.alias:
            self.alias = self.name


@dataclass
class QueryEdge:
    """Represents a relationship between nodes"""
    source: str
    target: str
    join_type: JoinType
    join_conditions: List[JoinCondition] = field(default_factory=list)
    edge_type: str = "join"  # join, cte_dependency, data_flow


class EnhancedJoinAnalyzer:
    """Enhanced SQL parser focused on join analysis"""
    
    def __init__(self, dialect: str = ""):
        self.dialect = dialect
        self.nodes: Dict[str, QueryNode] = {}
        self.edges: List[QueryEdge] = []
        self.table_aliases: Dict[str, str] = {}  # alias -> actual_table_name
        self.reverse_aliases: Dict[str, str] = {}  # actual_table_name -> alias
        self.cte_hierarchy: Dict[str, List[str]] = {}
        
    def parse_query(self, sql: str) -> Dict[str, Any]:
        """Parse SQL query with enhanced join analysis"""
        try:
            # Parse the SQL
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            
            # Reset state
            self.nodes = {}
            self.edges = []
            self.table_aliases = {}
            self.reverse_aliases = {}
            self.cte_hierarchy = {}
            
            # Phase 1: Extract table aliases first
            self._extract_all_table_aliases(parsed)
            
            # Phase 2: Extract CTEs
            self._extract_ctes(parsed)
            
            # Phase 3: Extract main query tables
            self._extract_main_query_tables(parsed)
            
            # Phase 4: Analyze joins comprehensively
            self._analyze_all_joins(parsed)
            
            # Phase 5: Build CTE dependencies
            self._build_cte_dependencies(parsed)
            
            return {
                'nodes': self.nodes,
                'edges': self.edges,
                'table_aliases': self.table_aliases,
                'cte_hierarchy': self.cte_hierarchy,
                'join_summary': self._generate_join_summary()
            }
            
        except Exception as e:
            print(f"Error parsing SQL: {e}")
            return {
                'nodes': self.nodes,
                'edges': self.edges,
                'table_aliases': self.table_aliases,
                'cte_hierarchy': self.cte_hierarchy,
                'join_summary': {}
            }
    
    def _extract_all_table_aliases(self, parsed_query):
        """Extract all table aliases from the entire query"""
        # Find all FROM clauses
        selects = parsed_query.find_all(exp.Select)
        
        for select in selects:
            # Extract from FROM clause
            if hasattr(select, 'from_') and select.from_:
                self._extract_table_alias_from_expression(select.from_.this)
            
            # Extract from JOINs
            if hasattr(select, 'joins') and select.joins:
                for join in select.joins:
                    if hasattr(join, 'this'):
                        self._extract_table_alias_from_expression(join.this)
        
        # Find CTEs
        with_clauses = parsed_query.find_all(exp.With)
        for with_clause in with_clauses:
            for cte in with_clause.expressions:
                if hasattr(cte, 'alias'):
                    cte_name = str(cte.alias)
                    self.table_aliases[cte_name] = cte_name
                    self.reverse_aliases[cte_name] = cte_name
    
    def _extract_table_alias_from_expression(self, expr):
        """Extract table name and alias from a table expression"""
        if not expr:
            return
            
        try:
            table_name = None
            alias = None
            
            if hasattr(expr, 'name'):
                table_name = str(expr.name)
            
            if hasattr(expr, 'alias') and expr.alias:
                alias = str(expr.alias)
            else:
                alias = table_name
            
            if table_name and alias:
                self.table_aliases[alias] = table_name
                self.reverse_aliases[table_name] = alias
                
        except Exception as e:
            pass  # Skip problematic expressions
    
    def _extract_ctes(self, parsed_query):
        """Extract Common Table Expressions"""
        with_clauses = parsed_query.find_all(exp.With)
        
        for with_clause in with_clauses:
            for cte in with_clause.expressions:
                if hasattr(cte, 'alias') and hasattr(cte, 'this'):
                    cte_name = str(cte.alias)
                    
                    node = QueryNode(
                        name=cte_name,
                        node_type=NodeType.CTE,
                        alias=cte_name
                    )
                    
                    # Extract columns from CTE definition
                    node.all_columns = self._extract_select_columns(cte.this)
                    
                    self.nodes[cte_name] = node
    
    def _extract_main_query_tables(self, parsed_query):
        """Extract tables from main query"""
        # Find all table references
        tables = parsed_query.find_all(exp.Table)
        
        for table in tables:
            try:
                table_name = str(table.name) if hasattr(table, 'name') else str(table)
                
                # Skip if it's a CTE
                if table_name in self.nodes:
                    continue
                
                alias = str(table.alias) if hasattr(table, 'alias') and table.alias else table_name
                schema = str(table.db) if hasattr(table, 'db') and table.db else ""
                
                node = QueryNode(
                    name=table_name,
                    node_type=NodeType.TABLE,
                    alias=alias,
                    schema=schema
                )
                
                self.nodes[table_name] = node
                
            except Exception as e:
                continue  # Skip problematic tables
    
    def _extract_select_columns(self, select_expr) -> List[str]:
        """Extract column names from SELECT expression"""
        columns = []
        
        try:
            if hasattr(select_expr, 'expressions'):
                for expr in select_expr.expressions:
                    col_name = self._get_column_name(expr)
                    if col_name:
                        columns.append(col_name)
            elif hasattr(select_expr, 'find_all'):
                selects = select_expr.find_all(exp.Select)
                for select in selects:
                    if hasattr(select, 'expressions'):
                        for expr in select.expressions:
                            col_name = self._get_column_name(expr)
                            if col_name:
                                columns.append(col_name)
        except Exception:
            pass
            
        return columns[:10]  # Limit to first 10 columns
    
    def _get_column_name(self, expr) -> Optional[str]:
        """Extract column name from various expression types"""
        try:
            if hasattr(expr, 'alias') and expr.alias:
                return str(expr.alias)
            elif hasattr(expr, 'name'):
                return str(expr.name)
            elif str(expr) == '*':
                return '*'
            else:
                expr_str = str(expr)
                if len(expr_str) < 30:
                    return expr_str
        except Exception:
            pass
        return None
    
    def _analyze_all_joins(self, parsed_query):
        """Comprehensively analyze all joins in the query"""
        selects = parsed_query.find_all(exp.Select)
        
        for select in selects:
            self._analyze_select_joins(select)
            
        # Also check for implicit joins (comma-separated tables in FROM)
        self._analyze_implicit_joins(parsed_query)
    
    def _analyze_select_joins(self, select):
        """Analyze joins in a single SELECT statement"""
        if not hasattr(select, 'joins') or not select.joins:
            return
        
        # Get the base table from FROM clause
        base_table = None
        if hasattr(select, 'from_') and select.from_:
            base_table = self._get_table_name_from_expr(select.from_.this)
        
        # Analyze each join
        current_table = base_table
        
        for join in select.joins:
            joined_table = self._get_table_name_from_expr(join.this)
            
            if current_table and joined_table:
                join_type = self._determine_join_type(join)
                join_conditions = self._extract_join_conditions(join, current_table, joined_table)
                
                # Create edge for this join
                edge = QueryEdge(
                    source=current_table,
                    target=joined_table,
                    join_type=join_type,
                    join_conditions=join_conditions,
                    edge_type="join"
                )
                
                self.edges.append(edge)
                
                # Update join columns in nodes
                self._update_node_join_columns(current_table, joined_table, join_conditions)
                
                # For chain joins, next join builds on this result
                current_table = joined_table
    
    def _analyze_implicit_joins(self, parsed_query):
        """Analyze implicit joins (FROM table1, table2 WHERE table1.id = table2.id)"""
        selects = parsed_query.find_all(exp.Select)
        
        for select in selects:
            if hasattr(select, 'from_') and hasattr(select.from_, 'expressions'):
                # Multiple tables in FROM clause
                tables = []
                for expr in select.from_.expressions:
                    table_name = self._get_table_name_from_expr(expr)
                    if table_name:
                        tables.append(table_name)
                
                if len(tables) > 1:
                    # Look for join conditions in WHERE clause
                    if hasattr(select, 'where') and select.where:
                        join_conditions = self._extract_where_join_conditions(select.where, tables)
                        
                        # Create implicit join edges
                        for i in range(len(tables) - 1):
                            edge = QueryEdge(
                                source=tables[i],
                                target=tables[i + 1],
                                join_type=JoinType.IMPLICIT,
                                join_conditions=join_conditions,
                                edge_type="join"
                            )
                            self.edges.append(edge)
    
    def _get_table_name_from_expr(self, expr) -> Optional[str]:
        """Get table name from table expression"""
        if not expr:
            return None
            
        try:
            if hasattr(expr, 'alias') and expr.alias:
                alias = str(expr.alias)
                return self.table_aliases.get(alias, alias)
            elif hasattr(expr, 'name'):
                table_name = str(expr.name)
                return table_name
            else:
                return str(expr)
        except Exception:
            return None
    
    def _determine_join_type(self, join) -> JoinType:
        """Determine the type of join"""
        if not hasattr(join, 'kind') or not join.kind:
            return JoinType.INNER
        
        join_kind = str(join.kind).upper()
        
        if 'LEFT' in join_kind:
            return JoinType.LEFT
        elif 'RIGHT' in join_kind:
            return JoinType.RIGHT
        elif 'FULL' in join_kind:
            return JoinType.FULL
        elif 'CROSS' in join_kind:
            return JoinType.CROSS
        else:
            return JoinType.INNER
    
    def _extract_join_conditions(self, join, left_table: str, right_table: str) -> List[JoinCondition]:
        """Extract join conditions from ON clause"""
        conditions = []
        
        if not hasattr(join, 'on') or not join.on:
            return conditions
        
        try:
            condition_str = str(join.on)
            
            # Handle equality conditions
            eq_exprs = join.on.find_all(exp.EQ) if hasattr(join.on, 'find_all') else []
            
            for eq in eq_exprs:
                if hasattr(eq, 'left') and hasattr(eq, 'right'):
                    left_col = self._extract_column_reference(eq.left, left_table, right_table)
                    right_col = self._extract_column_reference(eq.right, left_table, right_table)
                    
                    if left_col and right_col:
                        condition = JoinCondition(
                            left_table=left_col['table'],
                            left_column=left_col['column'],
                            right_table=right_col['table'],
                            right_column=right_col['column'],
                            operator="="
                        )
                        conditions.append(condition)
            
            # Fallback: parse string patterns
            if not conditions:
                conditions = self._parse_join_condition_string(condition_str, left_table, right_table)
                
        except Exception as e:
            pass
        
        return conditions
    
    def _extract_column_reference(self, expr, left_table: str, right_table: str) -> Optional[Dict[str, str]]:
        """Extract table and column from a column reference expression"""
        try:
            if hasattr(expr, 'table') and hasattr(expr, 'name'):
                # Qualified column reference (table.column)
                table_ref = str(expr.table)
                column_name = str(expr.name)
                
                # Resolve alias to actual table name
                actual_table = self.table_aliases.get(table_ref, table_ref)
                
                return {'table': actual_table, 'column': column_name}
                
            elif hasattr(expr, 'name'):
                # Unqualified column reference - try to determine table from context
                column_name = str(expr.name)
                
                # Simple heuristic: assume it belongs to one of the join tables
                # In practice, you'd need more sophisticated logic
                return {'table': left_table, 'column': column_name}
                
        except Exception:
            pass
        
        return None
    
    def _parse_join_condition_string(self, condition_str: str, left_table: str, right_table: str) -> List[JoinCondition]:
        """Parse join conditions from string (fallback method)"""
        conditions = []
        
        # Pattern for qualified column references: table.column = table.column
        pattern = r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
        matches = re.findall(pattern, condition_str)
        
        for match in matches:
            left_table_alias, left_col, right_table_alias, right_col = match
            
            # Resolve aliases
            left_table_name = self.table_aliases.get(left_table_alias, left_table_alias)
            right_table_name = self.table_aliases.get(right_table_alias, right_table_alias)
            
            condition = JoinCondition(
                left_table=left_table_name,
                left_column=left_col,
                right_table=right_table_name,
                right_column=right_col,
                operator="="
            )
            conditions.append(condition)
        
        return conditions
    
    def _extract_where_join_conditions(self, where_clause, tables: List[str]) -> List[JoinCondition]:
        """Extract join conditions from WHERE clause for implicit joins"""
        conditions = []
        
        try:
            where_str = str(where_clause)
            
            # Look for equality conditions between different tables
            pattern = r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
            matches = re.findall(pattern, where_str)
            
            for match in matches:
                left_table_alias, left_col, right_table_alias, right_col = match
                
                # Resolve aliases
                left_table_name = self.table_aliases.get(left_table_alias, left_table_alias)
                right_table_name = self.table_aliases.get(right_table_alias, right_table_alias)
                
                # Only include if both tables are in our table list
                if left_table_name in tables and right_table_name in tables:
                    condition = JoinCondition(
                        left_table=left_table_name,
                        left_column=left_col,
                        right_table=right_table_name,
                        right_column=right_col,
                        operator="="
                    )
                    conditions.append(condition)
        
        except Exception:
            pass
        
        return conditions
    
    def _update_node_join_columns(self, left_table: str, right_table: str, conditions: List[JoinCondition]):
        """Update nodes with join-specific columns"""
        for condition in conditions:
            # Add join columns to respective nodes
            if condition.left_table in self.nodes:
                if condition.left_column not in self.nodes[condition.left_table].join_columns:
                    self.nodes[condition.left_table].join_columns.append(condition.left_column)
            
            if condition.right_table in self.nodes:
                if condition.right_column not in self.nodes[condition.right_table].join_columns:
                    self.nodes[condition.right_table].join_columns.append(condition.right_column)
    
    def _build_cte_dependencies(self, parsed_query):
        """Build CTE dependency relationships"""
        for cte_name, node in self.nodes.items():
            if node.node_type == NodeType.CTE:
                # Find what this CTE depends on
                dependencies = self._find_cte_table_dependencies(parsed_query, cte_name)
                
                for dep in dependencies:
                    if dep in self.nodes and dep != cte_name:
                        edge = QueryEdge(
                            source=dep,
                            target=cte_name,
                            join_type=JoinType.INNER,  # Default for dependencies
                            edge_type="cte_dependency"
                        )
                        self.edges.append(edge)
    
    def _find_cte_table_dependencies(self, parsed_query, cte_name: str) -> List[str]:
        """Find what tables/CTEs a CTE depends on"""
        dependencies = []
        
        with_clauses = parsed_query.find_all(exp.With)
        for with_clause in with_clauses:
            for cte in with_clause.expressions:
                if hasattr(cte, 'alias') and str(cte.alias) == cte_name:
                    # Find table references in this CTE
                    tables = cte.this.find_all(exp.Table)
                    for table in tables:
                        table_name = str(table.name) if hasattr(table, 'name') else str(table)
                        if table_name != cte_name:
                            dependencies.append(table_name)
        
        return list(set(dependencies))
    
    def _generate_join_summary(self) -> Dict[str, Any]:
        """Generate a summary of join analysis"""
        join_edges = [e for e in self.edges if e.edge_type == "join"]
        
        summary = {
            'total_joins': len(join_edges),
            'join_types': {},
            'tables_with_joins': set(),
            'join_conditions_count': 0
        }
        
        for edge in join_edges:
            # Count join types
            join_type = edge.join_type.value
            summary['join_types'][join_type] = summary['join_types'].get(join_type, 0) + 1
            
            # Track tables involved in joins
            summary['tables_with_joins'].add(edge.source)
            summary['tables_with_joins'].add(edge.target)
            
            # Count join conditions
            summary['join_conditions_count'] += len(edge.join_conditions)
        
        summary['tables_with_joins'] = list(summary['tables_with_joins'])
        
        return summary


class EnhancedJoinVisualizer:
    """Enhanced visualizer focused on join relationships"""
    
    def __init__(self):
        self.colors = {
            NodeType.TABLE: '#E3F2FD',      # Light blue
            NodeType.CTE: '#E8F5E8',        # Light green
            NodeType.SUBQUERY: '#FFF3E0',   # Light orange
            NodeType.DERIVED_TABLE: '#F3E5F5'  # Light purple
        }
        
        self.join_type_colors = {
            JoinType.INNER: '#1976D2',      # Blue
            JoinType.LEFT: '#4CAF50',       # Green
            JoinType.RIGHT: '#FF9800',      # Orange
            JoinType.FULL: '#9C27B0',       # Purple
            JoinType.CROSS: '#F44336',      # Red
            JoinType.IMPLICIT: '#607D8B'    # Blue Grey
        }
    
    def generate_diagram(self, query_data: Dict[str, Any], output_path: str = "enhanced_join_diagram"):
        """Generate enhanced diagram focused on joins"""
        nodes = query_data['nodes']
        edges = query_data['edges']
        join_summary = query_data.get('join_summary', {})
        
        # Create graphviz digraph
        dot = graphviz.Digraph(comment='Enhanced SQL Join Diagram')
        dot.attr(rankdir='LR')
        dot.attr('node', shape='record', style='filled', fontname='Arial', fontsize='10')
        dot.attr('edge', fontname='Arial', fontsize='9')
        dot.attr('graph', splines='ortho', nodesep='0.6', ranksep='1.0')
        
        # Add title with join summary
        if join_summary:
            title = f"SQL Join Analysis\\n"
            title += f"Total Joins: {join_summary.get('total_joins', 0)} | "
            title += f"Join Conditions: {join_summary.get('join_conditions_count', 0)}"
            
            join_types = join_summary.get('join_types', {})
            if join_types:
                title += "\\nJoin Types: " + ", ".join([f"{k}: {v}" for k, v in join_types.items()])
            
            dot.attr(label=title, labelloc='top', fontsize='12')
        
        # Add nodes with join-focused information
        for node_name, node in nodes.items():
            self._add_join_focused_node(dot, node_name, node)
        
        # Add edges with detailed join information
        for edge in edges:
            self._add_detailed_join_edge(dot, edge)
        
        # Render diagram
        try:
            dot.render(output_path, format='svg', cleanup=True)
            print(f"Enhanced join diagram saved as {output_path}.svg")
            
            dot.render(output_path, format='png', cleanup=True)
            print(f"Enhanced join diagram saved as {output_path}.png")
            
        except Exception as e:
            print(f"Error generating diagram: {e}")
    
    def _add_join_focused_node(self, dot, node_name: str, node: QueryNode):
        """Add node with focus on join columns"""
        color = self.colors.get(node.node_type, '#FFFFFF')
        
        # Create label focusing on join information
        label_parts = []
        
        # Node title
        if node.alias != node.name:
            label_parts.append(f"{{<title>{node.alias}\\n({node.name})}}")
        else:
            label_parts.append(f"{{<title>{node.name}}}")
        
        # Join columns (most important)
        if node.join_columns:
            join_cols = " | ".join([f"<{col}>{col}" for col in node.join_columns[:5]])
            label_parts.append(f"{{JOIN COLS|{join_cols}}}")
        
        # Additional columns if any
        other_cols = [col for col in node.all_columns if col not in node.join_columns]
        if other_cols:
            display_cols = other_cols[:3]
            if len(other_cols) > 3:
                display_cols.append(f"... (+{len(other_cols) - 3})")
            other_cols_str = " | ".join(display_cols)
            label_parts.append(f"{{OTHER|{other_cols_str}}}")
        
        # Schema info
        if node.schema:
            label_parts.append(f"{{SCHEMA|{node.schema}}}")
        
        label = " | ".join(label_parts)
        
        dot.node(node_name, label=label, fillcolor=color)
    
    def _add_detailed_join_edge(self, dot, edge: QueryEdge):
        """Add edge with detailed join information"""
        if edge.edge_type == "join":
            color = self.join_type_colors.get(edge.join_type, '#000000')
            
            # Create detailed join label
            label_parts = [f"{edge.join_type.value} JOIN"]
            
            # Add join conditions
            for condition in edge.join_conditions:
                label_parts.append(str(condition))
            
            label = "\\n".join(label_parts)
            
            # Style based on join type
            style = 'bold' if edge.join_type == JoinType.INNER else 'solid'
            penwidth = '3' if edge.join_type == JoinType.INNER else '2'
            
            dot.edge(edge.source, edge.target, 
                    label=label, 
                    color=color,
                    style=style,
                    penwidth=penwidth,
                    arrowsize='1.0')
        else:
            # CTE dependency or other relationship
            dot.edge(edge.source, edge.target, 
                    label=edge.edge_type.replace('_', ' ').title(),
                    color='#757575',
                    style='dashed')


@click.command()
@click.option('--sql-file', '-f', type=click.Path(exists=True), help='Path to SQL file')
@click.option('--sql', '-s', type=str, help='SQL query string')
@click.option('--output', '-o', default='enhanced_join_diagram', help='Output file name (without extension)')
@click.option('--dialect', '-d', default='', help='SQL dialect (postgres, mysql, bigquery, etc.)')
@click.option('--verbose', '-v', is_flag=True, help='Show detailed join analysis')
def main(sql_file, sql, output, dialect, verbose):
    """Enhanced SQL Query Visualizer - Focus on Join Analysis"""
    
    if not sql_file and not sql:
        click.echo("Error: Must provide either --sql-file or --sql")
        return
    
    # Read SQL content
    sql_content = ""
    if sql_file:
        with open(sql_file, 'r') as f:
            sql_content = f.read()
    else:
        sql_content = sql
    
    try:
        # Parse SQL with enhanced join analyzer
        analyzer = EnhancedJoinAnalyzer(dialect=dialect)
        query_data = analyzer.parse_query(sql_content)
        
        # Generate enhanced join diagram
        visualizer = EnhancedJoinVisualizer()
        visualizer.generate_diagram(query_data, output)
        
        # Print detailed analysis
        click.echo(f"\n✅ Enhanced join analysis complete!")
        
        join_summary = query_data.get('join_summary', {})
        click.echo(f"📊 Join Summary:")
        click.echo(f"   Total Joins: {join_summary.get('total_joins', 0)}")
        click.echo(f"   Join Conditions: {join_summary.get('join_conditions_count', 0)}")
        
        join_types = join_summary.get('join_types', {})
        if join_types:
            click.echo("   Join Types:")
            for join_type, count in join_types.items():
                click.echo(f"     {join_type}: {count}")
        
        if verbose:
            click.echo(f"\n🔍 Detailed Join Information:")
            join_edges = [e for e in query_data['edges'] if e.edge_type == "join"]
            for edge in join_edges:
                click.echo(f"   {edge.source} → {edge.target} ({edge.join_type.value})")
                for condition in edge.join_conditions:
                    click.echo(f"     {condition}")
        
        click.echo(f"\n📁 Output: {output}.svg and {output}.png")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}")


if __name__ == '__main__':
    main()