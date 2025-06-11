#!/usr/bin/env python3
"""
Hierarchical SQL Query Visualizer
Properly handles nested SELECT statements, WITH clauses, and query encapsulation.
Shows progression from isolated SELECT statements to grouped joins to nested structures.
"""

import sqlglot
from sqlglot import exp
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Optional, Any, Union
import graphviz
import click
import json
from enum import Enum
import uuid


class QueryNodeType(Enum):
    SELECT_STATEMENT = "select"
    WITH_CLAUSE = "with"
    SUBQUERY = "subquery"
    CTE = "cte"
    TABLE_REFERENCE = "table"


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"


@dataclass
class QueryElement:
    """Represents any element in the query hierarchy"""
    id: str
    node_type: QueryNodeType
    name: str
    alias: str = ""
    level: int = 0  # Nesting level
    parent_id: Optional[str] = None
    children_ids: List[str] = field(default_factory=list)
    tables: List[str] = field(default_factory=list)  # Tables referenced
    join_columns: List[str] = field(default_factory=list)
    sql_snippet: str = ""
    
    def __post_init__(self):
        if not self.alias:
            self.alias = self.name


@dataclass
class QueryJoin:
    """Represents a join relationship between query elements"""
    left_element_id: str
    right_element_id: str
    join_type: JoinType
    join_conditions: List[Tuple[str, str]] = field(default_factory=list)  # (left_col, right_col)
    level: int = 0  # At which nesting level this join occurs


class HierarchicalSQLParser:
    """Parser that understands the hierarchical structure of SQL queries"""
    
    def __init__(self):
        self.elements: Dict[str, QueryElement] = {}
        self.joins: List[QueryJoin] = []
        self.hierarchy_levels: Dict[int, List[str]] = {}  # level -> element_ids
        self.max_level: int = 0
    
    def parse_query(self, sql: str) -> Dict[str, Any]:
        """Parse SQL with full hierarchical understanding"""
        try:
            # Parse with SQLGlot
            parsed = sqlglot.parse_one(sql, read='')
            
            # Reset state
            self.elements = {}
            self.joins = []
            self.hierarchy_levels = {}
            self.max_level = 0
            
            # Build hierarchical structure
            self._analyze_query_hierarchy(parsed, level=0, parent_id=None)
            
            # Organize elements by level
            self._organize_by_levels()
            
            # Analyze joins within and across levels
            self._analyze_hierarchical_joins()
            
            return {
                'elements': self.elements,
                'joins': self.joins,
                'hierarchy_levels': self.hierarchy_levels,
                'max_level': self.max_level,
                'summary': self._generate_hierarchy_summary()
            }
            
        except Exception as e:
            print(f"Error parsing SQL: {e}")
            return {
                'elements': {},
                'joins': [],
                'hierarchy_levels': {},
                'max_level': 0,
                'summary': {}
            }
    
    def _analyze_query_hierarchy(self, node, level: int, parent_id: Optional[str]):
        """Recursively analyze the query hierarchy"""
        if isinstance(node, exp.Select):
            return self._process_select_statement(node, level, parent_id)
            
        elif isinstance(node, exp.With):
            return self._process_with_clause(node, level, parent_id)
            
        elif isinstance(node, exp.Subquery):
            return self._process_subquery(node, level, parent_id)
            
        elif isinstance(node, exp.Table):
            return self._process_table_reference(node, level, parent_id)
        
        # For other node types, continue traversing
        element_ids = []
        
        # Check if node has common SQL structure attributes
        if hasattr(node, 'args') and hasattr(node.args, 'get'):
            # This is a SQLGlot expression with arguments
            for key, value in node.args.items():
                if value is not None:
                    if isinstance(value, list):
                        for item in value:
                            if hasattr(item, '__class__') and 'exp.' in str(item.__class__):
                                child_ids = self._analyze_query_hierarchy(item, level, parent_id)
                                element_ids.extend(child_ids)
                    elif hasattr(value, '__class__') and 'exp.' in str(value.__class__):
                        child_ids = self._analyze_query_hierarchy(value, level, parent_id)
                        element_ids.extend(child_ids)
        
        return element_ids
    
    def _process_select_statement(self, select_node, level: int, parent_id: Optional[str]) -> List[str]:
        """Process a SELECT statement"""
        element_id = str(uuid.uuid4())[:8]
        
        # Extract basic info
        tables = self._extract_tables_from_select(select_node)
        sql_snippet = str(select_node)[:100] + "..." if len(str(select_node)) > 100 else str(select_node)
        
        element = QueryElement(
            id=element_id,
            node_type=QueryNodeType.SELECT_STATEMENT,
            name=f"SELECT_{level}_{len([e for e in self.elements.values() if e.node_type == QueryNodeType.SELECT_STATEMENT])}",
            level=level,
            parent_id=parent_id,
            tables=tables,
            sql_snippet=sql_snippet
        )
        
        self.elements[element_id] = element
        self.max_level = max(self.max_level, level)
        
        # Add to parent's children
        if parent_id and parent_id in self.elements:
            self.elements[parent_id].children_ids.append(element_id)
        
        # Process child elements (subqueries, joins, etc.)
        child_ids = []
        
        # Process FROM clause
        if hasattr(select_node, 'from_') and select_node.from_:
            from_child_ids = self._analyze_query_hierarchy(select_node.from_, level + 1, element_id)
            child_ids.extend(from_child_ids)
        
        # Process JOINs
        if hasattr(select_node, 'joins') and select_node.joins:
            for join in select_node.joins:
                join_child_ids = self._analyze_query_hierarchy(join, level + 1, element_id)
                child_ids.extend(join_child_ids)
        
        # Process WHERE clause for subqueries
        if hasattr(select_node, 'where') and select_node.where:
            where_child_ids = self._analyze_query_hierarchy(select_node.where, level + 1, element_id)
            child_ids.extend(where_child_ids)
        
        # Process SELECT expressions for subqueries
        if hasattr(select_node, 'expressions') and select_node.expressions:
            for expr in select_node.expressions:
                expr_child_ids = self._analyze_query_hierarchy(expr, level + 1, element_id)
                child_ids.extend(expr_child_ids)
        
        element.children_ids.extend(child_ids)
        
        return [element_id]
    
    def _process_with_clause(self, with_node, level: int, parent_id: Optional[str]) -> List[str]:
        """Process a WITH clause (CTE container)"""
        element_id = str(uuid.uuid4())[:8]
        
        cte_names = []
        if hasattr(with_node, 'expressions'):
            for cte_expr in with_node.expressions:
                if hasattr(cte_expr, 'alias'):
                    cte_names.append(str(cte_expr.alias))
        
        element = QueryElement(
            id=element_id,
            node_type=QueryNodeType.WITH_CLAUSE,
            name=f"WITH_CLAUSE",
            alias=f"WITH ({', '.join(cte_names)})",
            level=level,
            parent_id=parent_id,
            sql_snippet=f"WITH {', '.join(cte_names)} AS ..."
        )
        
        self.elements[element_id] = element
        self.max_level = max(self.max_level, level)
        
        # Process individual CTEs
        child_ids = []
        if hasattr(with_node, 'expressions'):
            for cte_expr in with_node.expressions:
                cte_id = self._process_cte(cte_expr, level + 1, element_id)
                child_ids.extend(cte_id)
        
        # Process the main query that comes after WITH
        if hasattr(with_node, 'this') and with_node.this:
            main_query_ids = self._analyze_query_hierarchy(with_node.this, level + 1, element_id)
            child_ids.extend(main_query_ids)
        
        element.children_ids = child_ids
        
        return [element_id]
    
    def _process_cte(self, cte_node, level: int, parent_id: str) -> List[str]:
        """Process a single CTE"""
        element_id = str(uuid.uuid4())[:8]
        
        cte_name = str(cte_node.alias) if hasattr(cte_node, 'alias') else f"CTE_{element_id}"
        
        element = QueryElement(
            id=element_id,
            node_type=QueryNodeType.CTE,
            name=cte_name,
            alias=cte_name,
            level=level,
            parent_id=parent_id,
            sql_snippet=f"{cte_name} AS (SELECT ...)"
        )
        
        self.elements[element_id] = element
        self.max_level = max(self.max_level, level)
        
        # Process the CTE's SELECT statement
        child_ids = []
        if hasattr(cte_node, 'this'):
            cte_select_ids = self._analyze_query_hierarchy(cte_node.this, level + 1, element_id)
            child_ids.extend(cte_select_ids)
        
        element.children_ids = child_ids
        
        return [element_id]
    
    def _process_subquery(self, subquery_node, level: int, parent_id: Optional[str]) -> List[str]:
        """Process a subquery"""
        element_id = str(uuid.uuid4())[:8]
        
        alias = str(subquery_node.alias) if hasattr(subquery_node, 'alias') and subquery_node.alias else f"SUBQ_{element_id}"
        
        element = QueryElement(
            id=element_id,
            node_type=QueryNodeType.SUBQUERY,
            name=f"SUBQUERY_{element_id}",
            alias=alias,
            level=level,
            parent_id=parent_id,
            sql_snippet=f"({str(subquery_node.this)[:50]}...)" if hasattr(subquery_node, 'this') else "(SELECT ...)"
        )
        
        self.elements[element_id] = element
        self.max_level = max(self.max_level, level)
        
        # Process the subquery's content
        child_ids = []
        if hasattr(subquery_node, 'this'):
            subq_content_ids = self._analyze_query_hierarchy(subquery_node.this, level + 1, element_id)
            child_ids.extend(subq_content_ids)
        
        element.children_ids = child_ids
        
        return [element_id]
    
    def _process_table_reference(self, table_node, level: int, parent_id: Optional[str]) -> List[str]:
        """Process a table reference"""
        element_id = str(uuid.uuid4())[:8]
        
        table_name = str(table_node.name) if hasattr(table_node, 'name') else str(table_node)
        alias = str(table_node.alias) if hasattr(table_node, 'alias') and table_node.alias else table_name
        
        element = QueryElement(
            id=element_id,
            node_type=QueryNodeType.TABLE_REFERENCE,
            name=table_name,
            alias=alias,
            level=level,
            parent_id=parent_id,
            tables=[table_name],
            sql_snippet=f"{table_name} {alias}" if alias != table_name else table_name
        )
        
        self.elements[element_id] = element
        self.max_level = max(self.max_level, level)
        
        return [element_id]
    
    def _extract_tables_from_select(self, select_node) -> List[str]:
        """Extract table names from a SELECT statement"""
        tables = []
        
        # Get tables from FROM clause
        if hasattr(select_node, 'from_') and select_node.from_:
            from_tables = select_node.from_.find_all(exp.Table)
            for table in from_tables:
                if hasattr(table, 'name'):
                    tables.append(str(table.name))
        
        # Get tables from JOINs
        if hasattr(select_node, 'joins') and select_node.joins:
            for join in select_node.joins:
                join_tables = join.find_all(exp.Table)
                for table in join_tables:
                    if hasattr(table, 'name'):
                        tables.append(str(table.name))
        
        return list(set(tables))  # Remove duplicates
    
    def _organize_by_levels(self):
        """Organize elements by their hierarchy levels"""
        for element in self.elements.values():
            level = element.level
            if level not in self.hierarchy_levels:
                self.hierarchy_levels[level] = []
            self.hierarchy_levels[level].append(element.id)
    
    def _analyze_hierarchical_joins(self):
        """Analyze joins within the hierarchical structure"""
        # This is simplified - would need more sophisticated join analysis
        # For now, we'll detect when elements reference similar tables
        pass
    
    def _generate_hierarchy_summary(self) -> Dict[str, Any]:
        """Generate summary of the hierarchy"""
        summary = {
            'total_elements': len(self.elements),
            'max_nesting_level': self.max_level,
            'elements_by_type': {},
            'elements_by_level': {}
        }
        
        # Count by type
        for element in self.elements.values():
            node_type = element.node_type.value
            summary['elements_by_type'][node_type] = summary['elements_by_type'].get(node_type, 0) + 1
        
        # Count by level
        for level, element_ids in self.hierarchy_levels.items():
            summary['elements_by_level'][level] = len(element_ids)
        
        return summary


class HierarchicalDiagramGenerator:
    """Generates diagrams showing hierarchical query structure"""
    
    def __init__(self):
        self.node_colors = {
            QueryNodeType.SELECT_STATEMENT: '#E3F2FD',    # Light blue
            QueryNodeType.WITH_CLAUSE: '#E8F5E8',         # Light green
            QueryNodeType.CTE: '#FFF3E0',                 # Light orange
            QueryNodeType.SUBQUERY: '#F3E5F5',            # Light purple
            QueryNodeType.TABLE_REFERENCE: '#F0F0F0'      # Light gray
        }
        
        self.border_colors = {
            QueryNodeType.SELECT_STATEMENT: '#1976D2',    # Blue
            QueryNodeType.WITH_CLAUSE: '#388E3C',         # Green
            QueryNodeType.CTE: '#F57C00',                 # Orange
            QueryNodeType.SUBQUERY: '#7B1FA2',            # Purple
            QueryNodeType.TABLE_REFERENCE: '#757575'      # Gray
        }
    
    def generate_diagram(self, data: Dict[str, Any], output_path: str):
        """Generate hierarchical diagram"""
        elements = data['elements']
        joins = data['joins']
        hierarchy_levels = data['hierarchy_levels']
        max_level = data['max_level']
        summary = data['summary']
        
        dot = graphviz.Digraph(comment='Hierarchical SQL Query Structure')
        dot.attr(rankdir='LR')  # Left to right
        dot.attr('node', shape='box', style='filled', fontname='Arial')
        dot.attr('edge', fontname='Arial', fontsize='9')
        dot.attr('graph', 
                 splines='ortho',
                 nodesep='1.2',
                 ranksep='2.0',
                 fontname='Arial')
        
        # Add title
        title = f"SQL Query Hierarchy\\n{summary.get('total_elements', 0)} Elements, {max_level + 1} Levels"
        dot.attr(label=title, labelloc='top', fontsize='14')
        
        # Create level-based layout
        self._create_level_based_layout(dot, elements, hierarchy_levels)
        
        # Add containment relationships
        self._add_containment_edges(dot, elements)
        
        # Add join relationships
        for join in joins:
            self._add_join_edge(dot, join, elements)
        
        # Render
        try:
            dot.render(output_path, format='svg', cleanup=True)
            dot.render(output_path, format='png', cleanup=True)
            print(f"✅ Hierarchical diagram saved: {output_path}.svg and {output_path}.png")
        except Exception as e:
            print(f"❌ Error generating diagram: {e}")
    
    def _create_level_based_layout(self, dot, elements: Dict[str, QueryElement], hierarchy_levels: Dict[int, List[str]]):
        """Create layout with elements grouped by hierarchy level"""
        for level in sorted(hierarchy_levels.keys()):
            element_ids = hierarchy_levels[level]
            
            # Create subgraph for this level
            with dot.subgraph() as level_graph:
                level_graph.attr(rank='same')
                level_graph.attr(label=f'Level {level}', style='dashed', color='gray')
                
                for element_id in element_ids:
                    if element_id in elements:
                        element = elements[element_id]
                        self._add_element_node(level_graph, element)
    
    def _add_element_node(self, graph, element: QueryElement):
        """Add a node for a query element"""
        fill_color = self.node_colors.get(element.node_type, '#FFFFFF')
        border_color = self.border_colors.get(element.node_type, '#000000')
        
        # Create label
        label = self._create_element_label(element)
        
        graph.node(element.id,
                  label=label,
                  fillcolor=fill_color,
                  color=border_color,
                  penwidth='2')
    
    def _create_element_label(self, element: QueryElement) -> str:
        """Create label for query element"""
        label_parts = []
        
        # Type icon and name
        type_icons = {
            QueryNodeType.SELECT_STATEMENT: "🔍",
            QueryNodeType.WITH_CLAUSE: "📦",
            QueryNodeType.CTE: "🔄",
            QueryNodeType.SUBQUERY: "📊",
            QueryNodeType.TABLE_REFERENCE: "📋"
        }
        
        icon = type_icons.get(element.node_type, "❓")
        label_parts.append(f"{icon} {element.alias}")
        
        # Add type
        label_parts.append(f"({element.node_type.value.upper()})")
        
        # Add level
        label_parts.append(f"Level {element.level}")
        
        # Add tables if any
        if element.tables:
            tables_str = ", ".join(element.tables[:3])
            if len(element.tables) > 3:
                tables_str += f" (+{len(element.tables) - 3})"
            label_parts.append(f"Tables: {tables_str}")
        
        # Add children count
        if element.children_ids:
            label_parts.append(f"Contains: {len(element.children_ids)} elements")
        
        return "\\n".join(label_parts)
    
    def _add_containment_edges(self, dot, elements: Dict[str, QueryElement]):
        """Add edges showing containment relationships"""
        for element in elements.values():
            if element.parent_id and element.parent_id in elements:
                dot.edge(element.parent_id, element.id,
                        label="contains",
                        style='dashed',
                        color='gray',
                        arrowsize='0.7')
    
    def _add_join_edge(self, dot, join: QueryJoin, elements: Dict[str, QueryElement]):
        """Add edge for join relationship"""
        if join.left_element_id in elements and join.right_element_id in elements:
            label = f"{join.join_type.value} JOIN"
            if join.join_conditions:
                conditions = [f"{left}={right}" for left, right in join.join_conditions[:2]]
                label += "\\n" + "\\n".join(conditions)
            
            dot.edge(join.left_element_id, join.right_element_id,
                    label=label,
                    color='blue',
                    style='bold',
                    penwidth='2')


@click.command()
@click.option('--sql-file', '-f', type=click.Path(exists=True), help='Path to SQL file')
@click.option('--sql', '-s', type=str, help='SQL query string')
@click.option('--output', '-o', default='hierarchical_query_diagram', help='Output file name')
@click.option('--verbose', '-v', is_flag=True, help='Show detailed hierarchy information')
def main(sql_file, sql, output, verbose):
    """Hierarchical SQL Query Visualizer"""
    
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
    
    # Parse with hierarchical understanding
    parser = HierarchicalSQLParser()
    data = parser.parse_query(sql_content)
    
    # Generate hierarchical diagram
    visualizer = HierarchicalDiagramGenerator()
    visualizer.generate_diagram(data, output)
    
    # Print analysis
    summary = data['summary']
    click.echo(f"\n📊 Hierarchical Query Analysis:")
    click.echo(f"   Total Elements: {summary.get('total_elements', 0)}")
    click.echo(f"   Nesting Levels: {summary.get('max_nesting_level', 0) + 1}")
    
    elements_by_type = summary.get('elements_by_type', {})
    if elements_by_type:
        click.echo("   Elements by Type:")
        for element_type, count in elements_by_type.items():
            click.echo(f"     {element_type}: {count}")
    
    if verbose:
        elements = data['elements']
        hierarchy_levels = data['hierarchy_levels']
        
        click.echo(f"\n🏗️  Hierarchy Structure:")
        for level in sorted(hierarchy_levels.keys()):
            click.echo(f"   Level {level}:")
            for element_id in hierarchy_levels[level]:
                element = elements[element_id]
                click.echo(f"     📍 {element.alias} ({element.node_type.value})")
                if element.children_ids:
                    click.echo(f"       └─ Contains {len(element.children_ids)} child elements")


if __name__ == '__main__':
    main()