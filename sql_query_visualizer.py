#!/usr/bin/env python3
"""
SQL Query Visualizer
A tool to parse complex SQL queries and generate static diagrams showing lineage,
relationships, and data flow.
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


@dataclass
class QueryNode:
    """Represents a node in the query graph (table, CTE, etc.)"""
    name: str
    node_type: NodeType
    alias: str = ""
    columns: List[str] = field(default_factory=list)
    level: int = 0  # For nested CTEs
    parent_cte: str = ""  # If this is nested within a CTE
    schema: str = ""
    
    def __post_init__(self):
        if not self.alias:
            self.alias = self.name


@dataclass
class QueryEdge:
    """Represents a relationship between nodes"""
    source: str
    target: str
    join_type: Optional[JoinType] = None
    join_keys: List[Tuple[str, str]] = field(default_factory=list)  # (source_col, target_col)
    edge_type: str = "data_flow"  # data_flow, join, cte_dependency


class SQLQueryParser:
    """Main parser class for SQL queries"""
    
    def __init__(self, dialect: str = ""):
        self.dialect = dialect
        self.nodes: Dict[str, QueryNode] = {}
        self.edges: List[QueryEdge] = []
        self.cte_hierarchy: Dict[str, List[str]] = {}  # CTE -> nested CTEs
        
    def parse_query(self, sql: str) -> Dict[str, Any]:
        """Parse SQL query and extract structure"""
        try:
            # Parse the SQL
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            
            # Reset state
            self.nodes = {}
            self.edges = []
            self.cte_hierarchy = {}
            
            # Extract CTEs first
            self._extract_ctes(parsed)
            
            # Extract main query components
            self._extract_main_query(parsed)
            
            # Build relationships
            self._build_relationships(parsed)
            
            return {
                'nodes': self.nodes,
                'edges': self.edges,
                'cte_hierarchy': self.cte_hierarchy
            }
            
        except Exception as e:
            raise Exception(f"Failed to parse SQL: {str(e)}")
    
    def _extract_ctes(self, parsed_query, level: int = 0, parent_cte: str = ""):
        """Extract Common Table Expressions"""
        if not hasattr(parsed_query, 'find_all'):
            return
            
        # Find WITH clauses
        with_clauses = parsed_query.find_all(exp.With)
        
        for with_clause in with_clauses:
            for cte in with_clause.expressions:
                if hasattr(cte, 'alias') and hasattr(cte, 'this'):
                    cte_name = cte.alias
                    cte_query = cte.this
                    
                    # Create CTE node
                    node = QueryNode(
                        name=cte_name,
                        node_type=NodeType.CTE,
                        alias=cte_name,
                        level=level,
                        parent_cte=parent_cte
                    )
                    
                    # Extract columns from CTE query
                    if hasattr(cte_query, 'find_all'):
                        select_exprs = cte_query.find_all(exp.Select)
                        for select_expr in select_exprs:
                            if hasattr(select_expr, 'expressions'):
                                for expr in select_expr.expressions:
                                    if hasattr(expr, 'alias'):
                                        node.columns.append(expr.alias)
                                    elif hasattr(expr, 'name'):
                                        node.columns.append(expr.name)
                    
                    self.nodes[cte_name] = node
                    
                    # Track CTE hierarchy
                    if parent_cte:
                        if parent_cte not in self.cte_hierarchy:
                            self.cte_hierarchy[parent_cte] = []
                        self.cte_hierarchy[parent_cte].append(cte_name)
                    
                    # Recursively extract nested CTEs
                    self._extract_ctes(cte_query, level + 1, cte_name)
    
    def _extract_main_query(self, parsed_query):
        """Extract tables and derived tables from main query"""
        # Find all table references
        tables = parsed_query.find_all(exp.Table)
        
        for table in tables:
            table_name = str(table.name) if hasattr(table, 'name') else str(table)
            
            # Skip if it's a CTE (already processed)
            if table_name in self.nodes:
                continue
                
            # Extract schema if present
            schema = ""
            if hasattr(table, 'db') and table.db:
                schema = str(table.db)
            
            # Create table node
            node = QueryNode(
                name=table_name,
                node_type=NodeType.TABLE,
                alias=str(table.alias) if hasattr(table, 'alias') and table.alias else table_name,
                schema=schema
            )
            
            self.nodes[table_name] = node
        
        # Find subqueries
        subqueries = parsed_query.find_all(exp.Subquery)
        for i, subquery in enumerate(subqueries):
            subquery_name = f"subquery_{i}"
            alias = str(subquery.alias) if hasattr(subquery, 'alias') and subquery.alias else subquery_name
            
            node = QueryNode(
                name=subquery_name,
                node_type=NodeType.SUBQUERY,
                alias=alias
            )
            
            self.nodes[subquery_name] = node
    
    def _build_relationships(self, parsed_query):
        """Build relationships between nodes"""
        # Find all FROM and JOIN clauses to build comprehensive relationships
        self._analyze_query_relationships(parsed_query)
        
        # Build CTE dependencies
        for cte_name in self.nodes:
            if self.nodes[cte_name].node_type == NodeType.CTE:
                # Find tables/CTEs referenced in this CTE
                dependencies = self._find_cte_dependencies(parsed_query, cte_name)
                for dep in dependencies:
                    if dep in self.nodes and dep != cte_name:
                        edge = QueryEdge(
                            source=dep,
                            target=cte_name,
                            edge_type="cte_dependency"
                        )
                        self.edges.append(edge)
    
    def _get_join_type(self, join) -> JoinType:
        """Extract join type from join expression"""
        if hasattr(join, 'kind') and join.kind:
            join_kind = str(join.kind).upper()
            if 'LEFT' in join_kind:
                return JoinType.LEFT
            elif 'RIGHT' in join_kind:
                return JoinType.RIGHT
            elif 'FULL' in join_kind:
                return JoinType.FULL
            elif 'CROSS' in join_kind:
                return JoinType.CROSS
        return JoinType.INNER
    
    def _extract_join_keys(self, join_condition) -> List[Tuple[str, str]]:
        """Extract join keys from join condition"""
        join_keys = []
        
        # This is a simplified extraction - in practice, you'd need more sophisticated parsing
        condition_str = str(join_condition)
        
        # Look for equality conditions like table1.col = table2.col
        equality_pattern = r'(\w+\.\w+)\s*=\s*(\w+\.\w+)'
        matches = re.findall(equality_pattern, condition_str)
        
        for match in matches:
            join_keys.append((match[0], match[1]))
        
        return join_keys
    
    def _get_table_from_join(self, join, side: str) -> Optional[str]:
        """Get table name from join (simplified)"""
        # This is a placeholder - would need more sophisticated logic
        return None
    
    def _find_cte_dependencies(self, parsed_query, cte_name: str) -> List[str]:
        """Find what tables/CTEs a given CTE depends on"""
        dependencies = []
        
        # Find the CTE definition
        with_clauses = parsed_query.find_all(exp.With)
        for with_clause in with_clauses:
            for cte in with_clause.expressions:
                if hasattr(cte, 'alias') and cte.alias == cte_name:
                    # Find all table/CTE references in this CTE's query
                    tables = cte.this.find_all(exp.Table)
                    for table in tables:
                        table_name = str(table.name) if hasattr(table, 'name') else str(table)
                        if table_name != cte_name:  # Don't include self-reference
                            dependencies.append(table_name)
        
        return dependencies


class DiagramGenerator:
    """Generates visual diagrams from parsed query structure"""
    
    def __init__(self):
        self.colors = {
            NodeType.TABLE: '#E3F2FD',      # Light blue
            NodeType.CTE: '#E8F5E8',        # Light green
            NodeType.SUBQUERY: '#FFF3E0',   # Light orange
            NodeType.DERIVED_TABLE: '#F3E5F5'  # Light purple
        }
        
        self.edge_colors = {
            'join': '#1976D2',           # Blue
            'cte_dependency': '#388E3C',  # Green
            'data_flow': '#757575'        # Gray
        }
    
    def generate_diagram(self, query_data: Dict[str, Any], output_path: str = "query_diagram"):
        """Generate diagram from parsed query data"""
        nodes = query_data['nodes']
        edges = query_data['edges']
        cte_hierarchy = query_data.get('cte_hierarchy', {})
        
        # Create graphviz digraph
        dot = graphviz.Digraph(comment='SQL Query Diagram')
        dot.attr(rankdir='LR')  # Left to right layout
        dot.attr('node', shape='box', style='rounded,filled')
        
        # Add CTE clusters (subgraphs)
        self._add_cte_clusters(dot, nodes, cte_hierarchy)
        
        # Add nodes
        for node_name, node in nodes.items():
            self._add_node(dot, node_name, node)
        
        # Add edges
        for edge in edges:
            self._add_edge(dot, edge)
        
        # Render diagram
        try:
            dot.render(output_path, format='svg', cleanup=True)
            print(f"Diagram saved as {output_path}.svg")
            
            # Also save as PNG
            dot.render(output_path, format='png', cleanup=True)
            print(f"Diagram saved as {output_path}.png")
            
        except Exception as e:
            print(f"Error generating diagram: {e}")
    
    def _add_cte_clusters(self, dot, nodes: Dict[str, QueryNode], cte_hierarchy: Dict[str, List[str]]):
        """Add CTE clusters as subgraphs"""
        cluster_id = 0
        
        for parent_cte, nested_ctes in cte_hierarchy.items():
            with dot.subgraph(name=f'cluster_{cluster_id}') as cluster:
                cluster.attr(label=f'CTE: {parent_cte}', style='dashed', color='blue')
                cluster_id += 1
                
                # Add nested CTEs to cluster
                for nested_cte in nested_ctes:
                    if nested_cte in nodes:
                        node = nodes[nested_cte]
                        self._add_node_to_cluster(cluster, nested_cte, node)
    
    def _add_node_to_cluster(self, cluster, node_name: str, node: QueryNode):
        """Add a node to a cluster"""
        color = self.colors.get(node.node_type, '#FFFFFF')
        
        # Create label with node details
        label = self._create_node_label(node_name, node)
        
        cluster.node(node_name, label=label, fillcolor=color)
    
    def _add_node(self, dot, node_name: str, node: QueryNode):
        """Add a node to the diagram"""
        color = self.colors.get(node.node_type, '#FFFFFF')
        
        # Create label with node details
        label = self._create_node_label(node_name, node)
        
        dot.node(node_name, label=label, fillcolor=color)
    
    def _create_node_label(self, node_name: str, node: QueryNode) -> str:
        """Create a formatted label for a node"""
        label_parts = []
        
        # Node name/alias
        if node.alias and node.alias != node.name:
            label_parts.append(f"{node.alias}\\n({node.name})")
        else:
            label_parts.append(node.name)
        
        # Schema if present
        if node.schema:
            label_parts.append(f"Schema: {node.schema}")
        
        # Type
        label_parts.append(f"Type: {node.node_type.value}")
        
        # Key columns (first few)
        if node.columns:
            cols_display = node.columns[:3]  # Show first 3 columns
            if len(node.columns) > 3:
                cols_display.append(f"... (+{len(node.columns) - 3} more)")
            label_parts.append("Columns:\\n" + "\\n".join(cols_display))
        
        return "\\n".join(label_parts)
    
    def _add_edge(self, dot, edge: QueryEdge):
        """Add an edge to the diagram"""
        color = self.edge_colors.get(edge.edge_type, '#000000')
        
        # Create edge label
        label_parts = []
        
        if edge.join_type:
            label_parts.append(f"{edge.join_type.value} JOIN")
        
        if edge.join_keys:
            key_strs = [f"{src} = {tgt}" for src, tgt in edge.join_keys]
            label_parts.extend(key_strs)
        
        label = "\\n".join(label_parts) if label_parts else ""
        
        dot.edge(edge.source, edge.target, label=label, color=color)


@click.command()
@click.option('--sql-file', '-f', type=click.Path(exists=True), help='Path to SQL file')
@click.option('--sql', '-s', type=str, help='SQL query string')
@click.option('--output', '-o', default='query_diagram', help='Output file name (without extension)')
@click.option('--dialect', '-d', default='', help='SQL dialect (postgres, mysql, bigquery, etc.)')
def main(sql_file, sql, output, dialect):
    """SQL Query Visualizer - Generate diagrams from SQL queries"""
    
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
        # Parse SQL
        parser = SQLQueryParser(dialect=dialect)
        query_data = parser.parse_query(sql_content)
        
        # Generate diagram
        generator = DiagramGenerator()
        generator.generate_diagram(query_data, output)
        
        # Print summary
        click.echo(f"\n✅ Successfully generated diagram!")
        click.echo(f"📊 Nodes: {len(query_data['nodes'])}")
        click.echo(f"🔗 Edges: {len(query_data['edges'])}")
        click.echo(f"📁 Output: {output}.svg and {output}.png")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}")


if __name__ == '__main__':
    main()
