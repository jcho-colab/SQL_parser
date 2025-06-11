#!/usr/bin/env python3
"""
Advanced SQL Query Visualizer
Enhanced version with better join analysis, improved CTE visualization, and more sophisticated diagram generation.
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
    VIEW = "view"


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
    dependencies: Set[str] = field(default_factory=set)  # What this node depends on
    size_estimate: str = ""  # Estimated relative size (small, medium, large)
    
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
    cardinality: str = ""  # one-to-one, one-to-many, many-to-many
    strength: float = 1.0  # Edge strength for layout


class AdvancedSQLQueryParser:
    """Advanced parser class for SQL queries with enhanced analysis"""
    
    def __init__(self, dialect: str = ""):
        self.dialect = dialect
        self.nodes: Dict[str, QueryNode] = {}
        self.edges: List[QueryEdge] = []
        self.cte_hierarchy: Dict[str, List[str]] = {}  # CTE -> nested CTEs
        self.table_aliases: Dict[str, str] = {}  # alias -> table_name mapping
        self.query_complexity: Dict[str, Any] = {}
        
    def parse_query(self, sql: str) -> Dict[str, Any]:
        """Parse SQL query and extract structure with advanced analysis"""
        try:
            # Parse the SQL
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            
            # Reset state
            self.nodes = {}
            self.edges = []
            self.cte_hierarchy = {}
            self.table_aliases = {}
            self.query_complexity = {}
            
            # Phase 1: Extract all components
            self._extract_ctes(parsed)
            self._extract_main_query(parsed)
            self._extract_subqueries(parsed)
            
            # Phase 2: Build relationships and analyze
            self._build_comprehensive_relationships(parsed)
            self._analyze_query_complexity()
            self._estimate_node_sizes()
            
            return {
                'nodes': self.nodes,
                'edges': self.edges,
                'cte_hierarchy': self.cte_hierarchy,
                'table_aliases': self.table_aliases,
                'complexity': self.query_complexity
            }
            
        except Exception as e:
            raise Exception(f"Failed to parse SQL: {str(e)}")
    
    def _extract_ctes(self, parsed_query, level: int = 0, parent_cte: str = ""):
        """Extract Common Table Expressions with enhanced analysis"""
        if not hasattr(parsed_query, 'find_all'):
            return
            
        # Find WITH clauses
        with_clauses = parsed_query.find_all(exp.With)
        
        for with_clause in with_clauses:
            for cte in with_clause.expressions:
                if hasattr(cte, 'alias') and hasattr(cte, 'this'):
                    cte_name = str(cte.alias)
                    cte_query = cte.this
                    
                    # Create CTE node with enhanced information
                    node = QueryNode(
                        name=cte_name,
                        node_type=NodeType.CTE,
                        alias=cte_name,
                        level=level,
                        parent_cte=parent_cte
                    )
                    
                    # Extract columns from CTE query
                    node.columns = self._extract_columns_from_query(cte_query)
                    
                    # Extract dependencies
                    node.dependencies = self._extract_dependencies_from_query(cte_query)
                    
                    self.nodes[cte_name] = node
                    
                    # Track CTE hierarchy
                    if parent_cte:
                        if parent_cte not in self.cte_hierarchy:
                            self.cte_hierarchy[parent_cte] = []
                        self.cte_hierarchy[parent_cte].append(cte_name)
                    
                    # Recursively extract nested CTEs
                    self._extract_ctes(cte_query, level + 1, cte_name)
    
    def _extract_main_query(self, parsed_query):
        """Extract tables and derived tables from main query with enhanced analysis"""
        # Extract table aliases first
        self._extract_table_aliases(parsed_query)
        
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
            
            # Get alias
            alias = str(table.alias) if hasattr(table, 'alias') and table.alias else table_name
            
            # Create table node
            node = QueryNode(
                name=table_name,
                node_type=NodeType.TABLE,
                alias=alias,
                schema=schema
            )
            
            # Store alias mapping
            if alias != table_name:
                self.table_aliases[alias] = table_name
            
            self.nodes[table_name] = node
    
    def _extract_subqueries(self, parsed_query, level: int = 0):
        """Extract subqueries with better identification"""
        subqueries = parsed_query.find_all(exp.Subquery)
        
        for i, subquery in enumerate(subqueries):
            subquery_name = f"subquery_{level}_{i}"
            alias = str(subquery.alias) if hasattr(subquery, 'alias') and subquery.alias else subquery_name
            
            node = QueryNode(
                name=subquery_name,
                node_type=NodeType.SUBQUERY,
                alias=alias,
                level=level
            )
            
            # Extract columns and dependencies from subquery
            if hasattr(subquery, 'this'):
                node.columns = self._extract_columns_from_query(subquery.this)
                node.dependencies = self._extract_dependencies_from_query(subquery.this)
            
            self.nodes[subquery_name] = node
            
            # Recursively process nested subqueries
            if hasattr(subquery, 'this'):
                self._extract_subqueries(subquery.this, level + 1)
    
    def _extract_table_aliases(self, parsed_query):
        """Extract table alias mappings"""
        # Find all FROM clauses and JOINs to get alias mappings
        select_statements = parsed_query.find_all(exp.Select)
        
        for select_stmt in select_statements:
            # Extract from FROM clause
            if hasattr(select_stmt, 'from_') and select_stmt.from_:
                if hasattr(select_stmt.from_, 'this'):
                    table_expr = select_stmt.from_.this
                    if hasattr(table_expr, 'name') and hasattr(table_expr, 'alias'):
                        table_name = str(table_expr.name)
                        alias = str(table_expr.alias) if table_expr.alias else table_name
                        if alias != table_name:
                            self.table_aliases[alias] = table_name
            
            # Extract from JOINs
            if hasattr(select_stmt, 'joins') and select_stmt.joins:
                for join in select_stmt.joins:
                    if hasattr(join, 'this'):
                        table_expr = join.this
                        if hasattr(table_expr, 'name') and hasattr(table_expr, 'alias'):
                            table_name = str(table_expr.name)
                            alias = str(table_expr.alias) if table_expr.alias else table_name
                            if alias != table_name:
                                self.table_aliases[alias] = table_name
    
    def _build_comprehensive_relationships(self, parsed_query):
        """Build comprehensive relationships with enhanced join analysis"""
        # Analyze all SELECT statements
        select_statements = parsed_query.find_all(exp.Select)
        
        for select_stmt in select_statements:
            self._analyze_select_statement(select_stmt)
        
        # Build CTE dependencies
        self._build_cte_dependencies()
        
        # Build subquery dependencies
        self._build_subquery_dependencies(parsed_query)
    
    def _analyze_select_statement(self, select_stmt):
        """Analyze a single SELECT statement for relationships"""
        # Get the main table from FROM clause
        main_table = None
        if hasattr(select_stmt, 'from_') and select_stmt.from_:
            main_table = self._get_table_name_from_expression(select_stmt.from_.this)
        
        # Analyze JOINs
        if hasattr(select_stmt, 'joins') and select_stmt.joins:
            for join in select_stmt.joins:
                self._analyze_join_comprehensive(join, main_table)
    
    def _analyze_join_comprehensive(self, join, main_table: Optional[str]):
        """Comprehensive join analysis"""
        join_type = self._get_join_type(join)
        joined_table = self._get_table_name_from_expression(join.this)
        
        if not joined_table:
            return
        
        # Extract join condition
        join_keys = []
        cardinality = ""
        
        if hasattr(join, 'on') and join.on:
            join_keys = self._extract_join_keys_comprehensive(join.on)
            cardinality = self._estimate_join_cardinality(join_keys)
        
        # Create edge for the join
        if main_table and joined_table:
            # Resolve aliases to actual table names
            source_table = self.table_aliases.get(main_table, main_table)
            target_table = self.table_aliases.get(joined_table, joined_table)
            
            edge = QueryEdge(
                source=source_table,
                target=target_table,
                join_type=join_type,
                join_keys=join_keys,
                edge_type="join",
                cardinality=cardinality,
                strength=self._calculate_join_strength(join_type, join_keys)
            )
            self.edges.append(edge)
    
    def _get_table_name_from_expression(self, expr) -> Optional[str]:
        """Extract table name from various expression types"""
        if not expr:
            return None
            
        if hasattr(expr, 'name'):
            return str(expr.name)
        elif hasattr(expr, 'this') and hasattr(expr.this, 'name'):
            return str(expr.this.name)
        elif hasattr(expr, 'alias'):
            return str(expr.alias)
        
        return str(expr) if expr else None
    
    def _extract_join_keys_comprehensive(self, join_condition) -> List[Tuple[str, str]]:
        """Enhanced join key extraction"""
        join_keys = []
        condition_str = str(join_condition)
        
        # Pattern for table.column = table.column
        qualified_pattern = r'(\w+\.\w+)\s*=\s*(\w+\.\w+)'
        matches = re.findall(qualified_pattern, condition_str)
        join_keys.extend(matches)
        
        # Pattern for simple column = column (when tables are clear from context)
        simple_pattern = r'\b(\w+)\s*=\s*(\w+)\b'
        simple_matches = re.findall(simple_pattern, condition_str)
        
        # Filter out simple matches that might be values rather than columns
        for match in simple_matches:
            if not any(match[0] in qualified for qualified in [m[0] + '.' + m[1] for m in matches]):
                if not match[0].isdigit() and not match[1].isdigit():  # Not numeric values
                    join_keys.append(match)
        
        return join_keys
    
    def _estimate_join_cardinality(self, join_keys: List[Tuple[str, str]]) -> str:
        """Estimate join cardinality based on join keys"""
        if not join_keys:
            return "unknown"
        
        # Simple heuristic: if join key contains 'id', likely one-to-many
        for source, target in join_keys:
            if 'id' in source.lower() or 'id' in target.lower():
                return "one-to-many"
        
        return "many-to-many"
    
    def _calculate_join_strength(self, join_type: JoinType, join_keys: List[Tuple[str, str]]) -> float:
        """Calculate join strength for layout purposes"""
        strength = 1.0
        
        # Stronger connections for INNER JOINs
        if join_type == JoinType.INNER:
            strength = 2.0
        elif join_type in [JoinType.LEFT, JoinType.RIGHT]:
            strength = 1.5
        
        # More join keys = stronger connection
        strength += len(join_keys) * 0.3
        
        return min(strength, 3.0)
    
    def _build_cte_dependencies(self):
        """Build CTE dependency edges"""
        for cte_name, node in self.nodes.items():
            if node.node_type == NodeType.CTE:
                for dep in node.dependencies:
                    if dep in self.nodes and dep != cte_name:
                        edge = QueryEdge(
                            source=dep,
                            target=cte_name,
                            edge_type="cte_dependency",
                            strength=1.8  # CTEs have strong dependencies
                        )
                        self.edges.append(edge)
    
    def _build_subquery_dependencies(self, parsed_query):
        """Build subquery dependency edges"""
        for node_name, node in self.nodes.items():
            if node.node_type == NodeType.SUBQUERY:
                for dep in node.dependencies:
                    if dep in self.nodes and dep != node_name:
                        edge = QueryEdge(
                            source=dep,
                            target=node_name,
                            edge_type="subquery_dependency",
                            strength=1.2
                        )
                        self.edges.append(edge)
    
    def _extract_dependencies_from_query(self, query) -> Set[str]:
        """Extract what tables/CTEs a query depends on"""
        dependencies = set()
        
        if not hasattr(query, 'find_all'):
            return dependencies
        
        # Find all table references
        tables = query.find_all(exp.Table)
        for table in tables:
            table_name = str(table.name) if hasattr(table, 'name') else str(table)
            dependencies.add(table_name)
        
        # Find CTE references (identifiers that match known CTEs)
        identifiers = query.find_all(exp.Identifier)
        for identifier in identifiers:
            identifier_name = str(identifier.name) if hasattr(identifier, 'name') else str(identifier)
            if identifier_name in self.nodes and self.nodes[identifier_name].node_type == NodeType.CTE:
                dependencies.add(identifier_name)
        
        return dependencies
    
    def _extract_columns_from_query(self, query) -> List[str]:
        """Extract column names from a query with better analysis"""
        columns = []
        
        if not hasattr(query, 'find_all'):
            return columns
            
        # Find SELECT expressions
        select_expressions = query.find_all(exp.Select)
        for select_expr in select_expressions:
            if hasattr(select_expr, 'expressions'):
                for expr in select_expr.expressions:
                    column_name = self._extract_column_name_comprehensive(expr)
                    if column_name:
                        columns.append(column_name)
        
        return list(set(columns))[:10]  # Limit to first 10 columns to avoid clutter
    
    def _extract_column_name_comprehensive(self, expression) -> Optional[str]:
        """Enhanced column name extraction"""
        try:
            # Handle aliased expressions
            if hasattr(expression, 'alias') and expression.alias:
                alias = str(expression.alias)
                if len(alias) < 30:  # Reasonable length
                    return alias
            
            # Handle simple column references
            if hasattr(expression, 'name'):
                name = str(expression.name)
                if len(name) < 30:
                    return name
            
            # Handle qualified column references (table.column)
            if hasattr(expression, 'this') and hasattr(expression.this, 'name'):
                name = str(expression.this.name)
                if len(name) < 30:
                    return name
            
            # Handle function calls - show function name
            if hasattr(expression, 'sql_name') and callable(expression.sql_name):
                try:
                    func_name = str(expression.sql_name())
                    if len(func_name) < 20:
                        return f"{func_name}(...)"
                except:
                    pass
            
            # Handle star expressions
            expr_str = str(expression)
            if expr_str in ['*', 'COUNT(*)', 'COUNT(1)']:
                return expr_str
            
            # For complex expressions, create a simplified representation
            if len(expr_str) < 40:
                return expr_str
            
            return None
        except Exception:
            # Fallback for any parsing issues
            try:
                expr_str = str(expression)
                if len(expr_str) < 30:
                    return expr_str
            except:
                pass
            return None
    
    def _analyze_query_complexity(self):
        """Analyze overall query complexity"""
        self.query_complexity = {
            'total_nodes': len(self.nodes),
            'total_edges': len(self.edges),
            'cte_count': len([n for n in self.nodes.values() if n.node_type == NodeType.CTE]),
            'table_count': len([n for n in self.nodes.values() if n.node_type == NodeType.TABLE]),
            'subquery_count': len([n for n in self.nodes.values() if n.node_type == NodeType.SUBQUERY]),
            'max_cte_depth': max([n.level for n in self.nodes.values() if n.node_type == NodeType.CTE], default=0),
            'join_count': len([e for e in self.edges if e.edge_type == "join"]),
            'complexity_score': self._calculate_complexity_score()
        }
    
    def _calculate_complexity_score(self) -> str:
        """Calculate overall complexity score"""
        score = 0
        score += len(self.nodes) * 1
        score += len(self.edges) * 2
        score += len([n for n in self.nodes.values() if n.node_type == NodeType.CTE]) * 3
        score += len([n for n in self.nodes.values() if n.node_type == NodeType.SUBQUERY]) * 2
        
        if score < 10:
            return "Simple"
        elif score < 25:
            return "Moderate"
        elif score < 50:
            return "Complex"
        else:
            return "Very Complex"
    
    def _estimate_node_sizes(self):
        """Estimate relative sizes of nodes for visualization"""
        for node_name, node in self.nodes.items():
            # Simple heuristic based on dependencies and node type
            size_score = len(node.dependencies)
            
            if node.node_type == NodeType.TABLE:
                size_score += 2  # Base tables are typically larger
            elif node.node_type == NodeType.CTE:
                size_score += 1
            
            if size_score <= 2:
                node.size_estimate = "small"
            elif size_score <= 4:
                node.size_estimate = "medium"
            else:
                node.size_estimate = "large"
    
    def _get_join_type(self, join) -> JoinType:
        """Extract join type from join expression"""
        if hasattr(join, 'kind') and join.kind:
            join_kind = str(join.kind).upper()
            if 'LEFT' in join_kind or 'LEFT_OUTER' in join_kind:
                return JoinType.LEFT
            elif 'RIGHT' in join_kind or 'RIGHT_OUTER' in join_kind:
                return JoinType.RIGHT
            elif 'FULL' in join_kind or 'FULL_OUTER' in join_kind:
                return JoinType.FULL
            elif 'CROSS' in join_kind:
                return JoinType.CROSS
        return JoinType.INNER


class AdvancedDiagramGenerator:
    """Advanced diagram generator with enhanced visualization"""
    
    def __init__(self):
        self.colors = {
            NodeType.TABLE: '#E3F2FD',      # Light blue
            NodeType.CTE: '#E8F5E8',        # Light green
            NodeType.SUBQUERY: '#FFF3E0',   # Light orange
            NodeType.DERIVED_TABLE: '#F3E5F5',  # Light purple
            NodeType.VIEW: '#F1F8E9'        # Light lime
        }
        
        self.border_colors = {
            NodeType.TABLE: '#1976D2',      # Blue
            NodeType.CTE: '#388E3C',        # Green
            NodeType.SUBQUERY: '#F57C00',   # Orange
            NodeType.DERIVED_TABLE: '#7B1FA2',  # Purple
            NodeType.VIEW: '#689F38'        # Lime
        }
        
        self.edge_colors = {
            'join': '#1976D2',           # Blue
            'cte_dependency': '#388E3C',  # Green
            'subquery_dependency': '#F57C00',  # Orange
            'data_flow': '#757575'        # Gray
        }
        
        self.size_attributes = {
            'small': {'width': '1.5', 'height': '1.0'},
            'medium': {'width': '2.0', 'height': '1.2'},
            'large': {'width': '2.5', 'height': '1.5'}
        }
    
    def generate_diagram(self, query_data: Dict[str, Any], output_path: str = "query_diagram"):
        """Generate enhanced diagram from parsed query data"""
        nodes = query_data['nodes']
        edges = query_data['edges']
        cte_hierarchy = query_data.get('cte_hierarchy', {})
        complexity = query_data.get('complexity', {})
        
        # Create graphviz digraph with enhanced attributes
        dot = graphviz.Digraph(comment='SQL Query Diagram')
        dot.attr(rankdir='LR')  # Left to right layout
        dot.attr('graph', 
                 splines='ortho',     # Orthogonal edges
                 nodesep='0.8',       # Space between nodes
                 ranksep='1.2',       # Space between ranks
                 fontname='Arial',
                 fontsize='12'
        )
        dot.attr('node', 
                 shape='box', 
                 style='rounded,filled',
                 fontname='Arial',
                 fontsize='10'
        )
        dot.attr('edge', 
                 fontname='Arial',
                 fontsize='9'
        )
        
        # Add title with complexity information
        if complexity:
            title = f"SQL Query Diagram\\n{complexity.get('complexity_score', 'Unknown')} Complexity"
            title += f" | {complexity.get('total_nodes', 0)} Nodes | {complexity.get('total_edges', 0)} Edges"
            dot.attr(label=title, labelloc='top', fontsize='14')
        
        # Add CTE clusters (subgraphs) with enhanced styling
        self._add_enhanced_cte_clusters(dot, nodes, cte_hierarchy)
        
        # Add nodes with enhanced styling
        for node_name, node in nodes.items():
            if node.parent_cte:  # Skip nodes that are in clusters
                continue
            self._add_enhanced_node(dot, node_name, node)
        
        # Add edges with enhanced styling
        for edge in edges:
            self._add_enhanced_edge(dot, edge)
        
        # Render diagram
        try:
            dot.render(output_path, format='svg', cleanup=True)
            print(f"Diagram saved as {output_path}.svg")
            
            # Also save as PNG
            dot.render(output_path, format='png', cleanup=True)
            print(f"Diagram saved as {output_path}.png")
            
        except Exception as e:
            print(f"Error generating diagram: {e}")
    
    def _add_enhanced_cte_clusters(self, dot, nodes: Dict[str, QueryNode], cte_hierarchy: Dict[str, List[str]]):
        """Add enhanced CTE clusters as subgraphs"""
        cluster_id = 0
        
        # Group CTEs by level for better organization
        cte_levels = defaultdict(list)
        for node_name, node in nodes.items():
            if node.node_type == NodeType.CTE and not node.parent_cte:
                cte_levels[node.level].append(node_name)
        
        for level, ctes in cte_levels.items():
            if len(ctes) > 1:  # Only create clusters for multiple CTEs
                with dot.subgraph(name=f'cluster_{cluster_id}') as cluster:
                    cluster.attr(
                        label=f'CTE Level {level}',
                        style='dashed,rounded',
                        color='darkgreen',
                        bgcolor='#F0F8F0',
                        fontcolor='darkgreen',
                        fontsize='11'
                    )
                    
                    for cte_name in ctes:
                        if cte_name in nodes:
                            self._add_enhanced_node(cluster, cte_name, nodes[cte_name])
                    
                    cluster_id += 1
        
        # Handle nested CTE clusters
        for parent_cte, nested_ctes in cte_hierarchy.items():
            if len(nested_ctes) > 0:
                with dot.subgraph(name=f'cluster_{cluster_id}') as cluster:
                    cluster.attr(
                        label=f'Nested under {parent_cte}',
                        style='dotted,rounded',
                        color='blue',
                        bgcolor='#F0F0FF',
                        fontcolor='blue',
                        fontsize='10'
                    )
                    
                    for nested_cte in nested_ctes:
                        if nested_cte in nodes:
                            self._add_enhanced_node(cluster, nested_cte, nodes[nested_cte])
                    
                    cluster_id += 1
    
    def _add_enhanced_node(self, dot, node_name: str, node: QueryNode):
        """Add an enhanced node to the diagram"""
        # Get colors and styling
        fill_color = self.colors.get(node.node_type, '#FFFFFF')
        border_color = self.border_colors.get(node.node_type, '#000000')
        size_attrs = self.size_attributes.get(node.size_estimate, self.size_attributes['medium'])
        
        # Create enhanced label
        label = self._create_enhanced_node_label(node_name, node)
        
        # Set node attributes
        dot.node(node_name, 
                label=label, 
                fillcolor=fill_color,
                color=border_color,
                penwidth='2',
                **size_attrs
        )
    
    def _create_enhanced_node_label(self, node_name: str, node: QueryNode) -> str:
        """Create an enhanced formatted label for a node"""
        label_parts = []
        
        # Node name/alias with type indicator
        type_icon = {
            NodeType.TABLE: "📋",
            NodeType.CTE: "🔄",
            NodeType.SUBQUERY: "📊",
            NodeType.DERIVED_TABLE: "🎯",
            NodeType.VIEW: "👁"
        }.get(node.node_type, "❓")
        
        if node.alias and node.alias != node.name:
            label_parts.append(f"{type_icon} {node.alias}\\n({node.name})")
        else:
            label_parts.append(f"{type_icon} {node.name}")
        
        # Schema if present
        if node.schema:
            label_parts.append(f"📁 {node.schema}")
        
        # Level for CTEs
        if node.node_type == NodeType.CTE and node.level > 0:
            label_parts.append(f"📊 Level {node.level}")
        
        # Key columns (first few)
        if node.columns:
            cols_display = node.columns[:4]  # Show first 4 columns
            if len(node.columns) > 4:
                cols_display.append(f"... (+{len(node.columns) - 4})")
            label_parts.append("📋 " + "\\n   ".join(cols_display))
        
        # Dependencies count
        if node.dependencies:
            label_parts.append(f"🔗 {len(node.dependencies)} deps")
        
        return "\\n".join(label_parts)
    
    def _add_enhanced_edge(self, dot, edge: QueryEdge):
        """Add an enhanced edge to the diagram"""
        color = self.edge_colors.get(edge.edge_type, '#000000')
        
        # Create enhanced edge label
        label_parts = []
        
        # Join type and cardinality
        if edge.join_type:
            label_parts.append(f"{edge.join_type.value}")
            
        if edge.cardinality:
            label_parts.append(f"({edge.cardinality})")
        
        # Join keys (limit to avoid clutter)
        if edge.join_keys:
            key_strs = [f"{src}={tgt}" for src, tgt in edge.join_keys[:2]]
            if len(edge.join_keys) > 2:
                key_strs.append(f"... +{len(edge.join_keys) - 2}")
            label_parts.extend(key_strs)
        
        label = "\\n".join(label_parts) if label_parts else ""
        
        # Set edge style based on strength
        penwidth = str(max(1, min(int(edge.strength), 4)))
        style = 'solid'
        
        if edge.edge_type == 'cte_dependency':
            style = 'bold'
        elif edge.edge_type == 'subquery_dependency':
            style = 'dashed'
        
        dot.edge(edge.source, edge.target, 
                label=label, 
                color=color,
                penwidth=penwidth,
                style=style,
                arrowsize='0.8'
        )


# Update the CLI to use the advanced parser
@click.command()
@click.option('--sql-file', '-f', type=click.Path(exists=True), help='Path to SQL file')
@click.option('--sql', '-s', type=str, help='SQL query string')
@click.option('--output', '-o', default='query_diagram', help='Output file name (without extension)')
@click.option('--dialect', '-d', default='', help='SQL dialect (postgres, mysql, bigquery, etc.)')
@click.option('--verbose', '-v', is_flag=True, help='Show detailed analysis')
def main(sql_file, sql, output, dialect, verbose):
    """Advanced SQL Query Visualizer - Generate enhanced diagrams from SQL queries"""
    
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
        # Parse SQL with advanced parser
        parser = AdvancedSQLQueryParser(dialect=dialect)
        query_data = parser.parse_query(sql_content)
        
        # Generate enhanced diagram
        generator = AdvancedDiagramGenerator()
        generator.generate_diagram(query_data, output)
        
        # Print detailed summary
        click.echo(f"\n✅ Successfully generated enhanced diagram!")
        click.echo(f"📊 Analysis Summary:")
        complexity = query_data.get('complexity', {})
        for key, value in complexity.items():
            click.echo(f"   {key.replace('_', ' ').title()}: {value}")
        
        click.echo(f"📁 Output: {output}.svg and {output}.png")
        
        if verbose:
            click.echo(f"\n🔍 Detailed Node Information:")
            for node_name, node in query_data['nodes'].items():
                click.echo(f"   {node_name}: {node.node_type.value} (Level {node.level})")
                if node.dependencies:
                    click.echo(f"      Dependencies: {', '.join(node.dependencies)}")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}")


if __name__ == '__main__':
    main()