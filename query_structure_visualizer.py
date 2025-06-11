#!/usr/bin/env python3
"""
Query Structure Visualizer
Shows the hierarchical structure of SQL queries with proper nesting and encapsulation.
Displays progression from isolated SELECT statements to grouped joins to final nested structure.
"""

import sqlglot
from sqlglot import exp
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Optional, Any
import graphviz
import click
import re
from enum import Enum


class StructureType(Enum):
    MAIN_QUERY = "main_query"
    CTE = "cte"
    SUBQUERY = "subquery"
    WITH_BLOCK = "with_block"


@dataclass
class QueryStructure:
    """Represents a query structure element"""
    id: str
    structure_type: StructureType
    name: str
    level: int  # Left-to-right progression level
    nesting_depth: int  # How deeply nested this is
    contains: List[str] = field(default_factory=list)  # IDs of contained structures
    tables: List[str] = field(default_factory=list)
    join_keys: List[str] = field(default_factory=list)
    sql_preview: str = ""


@dataclass
class StructureRelation:
    """Represents relationships between structures"""
    source_id: str
    target_id: str
    relation_type: str  # "contains", "joins_with", "feeds_into"
    details: str = ""


class QueryStructureParser:
    """Parser that identifies query structures and their relationships"""
    
    def __init__(self):
        self.structures: Dict[str, QueryStructure] = {}
        self.relations: List[StructureRelation] = []
        self.level_groups: Dict[int, List[str]] = {}  # level -> structure_ids
        
    def parse_query(self, sql: str) -> Dict[str, Any]:
        """Parse SQL to understand structure hierarchy"""
        try:
            # Clean SQL
            sql = self._clean_sql(sql)
            
            # Reset state
            self.structures = {}
            self.relations = []
            self.level_groups = {}
            
            # Strategy: Parse from outside in, identifying encapsulation levels
            self._identify_query_structures(sql)
            
            # Organize by progression levels
            self._organize_progression_levels()
            
            # Identify relationships
            self._identify_relationships()
            
            return {
                'structures': self.structures,
                'relations': self.relations,
                'level_groups': self.level_groups,
                'summary': self._create_summary()
            }
            
        except Exception as e:
            print(f"Error parsing query structure: {e}")
            import traceback
            traceback.print_exc()
            return {'structures': {}, 'relations': [], 'level_groups': {}, 'summary': {}}
    
    def _clean_sql(self, sql: str) -> str:
        """Clean and normalize SQL"""
        # Remove comments
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        # Normalize whitespace
        sql = re.sub(r'\s+', ' ', sql.strip())
        return sql
    
    def _identify_query_structures(self, sql: str):
        """Identify all query structures in order of encapsulation"""
        
        # Level 0: Find main structure (WITH blocks or main SELECT)
        main_structure = self._identify_main_structure(sql)
        
        # Level 1: Find WITH clauses and their CTEs
        with_structures = self._identify_with_structures(sql)
        
        # Level 2: Find main SELECT statements
        select_structures = self._identify_select_structures(sql)
        
        # Level 3: Find subqueries within SELECTs
        subquery_structures = self._identify_subquery_structures(sql)
        
        # Combine all structures
        all_structures = [main_structure] + with_structures + select_structures + subquery_structures
        
        # Add valid structures
        for structure in all_structures:
            if structure:
                self.structures[structure.id] = structure
    
    def _identify_main_structure(self, sql: str) -> Optional[QueryStructure]:
        """Identify the main query structure"""
        structure_id = "main_0"
        
        if sql.strip().upper().startswith('WITH'):
            return QueryStructure(
                id=structure_id,
                structure_type=StructureType.WITH_BLOCK,
                name="Main Query Block",
                level=0,
                nesting_depth=0,
                sql_preview=sql[:100] + "..." if len(sql) > 100 else sql
            )
        else:
            return QueryStructure(
                id=structure_id,
                structure_type=StructureType.MAIN_QUERY,
                name="Main SELECT",
                level=0,
                nesting_depth=0,
                sql_preview=sql[:100] + "..." if len(sql) > 100 else sql
            )
    
    def _identify_with_structures(self, sql: str) -> List[QueryStructure]:
        """Identify WITH clauses and CTEs"""
        structures = []
        
        # Find WITH clause
        with_match = re.search(r'WITH\s+(.+?)(?=\s+SELECT\s+)', sql, re.IGNORECASE | re.DOTALL)
        if not with_match:
            return structures
        
        with_content = with_match.group(1)
        
        # Find individual CTEs
        # Pattern: cte_name AS (...)
        cte_pattern = r'(\w+)\s+AS\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)'
        cte_matches = re.findall(cte_pattern, with_content, re.IGNORECASE | re.DOTALL)
        
        for i, (cte_name, cte_content) in enumerate(cte_matches):
            structure = QueryStructure(
                id=f"cte_{i}",
                structure_type=StructureType.CTE,
                name=cte_name,
                level=1,
                nesting_depth=1,
                tables=self._extract_tables_from_text(cte_content),
                join_keys=self._extract_join_keys_from_text(cte_content),
                sql_preview=f"{cte_name} AS ({cte_content[:50]}...)"
            )
            structures.append(structure)
        
        return structures
    
    def _identify_select_structures(self, sql: str) -> List[QueryStructure]:
        """Identify main SELECT statements"""
        structures = []
        
        # Find the main SELECT (after WITH clause if present)
        main_select_match = re.search(r'(?:WITH.*?)?(?:^|\s)(SELECT\s+.*?)(?:\s*;|\s*$)', sql, re.IGNORECASE | re.DOTALL)
        
        if main_select_match:
            select_content = main_select_match.group(1) if main_select_match.lastindex else main_select_match.group(0)
            
            structure = QueryStructure(
                id="main_select_0",
                structure_type=StructureType.MAIN_QUERY,
                name="Main SELECT",
                level=2,
                nesting_depth=1,
                tables=self._extract_tables_from_text(select_content),
                join_keys=self._extract_join_keys_from_text(select_content),
                sql_preview=select_content[:100] + "..." if len(select_content) > 100 else select_content
            )
            structures.append(structure)
        
        return structures
    
    def _identify_subquery_structures(self, sql: str) -> List[QueryStructure]:
        """Identify subqueries within the SQL"""
        structures = []
        
        # Find subqueries in parentheses
        # Look for (SELECT ... FROM ...)
        subquery_pattern = r'\(\s*(SELECT\s+.*?)\)'
        subquery_matches = re.findall(subquery_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        for i, subquery_content in enumerate(subquery_matches):
            # Skip if this is likely a CTE (has AS before it)
            context_before = sql[:sql.find(subquery_content)]
            if re.search(r'\w+\s+AS\s*$', context_before):
                continue  # This is a CTE, not a subquery
            
            structure = QueryStructure(
                id=f"subquery_{i}",
                structure_type=StructureType.SUBQUERY,
                name=f"Subquery {i+1}",
                level=3,
                nesting_depth=2,
                tables=self._extract_tables_from_text(subquery_content),
                join_keys=self._extract_join_keys_from_text(subquery_content),
                sql_preview=subquery_content[:60] + "..." if len(subquery_content) > 60 else subquery_content
            )
            structures.append(structure)
        
        return structures
    
    def _extract_tables_from_text(self, text: str) -> List[str]:
        """Extract table names from SQL text"""
        tables = []
        
        # Pattern for FROM table_name
        from_pattern = r'FROM\s+(\w+)'
        from_matches = re.findall(from_pattern, text, re.IGNORECASE)
        tables.extend(from_matches)
        
        # Pattern for JOIN table_name
        join_pattern = r'JOIN\s+(\w+)'
        join_matches = re.findall(join_pattern, text, re.IGNORECASE)
        tables.extend(join_matches)
        
        return list(set(tables))  # Remove duplicates
    
    def _extract_join_keys_from_text(self, text: str) -> List[str]:
        """Extract join keys from SQL text"""
        join_keys = []
        
        # Pattern for table.column = table.column
        join_pattern = r'(\w+\.\w+)\s*=\s*(\w+\.\w+)'
        join_matches = re.findall(join_pattern, text, re.IGNORECASE)
        
        for left, right in join_matches:
            join_keys.append(f"{left} = {right}")
        
        return join_keys
    
    def _organize_progression_levels(self):
        """Organize structures by their progression levels"""
        for structure in self.structures.values():
            level = structure.level
            if level not in self.level_groups:
                self.level_groups[level] = []
            self.level_groups[level].append(structure.id)
    
    def _identify_relationships(self):
        """Identify relationships between structures"""
        # Containment relationships
        for structure in self.structures.values():
            if structure.structure_type == StructureType.WITH_BLOCK:
                # WITH block contains CTEs
                for other_id, other in self.structures.items():
                    if other.structure_type == StructureType.CTE:
                        structure.contains.append(other_id)
                        self.relations.append(StructureRelation(
                            source_id=structure.id,
                            target_id=other_id,
                            relation_type="contains",
                            details="WITH contains CTE"
                        ))
        
        # Table usage relationships
        for structure in self.structures.values():
            for other_id, other in self.structures.items():
                if structure.id != other_id:
                    # Check if structure uses tables defined in other
                    if structure.structure_type in [StructureType.MAIN_QUERY, StructureType.SUBQUERY]:
                        if other.structure_type == StructureType.CTE and other.name in ' '.join(structure.tables):
                            self.relations.append(StructureRelation(
                                source_id=other_id,
                                target_id=structure.id,
                                relation_type="feeds_into",
                                details=f"CTE '{other.name}' used in query"
                            ))
    
    def _create_summary(self) -> Dict[str, Any]:
        """Create summary of the structure analysis"""
        return {
            'total_structures': len(self.structures),
            'max_level': max(self.level_groups.keys()) if self.level_groups else 0,
            'structures_by_type': {
                st.value: len([s for s in self.structures.values() if s.structure_type == st])
                for st in StructureType
            },
            'total_relations': len(self.relations)
        }


class StructureDiagramGenerator:
    """Generates diagrams showing query structure progression"""
    
    def __init__(self):
        self.colors = {
            StructureType.MAIN_QUERY: '#E3F2FD',
            StructureType.WITH_BLOCK: '#E8F5E8',
            StructureType.CTE: '#FFF3E0',
            StructureType.SUBQUERY: '#F3E5F5'
        }
    
    def generate_diagram(self, data: Dict[str, Any], output_path: str):
        """Generate structure progression diagram"""
        structures = data['structures']
        relations = data['relations']
        level_groups = data['level_groups']
        summary = data['summary']
        
        dot = graphviz.Digraph(comment='SQL Query Structure Progression')
        dot.attr(rankdir='LR')  # Left to right progression
        dot.attr('node', shape='box', style='filled', fontname='Arial')
        dot.attr('edge', fontname='Arial', fontsize='9')
        dot.attr('graph', 
                 splines='ortho',
                 nodesep='1.5',
                 ranksep='2.5',
                 fontname='Arial')
        
        # Title
        title = f"Query Structure Progression\\n{summary['total_structures']} Structures, {summary['max_level'] + 1} Levels"
        dot.attr(label=title, labelloc='top', fontsize='14')
        
        # Create level-based columns
        for level in sorted(level_groups.keys()):
            with dot.subgraph() as level_subgraph:
                level_subgraph.attr(rank='same')
                
                for structure_id in level_groups[level]:
                    structure = structures[structure_id]
                    self._add_structure_node(level_subgraph, structure)
        
        # Add relationships
        for relation in relations:
            self._add_relation_edge(dot, relation, structures)
        
        # Render
        try:
            dot.render(output_path, format='svg', cleanup=True)
            dot.render(output_path, format='png', cleanup=True)
            print(f"✅ Structure diagram saved: {output_path}.svg and {output_path}.png")
        except Exception as e:
            print(f"⚠️  Graphviz error: {e}")
            print("📋 Generating text-based visualization instead:")
            self._generate_text_visualization(data)
    
    def _generate_text_visualization(self, data: Dict[str, Any]):
        """Generate text-based visualization when Graphviz fails"""
        structures = data['structures']
        level_groups = data['level_groups']
        relations = data['relations']
        
        print("\n📊 QUERY STRUCTURE PROGRESSION (Left to Right):")
        print("=" * 70)
        
        for level in sorted(level_groups.keys()):
            print(f"\n📍 LEVEL {level}:")
            print("-" * 50)
            
            for structure_id in level_groups[level]:
                structure = structures[structure_id]
                
                # Structure header
                type_icons = {
                    StructureType.MAIN_QUERY: "🎯",
                    StructureType.WITH_BLOCK: "📦", 
                    StructureType.CTE: "🔄",
                    StructureType.SUBQUERY: "📊"
                }
                icon = type_icons.get(structure.structure_type, "❓")
                
                print(f"  {icon} {structure.name}")
                print(f"     Type: {structure.structure_type.value}")
                
                if structure.tables:
                    print(f"     📋 Tables: {', '.join(structure.tables)}")
                
                if structure.join_keys:
                    print(f"     🔗 Joins: {', '.join(structure.join_keys[:2])}")
                    if len(structure.join_keys) > 2:
                        print(f"             (+{len(structure.join_keys) - 2} more)")
                
                if structure.contains:
                    print(f"     📁 Contains: {len(structure.contains)} nested elements")
                
                print(f"     💬 Preview: {structure.sql_preview[:60]}...")
                print()
        
        # Show relationships
        if relations:
            print("🔗 STRUCTURE RELATIONSHIPS:")
            print("-" * 30)
            for relation in relations:
                source = structures.get(relation.source_id, {})
                target = structures.get(relation.target_id, {})
                source_name = getattr(source, 'name', relation.source_id)
                target_name = getattr(target, 'name', relation.target_id)
                print(f"  {source_name} --{relation.relation_type}--> {target_name}")
                if relation.details:
                    print(f"    └─ {relation.details}")
        
        print("\n" + "=" * 70)
    
    def _add_structure_node(self, graph, structure: QueryStructure):
        """Add a node for a query structure"""
        color = self.colors.get(structure.structure_type, '#F0F0F0')
        
        # Create label
        label_parts = []
        
        # Structure name and type
        type_icons = {
            StructureType.MAIN_QUERY: "🎯",
            StructureType.WITH_BLOCK: "📦",
            StructureType.CTE: "🔄",
            StructureType.SUBQUERY: "📊"
        }
        
        icon = type_icons.get(structure.structure_type, "❓")
        label_parts.append(f"{icon} {structure.name}")
        label_parts.append(f"Level {structure.level}")
        
        # Tables used
        if structure.tables:
            tables_str = ", ".join(structure.tables[:3])
            if len(structure.tables) > 3:
                tables_str += f" (+{len(structure.tables) - 3})"
            label_parts.append(f"📋 {tables_str}")
        
        # Join keys
        if structure.join_keys:
            label_parts.append("🔗 " + structure.join_keys[0])
            if len(structure.join_keys) > 1:
                label_parts.append(f"   (+{len(structure.join_keys) - 1} more)")
        
        # Contains
        if structure.contains:
            label_parts.append(f"📁 Contains {len(structure.contains)} items")
        
        label = "\\n".join(label_parts)
        
        graph.node(structure.id, label=label, fillcolor=color)
    
    def _add_relation_edge(self, dot, relation: StructureRelation, structures: Dict[str, QueryStructure]):
        """Add edge for structure relationship"""
        if relation.source_id in structures and relation.target_id in structures:
            style = 'dashed' if relation.relation_type == 'contains' else 'solid'
            color = 'blue' if relation.relation_type == 'feeds_into' else 'gray'
            
            dot.edge(relation.source_id, relation.target_id,
                    label=relation.relation_type.replace('_', ' '),
                    style=style,
                    color=color)


@click.command()
@click.option('--sql-file', '-f', type=click.Path(exists=True), help='Path to SQL file')
@click.option('--sql', '-s', type=str, help='SQL query string')
@click.option('--output', '-o', default='query_structure', help='Output file name')
@click.option('--verbose', '-v', is_flag=True, help='Show detailed structure information')
def main(sql_file, sql, output, verbose):
    """Query Structure Visualizer - Shows hierarchical SQL structure"""
    
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
    
    # Parse structure
    parser = QueryStructureParser()
    data = parser.parse_query(sql_content)
    
    # Generate diagram
    visualizer = StructureDiagramGenerator()
    visualizer.generate_diagram(data, output)
    
    # Print analysis
    summary = data['summary']
    click.echo(f"\n📊 Query Structure Analysis:")
    click.echo(f"   Total Structures: {summary['total_structures']}")
    click.echo(f"   Progression Levels: {summary['max_level'] + 1}")
    click.echo(f"   Relations: {summary['total_relations']}")
    
    structures_by_type = summary['structures_by_type']
    click.echo("   Structure Types:")
    for struct_type, count in structures_by_type.items():
        if count > 0:
            click.echo(f"     {struct_type}: {count}")
    
    if verbose:
        structures = data['structures']
        level_groups = data['level_groups']
        
        click.echo(f"\n🏗️  Structure Progression:")
        for level in sorted(level_groups.keys()):
            click.echo(f"   Level {level}:")
            for structure_id in level_groups[level]:
                structure = structures[structure_id]
                click.echo(f"     📍 {structure.name} ({structure.structure_type.value})")
                if structure.tables:
                    click.echo(f"       Tables: {', '.join(structure.tables)}")
                if structure.join_keys:
                    click.echo(f"       Joins: {', '.join(structure.join_keys)}")


if __name__ == '__main__':
    main()