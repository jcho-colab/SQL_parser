# SQL Query Visualizer

A Python tool to parse complex SQL queries and generate static diagrams showing lineage, relationships, and data flow.

## Features

- **Complex SQL Parsing**: Handles nested CTEs, subqueries, and complex joins
- **Visual Lineage**: Shows data flow from tables through CTEs to final output
- **Join Visualization**: Displays join types and join keys between tables
- **CTE Grouping**: Groups nested CTEs visually like packages
- **Color Coding**: Different colors for tables, CTEs, and subqueries
- **Left-to-Right Layout**: Easy to follow data flow direction
- **Multiple Output Formats**: SVG and PNG diagram generation

## Installation

```bash
pip install -r requirements.txt
```

**System Requirements:**
- Python 3.7+
- Graphviz system package (`apt-get install graphviz` on Ubuntu/Debian)

## Usage

### Command Line Interface

```bash
# Parse from SQL file
python sql_query_visualizer.py -f sample_queries.sql -o my_diagram

# Parse from SQL string
python sql_query_visualizer.py -s "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id" -o simple_diagram

# Specify SQL dialect
python sql_query_visualizer.py -f query.sql -d postgres -o postgres_diagram
```

### Options

- `-f, --sql-file`: Path to SQL file to parse
- `-s, --sql`: SQL query string to parse
- `-o, --output`: Output file name (without extension, defaults to 'query_diagram')
- `-d, --dialect`: SQL dialect (postgres, mysql, bigquery, snowflake, etc.)

## Examples

### Simple Query
```sql
WITH customer_orders AS (
    SELECT customer_id, COUNT(*) as order_count
    FROM orders
    GROUP BY customer_id
)
SELECT c.name, co.order_count
FROM customers c
JOIN customer_orders co ON c.id = co.customer_id;
```

### Complex Query with Nested CTEs
See `sample_queries.sql` for a comprehensive example with:
- Multiple CTE levels
- Various join types
- Subqueries
- Complex relationships

## Output

The tool generates two files:
- `{output_name}.svg` - Scalable vector graphics (recommended for web)
- `{output_name}.png` - Portable network graphics (for presentations)

## Diagram Legend

### Node Colors
- **Light Blue**: Database tables
- **Light Green**: Common Table Expressions (CTEs)
- **Light Orange**: Subqueries
- **Light Purple**: Derived tables

### Edge Colors
- **Blue**: JOIN relationships
- **Green**: CTE dependencies
- **Gray**: Data flow

### Node Information
Each node displays:
- Table/CTE name and alias
- Schema (if applicable)
- Node type
- Key columns (first 3 shown)

## Supported SQL Dialects

The tool uses SQLGlot parser which supports:
- PostgreSQL
- MySQL
- BigQuery
- Snowflake
- Redshift
- SQLite
- And many more...

## Architecture

### Core Components

1. **SQLQueryParser**: Extracts query structure using SQLGlot
2. **DiagramGenerator**: Creates visual representation using Graphviz
3. **Data Models**: QueryNode and QueryEdge for representing structure
4. **CLI Interface**: User-friendly command line interface

### Parsing Process

1. Parse SQL into AST using SQLGlot
2. Extract CTEs, tables, and subqueries
3. Analyze join relationships and dependencies
4. Build graph structure with nodes and edges
5. Generate visual diagram with proper layout

## Limitations

- Complex nested queries may require manual parsing adjustments
- Some proprietary SQL extensions may not be fully supported
- Very large queries may produce cluttered diagrams

## Contributing

Contributions welcome! Areas for improvement:
- Enhanced join key extraction
- Better layout algorithms for large queries
- Support for more SQL constructs
- Interactive diagram features

## License

MIT License - see LICENSE file for details.
