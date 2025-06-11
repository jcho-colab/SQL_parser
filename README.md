# SQL Query Visualizer

A comprehensive Python tool to parse complex SQL queries and generate static diagrams showing lineage, relationships, and data flow. This tool helps you understand how complex SQL queries are structured, how tables are connected, and how data flows through CTEs and subqueries.

## 🎯 Key Features

- **Complex SQL Parsing**: Handles nested CTEs, subqueries, and complex joins using SQLGlot
- **Visual Lineage**: Shows data flow from tables through CTEs to final output with left-to-right layout
- **Join Visualization**: Displays join types, join keys, and cardinality estimates
- **CTE Grouping**: Groups nested CTEs visually like packages with hierarchical clustering
- **Color Coding**: Different colors for tables, CTEs, subqueries, and derived tables
- **Multiple Modes**: Basic and advanced visualization modes
- **Complexity Analysis**: Analyzes and reports query complexity metrics
- **Multiple Output Formats**: SVG (scalable) and PNG (presentation-ready)
- **CLI Interface**: User-friendly command-line interface with multiple options
- **Multi-Dialect Support**: Works with PostgreSQL, MySQL, BigQuery, Snowflake, and more

## 🚀 Quick Start

### Installation

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install system dependency (Ubuntu/Debian)
sudo apt-get install graphviz
```

### Basic Usage

```bash
# Parse from SQL file
python sql_query_visualizer.py -f your_query.sql -o diagram_name

# Parse from SQL string
python sql_query_visualizer.py -s "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id" -o simple_diagram

# Use advanced mode with detailed analysis
python advanced_sql_visualizer.py -f complex_query.sql -o advanced_diagram -v

# NEW: Use join-focused mode for join analysis ⭐
python final_join_visualizer.py -f query_with_joins.sql -o join_diagram -v

# NEW: Analyze hierarchical query structure with nesting levels ⭐
python query_structure_visualizer.py -f complex_query.sql -o structure_analysis -v

# Specify SQL dialect
python sql_query_visualizer.py -f query.sql -d postgres -o postgres_diagram
```

## 📊 What Gets Visualized

### Node Types (Color-Coded)
- **Tables** (Light Blue): Database tables with schema information
- **CTEs** (Light Green): Common Table Expressions with hierarchy levels
- **Subqueries** (Light Orange): Inline subqueries and derived tables  
- **Views** (Light Lime): Database views (advanced mode)

### Relationships (Edge Types)
- **JOIN relationships** (Blue): Shows join types and join keys
- **CTE dependencies** (Green): Data flow into CTEs
- **Subquery dependencies** (Orange): Subquery data sources
- **Data flow** (Gray): General data movement

### Enhanced Information
- Table/CTE names and aliases
- Schema information where available
- **Join columns prominently displayed (NEW!)** 🔑
- **Accurate join type detection and visualization (NEW!)** 
- Join conditions and cardinality estimates
- CTE hierarchy levels
- Complexity metrics and analysis

## 🏗️ Architecture

### Core Components

1. **SQLQueryParser**: Basic SQL parsing using SQLGlot
2. **AdvancedSQLQueryParser**: Enhanced parsing with relationship analysis
3. **DiagramGenerator**: Basic diagram generation with Graphviz
4. **AdvancedDiagramGenerator**: Enhanced visualization with styling
5. **CLI Interface**: Command-line interface with Click

### Data Models

- **QueryNode**: Represents tables, CTEs, and subqueries
- **QueryEdge**: Represents relationships between nodes
- **NodeType**: Enumeration of node types
- **JoinType**: Enumeration of join types

## 📋 Examples

### Simple Query with CTE
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

### Complex Nested CTEs
```sql
WITH customer_base AS (...),
     order_metrics AS (...),
     customer_segments AS (
         SELECT om.customer_id, om.total_spent,
                CASE WHEN om.total_spent > 1000 THEN 'Premium' 
                     ELSE 'Standard' END as segment
         FROM order_metrics om
     )
SELECT cs.*, regional_stats.avg_spend
FROM customer_segments cs
LEFT JOIN (...) regional_stats ON ...;
```

## 🛠️ Available Tools

### 1. Basic Visualizer (`sql_query_visualizer.py`)
- Core SQL parsing and visualization
- CTE hierarchy detection
- Basic join analysis
- Standard color coding

### 2. Advanced Visualizer (`advanced_sql_visualizer.py`)
- Enhanced SQL parsing with better error handling
- Comprehensive relationship analysis
- Cardinality estimation for joins
- Complexity scoring and metrics
- Enhanced styling and layout
- Verbose mode for detailed analysis

### 3. **NEW: Join-Focused Visualizer (`final_join_visualizer.py`)** ⭐
- **Specialized join detection and visualization**
- **Shows ONLY join-relevant columns in tables**
- **Accurate join type identification (INNER, LEFT, RIGHT, FULL, CROSS)**
- **Detailed join condition analysis**
- **Color-coded join relationships**
- **Perfect for understanding table relationships and join structure**

### 4. Test Suite (`test_visualizer.py`)
- Comprehensive test cases
- Various SQL pattern demonstrations
- Automated testing of different query types

### 5. Comprehensive Demo (`demo_comprehensive.py`)
- Real-world example queries from different domains
- E-commerce analytics, financial portfolio, healthcare
- Complete feature demonstration

### 7. **NEW: Query Structure Visualizer (`query_structure_visualizer.py`)** 🌟
- **Hierarchical SQL query analysis** - Exactly what you requested!
- **Isolates all SELECT statements** and shows their relationships
- **Displays query encapsulation** - shows which statements contain others
- **Left-to-right progression** from isolated → grouped → nested structures
- **Proper WITH AS handling** - shows CTE containers and contents
- **Nested SELECT detection** - identifies subqueries at all levels
- **Perfect for understanding complex query architecture**

## 📈 Supported SQL Constructs

- ✅ SELECT statements with complex expressions
- ✅ Common Table Expressions (CTEs) - including nested
- ✅ All JOIN types (INNER, LEFT, RIGHT, FULL, CROSS)
- ✅ Subqueries in SELECT, FROM, and WHERE clauses
- ✅ Window functions and analytical queries
- ✅ Complex WHERE conditions and join predicates
- ✅ UNION and set operations
- ✅ Aggregate functions and GROUP BY
- ✅ Multiple schema and database references

## 🗄️ Supported SQL Dialects

The tool uses SQLGlot parser which supports:
- PostgreSQL
- MySQL
- BigQuery
- Snowflake
- Redshift
- SQLite
- Oracle
- SQL Server
- And many more...

## 🎨 Output Examples

The tool generates both SVG and PNG files:

- **SVG files**: Scalable vector graphics, perfect for web viewing and documentation
- **PNG files**: Raster images, ideal for presentations and reports

### Sample Outputs Generated
- `test1_simple_join.svg` - Basic two-table join
- `test4_nested_ctes.svg` - Complex nested CTE structure  
- `demo_advanced_ecommerce.svg` - Real-world e-commerce analytics
- `demo_advanced_financial.svg` - Financial portfolio analysis
- `demo_advanced_healthcare.svg` - Healthcare patient journey

## ⚙️ Command Line Options

### Basic Visualizer
```bash
python sql_query_visualizer.py [OPTIONS]

Options:
  -f, --sql-file PATH     Path to SQL file to parse
  -s, --sql TEXT         SQL query string to parse  
  -o, --output TEXT      Output file name (without extension)
  -d, --dialect TEXT     SQL dialect (postgres, mysql, bigquery, etc.)
  --help                 Show help message
```

### Advanced Visualizer
```bash
python advanced_sql_visualizer.py [OPTIONS]

Options:
  -f, --sql-file PATH     Path to SQL file to parse
  -s, --sql TEXT         SQL query string to parse
  -o, --output TEXT      Output file name (without extension)  
  -d, --dialect TEXT     SQL dialect (postgres, mysql, bigquery, etc.)
  -v, --verbose          Show detailed analysis and node information
  --help                 Show help message
```

### Join-Focused Visualizer (NEW!) ⭐
```bash
python final_join_visualizer.py [OPTIONS]

Options:
  -f, --sql-file PATH     Path to SQL file to parse
  -s, --sql TEXT         SQL query string to parse
  -o, --output TEXT      Output file name (without extension)
  -v, --verbose          Show detailed join analysis with column information
  --help                 Show help message
```

## 🧪 Testing

Run the comprehensive test suite:

```bash
# Run all tests
python test_visualizer.py

# Run comprehensive demo
python demo_comprehensive.py
```

## 🔧 Advanced Configuration

### Customizing Visualization

The advanced visualizer allows customization through:

- **Color schemes**: Modify colors in `AdvancedDiagramGenerator`
- **Layout options**: Adjust Graphviz attributes
- **Node sizing**: Configure size estimates based on complexity
- **Edge styling**: Customize relationship line styles

### Error Handling

The tool includes comprehensive error handling:
- Graceful degradation when parsing fails
- Warning messages for unsupported constructs
- Continues processing even with partial failures
- Detailed error reporting in verbose mode

## 📝 Best Practices

1. **Start Simple**: Test with simple queries before complex ones
2. **Use Verbose Mode**: Enable `-v` for detailed analysis information
3. **Check Outputs**: Always verify both SVG and PNG outputs
4. **Dialect Specification**: Specify SQL dialect for better parsing accuracy
5. **File Organization**: Use descriptive output names for multiple diagrams

## 🤝 Contributing

Areas for future enhancement:
- Interactive diagram features
- Better layout algorithms for very large queries
- Support for more SQL constructs (MERGE, recursive CTEs)
- Integration with SQL development tools
- Web-based interface
- Export to additional formats (PDF, DOT)

## 📄 License

MIT License - see LICENSE file for details.

## 🎯 Use Cases

### Data Engineering
- Understand complex ETL pipeline queries
- Document data transformation workflows
- Analyze query dependencies and lineage

### Analytics & BI
- Visualize analytical query structure
- Document business logic in SQL
- Understand data mart and warehouse queries

### Database Development
- Review complex stored procedures
- Understand legacy query structures
- Plan query optimizations

### Education & Training
- Teach SQL query structure and best practices
- Demonstrate CTE and subquery relationships
- Visual learning aid for complex SQL concepts

---

**Created with ❤️ for the SQL community**

This tool aims to make complex SQL queries more understandable through visualization, helping developers, analysts, and data engineers better comprehend and document their SQL code.
