# Wikidata Graph Analytics Pipeline

Databricks-based data engineering project for processing large-scale Wikidata dumps and performing graph queries using PySpark, Delta Lake, and GraphFrames.

## Overview

Processes 1TB+ Wikidata JSON dumps (130GB compressed) into optimized Delta Lake tables and enables graph traversal queries to extract entity relationships.

**Key Results:**
- Reduced query times by 90%+ through partitioning and Z-ordering
- Processes 100M+ entities and 1B+ relationships
- Parameterized graph traversal with configurable depth
- Production-ready Azure integration with secure credential management

## Architecture

```
Wikidata JSON (1TB+) → PySpark ETL → Delta Lake → GraphFrames → CSV Export
```

## Technical Implementation

### ETL Pipeline
- Custom PySpark schemas for efficient JSON parsing
- Separate processing for labels, descriptions, and aliases
- Delta Lake with partitioning by entity type
- Z-ordering optimization on ID and property columns
- VACUUM for storage cleanup

### Graph Queries
- Dynamic pattern generation for multi-hop traversal
- Backward graph traversal from target entity
- Databricks widgets for runtime parameters (entity ID, depth, output file)

### Optimizations
- Partition pruning reduces scan by 90%+
- Z-ordering improves point lookups
- Broadcast joins for dimension tables
- Column projection minimizes data movement

## Tech Stack

- Databricks (Spark 3.5)
- Delta Lake
- GraphFrames 0.8.3
- Azure Data Lake Gen2 (ABFSS)
- PySpark

## Usage

```python
# Parameters
initial_element = "Q5"  # Starting entity (e.g., "Human")
depth = 3               # Traversal depth
file_name = "output.csv"

# Executes backward graph traversal and exports relationships
```

Output CSV contains: entity IDs, labels, descriptions, properties, and relationships.
