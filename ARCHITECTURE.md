# System Architecture: ADK-Enhanced Knowledge Graph for Multi-Source Querying

## Overview

This system connects structured CSV data and unstructured text reviews through an intelligent knowledge graph, enabling complex multi-hop queries across heterogeneous data sources.

## Core Architecture Components

```
┌─────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                          │
├───────────────────────────┬───────────────────────────────────┤
│     Structured (CSV)      │     Unstructured (Markdown)      │
│  • products.csv          │  • product_reviews/*.md          │
│  • suppliers.csv         │    - Customer feedback            │
│  • parts.csv             │    - Quality issues               │
│  • assemblies.csv        │    - Satisfaction ratings        │
│  • part_supplier_mapping │                                   │
└───────────────┬───────────┴───────────────┬───────────────────┘
                ↓                           ↓
┌───────────────────────────────────────────────────────────────┐
│              ADK INTELLIGENT PIPELINE                          │
├─────────────────────────────────────────────────────────────── │
│  1. Goal Determination (LLM Analysis)                         │
│  2. File Selection (Relevance Scoring)                        │
│  3. Schema Generation (Relationship Discovery)                │
│  4. Validation (Quality Scoring 0-100)                        │
└───────────────┬───────────────────────┬───────────────────────┘
                ↓                       ↓
┌───────────────────────────┬───────────────────────────────────┐
│    DOMAIN GRAPH           │      SUBJECT GRAPH                │
│  (Structured Entities)    │   (Extracted Entities)            │
│                           │                                   │
│  Nodes:                   │   Nodes:                          │
│  • Product                │   • Product (from text)           │
│  • Supplier               │   • User (reviewer)               │
│  • Part                   │   • Rating                        │
│  • Assembly               │   • Issue                         │
│                           │   • Feature                       │
│  Relationships:           │                                   │
│  • SUPPLIES               │   Relationships:                  │
│  • IS_PART_OF            │   • reviewed_by                   │
│  • CONTAINS              │   • has_rating                    │
│                           │   • has_issue                     │
└───────────────┬───────────┴───────────────┬───────────────────┘
                ↓                           ↓
┌───────────────────────────────────────────────────────────────┐
│                   ENTITY RESOLUTION                            │
│         (Jaro-Winkler Similarity Matching)                    │
│              Links Domain ← → Subject                         │
└───────────────────────────┬───────────────────────────────────┘
                            ↓
┌───────────────────────────────────────────────────────────────┐
│                    NEO4J GRAPH DATABASE                        │
│                 Unified Knowledge Graph                        │
│           (367 nodes, 295 relationships)                      │
└───────────────────────────┬───────────────────────────────────┘
                            ↓
┌───────────────────────────────────────────────────────────────┐
│                      QUERY ENGINE                              │
├─────────────────────────────────────────────────────────────── │
│  • Natural Language → Cypher Translation                      │
│  • Multi-hop Traversal                                        │
│  • Answer Synthesis with Traceability                         │
└───────────────────────────────────────────────────────────────┘
```

## Design Rationale

### 1. **Knowledge Graph Approach**
- **Why**: Graphs naturally represent complex relationships between business entities
- **Benefit**: Enables multi-hop queries like "suppliers → parts → products → reviews"
- **Evidence**: Neo4j's Cypher language provides powerful pattern matching for complex queries

### 2. **Dual Graph Construction**
- **Domain Graph**: Preserves structured relationships from CSV files
- **Subject Graph**: Extracts entities and sentiments from unstructured text
- **Why**: Separation allows specialized processing while maintaining data integrity

### 3. **LLM-Enhanced Pipeline**
- **Google ADK Integration**: Uses agents for intelligent decision-making
- **Validation Loops**: Quality scoring at each phase ensures data integrity
- **Why**: LLMs understand context and relationships humans might miss

### 4. **Entity Resolution Layer**
- **Similarity Matching**: Links "Stockholm Chair" in CSV to "Stockholm chair" in reviews
- **Why**: Bridges the gap between structured and unstructured data representations

## Key Capabilities

### 1. **Multi-Source Integration**
- Seamlessly combines CSV and markdown data
- Preserves relationships and context from both sources

### 2. **Intelligent Schema Discovery**
- Automatically detects entities and relationships
- Adapts to different data structures without manual configuration

### 3. **Complex Query Support**
- Simple lookups: "What products exist?"
- Text extraction: "What do customers say about X?"
- Multi-hop queries: "Which suppliers provide parts for product Y?"

### 4. **Traceability**
- Every answer includes source data references
- Query paths show reasoning steps
- Confidence scores for extracted information

## Technology Stack

- **Graph Database**: Neo4j 5.28
- **LLM Framework**: Google ADK 1.5.0 with OpenAI
- **NLP**: spaCy 3.8 for entity extraction
- **Data Processing**: pandas for CSV handling
- **Query Language**: Cypher for graph traversal

## Advantages Over Alternative Approaches

### vs. SQL Database
- **Better for relationships**: Graph traversal is more efficient than multiple JOINs
- **Flexible schema**: Can add new entity types without restructuring

### vs. Document Store
- **Structured relationships**: Maintains referential integrity from CSVs
- **Complex queries**: Multi-hop queries are natural in graphs

### vs. Vector Database
- **Exact matching**: Preserves IDs and foreign keys from structured data
- **Explainability**: Query paths provide clear reasoning

## System Workflow

1. **Data Ingestion**: ADK pipeline analyzes and loads all data sources
2. **Graph Construction**: Builds unified knowledge graph in Neo4j
3. **Query Processing**: Natural language questions converted to Cypher
4. **Answer Generation**: Results synthesized with source attribution
5. **Validation**: Quality metrics ensure answer accuracy

This architecture provides a robust, scalable solution for connecting disparate data sources to answer complex business questions with full traceability.