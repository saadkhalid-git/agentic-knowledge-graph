# Knowledge Graph System - PhD Exercise Solution

Multi-agent knowledge graph system that connects supply chain CSV data with customer review markdown files using Google's ADK and Neo4j.

## Quick Start

```bash
# Setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.template .env  # Add your credentials

# Run
python adk_pipeline.py
```

## Features

- **LLM-Driven Analysis**: Uses AI to determine graph structure from your data
- **Intelligent Validation**: Quality scoring at each pipeline phase
- **Multi-Agent Architecture**: Specialized agents for different tasks
- **Complete Regeneration**: Always builds fresh graph with latest data
- **Multi-Source Support**: Processes CSV and text files automatically
- **Entity Resolution**: Intelligently links entities across sources

## Pipeline Phases

1. **Goal Determination** - AI analyzes data to determine graph type
2. **File Selection** - Intelligently selects relevant files based on goal
3. **Schema Generation** - Creates construction and extraction plans
4. **Domain Graph** - Builds structured data from CSV files
5. **Subject Graph** - Extracts entities and facts from text
6. **Entity Resolution** - Links entities between graphs
7. **Quality Validation** - Scores and validates each phase

## Configuration

Environment variables in `.env`:
- `OPENAI_API_KEY` - Required for LLM operations
- `NEO4J_URI` - Neo4j database URI (default: bolt://localhost:7687)
- `NEO4J_USERNAME` - Neo4j username (default: neo4j)
- `NEO4J_PASSWORD` - Neo4j database password

## How It Works

The ADK pipeline:
1. Resets the existing graph for a clean slate
2. Discovers all CSV and text files in the data directory
3. Uses LLM to analyze and determine the knowledge graph goal
4. Validates goal quality (score: 0-100)
5. Selects relevant files with validation
6. Generates schema with quality checks
7. Builds domain graph from structured data
8. Extracts entities from unstructured text
9. Resolves and links entities
10. Calculates final quality metrics

All plans are regenerated on each run using LLM intelligence.

## Requirements

- Python 3.8+
- Neo4j 4.4+ with APOC and GenAI plugins
- OpenAI API access
- 8GB+ RAM recommended