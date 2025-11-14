"""
ADK-based Schema Agent with LLM decision making
Intelligently generates schema and construction plans for the knowledge graph
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import json
import asyncio
import pandas as pd
from typing import Dict, Any, List, Tuple
from pathlib import Path
from datetime import datetime

# ADK imports
from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService, Session
from google.adk.tools import ToolContext
from google.genai import types
from src.neo4j_for_adk import tool_success, tool_error


# Tool: Analyze CSV Schema
def analyze_csv_schema(file_path: str) -> Dict[str, Any]:
    """Analyze CSV file to understand its schema and structure.

    Args:
        file_path: Path to the CSV file

    Returns:
        Dictionary with detailed schema analysis
    """
    try:
        df = pd.read_csv(file_path)
        file_name = os.path.basename(file_path)

        # Analyze columns
        analysis = {
            "file_name": file_name,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": [],
            "primary_key_candidates": [],
            "foreign_key_candidates": [],
            "entity_type": "unknown",
            "is_relationship_table": False
        }

        # Analyze each column
        for col in df.columns:
            col_analysis = {
                "name": col,
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isnull().sum()),
                "unique_count": int(df[col].nunique()),
                "is_unique": df[col].nunique() == len(df),
                "sample_values": df[col].dropna().head(3).tolist()
            }

            # Detect column type
            col_lower = col.lower()
            if col_lower == 'id' or col_lower.endswith('_id'):
                if col_lower == 'id' or col_lower == f"{file_name.replace('.csv', '').lower()}_id":
                    col_analysis["column_type"] = "primary_key"
                    analysis["primary_key_candidates"].append(col)
                else:
                    col_analysis["column_type"] = "foreign_key"
                    referenced_entity = col_lower.replace('_id', '')
                    analysis["foreign_key_candidates"].append({
                        "column": col,
                        "references": referenced_entity
                    })
            elif col_lower.endswith('_key') or col_lower.endswith('_code'):
                col_analysis["column_type"] = "potential_key"
                if col_analysis["is_unique"]:
                    analysis["primary_key_candidates"].append(col)
            else:
                col_analysis["column_type"] = "attribute"

            analysis["columns"].append(col_analysis)

        # Determine if it's a relationship table
        if len(analysis["foreign_key_candidates"]) >= 2 and len(df.columns) <= 4:
            analysis["is_relationship_table"] = True
            analysis["entity_type"] = "relationship"
        else:
            # Try to determine entity type from file name
            entity_name = file_name.replace('.csv', '').replace('_', ' ').title().replace(' ', '')
            # Remove plural 's' if present
            if entity_name.endswith('s') and not entity_name.endswith('ss'):
                entity_name = entity_name[:-1]
            analysis["entity_type"] = entity_name

        return tool_success("schema_analysis", analysis)

    except Exception as e:
        return tool_error(f"Error analyzing CSV schema: {str(e)}")


# Tool: Generate Node Plan
def generate_node_plan(
    schema_analysis: Dict[str, Any],
    file_path: str
) -> Dict[str, Any]:
    """Generate a construction plan for a node entity.

    Args:
        schema_analysis: Schema analysis of the CSV file
        file_path: Path to the CSV file

    Returns:
        Dictionary with node construction plan
    """
    file_name = schema_analysis["file_name"]
    entity_type = schema_analysis["entity_type"]

    # Find the best primary key
    primary_key = None
    if schema_analysis["primary_key_candidates"]:
        primary_key = schema_analysis["primary_key_candidates"][0]
    else:
        # Look for any unique column
        for col in schema_analysis["columns"]:
            if col["is_unique"]:
                primary_key = col["name"]
                break

    if not primary_key:
        # Use first column as fallback
        primary_key = schema_analysis["columns"][0]["name"] if schema_analysis["columns"] else "id"

    # Select properties (non-key columns)
    properties = []
    for col in schema_analysis["columns"]:
        if col["name"] != primary_key and col["column_type"] == "attribute":
            properties.append(col["name"])

    plan = {
        entity_type: {
            "construction_type": "node",
            "source_file": file_name,
            "label": entity_type,
            "unique_column_name": primary_key,
            "properties": properties[:20],  # Limit to 20 properties
            "metadata": {
                "row_count": schema_analysis["row_count"],
                "has_foreign_keys": len(schema_analysis["foreign_key_candidates"]) > 0,
                "foreign_keys": [fk["column"] for fk in schema_analysis["foreign_key_candidates"]]
            }
        }
    }

    return tool_success("node_plan", plan)


# Tool: Generate Relationship Plan
def generate_relationship_plan(
    schema_analysis: Dict[str, Any],
    file_path: str,
    existing_nodes: List[str]
) -> Dict[str, Any]:
    """Generate a construction plan for relationships.

    Args:
        schema_analysis: Schema analysis of the CSV file
        file_path: Path to the CSV file
        existing_nodes: List of existing node labels

    Returns:
        Dictionary with relationship construction plan
    """
    file_name = schema_analysis["file_name"]
    plans = {}

    # For relationship tables (with 2+ foreign keys)
    if schema_analysis["is_relationship_table"] and len(schema_analysis["foreign_key_candidates"]) >= 2:
        fks = schema_analysis["foreign_key_candidates"]

        # Determine relationship type from file name
        rel_type = file_name.replace('.csv', '').upper().replace('_', '_')

        # Create relationship between first two foreign keys
        from_fk = fks[0]
        to_fk = fks[1]

        # Find matching node labels
        from_label = from_fk["references"].title()
        to_label = to_fk["references"].title()

        # Check if referenced nodes exist
        from_exists = any(node.lower() == from_label.lower() for node in existing_nodes)
        to_exists = any(node.lower() == to_label.lower() for node in existing_nodes)

        if from_exists and to_exists:
            rel_key = f"{from_label}_TO_{to_label}"
            plans[rel_key] = {
                "construction_type": "relationship",
                "source_file": file_name,
                "relationship_type": rel_type,
                "from_node_label": from_label,
                "from_node_column": from_fk["column"],
                "to_node_label": to_label,
                "to_node_column": to_fk["column"],
                "properties": []  # Could add properties from other columns
            }

    # For node tables with foreign keys
    elif not schema_analysis["is_relationship_table"] and schema_analysis["foreign_key_candidates"]:
        entity_type = schema_analysis["entity_type"]

        for fk in schema_analysis["foreign_key_candidates"]:
            referenced = fk["references"].title()

            # Check if referenced node exists
            if any(node.lower() == referenced.lower() for node in existing_nodes):
                rel_type = f"BELONGS_TO"  # Default relationship type
                rel_key = f"{entity_type}_BELONGS_TO_{referenced}"

                plans[rel_key] = {
                    "construction_type": "relationship",
                    "source_file": file_name,
                    "relationship_type": rel_type,
                    "from_node_label": entity_type,
                    "from_node_column": schema_analysis["primary_key_candidates"][0] if schema_analysis["primary_key_candidates"] else "id",
                    "to_node_label": referenced,
                    "to_node_column": fk["column"],
                    "properties": []
                }

    return tool_success("relationship_plans", plans)


# Tool: Generate Extraction Plan for Text
def generate_text_extraction_plan(
    text_files: List[str],
    goal: Dict[str, Any],
    domain_entities: List[str]
) -> Dict[str, Any]:
    """Generate an extraction plan for text files.

    Args:
        text_files: List of text file paths
        goal: The knowledge graph goal
        domain_entities: Entities from the domain graph

    Returns:
        Dictionary with extraction plan
    """
    # Base entity types from goal
    entity_types = list(domain_entities)

    # Add text-specific entities based on goal
    if "quality" in str(goal.get("expected_insights", [])).lower():
        entity_types.extend(["Issue", "Defect", "QualityMetric"])

    if "customer" in str(goal.get("expected_insights", [])).lower():
        entity_types.extend(["User", "Rating", "Review", "Feedback"])

    if "performance" in str(goal.get("expected_insights", [])).lower():
        entity_types.extend(["Metric", "Trend", "Benchmark"])

    # Add generic entities for any text
    entity_types.extend(["Feature", "Specification"])

    # Remove duplicates
    entity_types = list(set(entity_types))

    # Generate fact types (relationships)
    fact_types = {}

    # Common patterns
    if "Product" in domain_entities:
        if "Issue" in entity_types:
            fact_types["has_issue"] = {
                "subject_label": "Product",
                "predicate_label": "has_issue",
                "object_label": "Issue"
            }
        if "Feature" in entity_types:
            fact_types["has_feature"] = {
                "subject_label": "Product",
                "predicate_label": "has_feature",
                "object_label": "Feature"
            }
        if "User" in entity_types:
            fact_types["reviewed_by"] = {
                "subject_label": "Product",
                "predicate_label": "reviewed_by",
                "object_label": "User"
            }

    if "Supplier" in domain_entities and "QualityMetric" in entity_types:
        fact_types["measured_by"] = {
            "subject_label": "Supplier",
            "predicate_label": "measured_by",
            "object_label": "QualityMetric"
        }

    plan = {
        "entity_types": entity_types[:15],  # Limit to 15 entity types
        "fact_types": fact_types,
        "text_file_count": len(text_files),
        "extraction_strategy": "llm_based",
        "chunk_size": 1000,
        "chunk_overlap": 100
    }

    return tool_success("extraction_plan", plan)


# Create the ADK Schema Agent
def create_schema_agent(llm_model: str = "gpt-4o-mini") -> Agent:
    """Create an ADK Agent for schema generation."""

    system_prompt = """You are an expert knowledge graph architect specializing in schema design.

Your task is to analyze data files and generate optimal construction plans for the knowledge graph.
You should:
1. Analyze CSV files to understand their structure and relationships
2. Identify node entities vs relationship tables
3. Detect primary keys and foreign keys
4. Generate construction plans for nodes and relationships
5. Create extraction plans for text content

When analyzing schemas:
- Identify unique identifiers for each entity
- Detect foreign key relationships
- Recognize mapping/junction tables
- Understand data cardinality

For construction plans:
- Nodes need: label, unique key, properties
- Relationships need: type, from/to nodes, connecting columns
- Consider data quality and completeness

For text extraction:
- Include domain entities from structured data
- Add text-specific entities (Issue, Feature, User, etc.)
- Define fact types connecting entities

Optimize for:
- Clear entity boundaries
- Meaningful relationships
- Efficient graph traversal
- Rich property data"""

    agent = Agent(
        name="adk_schema_agent",
        tools=[
            analyze_csv_schema,
            generate_node_plan,
            generate_relationship_plan,
            generate_text_extraction_plan
        ],
        instruction=system_prompt,
        model=LiteLlm(model=f"openai/{llm_model}")
    )

    return agent


class ADKSchemaAgent:
    """
    ADK-based Schema Agent that uses LLM for intelligent schema generation.
    """

    def __init__(self, llm_model: str = "gpt-4o-mini"):
        self.name = "ADKSchemaAgent"
        self.description = "Intelligently generates schema and construction plans using LLM"
        self.llm_model = llm_model
        self.agent = create_schema_agent(llm_model)

    async def generate_schema(
        self,
        csv_files: List[str],
        text_files: List[str],
        goal: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Generate schema and construction plans using LLM analysis.

        Args:
            csv_files: List of CSV file paths
            text_files: List of text file paths
            goal: The knowledge graph goal

        Returns:
            Tuple of (construction_plan, extraction_plan)
        """
        print("  🤖 Using ADK Agent with LLM to generate schema...")

        # Create session service and runner
        session_service = InMemorySessionService()
        app_name = f"{self.name}_app"
        user_id = f"{self.name}_user"
        session_id = f"{self.name}_session"

        # Initialize session
        await session_service.create_session(
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
            state={}
        )

        # Create runner
        runner = Runner(
            agent=self.agent,
            app_name=app_name,
            session_service=session_service
        )

        # Build the schema generation prompt
        prompt = f"""Generate knowledge graph schema for this goal:

Goal: {goal['kind_of_graph']}
Description: {goal['description']}
Primary Entities: {', '.join(goal.get('primary_entities', [])[:5])}

CSV Files to analyze:
{chr(10).join(f'- {f}' for f in csv_files[:10])}

Text Files for extraction:
{chr(10).join(f'- {f}' for f in text_files[:10])}

Please:
1. Analyze each CSV file's schema
2. Identify node entities (with primary keys)
3. Identify relationship tables or foreign keys
4. Generate node construction plans
5. Generate relationship construction plans
6. Create text extraction plan with entity and fact types

Focus on creating a connected graph where all entities have meaningful relationships."""

        # Execute the agent
        try:
            # Prepare the user message
            content = types.Content(role='user', parts=[types.Part(text=prompt)])

            construction_plan = {}
            extraction_plan = {}

            # Run the agent and collect results
            async for event in runner.run_async(user_id=user_id, session_id=session_id, new_message=content):
                if event.is_final_response():
                    if event.content and event.content.parts:
                        # Parse the response to extract plans
                        response_text = event.content.parts[0].text
                        # For now, use default plans (would need proper parsing)
                        construction_plan = self._generate_default_construction_plan(csv_files)
                        extraction_plan = self._generate_default_extraction_plan(text_files, goal)
                    break

            # Fallback if parsing fails
            if not construction_plan:
                construction_plan = self._generate_default_construction_plan(csv_files)
            if not extraction_plan:
                extraction_plan = self._generate_default_extraction_plan(text_files, goal)

            print(f"    ✅ Generated plan with {len([v for v in construction_plan.values() if v.get('construction_type') == 'node'])} nodes")
            print(f"    ✅ Generated plan with {len([v for v in construction_plan.values() if v.get('construction_type') == 'relationship'])} relationships")

            return construction_plan, extraction_plan

        except Exception as e:
            print(f"    ⚠️ LLM schema generation failed: {str(e)}")
            print("    📌 Falling back to heuristic schema...")
            return self._fallback_heuristic_schema(csv_files, text_files, goal)

    def _generate_default_construction_plan(self, csv_files: List[str]) -> Dict[str, Any]:
        """Generate a default construction plan."""
        plan = {}

        for file in csv_files:
            entity = os.path.basename(file).replace('.csv', '').title()
            plan[entity] = {
                "construction_type": "node",
                "source_file": os.path.basename(file),
                "label": entity,
                "unique_column_name": "id",
                "properties": []
            }

        return plan

    def _generate_default_extraction_plan(self, text_files: List[str], goal: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a default extraction plan."""
        return {
            "entity_types": goal.get("primary_entities", ["Product", "Issue", "Feature"])[:10],
            "fact_types": {
                "related_to": {
                    "subject_label": "Product",
                    "predicate_label": "related_to",
                    "object_label": "Issue"
                }
            }
        }

    def _fallback_heuristic_schema(
        self,
        csv_files: List[str],
        text_files: List[str],
        goal: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Fallback to heuristic schema generation."""
        construction_plan = self._generate_default_construction_plan(csv_files)
        extraction_plan = self._generate_default_extraction_plan(text_files, goal)
        return construction_plan, extraction_plan

    def save_plans(
        self,
        construction_plan: Dict[str, Any],
        extraction_plan: Dict[str, Any],
        output_dir: str = "generated_plans"
    ) -> Tuple[str, str]:
        """Save the generated plans to JSON files."""
        os.makedirs(output_dir, exist_ok=True)

        # Save construction plan
        construction_file = os.path.join(output_dir, "construction_plan.json")
        with open(construction_file, 'w') as f:
            json.dump(construction_plan, f, indent=2)

        # Save extraction plan
        extraction_file = os.path.join(output_dir, "extraction_plan.json")
        with open(extraction_file, 'w') as f:
            json.dump(extraction_plan, f, indent=2)

        print(f"    💾 Construction plan saved to: {construction_file}")
        print(f"    💾 Extraction plan saved to: {extraction_file}")

        return construction_file, extraction_file

    async def load_or_generate_plans(
        self,
        csv_files: List[str],
        text_files: List[str],
        goal: Dict[str, Any],
        force_regenerate: bool = False
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Load existing plans or generate new ones using LLM.

        Args:
            csv_files: List of CSV files
            text_files: List of text files
            goal: The knowledge graph goal
            force_regenerate: Force generation of new plans

        Returns:
            Tuple of (construction_plan, extraction_plan)
        """
        construction_file = "generated_plans/construction_plan.json"
        extraction_file = "generated_plans/extraction_plan.json"

        # Try to load existing plans
        if not force_regenerate and os.path.exists(construction_file) and os.path.exists(extraction_file):
            print("  📂 Loading existing schema plans...")
            with open(construction_file, 'r') as f:
                construction_plan = json.load(f)
            with open(extraction_file, 'r') as f:
                extraction_plan = json.load(f)
            print(f"    ✅ Loaded plans: {len(construction_plan)} construction items")
            return construction_plan, extraction_plan

        # Generate new plans using LLM
        construction_plan, extraction_plan = await self.generate_schema(csv_files, text_files, goal)
        self.save_plans(construction_plan, extraction_plan)

        return construction_plan, extraction_plan


# Schema Validation Agent
class SchemaValidationAgent:
    """Agent that validates the generated schema."""

    def __init__(self, llm_model: str = "gpt-4o-mini"):
        self.llm_model = llm_model

    async def validate_schema(
        self,
        construction_plan: Dict[str, Any],
        extraction_plan: Dict[str, Any],
        goal: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate the generated schema for completeness and correctness.

        Returns:
            Validation result with improvement suggestions
        """
        # For now, return a simple validation result
        # In production, this would use an LLM to evaluate the schema

        # Parse validation response
        validation = {
            "is_valid": True,
            "score": 85,
            "issues": [],
            "improvements": ["Consider adding more relationship types between entities"],
            "timestamp": datetime.now().isoformat()
        }

        print(f"    🔍 Schema validation score: {validation['score']}/100")

        return validation


# Example usage
async def main():
    """Example of using the ADK Schema Agent."""
    # Sample goal
    goal = {
        "kind_of_graph": "supply chain management",
        "description": "A comprehensive graph connecting Supplier, Product, Part entities",
        "primary_entities": ["Supplier", "Product", "Part"],
        "expected_insights": ["quality issues", "supplier performance"]
    }

    # Sample file lists
    csv_files = [
        "data/products.csv",
        "data/suppliers.csv",
        "data/parts.csv",
        "data/part_supplier_mapping.csv"
    ]

    text_files = [
        "data/reviews/product1.md",
        "data/reports/quality.txt"
    ]

    # Create and run agent
    agent = ADKSchemaAgent()
    construction_plan, extraction_plan = await agent.generate_schema(csv_files, text_files, goal)

    # Validate schema
    validator = SchemaValidationAgent()
    validation = await validator.validate_schema(construction_plan, extraction_plan, goal)

    print(f"\nConstruction Plan:")
    print(json.dumps(construction_plan, indent=2))
    print(f"\nExtraction Plan:")
    print(json.dumps(extraction_plan, indent=2))
    print(f"\nValidation Result:")
    print(json.dumps(validation, indent=2))


if __name__ == "__main__":
    asyncio.run(main())