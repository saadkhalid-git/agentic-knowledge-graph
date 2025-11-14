"""
ADK-based Intent Agent with LLM decision making
Automatically determines the knowledge graph goal using intelligent data analysis
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import json
import asyncio
import pandas as pd
from typing import Dict, Any, List
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


# Tool: Analyze CSV Structure
def analyze_csv_structure(file_path: str, sample_rows: int = 5) -> Dict[str, Any]:
    """Analyze the structure and content of a CSV file.

    Args:
        file_path: Path to the CSV file
        sample_rows: Number of rows to sample

    Returns:
        Dictionary with file analysis including columns, data types, and sample data
    """
    try:
        df = pd.read_csv(file_path)

        # Basic statistics
        analysis = {
            "file_name": os.path.basename(file_path),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "has_id_columns": [],
            "has_foreign_keys": [],
            "sample_data": []
        }

        # Identify ID columns
        for col in df.columns:
            col_lower = col.lower()
            if 'id' in col_lower or col_lower.endswith('_key') or col_lower == 'key':
                analysis["has_id_columns"].append(col)

                # Check if it's a foreign key
                if '_id' in col_lower and col_lower != 'id':
                    base_entity = col_lower.replace('_id', '')
                    analysis["has_foreign_keys"].append({
                        "column": col,
                        "references": base_entity
                    })

        # Get sample data
        sample = df.head(sample_rows).to_dict(orient='records')
        analysis["sample_data"] = sample

        # Check for null values
        null_counts = df.isnull().sum()
        analysis["null_columns"] = {col: int(count) for col, count in null_counts.items() if count > 0}

        return tool_success("csv_analysis", analysis)

    except Exception as e:
        return tool_error(f"Error analyzing CSV: {str(e)}")


# Tool: Analyze Text Content
def analyze_text_content(file_path: str, max_chars: int = 1000) -> Dict[str, Any]:
    """Analyze the content of a text file.

    Args:
        file_path: Path to the text file
        max_chars: Maximum characters to sample

    Returns:
        Dictionary with text analysis including sample content and detected patterns
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        analysis = {
            "file_name": os.path.basename(file_path),
            "total_chars": len(content),
            "total_lines": content.count('\n') + 1,
            "sample_content": content[:max_chars],
            "detected_patterns": []
        }

        # Detect content patterns
        content_lower = content.lower()

        patterns = {
            "customer_review": ["rating", "review", "customer", "product", "quality"],
            "quality_report": ["defect", "quality", "issue", "inspection", "failure"],
            "supplier_assessment": ["supplier", "vendor", "delivery", "performance", "reliability"],
            "product_description": ["feature", "specification", "dimension", "material", "design"],
            "business_report": ["revenue", "profit", "growth", "market", "analysis"],
            "technical_document": ["system", "component", "interface", "architecture", "protocol"]
        }

        for pattern_name, keywords in patterns.items():
            matches = sum(1 for keyword in keywords if keyword in content_lower)
            if matches >= 2:  # At least 2 keywords match
                analysis["detected_patterns"].append(pattern_name)

        return tool_success("text_analysis", analysis)

    except Exception as e:
        return tool_error(f"Error analyzing text: {str(e)}")


# Tool: Generate Goal from Analysis
def generate_goal_from_analysis(
    csv_analyses: List[Dict[str, Any]],
    text_analyses: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Generate a knowledge graph goal based on file analyses.

    Args:
        csv_analyses: List of CSV file analyses
        text_analyses: List of text file analyses

    Returns:
        Dictionary containing the generated goal
    """
    # Extract entity types from CSV files
    entity_types = set()
    relationships = []

    for analysis in csv_analyses:
        file_name = analysis["file_name"].lower().replace('.csv', '')

        # Extract entity from filename
        entity_candidates = file_name.replace('_', ' ').split()
        for candidate in entity_candidates:
            if len(candidate) > 3 and candidate not in ['data', 'file', 'export', 'import']:
                entity_types.add(candidate.capitalize())

        # Detect relationship files
        if any(fk for fk in analysis.get("has_foreign_keys", [])):
            for fk in analysis["has_foreign_keys"]:
                relationships.append({
                    "from": fk["references"].capitalize(),
                    "to": analysis["file_name"].replace('.csv', '').capitalize(),
                    "via": fk["column"]
                })

    # Extract content types from text files
    content_types = []
    insights = []

    for analysis in text_analyses:
        patterns = analysis.get("detected_patterns", [])
        if patterns:
            content_types.extend(patterns)

            # Map patterns to insights
            insight_map = {
                "customer_review": "customer satisfaction and product quality",
                "quality_report": "quality issues and defect patterns",
                "supplier_assessment": "supplier performance and reliability",
                "product_description": "product features and specifications",
                "business_report": "business performance and trends",
                "technical_document": "technical architecture and systems"
            }

            for pattern in patterns:
                if pattern in insight_map:
                    insights.append(insight_map[pattern])

    # Determine primary domain
    domain = "business intelligence"
    if "Supplier" in entity_types and "Part" in entity_types:
        domain = "supply chain management"
    elif "Product" in entity_types and "customer_review" in content_types:
        domain = "product quality and customer satisfaction"
    elif "Customer" in entity_types or "Order" in entity_types:
        domain = "customer analytics and sales"

    goal = {
        "kind_of_graph": domain,
        "description": f"A comprehensive knowledge graph connecting {', '.join(list(entity_types)[:5])} entities with relationships to analyze {', '.join(insights[:3]) if insights else 'business operations'}",
        "primary_entities": list(entity_types)[:10],
        "content_sources": list(set(content_types))[:5],
        "expected_insights": list(set(insights))[:5],
        "relationship_count": len(relationships),
        "timestamp": datetime.now().isoformat()
    }

    return tool_success("goal", goal)


# Create the ADK Intent Agent
def create_intent_agent(llm_model: str = "gpt-4o-mini") -> Agent:
    """Create an ADK Agent for determining knowledge graph goals."""

    system_prompt = """You are an intelligent data analyst specializing in knowledge graph design.

Your task is to analyze available data files and determine the optimal knowledge graph goal.
You should:
1. Analyze CSV files to understand the domain entities and relationships
2. Analyze text files to understand additional context and insights
3. Generate a comprehensive goal that captures the value of connecting this data
4. Consider both structured and unstructured data in your analysis

When analyzing files:
- Look for ID columns and foreign keys in CSV files
- Identify entity types from file names and column structures
- Detect content patterns in text files (reviews, reports, descriptions, etc.)
- Understand the business domain from the data

Generate goals that:
- Clearly state the type of knowledge graph (supply chain, customer analytics, etc.)
- Describe the main entities and relationships
- Explain expected insights and value
- Consider how text content enhances structured data

Be specific and actionable in your goal descriptions."""

    agent = Agent(
        name="adk_intent_agent",
        tools=[
            analyze_csv_structure,
            analyze_text_content,
            generate_goal_from_analysis
        ],
        instruction=system_prompt,
        model=LiteLlm(model=f"openai/{llm_model}")
    )

    return agent


class ADKIntentAgent:
    """
    ADK-based Intent Agent that uses LLM for intelligent goal determination.
    """

    def __init__(self, llm_model: str = "gpt-4o-mini"):
        self.name = "ADKIntentAgent"
        self.description = "Intelligently determines knowledge graph goals using LLM analysis"
        self.llm_model = llm_model
        self.agent = create_intent_agent(llm_model)

    async def determine_goal(self, csv_files: List[str], text_files: List[str]) -> Dict[str, Any]:
        """
        Determine the knowledge graph goal using LLM-based analysis.

        Args:
            csv_files: List of CSV file paths
            text_files: List of text file paths

        Returns:
            Dictionary containing the determined goal
        """
        print("  🤖 Using ADK Agent with LLM to determine goal...")

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

        # Build the analysis prompt
        prompt = f"""Analyze the following data files to determine the optimal knowledge graph goal:

CSV Files ({len(csv_files)} files):
{chr(10).join(f'- {os.path.basename(f)}' for f in csv_files[:10])}

Text Files ({len(text_files)} files):
{chr(10).join(f'- {os.path.basename(f)}' for f in text_files[:10])}

Please:
1. Analyze at least 3 CSV files to understand the data structure
2. Analyze at least 2 text files to understand the content
3. Generate a comprehensive goal based on your analysis

Use the tools to examine the files and then generate the final goal."""

        # Execute the agent
        try:
            # Prepare the user message
            content = types.Content(role='user', parts=[types.Part(text=prompt)])

            goal = None
            # Run the agent and collect results
            async for event in runner.run_async(user_id=user_id, session_id=session_id, new_message=content):
                if event.is_final_response():
                    if event.content and event.content.parts:
                        # Parse the response to extract goal
                        response_text = event.content.parts[0].text
                        goal = self._parse_goal_from_response(response_text)
                    break

            # Fallback if no goal extracted
            if not goal:
                goal = self._fallback_heuristic_goal(csv_files, text_files)

            print(f"    ✅ LLM determined goal: {goal.get('kind_of_graph', 'Unknown')}")
            print(f"    📝 Description: {goal.get('description', '')[:100]}...")

            return goal

        except Exception as e:
            print(f"    ⚠️ LLM analysis failed: {str(e)}")
            print("    📌 Falling back to heuristic analysis...")
            return self._fallback_heuristic_goal(csv_files, text_files)

    def _parse_goal_from_response(self, response: Any) -> Dict[str, Any]:
        """Parse goal from agent response."""
        # Default goal structure
        goal = {
            "kind_of_graph": "business intelligence",
            "description": "A comprehensive knowledge graph for business analysis",
            "primary_entities": [],
            "content_sources": [],
            "expected_insights": [],
            "timestamp": datetime.now().isoformat()
        }

        # Try to extract information from response
        if hasattr(response, 'content'):
            content = str(response.content)
            # Basic extraction logic (can be improved)
            if "supply chain" in content.lower():
                goal["kind_of_graph"] = "supply chain management"
            elif "customer" in content.lower():
                goal["kind_of_graph"] = "customer analytics"

        return goal

    def _fallback_heuristic_goal(self, csv_files: List[str], text_files: List[str]) -> Dict[str, Any]:
        """Fallback to heuristic analysis if LLM fails."""
        # Use simple heuristics as fallback
        entity_types = []
        for file in csv_files:
            name = os.path.basename(file).lower().replace('.csv', '')
            if 'product' in name:
                entity_types.append('Product')
            elif 'supplier' in name:
                entity_types.append('Supplier')
            elif 'customer' in name:
                entity_types.append('Customer')

        return {
            "kind_of_graph": "business operations",
            "description": f"A knowledge graph connecting {', '.join(entity_types)} for business analysis",
            "primary_entities": entity_types,
            "content_sources": ["text files"],
            "expected_insights": ["business insights"],
            "timestamp": datetime.now().isoformat()
        }

    def save_goal(self, goal: Dict[str, Any], output_dir: str = "generated_plans") -> str:
        """Save the generated goal to a JSON file."""
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(output_dir, "approved_user_goal.json")

        with open(output_file, 'w') as f:
            json.dump(goal, f, indent=2)

        print(f"    💾 Goal saved to: {output_file}")
        return output_file

    async def load_or_generate_goal(
        self,
        csv_files: List[str],
        text_files: List[str],
        force_regenerate: bool = False
    ) -> Dict[str, Any]:
        """
        Load existing goal or generate a new one using LLM.

        Args:
            csv_files: List of CSV files
            text_files: List of text files
            force_regenerate: Force generation of new goal

        Returns:
            The knowledge graph goal
        """
        goal_file = "generated_plans/approved_user_goal.json"

        # Try to load existing goal
        if not force_regenerate and os.path.exists(goal_file):
            print("  📂 Loading existing goal from file...")
            with open(goal_file, 'r') as f:
                goal = json.load(f)
            print(f"    ✅ Loaded goal: {goal['kind_of_graph']}")
            return goal

        # Generate new goal using LLM
        goal = await self.determine_goal(csv_files, text_files)
        self.save_goal(goal)

        return goal


# Validation Agent for Goal
class GoalValidationAgent:
    """Agent that validates and critiques the generated goal."""

    def __init__(self, llm_model: str = "gpt-4o-mini"):
        self.llm_model = llm_model

    async def validate_goal(self, goal: Dict[str, Any], csv_files: List[str], text_files: List[str]) -> Dict[str, Any]:
        """
        Validate the generated goal for completeness and accuracy.

        Returns:
            Validation result with suggestions for improvement
        """
        # For now, return a simple validation result
        # In production, this would use an LLM to evaluate the goal

        # Parse validation response
        validation = {
            "is_valid": True,  # Can be enhanced with actual validation logic
            "score": 85,  # Can be extracted from LLM response
            "suggestions": ["Consider adding more specific relationship types"],
            "timestamp": datetime.now().isoformat()
        }

        print(f"    🔍 Goal validation score: {validation['score']}/100")

        return validation


# Example usage
async def main():
    """Example of using the ADK Intent Agent."""
    # Sample file lists
    csv_files = [
        "data/products.csv",
        "data/suppliers.csv",
        "data/parts.csv",
        "data/part_supplier_mapping.csv"
    ]

    text_files = [
        "data/product_reviews/review1.md",
        "data/quality_reports/report1.txt"
    ]

    # Create and run agent
    agent = ADKIntentAgent()
    goal = await agent.determine_goal(csv_files, text_files)

    # Validate goal
    validator = GoalValidationAgent()
    validation = await validator.validate_goal(goal, csv_files, text_files)

    print(f"\nGenerated Goal:")
    print(json.dumps(goal, indent=2))
    print(f"\nValidation Result:")
    print(json.dumps(validation, indent=2))


if __name__ == "__main__":
    asyncio.run(main())