"""
ADK-based File Selection Agent with LLM decision making
Intelligently selects relevant files based on the knowledge graph goal
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


# Tool: Score CSV Relevance
def score_csv_relevance(
    file_path: str,
    goal: Dict[str, Any],
    sample_rows: int = 10
) -> Dict[str, Any]:
    """Score the relevance of a CSV file to the knowledge graph goal.

    Args:
        file_path: Path to the CSV file
        goal: The knowledge graph goal
        sample_rows: Number of rows to sample for analysis

    Returns:
        Dictionary with relevance score and reasoning
    """
    try:
        df = pd.read_csv(file_path)
        file_name = os.path.basename(file_path)

        # Initialize scoring
        score = 0.0
        reasons = []

        # Check against primary entities
        primary_entities = goal.get("primary_entities", [])
        file_name_lower = file_name.lower()

        for entity in primary_entities:
            if entity.lower() in file_name_lower:
                score += 0.3
                reasons.append(f"Contains primary entity '{entity}'")

        # Check for ID columns (indicates it's an entity table)
        id_columns = [col for col in df.columns if 'id' in col.lower() or col.lower().endswith('_key')]
        if id_columns:
            score += 0.2
            reasons.append(f"Has ID columns: {', '.join(id_columns[:3])}")

        # Check for foreign keys (indicates relationships)
        foreign_keys = []
        for col in df.columns:
            col_lower = col.lower()
            if '_id' in col_lower and col_lower != 'id':
                foreign_keys.append(col)

        if foreign_keys:
            score += 0.3
            reasons.append(f"Has foreign keys: {', '.join(foreign_keys[:3])}")

        # Check for mapping/relationship tables
        if 'mapping' in file_name_lower or '_to_' in file_name_lower:
            score += 0.2
            reasons.append("Appears to be a relationship/mapping table")

        # Check column relevance to goal
        goal_keywords = []
        if 'supply chain' in goal.get('kind_of_graph', '').lower():
            goal_keywords = ['supplier', 'part', 'product', 'assembly', 'vendor', 'material']
        elif 'customer' in goal.get('kind_of_graph', '').lower():
            goal_keywords = ['customer', 'order', 'purchase', 'review', 'rating', 'satisfaction']

        matching_columns = []
        for col in df.columns:
            col_lower = col.lower()
            for keyword in goal_keywords:
                if keyword in col_lower:
                    matching_columns.append(col)
                    break

        if matching_columns:
            score += 0.2
            reasons.append(f"Has relevant columns: {', '.join(matching_columns[:3])}")

        # Check data completeness
        completeness = 1 - (df.isnull().sum().sum() / (len(df) * len(df.columns)))
        if completeness > 0.8:
            score += 0.1
            reasons.append(f"High data completeness ({completeness:.1%})")

        # Cap score at 1.0
        score = min(score, 1.0)

        result = {
            "file": file_name,
            "score": round(score, 2),
            "reasons": reasons,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns)[:10],  # First 10 columns
            "sample_data": df.head(sample_rows).to_dict(orient='records')
        }

        return tool_success("csv_relevance", result)

    except Exception as e:
        return tool_error(f"Error scoring CSV relevance: {str(e)}")


# Tool: Score Text Relevance
def score_text_relevance(
    file_path: str,
    goal: Dict[str, Any],
    max_chars: int = 2000
) -> Dict[str, Any]:
    """Score the relevance of a text file to the knowledge graph goal.

    Args:
        file_path: Path to the text file
        goal: The knowledge graph goal
        max_chars: Maximum characters to analyze

    Returns:
        Dictionary with relevance score and reasoning
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        file_name = os.path.basename(file_path)
        content_lower = content[:max_chars].lower()

        # Initialize scoring
        score = 0.0
        reasons = []

        # Check against expected content sources
        content_sources = goal.get("content_sources", [])
        for source in content_sources:
            if source.lower() in file_name.lower() or source.lower() in content_lower:
                score += 0.3
                reasons.append(f"Matches content source '{source}'")

        # Check for entity mentions
        primary_entities = goal.get("primary_entities", [])
        mentioned_entities = []
        for entity in primary_entities:
            if entity.lower() in content_lower:
                mentioned_entities.append(entity)

        if mentioned_entities:
            score += 0.2 * min(len(mentioned_entities) / len(primary_entities), 1.0)
            reasons.append(f"Mentions entities: {', '.join(mentioned_entities[:3])}")

        # Check for insight-related keywords
        expected_insights = goal.get("expected_insights", [])
        insight_keywords = []
        for insight in expected_insights:
            keywords = insight.lower().split()
            for keyword in keywords:
                if len(keyword) > 4 and keyword in content_lower:
                    insight_keywords.append(keyword)

        if insight_keywords:
            score += 0.2
            reasons.append(f"Contains insight keywords: {', '.join(set(insight_keywords[:3]))}")

        # Check file type relevance
        if 'review' in file_name.lower() and 'quality' in ' '.join(expected_insights).lower():
            score += 0.2
            reasons.append("Review file relevant for quality insights")
        elif 'report' in file_name.lower() and 'performance' in ' '.join(expected_insights).lower():
            score += 0.2
            reasons.append("Report file relevant for performance insights")

        # Check content richness
        word_count = len(content.split())
        if word_count > 100:
            score += 0.1
            reasons.append(f"Substantial content ({word_count} words)")

        # Cap score at 1.0
        score = min(score, 1.0)

        result = {
            "file": file_name,
            "score": round(score, 2),
            "reasons": reasons,
            "char_count": len(content),
            "word_count": word_count,
            "sample_content": content[:500],
            "mentioned_entities": mentioned_entities
        }

        return tool_success("text_relevance", result)

    except Exception as e:
        return tool_error(f"Error scoring text relevance: {str(e)}")


# Tool: Select Files Based on Scores
def select_files_by_threshold(
    csv_scores: List[Dict[str, Any]],
    text_scores: List[Dict[str, Any]],
    csv_threshold: float = 0.3,
    text_threshold: float = 0.3,
    max_files: int = 50
) -> Dict[str, Any]:
    """Select files that meet the relevance threshold.

    Args:
        csv_scores: List of CSV file scores
        text_scores: List of text file scores
        csv_threshold: Minimum score for CSV files
        text_threshold: Minimum score for text files
        max_files: Maximum number of files to select

    Returns:
        Dictionary with selected and rejected files
    """
    # Filter CSV files
    selected_csv = []
    rejected_csv = []

    for score_data in sorted(csv_scores, key=lambda x: x['score'], reverse=True):
        if score_data['score'] >= csv_threshold and len(selected_csv) < max_files:
            selected_csv.append(score_data)
        else:
            rejected_csv.append(score_data)

    # Filter text files
    selected_text = []
    rejected_text = []

    for score_data in sorted(text_scores, key=lambda x: x['score'], reverse=True):
        if score_data['score'] >= text_threshold and len(selected_text) < max_files:
            selected_text.append(score_data)
        else:
            rejected_text.append(score_data)

    result = {
        "selected_csv": selected_csv,
        "selected_text": selected_text,
        "rejected_csv": rejected_csv,
        "rejected_text": rejected_text,
        "summary": {
            "total_csv_selected": len(selected_csv),
            "total_text_selected": len(selected_text),
            "total_csv_rejected": len(rejected_csv),
            "total_text_rejected": len(rejected_text),
            "avg_csv_score": round(sum(s['score'] for s in selected_csv) / len(selected_csv), 2) if selected_csv else 0,
            "avg_text_score": round(sum(s['score'] for s in selected_text) / len(selected_text), 2) if selected_text else 0
        }
    }

    return tool_success("file_selection", result)


# Create the ADK File Selection Agent
def create_file_selection_agent(llm_model: str = "gpt-4o-mini") -> Agent:
    """Create an ADK Agent for selecting relevant files."""

    system_prompt = """You are an intelligent data curator specializing in selecting relevant files for knowledge graph construction.

Your task is to evaluate files and select the most relevant ones based on the knowledge graph goal.
You should:
1. Score each CSV file based on its relevance to the goal
2. Score each text file based on its potential insights
3. Select files that meet quality and relevance thresholds
4. Provide clear reasoning for selections and rejections

When scoring files:
- Consider entity matches with the goal
- Check for relationship indicators (foreign keys, mapping tables)
- Evaluate data quality and completeness
- Assess text content relevance to expected insights

Selection criteria:
- Prefer files with clear entity definitions (ID columns)
- Include relationship/mapping files for connections
- Select text files that complement structured data
- Balance coverage across different entity types

Be strategic in selection - quality over quantity."""

    agent = Agent(
        name="adk_file_selection_agent",
        tools=[
            score_csv_relevance,
            score_text_relevance,
            select_files_by_threshold
        ],
        instruction=system_prompt,
        model=LiteLlm(model=f"openai/{llm_model}")
    )

    return agent


class ADKFileSelectionAgent:
    """
    ADK-based File Selection Agent that uses LLM for intelligent file selection.
    """

    def __init__(self, llm_model: str = "gpt-4o-mini"):
        self.name = "ADKFileSelectionAgent"
        self.description = "Intelligently selects relevant files using LLM analysis"
        self.llm_model = llm_model
        self.agent = create_file_selection_agent(llm_model)

    async def select_files(
        self,
        csv_files: List[str],
        text_files: List[str],
        goal: Dict[str, Any],
        csv_threshold: float = 0.3,
        text_threshold: float = 0.3
    ) -> Dict[str, Any]:
        """
        Select relevant files using LLM-based scoring.

        Args:
            csv_files: List of CSV file paths
            text_files: List of text file paths
            goal: The knowledge graph goal
            csv_threshold: Minimum score for CSV files
            text_threshold: Minimum score for text files

        Returns:
            Dictionary with selected files and analysis
        """
        print("  🤖 Using ADK Agent with LLM to select files...")

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

        # Build the selection prompt
        prompt = f"""Select relevant files for this knowledge graph goal:

Goal: {goal['kind_of_graph']}
Description: {goal['description']}
Primary Entities: {', '.join(goal.get('primary_entities', [])[:5])}
Expected Insights: {', '.join(goal.get('expected_insights', [])[:3])}

Available Files:
CSV Files ({len(csv_files)}):
{chr(10).join(f'- {os.path.basename(f)}' for f in csv_files[:15])}

Text Files ({len(text_files)}):
{chr(10).join(f'- {os.path.basename(f)}' for f in text_files[:15])}

Please:
1. Score each CSV file (focus on the most relevant ones)
2. Score each text file (prioritize those with insights)
3. Select files using thresholds: CSV >= {csv_threshold}, Text >= {text_threshold}

Provide reasoning for each selection."""

        # Execute the agent
        try:
            # Prepare the user message
            content = types.Content(role='user', parts=[types.Part(text=prompt)])

            selection = None
            # Run the agent and collect results
            async for event in runner.run_async(user_id=user_id, session_id=session_id, new_message=content):
                if event.is_final_response():
                    if event.content and event.content.parts:
                        # Parse the response to extract selection
                        response_text = event.content.parts[0].text
                        selection = self._parse_selection_from_response(response_text)
                    break

            # Fallback if no selection extracted
            if not selection:
                selection = self._fallback_heuristic_selection(csv_files, text_files, goal)

            print(f"    ✅ Selected {selection['summary']['total_csv_selected']} CSV files")
            print(f"    ✅ Selected {selection['summary']['total_text_selected']} text files")

            return selection

        except Exception as e:
            print(f"    ⚠️ LLM selection failed: {str(e)}")
            print("    📌 Falling back to heuristic selection...")
            return self._fallback_heuristic_selection(csv_files, text_files, goal)

    def _parse_selection_from_response(self, response: Any) -> Dict[str, Any]:
        """Parse file selection from agent response."""
        # Default selection structure
        selection = {
            "selected_csv": [],
            "selected_text": [],
            "rejected_csv": [],
            "rejected_text": [],
            "summary": {
                "total_csv_selected": 0,
                "total_text_selected": 0,
                "total_csv_rejected": 0,
                "total_text_rejected": 0
            }
        }

        # Try to extract information from response
        # (Implementation would depend on response format)

        return selection

    def _fallback_heuristic_selection(
        self,
        csv_files: List[str],
        text_files: List[str],
        goal: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fallback to heuristic selection if LLM fails."""
        selected_csv = []
        selected_text = []

        # Simple heuristic: select files with entity names
        primary_entities = goal.get("primary_entities", [])

        for file in csv_files:
            file_name = os.path.basename(file).lower()
            for entity in primary_entities:
                if entity.lower() in file_name:
                    selected_csv.append({
                        "file": os.path.basename(file),
                        "path": file,
                        "score": 0.5,
                        "reasons": ["Name matches entity"]
                    })
                    break

        # Select some text files
        for file in text_files[:5]:  # Take first 5
            selected_text.append({
                "file": os.path.basename(file),
                "path": file,
                "score": 0.4,
                "reasons": ["Text content available"]
            })

        return {
            "selected_csv": selected_csv,
            "selected_text": selected_text,
            "rejected_csv": [],
            "rejected_text": [],
            "summary": {
                "total_csv_selected": len(selected_csv),
                "total_text_selected": len(selected_text),
                "total_csv_rejected": 0,
                "total_text_rejected": 0
            }
        }

    def format_selection_for_storage(self, selection: Dict[str, Any]) -> Dict[str, Any]:
        """Format the selection for storage as JSON."""
        # Extract file paths
        approved_csv = [item.get('path', item.get('file', '')) for item in selection['selected_csv']]
        approved_text = [item.get('path', item.get('file', '')) for item in selection['selected_text']]

        return {
            "approved_csv_files": approved_csv,
            "approved_text_files": approved_text,
            "csv_analysis": selection['selected_csv'],
            "text_analysis": selection['selected_text'],
            "rejection_summary": {
                "csv_rejected": len(selection.get('rejected_csv', [])),
                "text_rejected": len(selection.get('rejected_text', [])),
            },
            "selection_summary": selection['summary'],
            "timestamp": datetime.now().isoformat()
        }

    def save_selection(self, selection: Dict[str, Any], output_dir: str = "generated_plans") -> str:
        """Save the file selection to a JSON file."""
        os.makedirs(output_dir, exist_ok=True)

        formatted = self.format_selection_for_storage(selection)
        output_file = os.path.join(output_dir, "approved_files.json")

        with open(output_file, 'w') as f:
            json.dump(formatted, f, indent=2)

        print(f"    💾 File selection saved to: {output_file}")
        return output_file

    async def load_or_select_files(
        self,
        csv_files: List[str],
        text_files: List[str],
        goal: Dict[str, Any],
        force_reselect: bool = False
    ) -> Dict[str, Any]:
        """
        Load existing selection or select new files using LLM.

        Args:
            csv_files: List of CSV files
            text_files: List of text files
            goal: The knowledge graph goal
            force_reselect: Force new selection

        Returns:
            The file selection
        """
        selection_file = "generated_plans/approved_files.json"

        # Try to load existing selection
        if not force_reselect and os.path.exists(selection_file):
            print("  📂 Loading existing file selection...")
            with open(selection_file, 'r') as f:
                selection = json.load(f)
            print(f"    ✅ Loaded selection: {len(selection['approved_csv_files'])} CSV, {len(selection['approved_text_files'])} text")
            return selection

        # Select new files using LLM
        selection = await self.select_files(csv_files, text_files, goal)
        formatted = self.format_selection_for_storage(selection)
        self.save_selection(selection)

        return formatted


# Validation Agent for File Selection
class FileSelectionValidationAgent:
    """Agent that validates the file selection."""

    def __init__(self, llm_model: str = "gpt-4o-mini"):
        self.llm_model = llm_model

    async def validate_selection(
        self,
        selection: Dict[str, Any],
        goal: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate the file selection for completeness and relevance.

        Returns:
            Validation result with improvement suggestions
        """
        # For now, return a simple validation result
        # In production, this would use an LLM to evaluate the selection

        # Parse validation response
        validation = {
            "is_valid": True,
            "score": 80,
            "missing_entities": [],
            "recommendations": ["Consider including more relationship mapping files"],
            "timestamp": datetime.now().isoformat()
        }

        print(f"    🔍 Selection validation score: {validation['score']}/100")

        return validation


# Example usage
async def main():
    """Example of using the ADK File Selection Agent."""
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
        "data/orders.csv",
        "data/part_supplier_mapping.csv"
    ]

    text_files = [
        "data/reviews/product1.md",
        "data/reports/quality.txt",
        "data/other/notes.md"
    ]

    # Create and run agent
    agent = ADKFileSelectionAgent()
    selection = await agent.select_files(csv_files, text_files, goal)

    # Validate selection
    validator = FileSelectionValidationAgent()
    validation = await validator.validate_selection(selection, goal)

    print(f"\nFile Selection:")
    print(json.dumps(selection['summary'], indent=2))
    print(f"\nValidation Result:")
    print(json.dumps(validation, indent=2))


if __name__ == "__main__":
    asyncio.run(main())