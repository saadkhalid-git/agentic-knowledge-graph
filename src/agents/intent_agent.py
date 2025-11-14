"""
Intent Agent for Automated Pipeline
Automatically determines the knowledge graph goal based on available data
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import json
from typing import Dict, Any, List
from pathlib import Path
from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm
from google.adk.tools import ToolContext
from src.neo4j_for_adk import tool_success, tool_error


class AutomatedIntentAgent:
    """
    Agent that automatically determines the knowledge graph goal
    by analyzing available data and common patterns.
    """

    def __init__(self, llm_model: str = "gpt-4o"):
        self.name = "AutomatedIntentAgent"
        self.description = "Automatically determines knowledge graph goals based on data analysis"
        self.llm = LiteLlm(model=f"openai/{llm_model}")

    def analyze_csv_files(self, csv_files: List[str]) -> Dict[str, Any]:
        """Analyze CSV files to understand the data domain."""
        analysis = {
            "domain_indicators": [],
            "entity_types": [],
            "relationship_indicators": []
        }

        for file in csv_files:
            filename = os.path.basename(file).lower()

            # Domain detection
            if "product" in filename:
                analysis["domain_indicators"].append("e-commerce/retail")
                analysis["entity_types"].append("Product")
            elif "supplier" in filename:
                analysis["domain_indicators"].append("supply chain")
                analysis["entity_types"].append("Supplier")
            elif "customer" in filename:
                analysis["domain_indicators"].append("customer relationship")
                analysis["entity_types"].append("Customer")
            elif "part" in filename or "component" in filename:
                analysis["domain_indicators"].append("manufacturing")
                analysis["entity_types"].append("Part")
            elif "assembly" in filename:
                analysis["domain_indicators"].append("bill of materials")
                analysis["entity_types"].append("Assembly")
            elif "order" in filename:
                analysis["domain_indicators"].append("order management")
                analysis["entity_types"].append("Order")
            elif "employee" in filename or "staff" in filename:
                analysis["domain_indicators"].append("human resources")
                analysis["entity_types"].append("Employee")

            # Relationship detection
            if "mapping" in filename or "_to_" in filename or "relationship" in filename:
                parts = filename.replace(".csv", "").split("_")
                analysis["relationship_indicators"].append(f"Links between {' and '.join(parts)}")

        # Remove duplicates
        analysis["domain_indicators"] = list(set(analysis["domain_indicators"]))
        analysis["entity_types"] = list(set(analysis["entity_types"]))

        return analysis

    def analyze_text_files(self, text_files: List[str]) -> Dict[str, Any]:
        """Analyze text files to understand additional context."""
        analysis = {
            "content_types": [],
            "potential_insights": []
        }

        for file in text_files:
            filename = os.path.basename(file).lower()

            if "review" in filename:
                analysis["content_types"].append("customer reviews")
                analysis["potential_insights"].append("quality issues, customer satisfaction")
            elif "report" in filename:
                analysis["content_types"].append("business reports")
                analysis["potential_insights"].append("performance metrics, trends")
            elif "email" in filename or "message" in filename:
                analysis["content_types"].append("communications")
                analysis["potential_insights"].append("interactions, sentiments")
            elif "description" in filename:
                analysis["content_types"].append("product descriptions")
                analysis["potential_insights"].append("features, specifications")
            elif "feedback" in filename:
                analysis["content_types"].append("feedback data")
                analysis["potential_insights"].append("issues, improvements")
            elif "log" in filename:
                analysis["content_types"].append("system logs")
                analysis["potential_insights"].append("events, errors")

        # Remove duplicates
        analysis["content_types"] = list(set(analysis["content_types"]))
        analysis["potential_insights"] = list(set(analysis["potential_insights"]))

        return analysis

    def generate_goal(self, csv_analysis: Dict[str, Any], text_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a knowledge graph goal based on data analysis."""

        # Determine primary domain
        domains = csv_analysis.get("domain_indicators", [])
        primary_domain = "business operations"

        if "supply chain" in domains and "bill of materials" in domains:
            primary_domain = "supply chain analysis"
        elif "e-commerce/retail" in domains and "customer relationship" in domains:
            primary_domain = "customer analytics"
        elif "human resources" in domains:
            primary_domain = "organizational analysis"
        elif "manufacturing" in domains:
            primary_domain = "production management"

        # Build description based on available data
        description_parts = []

        # Add entity information
        entity_types = csv_analysis.get("entity_types", [])
        if entity_types:
            description_parts.append(f"A comprehensive graph connecting {', '.join(entity_types)}")

        # Add relationship information
        relationships = csv_analysis.get("relationship_indicators", [])
        if relationships:
            description_parts.append(f"with relationships showing {'; '.join(relationships)}")

        # Add text analysis insights
        content_types = text_analysis.get("content_types", [])
        insights = text_analysis.get("potential_insights", [])

        if content_types:
            description_parts.append(f"Enhanced with {', '.join(content_types)}")

        if insights:
            description_parts.append(f"to analyze {', '.join(insights)}")

        # Add use case based on domain
        use_cases = {
            "supply chain analysis": "Enables root cause analysis, supplier risk assessment, and cost optimization",
            "customer analytics": "Supports customer segmentation, recommendation systems, and satisfaction tracking",
            "organizational analysis": "Facilitates team dynamics, skill mapping, and resource planning",
            "production management": "Optimizes production flow, quality control, and inventory management",
            "business operations": "Provides insights into operational efficiency and business intelligence"
        }

        use_case = use_cases.get(primary_domain, "Provides comprehensive business insights")
        description_parts.append(use_case)

        # Create the goal
        goal = {
            "kind_of_graph": primary_domain,
            "description": ". ".join(description_parts),
            "primary_entities": entity_types,
            "content_sources": content_types,
            "expected_insights": insights,
            "timestamp": os.environ.get("PIPELINE_TIMESTAMP", "2024-01-01T00:00:00")
        }

        return goal

    def determine_goal(self, csv_files: List[str], text_files: List[str]) -> Dict[str, Any]:
        """
        Main method to determine the knowledge graph goal.

        Args:
            csv_files: List of CSV file paths
            text_files: List of text/markdown file paths

        Returns:
            Dictionary containing the determined goal
        """
        print("  🎯 Analyzing data to determine knowledge graph goal...")

        # Analyze files
        csv_analysis = self.analyze_csv_files(csv_files)
        text_analysis = self.analyze_text_files(text_files)

        # Generate goal
        goal = self.generate_goal(csv_analysis, text_analysis)

        print(f"    ✅ Determined goal: {goal['kind_of_graph']}")
        print(f"    📝 Description: {goal['description'][:100]}...")

        return goal

    def save_goal(self, goal: Dict[str, Any], output_dir: str = "generated_plans") -> str:
        """Save the generated goal to a JSON file."""
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(output_dir, "approved_user_goal.json")

        with open(output_file, 'w') as f:
            json.dump(goal, f, indent=2)

        print(f"    💾 Goal saved to: {output_file}")
        return output_file

    def load_or_generate_goal(
        self,
        csv_files: List[str],
        text_files: List[str],
        force_regenerate: bool = False
    ) -> Dict[str, Any]:
        """
        Load existing goal or generate a new one.

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

        # Generate new goal
        goal = self.determine_goal(csv_files, text_files)
        self.save_goal(goal)

        return goal