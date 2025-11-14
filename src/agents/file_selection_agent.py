"""
File Selection Agent for Automated Pipeline
Automatically selects relevant files based on the knowledge graph goal
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import json
import csv
from typing import Dict, Any, List, Tuple
from pathlib import Path
from itertools import islice


class AutomatedFileSelectionAgent:
    """
    Agent that automatically selects relevant files based on the goal.
    Analyzes file content and relevance without human intervention.
    """

    def __init__(self):
        self.name = "AutomatedFileSelectionAgent"
        self.description = "Automatically selects relevant files for knowledge graph construction"

    def sample_csv_file(self, file_path: str, num_lines: int = 5) -> Dict[str, Any]:
        """Sample a CSV file to understand its structure and content."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames

                # Read sample rows
                sample_rows = list(islice(reader, num_lines))

            return {
                "headers": headers,
                "sample_rows": sample_rows,
                "row_count": len(sample_rows)
            }
        except Exception as e:
            return {
                "error": str(e),
                "headers": [],
                "sample_rows": []
            }

    def sample_text_file(self, file_path: str, num_lines: int = 10) -> str:
        """Sample a text/markdown file to understand its content."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = list(islice(f, num_lines))
                return ''.join(lines)
        except Exception as e:
            return f"Error reading file: {str(e)}"

    def analyze_csv_relevance(self, file_path: str, goal: Dict[str, Any]) -> Tuple[float, str]:
        """
        Analyze CSV file relevance to the goal.

        Returns:
            Tuple of (relevance_score, reason)
        """
        filename = os.path.basename(file_path).lower()
        sample = self.sample_csv_file(file_path)

        if "error" in sample:
            return 0.0, f"Cannot read file: {sample['error']}"

        headers = sample.get("headers", [])
        relevance_score = 0.0
        reasons = []

        # Check if file contains primary entities mentioned in goal
        primary_entities = [e.lower() for e in goal.get("primary_entities", [])]

        for entity in primary_entities:
            if entity in filename:
                relevance_score += 0.3
                reasons.append(f"Filename contains entity '{entity}'")

            # Check headers for entity references
            for header in headers:
                if entity in header.lower():
                    relevance_score += 0.2
                    reasons.append(f"Has column related to '{entity}'")
                    break

        # Check for ID columns (indicates entity or relationship data)
        id_columns = [h for h in headers if "_id" in h.lower() or h.lower().endswith("id")]
        if id_columns:
            relevance_score += 0.2
            reasons.append(f"Contains ID columns: {', '.join(id_columns[:3])}")

        # Check for relationship indicators
        if "mapping" in filename or any("_to_" in h.lower() for h in headers):
            relevance_score += 0.3
            reasons.append("Appears to contain relationship data")

        # Domain-specific checks based on goal
        goal_type = goal.get("kind_of_graph", "").lower()

        if "supply chain" in goal_type:
            supply_keywords = ["supplier", "part", "assembly", "product", "component", "inventory"]
            if any(keyword in filename for keyword in supply_keywords):
                relevance_score += 0.2
                reasons.append("Supply chain related content")

        elif "customer" in goal_type:
            customer_keywords = ["customer", "order", "purchase", "transaction", "review"]
            if any(keyword in filename for keyword in customer_keywords):
                relevance_score += 0.2
                reasons.append("Customer related content")

        # Cap at 1.0
        relevance_score = min(1.0, relevance_score)

        reason = "; ".join(reasons) if reasons else "No clear relevance indicators"
        return relevance_score, reason

    def analyze_text_relevance(self, file_path: str, goal: Dict[str, Any]) -> Tuple[float, str]:
        """
        Analyze text file relevance to the goal.

        Returns:
            Tuple of (relevance_score, reason)
        """
        filename = os.path.basename(file_path).lower()
        sample = self.sample_text_file(file_path)
        sample_lower = sample.lower()

        relevance_score = 0.0
        reasons = []

        # Check for content types mentioned in goal
        content_sources = goal.get("content_sources", [])

        for content_type in content_sources:
            if content_type.replace(" ", "_") in filename:
                relevance_score += 0.3
                reasons.append(f"Matches content type '{content_type}'")

        # Check for entity mentions in content
        primary_entities = [e.lower() for e in goal.get("primary_entities", [])]

        entities_found = []
        for entity in primary_entities:
            if entity in sample_lower:
                entities_found.append(entity)
                relevance_score += 0.2

        if entities_found:
            reasons.append(f"Contains entities: {', '.join(entities_found)}")

        # Check for insight-related content
        expected_insights = goal.get("expected_insights", [])

        for insight in expected_insights:
            insight_keywords = insight.lower().split(", ")
            if any(keyword in sample_lower for keyword in insight_keywords):
                relevance_score += 0.1
                reasons.append(f"Relevant for insight: {insight}")

        # Domain-specific content checks
        goal_type = goal.get("kind_of_graph", "").lower()

        if "supply chain" in goal_type or "quality" in expected_insights:
            quality_keywords = ["quality", "issue", "problem", "defect", "complaint", "review"]
            if any(keyword in sample_lower for keyword in quality_keywords):
                relevance_score += 0.2
                reasons.append("Contains quality/issue information")

        # Check if it's structured review/feedback data
        if "review" in filename and ("rating" in sample_lower or "stars" in sample_lower):
            relevance_score += 0.2
            reasons.append("Structured review data with ratings")

        # Cap at 1.0
        relevance_score = min(1.0, relevance_score)

        reason = "; ".join(reasons) if reasons else "No clear relevance indicators"
        return relevance_score, reason

    def select_files(
        self,
        csv_files: List[str],
        text_files: List[str],
        goal: Dict[str, Any],
        threshold: float = 0.3
    ) -> Dict[str, Any]:
        """
        Select relevant files based on the goal.

        Args:
            csv_files: List of available CSV files
            text_files: List of available text files
            goal: The knowledge graph goal
            threshold: Minimum relevance score for selection

        Returns:
            Dictionary with selected files and analysis
        """
        print("  📂 Analyzing file relevance...")

        selected_csv = []
        rejected_csv = []
        selected_text = []
        rejected_text = []

        # Analyze CSV files
        for file_path in csv_files:
            score, reason = self.analyze_csv_relevance(file_path, goal)

            file_info = {
                "file": file_path,
                "score": score,
                "reason": reason
            }

            if score >= threshold:
                selected_csv.append(file_info)
                print(f"    ✅ Selected CSV: {os.path.basename(file_path)} (score: {score:.2f})")
            else:
                rejected_csv.append(file_info)

        # Analyze text files
        for file_path in text_files:
            score, reason = self.analyze_text_relevance(file_path, goal)

            file_info = {
                "file": file_path,
                "score": score,
                "reason": reason
            }

            if score >= threshold:
                selected_text.append(file_info)
                print(f"    ✅ Selected text: {os.path.basename(file_path)} (score: {score:.2f})")
            else:
                rejected_text.append(file_info)

        # Sort by relevance score
        selected_csv.sort(key=lambda x: x['score'], reverse=True)
        selected_text.sort(key=lambda x: x['score'], reverse=True)

        result = {
            "approved_csv_files": [f['file'] for f in selected_csv],
            "approved_text_files": [f['file'] for f in selected_text],
            "csv_analysis": selected_csv,
            "text_analysis": selected_text,
            "rejected_csv": rejected_csv,
            "rejected_text": rejected_text,
            "selection_threshold": threshold,
            "total_selected": len(selected_csv) + len(selected_text),
            "total_rejected": len(rejected_csv) + len(rejected_text)
        }

        print(f"    📊 Selected {len(selected_csv)} CSV files, {len(selected_text)} text files")
        print(f"    ❌ Rejected {len(rejected_csv)} CSV files, {len(rejected_text)} text files")

        return result

    def save_file_selection(self, selection: Dict[str, Any], output_dir: str = "generated_plans") -> str:
        """Save the file selection to a JSON file."""
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(output_dir, "approved_files.json")

        with open(output_file, 'w') as f:
            json.dump(selection, f, indent=2)

        print(f"    💾 File selection saved to: {output_file}")
        return output_file

    def load_or_select_files(
        self,
        csv_files: List[str],
        text_files: List[str],
        goal: Dict[str, Any],
        force_reselect: bool = False,
        threshold: float = 0.3
    ) -> Dict[str, Any]:
        """
        Load existing file selection or create a new one.

        Args:
            csv_files: Available CSV files
            text_files: Available text files
            goal: The knowledge graph goal
            force_reselect: Force new selection
            threshold: Minimum relevance score

        Returns:
            File selection dictionary
        """
        selection_file = "generated_plans/approved_files.json"

        # Try to load existing selection
        if not force_reselect and os.path.exists(selection_file):
            print("  📂 Loading existing file selection...")
            with open(selection_file, 'r') as f:
                selection = json.load(f)
            print(f"    ✅ Loaded selection: {selection['total_selected']} files")
            return selection

        # Create new selection
        selection = self.select_files(csv_files, text_files, goal, threshold)
        self.save_file_selection(selection)

        return selection