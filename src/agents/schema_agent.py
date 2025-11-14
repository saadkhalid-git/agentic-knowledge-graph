"""
Schema Agent for Automated Pipeline
Automatically generates schema and construction plan by analyzing file content
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import json
import csv
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path
from collections import defaultdict


class AutomatedSchemaAgent:
    """
    Agent that automatically generates schema and construction plans
    by analyzing file structure and content.
    """

    def __init__(self):
        self.name = "AutomatedSchemaAgent"
        self.description = "Automatically generates graph schema from file analysis"

    def analyze_csv_structure(self, file_path: str) -> Dict[str, Any]:
        """Analyze CSV file structure to understand its schema."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames

                # Sample rows for analysis
                sample_rows = []
                for i, row in enumerate(reader):
                    sample_rows.append(row)
                    if i >= 10:  # Sample first 10 rows
                        break

            # Analyze headers
            filename = os.path.basename(file_path).lower()
            analysis = {
                "file": os.path.basename(file_path),
                "headers": headers,
                "id_columns": [],
                "foreign_keys": [],
                "properties": [],
                "is_relationship_table": False,
                "entity_type": None
            }

            # Identify ID columns
            for header in headers:
                header_lower = header.lower()

                # Primary key detection
                if header_lower.endswith("_id") or header_lower == "id":
                    # Check if values are unique in sample
                    values = [row.get(header) for row in sample_rows]
                    if len(values) == len(set(values)):  # Likely unique
                        analysis["id_columns"].append(header)
                    # For relationship tables, ID columns might not be unique
                    # Check if this looks like a mapping table
                    elif "mapping" in filename or "relationship" in filename or "_to_" in filename:
                        # In mapping tables, _id columns are foreign keys
                        analysis["foreign_keys"].append(header)
                    # Also add as foreign key if not primary
                    elif header_lower.endswith("_id"):
                        analysis["foreign_keys"].append(header)

            # Determine if this is a relationship table
            # Heuristics: Has 2+ foreign keys, or name contains "mapping", "relationship", etc.
            if ("mapping" in filename or
                "relationship" in filename or
                "_to_" in filename or
                len(analysis["foreign_keys"]) >= 2 or
                (len(analysis["id_columns"]) >= 2 and not any(col.lower() == "id" for col in analysis["id_columns"]))):
                analysis["is_relationship_table"] = True

            # Determine entity type from filename
            if not analysis["is_relationship_table"]:
                # Extract entity name from filename
                entity_name = filename.replace(".csv", "").replace("_", " ").title().replace(" ", "")
                # Remove plural 's' if present
                if entity_name.endswith("ies"):
                    entity_name = entity_name[:-3] + "y"  # e.g., "Assemblies" -> "Assembly"
                elif entity_name.endswith("s"):
                    entity_name = entity_name[:-1]  # e.g., "Products" -> "Product"

                analysis["entity_type"] = entity_name

            # Properties are non-ID columns
            analysis["properties"] = [h for h in headers
                                     if h not in analysis["id_columns"]
                                     and h not in analysis["foreign_keys"]]

            return analysis

        except Exception as e:
            return {
                "file": os.path.basename(file_path),
                "error": str(e),
                "headers": [],
                "id_columns": [],
                "foreign_keys": [],
                "properties": []
            }

    def infer_relationships(self, file_analyses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Infer relationships between entities based on foreign keys."""
        relationships = []
        entity_id_map = {}

        # Build map of entity to its ID column
        for analysis in file_analyses:
            if not analysis["is_relationship_table"] and analysis["entity_type"]:
                if analysis["id_columns"]:
                    entity_id_map[analysis["entity_type"]] = analysis["id_columns"][0]

        # Find relationships
        for analysis in file_analyses:
            filename = analysis["file"]

            if analysis["is_relationship_table"]:
                # This is a dedicated relationship file
                # Try to determine the relationship type and entities
                foreign_keys = analysis["foreign_keys"] + analysis["id_columns"]

                if len(foreign_keys) >= 2:
                    # Special handling for part_supplier_mapping
                    if "part_supplier_mapping" in filename.lower():
                        # This should create a SUPPLIES relationship
                        relationships.append({
                            "type": "relationship",
                            "source_file": filename,
                            "relationship_type": "SUPPLIES",
                            "from_entity": "Supplier",
                            "from_column": "supplier_id",
                            "to_entity": "Part",
                            "to_column": "part_id",
                            "properties": []
                        })
                        continue

                    # Determine relationship name from filename
                    rel_name = filename.replace(".csv", "").upper()
                    if "mapping" in rel_name.lower():
                        rel_name = "MAPPED_TO"
                    elif "_to_" in rel_name:
                        parts = rel_name.split("_TO_")
                        rel_name = f"{parts[0]}_TO_{parts[1]}" if len(parts) > 1 else "RELATES_TO"
                    else:
                        rel_name = rel_name.replace("_", "_")

                    # Try to match foreign keys to entities
                    from_entity = None
                    to_entity = None
                    from_column = None
                    to_column = None

                    for fk in foreign_keys:
                        fk_base = fk.lower().replace("_id", "")
                        for entity, id_col in entity_id_map.items():
                            if fk_base in entity.lower():
                                if not from_entity:
                                    from_entity = entity
                                    from_column = fk
                                elif not to_entity:
                                    to_entity = entity
                                    to_column = fk
                                    break

                    if from_entity and to_entity:
                        relationships.append({
                            "type": "relationship",
                            "source_file": filename,
                            "relationship_type": rel_name,
                            "from_entity": from_entity,
                            "from_column": from_column,
                            "to_entity": to_entity,
                            "to_column": to_column,
                            "properties": analysis["properties"]
                        })

            else:
                # Check for foreign keys in entity files
                for fk in analysis["foreign_keys"]:
                    # Try to determine target entity from foreign key name
                    fk_base = fk.lower().replace("_id", "")

                    for target_entity, target_id in entity_id_map.items():
                        if fk_base in target_entity.lower():
                            # Found a reference relationship
                            rel_type = f"HAS_{target_entity.upper()}"

                            # Special cases for common patterns
                            if "parent" in fk_base:
                                rel_type = "PARENT_OF"
                            elif "child" in fk_base:
                                rel_type = "CHILD_OF"
                            elif "supplier" in fk_base:
                                rel_type = "SUPPLIED_BY"
                            elif "product" in fk_base:
                                rel_type = "CONTAINS"
                            elif "assembly" in fk_base:
                                rel_type = "IS_PART_OF"

                            relationships.append({
                                "type": "reference",
                                "source_file": filename,
                                "relationship_type": rel_type,
                                "from_entity": analysis["entity_type"],
                                "from_column": fk,
                                "to_entity": target_entity,
                                "to_column": target_id,
                                "properties": []
                            })

        return relationships

    def generate_construction_plan(
        self,
        csv_files: List[str],
        goal: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a construction plan by analyzing CSV files.

        Args:
            csv_files: List of CSV file paths to analyze
            goal: The knowledge graph goal

        Returns:
            Construction plan dictionary
        """
        print("  🔍 Analyzing files to generate schema...")

        construction_plan = {}
        file_analyses = []

        # Analyze each CSV file
        for file_path in csv_files:
            analysis = self.analyze_csv_structure(file_path)
            file_analyses.append(analysis)

            if not analysis.get("error"):
                print(f"    📊 Analyzed: {analysis['file']}")
                if analysis["is_relationship_table"]:
                    print(f"      → Relationship table with foreign keys: {', '.join(analysis['foreign_keys'][:3])}")
                else:
                    print(f"      → Entity: {analysis['entity_type']} with ID: {analysis['id_columns'][0] if analysis['id_columns'] else 'unknown'}")

        # Generate node constructions
        for analysis in file_analyses:
            if not analysis["is_relationship_table"] and analysis["entity_type"] and analysis["id_columns"]:
                node_key = analysis["entity_type"]
                construction_plan[node_key] = {
                    "construction_type": "node",
                    "source_file": analysis["file"],
                    "label": analysis["entity_type"],
                    "unique_column_name": analysis["id_columns"][0],
                    "properties": analysis["properties"][:20]  # Limit properties to avoid too many
                }

        # Generate relationship constructions
        relationships = self.infer_relationships(file_analyses)

        for i, rel in enumerate(relationships):
            # Use relationship type as key, with index for uniqueness
            rel_key = f"{rel['relationship_type']}_{i}" if i > 0 else rel['relationship_type']

            construction_plan[rel_key] = {
                "construction_type": "relationship",
                "source_file": rel["source_file"],
                "relationship_type": rel["relationship_type"],
                "from_node_label": rel["from_entity"],
                "from_node_column": rel["from_column"],
                "to_node_label": rel["to_entity"],
                "to_node_column": rel["to_column"],
                "properties": rel["properties"][:10]  # Limit properties
            }

        print(f"    ✅ Generated plan with {len([v for v in construction_plan.values() if v['construction_type'] == 'node'])} nodes")
        print(f"    ✅ Generated plan with {len([v for v in construction_plan.values() if v['construction_type'] == 'relationship'])} relationships")

        return construction_plan

    def generate_entity_extraction_plan(
        self,
        text_files: List[str],
        construction_plan: Dict[str, Any],
        goal: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate entity and fact types for unstructured data extraction.

        Args:
            text_files: List of text files
            construction_plan: The domain graph construction plan
            goal: The knowledge graph goal

        Returns:
            Dictionary with entity_types and fact_types
        """
        # Extract node labels from construction plan
        domain_entities = [
            v["label"] for v in construction_plan.values()
            if v.get("construction_type") == "node"
        ]

        # Determine additional entities based on text file types
        additional_entities = []

        for file_path in text_files:
            filename = os.path.basename(file_path).lower()

            if "review" in filename:
                additional_entities.extend(["Issue", "Feature", "User", "Rating"])
            elif "report" in filename:
                additional_entities.extend(["Metric", "Trend", "Finding"])
            elif "email" in filename or "message" in filename:
                additional_entities.extend(["Person", "Topic", "Sentiment"])
            elif "log" in filename:
                additional_entities.extend(["Event", "Error", "System"])

        # Combine and deduplicate
        entity_types = list(set(domain_entities + additional_entities))

        # Generate fact types based on entities and goal
        fact_types = {}

        # Common patterns
        if "Product" in entity_types and "Issue" in entity_types:
            fact_types["has_issue"] = {
                "subject_label": "Product",
                "predicate_label": "has_issue",
                "object_label": "Issue"
            }

        if "Product" in entity_types and "Feature" in entity_types:
            fact_types["includes_feature"] = {
                "subject_label": "Product",
                "predicate_label": "includes_feature",
                "object_label": "Feature"
            }

        if "Product" in entity_types and "User" in entity_types:
            fact_types["reviewed_by"] = {
                "subject_label": "Product",
                "predicate_label": "reviewed_by",
                "object_label": "User"
            }

        if "Product" in entity_types and "Rating" in entity_types:
            fact_types["has_rating"] = {
                "subject_label": "Product",
                "predicate_label": "has_rating",
                "object_label": "Rating"
            }

        # Add domain-specific fact types
        goal_type = goal.get("kind_of_graph", "").lower()

        if "supply chain" in goal_type:
            if "Supplier" in entity_types and "Issue" in entity_types:
                fact_types["causes_issue"] = {
                    "subject_label": "Supplier",
                    "predicate_label": "causes_issue",
                    "object_label": "Issue"
                }

        return {
            "entity_types": entity_types,
            "fact_types": fact_types
        }

    def save_construction_plan(
        self,
        construction_plan: Dict[str, Any],
        extraction_plan: Dict[str, Any],
        output_dir: str = "generated_plans"
    ) -> Tuple[str, str]:
        """Save the construction and extraction plans to JSON files."""
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

    def load_or_generate_plans(
        self,
        csv_files: List[str],
        text_files: List[str],
        goal: Dict[str, Any],
        force_regenerate: bool = False
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Load existing plans or generate new ones.

        Args:
            csv_files: CSV files to analyze
            text_files: Text files for extraction planning
            goal: The knowledge graph goal
            force_regenerate: Force regeneration

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
            print(f"    ✅ Loaded construction plan with {len(construction_plan)} rules")
            return construction_plan, extraction_plan

        # Generate new plans
        construction_plan = self.generate_construction_plan(csv_files, goal)
        extraction_plan = self.generate_entity_extraction_plan(text_files, construction_plan, goal)
        self.save_construction_plan(construction_plan, extraction_plan)

        return construction_plan, extraction_plan