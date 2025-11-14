"""
Structured Data Agent for Automated Pipeline
Handles CSV data import without human intervention
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from typing import Dict, Any, List
from src.neo4j_for_adk import graphdb, tool_success, tool_error


class AutomatedStructuredAgent:
    """
    Agent for constructing domain graph from structured CSV data.
    Automated version - no human intervention required.
    """

    def __init__(self):
        self.name = "AutomatedStructuredAgent"
        self.description = "Constructs domain graph from CSV files automatically"

    def create_uniqueness_constraint(self, label: str, unique_property_key: str) -> Dict[str, Any]:
        """Creates a uniqueness constraint for a node label and property key."""
        constraint_name = f"{label}_{unique_property_key}_constraint"
        query = f"""CREATE CONSTRAINT `{constraint_name}` IF NOT EXISTS
        FOR (n:`{label}`)
        REQUIRE n.`{unique_property_key}` IS UNIQUE"""

        return graphdb.send_query(query)

    def load_nodes_from_csv(
        self,
        source_file: str,
        label: str,
        unique_column_name: str,
        properties: List[str]
    ) -> Dict[str, Any]:
        """Batch load nodes from a CSV file."""
        # Build the SET clause for properties
        set_clauses = []
        for prop in properties:
            set_clauses.append(f"n.`{prop}` = row.`{prop}`")
        set_clause = ", ".join(set_clauses) if set_clauses else ""

        query = f"""
        LOAD CSV WITH HEADERS FROM "file:///" + $source_file AS row
        CALL (row) {{
            MERGE (n:`{label}` {{ `{unique_column_name}` : row.`{unique_column_name}` }})
            {"SET " + set_clause if set_clause else ""}
        }} IN TRANSACTIONS OF 1000 ROWS
        """

        return graphdb.send_query(query, {"source_file": source_file})

    def import_nodes(self, node_construction: Dict[str, Any]) -> Dict[str, Any]:
        """Import nodes according to a node construction rule."""
        print(f"  📦 Importing {node_construction['label']} nodes from {node_construction['source_file']}...")

        # Create uniqueness constraint
        uniqueness_result = self.create_uniqueness_constraint(
            node_construction["label"],
            node_construction["unique_column_name"]
        )

        if uniqueness_result["status"] == "error":
            return uniqueness_result

        # Import nodes from CSV
        result = self.load_nodes_from_csv(
            node_construction["source_file"],
            node_construction["label"],
            node_construction["unique_column_name"],
            node_construction.get("properties", [])
        )

        if result["status"] == "success":
            print(f"    ✅ Successfully imported {node_construction['label']} nodes")
        else:
            print(f"    ❌ Failed: {result.get('error_message', 'Unknown error')}")

        return result

    def import_relationships(self, relationship_construction: Dict[str, Any]) -> Dict[str, Any]:
        """Import relationships according to a relationship construction rule."""
        print(f"  🔗 Creating {relationship_construction['relationship_type']} relationships...")

        from_node_column = relationship_construction["from_node_column"]
        to_node_column = relationship_construction["to_node_column"]
        from_node_label = relationship_construction["from_node_label"]
        to_node_label = relationship_construction["to_node_label"]
        relationship_type = relationship_construction["relationship_type"]

        # Build the SET clause for properties
        properties = relationship_construction.get("properties", [])
        set_clauses = []
        for prop in properties:
            set_clauses.append(f"r.`{prop}` = row.`{prop}`")
        set_clause = ", ".join(set_clauses) if set_clauses else ""

        query = f"""
        LOAD CSV WITH HEADERS FROM "file:///" + $source_file AS row
        CALL (row) {{
            MATCH (from_node:`{from_node_label}` {{ `{from_node_column}` : row.`{from_node_column}` }}),
                  (to_node:`{to_node_label}` {{ `{to_node_column}` : row.`{to_node_column}` }})
            MERGE (from_node)-[r:`{relationship_type}`]->(to_node)
            {"SET " + set_clause if set_clause else ""}
        }} IN TRANSACTIONS OF 1000 ROWS
        """

        result = graphdb.send_query(query, {"source_file": relationship_construction["source_file"]})

        if result["status"] == "success":
            print(f"    ✅ Successfully created {relationship_type} relationships")
        else:
            print(f"    ❌ Failed: {result.get('error_message', 'Unknown error')}")

        return result

    def construct_domain_graph(self, construction_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Construct the complete domain graph from a construction plan."""
        results = {
            "nodes_created": [],
            "relationships_created": [],
            "errors": []
        }

        print("\n🏗️  Constructing Domain Graph...")

        # First, import all nodes
        node_constructions = [
            (key, value) for key, value in construction_plan.items()
            if value.get('construction_type') == 'node'
        ]

        for name, node_construction in node_constructions:
            result = self.import_nodes(node_construction)

            if result['status'] == 'error':
                error_msg = f"Failed to import {name}: {result.get('error_message', 'Unknown error')}"
                results['errors'].append(error_msg)
            else:
                results['nodes_created'].append(name)

        # Second, import all relationships
        relationship_constructions = [
            (key, value) for key, value in construction_plan.items()
            if value.get('construction_type') == 'relationship'
        ]

        for name, relationship_construction in relationship_constructions:
            result = self.import_relationships(relationship_construction)

            if result['status'] == 'error':
                error_msg = f"Failed to create {name}: {result.get('error_message', 'Unknown error')}"
                results['errors'].append(error_msg)
            else:
                results['relationships_created'].append(name)

        # Get statistics
        stats = self.get_graph_statistics()
        results['statistics'] = stats

        return results

    def get_graph_statistics(self) -> Dict[str, Any]:
        """Get statistics about the constructed domain graph."""
        node_stats = graphdb.send_query("""
            MATCH (n)
            WHERE NOT n:`__Entity__` AND NOT n:Chunk AND NOT n:Document
            WITH labels(n) as labels
            UNWIND labels as label
            RETURN label, count(*) as count
            ORDER BY label
        """)

        rel_stats = graphdb.send_query("""
            MATCH ()-[r]->()
            WHERE NOT type(r) IN ['MENTIONED_IN', 'CORRESPONDS_TO', 'HAS_CHUNK', 'NEXT_CHUNK']
            RETURN type(r) as type, count(r) as count
            ORDER BY count DESC
        """)

        stats = {
            "nodes": {},
            "relationships": {}
        }

        if node_stats['status'] == 'success':
            for row in node_stats['query_result']:
                stats['nodes'][row['label']] = row['count']

        if rel_stats['status'] == 'success':
            for row in rel_stats['query_result']:
                stats['relationships'][row['type']] = row['count']

        return stats


# Default construction plan for supply chain
DEFAULT_CONSTRUCTION_PLAN = {
    "Product": {
        "construction_type": "node",
        "source_file": "products.csv",
        "label": "Product",
        "unique_column_name": "product_id",
        "properties": ["product_name", "price", "description"]
    },
    "Assembly": {
        "construction_type": "node",
        "source_file": "assemblies.csv",
        "label": "Assembly",
        "unique_column_name": "assembly_id",
        "properties": ["assembly_name", "quantity", "product_id"]
    },
    "Part": {
        "construction_type": "node",
        "source_file": "parts.csv",
        "label": "Part",
        "unique_column_name": "part_id",
        "properties": ["part_name", "quantity", "assembly_id"]
    },
    "Supplier": {
        "construction_type": "node",
        "source_file": "suppliers.csv",
        "label": "Supplier",
        "unique_column_name": "supplier_id",
        "properties": ["name", "specialty", "city", "country", "website", "contact_email"]
    },
    "CONTAINS": {
        "construction_type": "relationship",
        "source_file": "assemblies.csv",
        "relationship_type": "CONTAINS",
        "from_node_label": "Product",
        "from_node_column": "product_id",
        "to_node_label": "Assembly",
        "to_node_column": "assembly_id",
        "properties": ["quantity"]
    },
    "IS_PART_OF": {
        "construction_type": "relationship",
        "source_file": "parts.csv",
        "relationship_type": "IS_PART_OF",
        "from_node_label": "Part",
        "from_node_column": "part_id",
        "to_node_label": "Assembly",
        "to_node_column": "assembly_id",
        "properties": ["quantity"]
    },
    "SUPPLIED_BY": {
        "construction_type": "relationship",
        "source_file": "part_supplier_mapping.csv",
        "relationship_type": "SUPPLIED_BY",
        "from_node_label": "Part",
        "from_node_column": "part_id",
        "to_node_label": "Supplier",
        "to_node_column": "supplier_id",
        "properties": ["lead_time_days", "unit_cost", "minimum_order_quantity", "preferred_supplier"]
    }
}