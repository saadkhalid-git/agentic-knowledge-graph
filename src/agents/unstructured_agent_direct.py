"""
Direct entity extraction agent for unstructured text data
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import re
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from src.neo4j_for_adk import graphdb
from openai import OpenAI

class DirectUnstructuredAgent:
    """
    Agent for extracting entities from text using direct LLM calls.
    Creates nodes and relationships directly in Neo4j.
    """

    def __init__(self, llm_model: str = "gpt-4o-mini"):
        self.name = "DirectUnstructuredAgent"
        self.description = "Extracts entities and relationships from text using direct LLM calls"
        self.llm_model = llm_model
        self.client = OpenAI()

    def extract_product_name(self, filepath: str) -> str:
        """Extract product name from filename."""
        filename = os.path.basename(filepath)
        # Remove _reviews.md suffix and convert underscores to spaces
        product_name = filename.replace('_reviews.md', '').replace('_', ' ').title()
        return product_name

    def extract_entities_from_text(
        self,
        text: str,
        product_name: str,
        entity_types: List[str],
        fact_types: Dict[str, Dict[str, str]]
    ) -> Dict[str, Any]:
        """Use LLM to extract entities and relationships from text."""

        prompt = f"""
        Extract entities and relationships from these product reviews for {product_name}.

        You must extract ONLY these entity types:
        - Product: The product being reviewed (should be "{product_name}")
        - User: Reviewer usernames (look for @username patterns)
        - Rating: Star ratings (look for ★ symbols or X/5 patterns)
        - Issue: Problems mentioned (wobbly, broken, difficult assembly, etc.)
        - Feature: Positive aspects (storage, design, cable management, etc.)

        Relationships to extract:
        - Product -REVIEWED_BY-> User
        - Product -HAS_RATING-> Rating
        - Product -HAS_ISSUE-> Issue
        - Product -INCLUDES_FEATURE-> Feature

        Return JSON in this exact format:
        {{
            "entities": [
                {{"type": "Product", "id": "product_name", "properties": {{"name": "Product Name"}}}},
                {{"type": "User", "id": "@username", "properties": {{"username": "@username"}}}},
                {{"type": "Rating", "id": "5_star", "properties": {{"value": 5, "display": "★★★★★"}}}},
                {{"type": "Issue", "id": "issue_name", "properties": {{"description": "issue description"}}}},
                {{"type": "Feature", "id": "feature_name", "properties": {{"description": "feature description"}}}}
            ],
            "relationships": [
                {{"from_id": "product_name", "type": "REVIEWED_BY", "to_id": "@username"}},
                {{"from_id": "product_name", "type": "HAS_RATING", "to_id": "5_star"}},
                {{"from_id": "product_name", "type": "HAS_ISSUE", "to_id": "issue_name"}},
                {{"from_id": "product_name", "type": "INCLUDES_FEATURE", "to_id": "feature_name"}}
            ]
        }}

        Important:
        - Extract ALL reviewers mentioned (usernames starting with @)
        - Extract ALL ratings (look for star symbols or X/5 patterns)
        - Extract specific issues mentioned
        - Extract specific features praised
        - Use meaningful IDs that describe the entity

        Text to analyze:
        {text}
        """

        try:
            response = self.client.chat.completions.create(
                model=self.llm_model,
                messages=[
                    {"role": "system", "content": "You are an entity extraction expert. Extract entities and relationships from product reviews."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)
            return result

        except Exception as e:
            print(f"      ❌ LLM extraction error: {str(e)[:100]}")
            return {"entities": [], "relationships": []}

    def create_nodes_and_relationships(self, extraction_result: Dict[str, Any], file_path: str) -> Dict[str, Any]:
        """Create nodes and relationships in Neo4j from extraction results."""

        stats = {
            "nodes_created": 0,
            "relationships_created": 0,
            "errors": []
        }

        # Track node mappings
        node_map = {}

        # Create nodes
        for entity in extraction_result.get("entities", []):
            entity_type = entity.get("type")
            entity_id = entity.get("id")
            properties = entity.get("properties", {})

            # Add source file to properties
            properties["source_file"] = os.path.basename(file_path)

            # Special handling for Product nodes - match with existing products
            if entity_type == "Product":
                # Try to match with existing product by name
                product_name = properties.get("name", entity_id)
                query = f"""
                MATCH (n:Product {{product_name: $product_name}})
                SET n.source_file = $source_file
                RETURN n
                """
                result = graphdb.send_query(query, parameters={
                    "product_name": product_name,
                    "source_file": os.path.basename(file_path)
                })

                if result['status'] == 'success' and result['query_result']:
                    # Found existing product
                    stats["nodes_created"] += 1
                    node_map[entity_id] = entity_type
                else:
                    # Create new Product node if not found
                    query = f"""
                    MERGE (n:Product {{product_name: $product_name}})
                    SET n += $properties
                    RETURN n
                    """
                    result = graphdb.send_query(query, parameters={
                        "product_name": product_name,
                        "properties": properties
                    })

                    if result['status'] == 'success':
                        stats["nodes_created"] += 1
                        node_map[entity_id] = entity_type
                    else:
                        stats["errors"].append(f"Failed to create {entity_type} node: {entity_id}")
            else:
                # For non-Product nodes, create as before
                query = f"""
                MERGE (n:{entity_type} {{id: $id}})
                SET n += $properties
                RETURN n
                """

                result = graphdb.send_query(query, parameters={
                    "id": entity_id,
                    "properties": properties
                })

                if result['status'] == 'success':
                    stats["nodes_created"] += 1
                    node_map[entity_id] = entity_type
                else:
                    stats["errors"].append(f"Failed to create {entity_type} node: {entity_id}")

        # Create relationships
        for rel in extraction_result.get("relationships", []):
            from_id = rel.get("from_id")
            rel_type = rel.get("type")
            to_id = rel.get("to_id")

            # Get node types from map
            from_type = node_map.get(from_id)
            to_type = node_map.get(to_id)

            if from_type and to_type:
                # Special handling for Product nodes
                if from_type == "Product":
                    # Extract product name from the ID
                    from_name = from_id.replace('_', ' ').title() if '_' in from_id else from_id

                    if to_type == "Product":
                        # Product to Product relationship (unlikely but handle it)
                        to_name = to_id.replace('_', ' ').title() if '_' in to_id else to_id
                        query = f"""
                        MATCH (a:Product {{product_name: $from_name}})
                        MATCH (b:Product {{product_name: $to_name}})
                        MERGE (a)-[r:{rel_type}]->(b)
                        RETURN r
                        """
                        result = graphdb.send_query(query, parameters={
                            "from_name": from_name,
                            "to_name": to_name
                        })
                    else:
                        # Product to other entity
                        query = f"""
                        MATCH (a:Product {{product_name: $from_name}})
                        MATCH (b:{to_type} {{id: $to_id}})
                        MERGE (a)-[r:{rel_type}]->(b)
                        RETURN r
                        """
                        result = graphdb.send_query(query, parameters={
                            "from_name": from_name,
                            "to_id": to_id
                        })
                elif to_type == "Product":
                    # Other entity to Product
                    to_name = to_id.replace('_', ' ').title() if '_' in to_id else to_id
                    query = f"""
                    MATCH (a:{from_type} {{id: $from_id}})
                    MATCH (b:Product {{product_name: $to_name}})
                    MERGE (a)-[r:{rel_type}]->(b)
                    RETURN r
                    """
                    result = graphdb.send_query(query, parameters={
                        "from_id": from_id,
                        "to_name": to_name
                    })
                else:
                    # Non-Product to Non-Product
                    query = f"""
                    MATCH (a:{from_type} {{id: $from_id}})
                    MATCH (b:{to_type} {{id: $to_id}})
                    MERGE (a)-[r:{rel_type}]->(b)
                    RETURN r
                    """
                    result = graphdb.send_query(query, parameters={
                        "from_id": from_id,
                        "to_id": to_id
                    })

                if result['status'] == 'success':
                    stats["relationships_created"] += 1
                else:
                    stats["errors"].append(f"Failed to create {rel_type} relationship between {from_id} and {to_id}")

        return stats

    async def process_file(
        self,
        file_path: str,
        entity_types: List[str] = None,
        fact_types: Dict[str, Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Process a single markdown file to extract entities and relationships."""
        try:
            print(f"    📄 Processing: {os.path.basename(file_path)}")

            # Read file content
            with open(file_path, 'r') as f:
                text = f.read()

            # Extract product name from filename
            product_name = self.extract_product_name(file_path)

            # Extract entities using LLM
            extraction = self.extract_entities_from_text(text, product_name, entity_types, fact_types)

            # Create nodes and relationships in Neo4j
            stats = self.create_nodes_and_relationships(extraction, file_path)

            print(f"      ✅ Created {stats['nodes_created']} nodes, {stats['relationships_created']} relationships")

            return {
                "status": "success",
                "file": file_path,
                "nodes_created": stats["nodes_created"],
                "relationships_created": stats["relationships_created"]
            }

        except Exception as e:
            print(f"      ❌ Error: {str(e)[:100]}")
            return {
                "status": "error",
                "file": file_path,
                "error": str(e)
            }

    async def construct_subject_graph(
        self,
        file_paths: List[str],
        entity_types: List[str],
        fact_types: Dict[str, Dict[str, str]],
        import_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        """Construct the complete subject graph from multiple files."""
        print("\n📚 Constructing Subject Graph from text files...")

        results = {
            "files_processed": [],
            "files_failed": [],
            "total_nodes": 0,
            "total_relationships": 0,
            "errors": []
        }

        print(f"  Processing {len(file_paths)} markdown files...")

        for file_path in file_paths:
            # Add import_dir if provided
            full_path = os.path.join(import_dir, file_path) if import_dir else file_path

            # Process file
            result = await self.process_file(full_path, entity_types, fact_types)

            if result["status"] == "success":
                results["files_processed"].append(file_path)
                results["total_nodes"] += result.get("nodes_created", 0)
                results["total_relationships"] += result.get("relationships_created", 0)
            else:
                results["files_failed"].append(file_path)
                results["errors"].append(result.get("error", "Unknown error"))

        # Get final statistics
        stats = self.get_graph_statistics()
        results.update(stats)

        print(f"\n  📊 Summary:")
        print(f"     Files processed: {len(results['files_processed'])}")
        print(f"     Files failed: {len(results['files_failed'])}")
        print(f"     Total nodes created: {results['total_nodes']}")
        print(f"     Total relationships created: {results['total_relationships']}")

        return results

    def get_graph_statistics(self) -> Dict[str, Any]:
        """Get statistics about the constructed subject graph."""
        stats = {"entities_by_type": {}}

        # Check each entity type
        for entity_type in ['Product', 'User', 'Rating', 'Issue', 'Feature']:
            query = f"MATCH (n:{entity_type}) RETURN count(n) as count"
            result = graphdb.send_query(query)
            if result['status'] == 'success' and result['query_result']:
                stats['entities_by_type'][entity_type] = result['query_result'][0]['count']

        return stats