"""
Entity Resolution Agent for Automated Pipeline
Links entities across domain and subject graphs without human intervention
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from typing import Dict, Any, List, Tuple
from src.neo4j_for_adk import graphdb
from Levenshtein import jaro_winkler


class AutomatedLinkageAgent:
    """
    Agent for resolving entities between subject and domain graphs.
    Automated version - no human intervention required.
    """

    def __init__(self, similarity_threshold: float = 0.6):
        self.name = "AutomatedLinkageAgent"
        self.description = "Automatically links entities across graphs using similarity matching"
        self.similarity_threshold = similarity_threshold

    def calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculate Jaro-Winkler similarity between two strings."""
        if not str1 or not str2:
            return 0.0

        # Normalize strings for comparison
        str1 = str1.lower().strip()
        str2 = str2.lower().strip()

        # Direct match
        if str1 == str2:
            return 1.0

        # Calculate Jaro-Winkler similarity
        return jaro_winkler(str1, str2)

    def get_entities_by_type(self, entity_type: str, graph: str = "subject") -> List[Dict[str, Any]]:
        """Get all entities of a specific type from either subject or domain graph."""

        if graph == "subject":
            # Subject graph entities have __Entity__ label
            query = """
            MATCH (n:`__Entity__`)
            WHERE $entity_type IN labels(n)
            RETURN n, labels(n) as labels
            """
        else:  # domain graph
            query = """
            MATCH (n)
            WHERE $entity_type IN labels(n)
              AND NOT n:`__Entity__`
              AND NOT n:Chunk
              AND NOT n:Document
            RETURN n, labels(n) as labels
            """

        result = graphdb.send_query(query, {"entity_type": entity_type})

        if result['status'] == 'success':
            return result['query_result']
        return []

    def find_best_match(
        self,
        subject_entity: Dict[str, Any],
        domain_entities: List[Dict[str, Any]],
        match_field: str = "name"
    ) -> Tuple[Dict[str, Any], float]:
        """Find the best matching domain entity for a subject entity."""

        best_match = None
        best_score = 0.0

        # Get the value to match from subject entity
        subject_value = None
        if 'name' in subject_entity.get('n', {}):
            subject_value = subject_entity['n']['name']
        elif match_field in subject_entity.get('n', {}):
            subject_value = subject_entity['n'][match_field]

        if not subject_value:
            return None, 0.0

        # Find best match in domain entities
        for domain_entity in domain_entities:
            domain_value = None

            # Try different field names for matching
            if match_field in domain_entity.get('n', {}):
                domain_value = domain_entity['n'][match_field]
            elif f"{match_field.lower()}_name" in domain_entity.get('n', {}):
                domain_value = domain_entity['n'][f"{match_field.lower()}_name"]
            elif 'name' in domain_entity.get('n', {}):
                domain_value = domain_entity['n']['name']

            if domain_value:
                score = self.calculate_similarity(subject_value, domain_value)
                if score > best_score:
                    best_score = score
                    best_match = domain_entity

        return best_match, best_score

    def create_correspondence(
        self,
        subject_entity_id: int,
        domain_entity_id: int,
        similarity_score: float
    ) -> Dict[str, Any]:
        """Create a CORRESPONDS_TO relationship between entities."""

        query = """
        MATCH (subject) WHERE id(subject) = $subject_id
        MATCH (domain) WHERE id(domain) = $domain_id
        MERGE (subject)-[r:CORRESPONDS_TO]->(domain)
        SET r.similarity = $score
        RETURN r
        """

        return graphdb.send_query(query, {
            "subject_id": subject_entity_id,
            "domain_id": domain_entity_id,
            "score": similarity_score
        })

    def resolve_entities_for_type(self, entity_type: str) -> Dict[str, Any]:
        """Resolve all entities of a specific type."""

        print(f"  🔗 Resolving {entity_type} entities...")

        results = {
            "type": entity_type,
            "resolved": 0,
            "unresolved": 0,
            "errors": []
        }

        # Get subject graph entities
        subject_entities = self.get_entities_by_type(entity_type, "subject")

        # Get domain graph entities
        domain_entities = self.get_entities_by_type(entity_type, "domain")

        if not subject_entities:
            print(f"    ℹ️  No {entity_type} entities found in subject graph")
            return results

        if not domain_entities:
            print(f"    ℹ️  No {entity_type} entities found in domain graph")
            results["unresolved"] = len(subject_entities)
            return results

        # Match field based on entity type
        match_field = "product" if entity_type == "Product" else "name"

        # Resolve each subject entity
        for subject_entity in subject_entities:
            try:
                # Find best match
                best_match, score = self.find_best_match(
                    subject_entity,
                    domain_entities,
                    match_field
                )

                if best_match and score >= self.similarity_threshold:
                    # Create correspondence relationship
                    result = self.create_correspondence(
                        subject_entity['n'].id,
                        best_match['n'].id,
                        score
                    )

                    if result['status'] == 'success':
                        results["resolved"] += 1
                    else:
                        results["unresolved"] += 1
                        results["errors"].append(result.get('error_message', 'Unknown error'))
                else:
                    results["unresolved"] += 1

            except Exception as e:
                results["unresolved"] += 1
                results["errors"].append(str(e))

        print(f"    ✅ Resolved {results['resolved']} of {len(subject_entities)} {entity_type} entities")

        return results

    def resolve_all_entities(
        self,
        entity_types: List[str] = None
    ) -> Dict[str, Any]:
        """Resolve entities across all types."""

        print("\n🔄 Resolving Entities Between Graphs...")
        print(f"  Similarity threshold: {self.similarity_threshold}")

        # Default entity types to resolve
        if entity_types is None:
            entity_types = ['Product', 'Supplier', 'Part', 'Assembly']

        overall_results = {
            "total_relationships": 0,
            "entities_resolved": {},
            "entities_unresolved": {},
            "errors": []
        }

        for entity_type in entity_types:
            type_results = self.resolve_entities_for_type(entity_type)

            overall_results["total_relationships"] += type_results["resolved"]
            overall_results["entities_resolved"][entity_type] = type_results["resolved"]
            overall_results["entities_unresolved"][entity_type] = type_results["unresolved"]
            overall_results["errors"].extend(type_results["errors"])

        return overall_results

    def get_resolution_statistics(self) -> Dict[str, Any]:
        """Get statistics about entity resolution."""

        # Count correspondence relationships
        corr_stats = graphdb.send_query("""
            MATCH ()-[r:CORRESPONDS_TO]->()
            RETURN count(r) as total_correspondences,
                   avg(r.similarity) as avg_similarity,
                   min(r.similarity) as min_similarity,
                   max(r.similarity) as max_similarity
        """)

        # Count unresolved entities
        unresolved_stats = graphdb.send_query("""
            MATCH (n:`__Entity__`)
            WHERE NOT (n)-[:CORRESPONDS_TO]->()
            WITH labels(n) as node_labels
            UNWIND node_labels as label
            WITH label
            WHERE NOT label STARTS WITH "__"
            RETURN label, count(*) as count
            ORDER BY count DESC
        """)

        stats = {
            "correspondence_relationships": 0,
            "avg_similarity": 0,
            "min_similarity": 0,
            "max_similarity": 0,
            "unresolved_by_type": {}
        }

        if corr_stats['status'] == 'success' and corr_stats['query_result']:
            result = corr_stats['query_result'][0]
            stats["correspondence_relationships"] = result.get('total_correspondences', 0)
            stats["avg_similarity"] = result.get('avg_similarity', 0)
            stats["min_similarity"] = result.get('min_similarity', 0)
            stats["max_similarity"] = result.get('max_similarity', 0)

        if unresolved_stats['status'] == 'success':
            for row in unresolved_stats['query_result']:
                stats["unresolved_by_type"][row['label']] = row['count']

        return stats

    def remove_existing_correspondences(self) -> Dict[str, Any]:
        """Remove all existing CORRESPONDS_TO relationships."""

        print("  🧹 Removing existing correspondence relationships...")

        query = """
        MATCH ()-[r:CORRESPONDS_TO]->()
        WITH count(r) as total
        CALL {
            MATCH ()-[r:CORRESPONDS_TO]->()
            DELETE r
        } IN TRANSACTIONS OF 1000 ROWS
        RETURN total
        """

        result = graphdb.send_query(query)

        if result['status'] == 'success' and result['query_result']:
            total = result['query_result'][0].get('total', 0)
            print(f"    ✅ Removed {total} existing correspondences")

        return result