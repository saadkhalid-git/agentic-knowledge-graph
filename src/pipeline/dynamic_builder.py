"""
Dynamic Pipeline Builder
Orchestrates agents to dynamically generate plans and build knowledge graphs
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import asyncio
import json
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

# Import dynamic agents
from src.agents.intent_agent import AutomatedIntentAgent
from src.agents.file_selection_agent import AutomatedFileSelectionAgent
from src.agents.schema_agent import AutomatedSchemaAgent

# Import execution agents
from src.agents.structured_agent import AutomatedStructuredAgent
from src.agents.unstructured_agent_direct import DirectUnstructuredAgent
from src.agents.linkage_agent import AutomatedLinkageAgent

# Import Neo4j utilities
from src.neo4j_for_adk import graphdb
from notebooks.tools import drop_neo4j_indexes, clear_neo4j_data


class DynamicKnowledgeGraphBuilder:
    """
    Dynamic orchestrator that uses agents to generate plans and build graphs.
    No hardcoded plans - everything is determined at runtime.
    """

    def __init__(self, data_dir: str = None):
        """
        Initialize the dynamic builder.

        Args:
            data_dir: Directory containing data files (uses default if not provided)
        """
        # Planning agents
        self.intent_agent = AutomatedIntentAgent()
        self.file_selection_agent = AutomatedFileSelectionAgent()
        self.schema_agent = AutomatedSchemaAgent()

        # Execution agents
        self.structured_agent = AutomatedStructuredAgent()
        self.unstructured_agent = DirectUnstructuredAgent()
        self.linkage_agent = AutomatedLinkageAgent()

        # Data directory
        if data_dir:
            self.data_dir = data_dir
        else:
            # Default to project data directory
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            self.data_dir = os.path.join(project_root, "data")

        # Execution tracking
        self.execution_log = []
        self.generated_plans = {}

    def log(self, message: str, level: str = "INFO"):
        """Log a message with timestamp."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        self.execution_log.append(log_entry)
        print(message)

    def discover_files(self) -> Tuple[List[str], List[str]]:
        """Discover available CSV and text files in the data directory."""
        self.log("🔍 Discovering available files...")

        csv_files = []
        text_files = []

        # Walk through data directory
        for root, dirs, files in os.walk(self.data_dir):
            for file in files:
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, self.data_dir)

                if file.lower().endswith('.csv'):
                    csv_files.append(file_path)
                elif file.lower().endswith(('.md', '.txt')):
                    text_files.append(file_path)

        self.log(f"  Found {len(csv_files)} CSV files and {len(text_files)} text files")
        return csv_files, text_files

    def reset_graph(self, confirm: bool = False) -> Dict[str, Any]:
        """Reset the Neo4j graph database."""
        if not confirm:
            return {
                "status": "error",
                "message": "Reset requires confirmation. Set confirm=True to proceed."
            }

        self.log("🔄 Resetting Neo4j graph...")

        # Drop indexes
        drop_result = drop_neo4j_indexes()
        self.log(f"  Indexes dropped: {drop_result['status']}")

        # Clear data
        clear_result = clear_neo4j_data()
        self.log(f"  Data cleared: {clear_result['status']}")

        return {
            "status": "success",
            "indexes_dropped": drop_result['status'],
            "data_cleared": clear_result['status']
        }

    def phase1_determine_goal(
        self,
        csv_files: List[str],
        text_files: List[str],
        force_regenerate: bool = False
    ) -> Dict[str, Any]:
        """Phase 1: Determine the knowledge graph goal."""
        self.log("\n" + "="*60)
        self.log("PHASE 1: GOAL DETERMINATION")
        self.log("="*60)

        goal = self.intent_agent.load_or_generate_goal(
            csv_files,
            text_files,
            force_regenerate=force_regenerate
        )

        self.generated_plans["goal"] = goal
        return goal

    def phase2_select_files(
        self,
        csv_files: List[str],
        text_files: List[str],
        goal: Dict[str, Any],
        force_reselect: bool = False
    ) -> Dict[str, Any]:
        """Phase 2: Select relevant files based on goal."""
        self.log("\n" + "="*60)
        self.log("PHASE 2: FILE SELECTION")
        self.log("="*60)

        selection = self.file_selection_agent.load_or_select_files(
            csv_files,
            text_files,
            goal,
            force_reselect=force_reselect,
            threshold=0.15  # Lowered from 0.3 to include more review files
        )

        self.generated_plans["file_selection"] = selection
        return selection

    def phase3_generate_schema(
        self,
        csv_files: List[str],
        text_files: List[str],
        goal: Dict[str, Any],
        force_regenerate: bool = False
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Phase 3: Generate schema and construction plans."""
        self.log("\n" + "="*60)
        self.log("PHASE 3: SCHEMA GENERATION")
        self.log("="*60)

        construction_plan, extraction_plan = self.schema_agent.load_or_generate_plans(
            csv_files,
            text_files,
            goal,
            force_regenerate=force_regenerate
        )

        self.generated_plans["construction_plan"] = construction_plan
        self.generated_plans["extraction_plan"] = extraction_plan

        return construction_plan, extraction_plan

    def phase4_build_domain_graph(self, construction_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Phase 4: Build the domain graph from CSV files."""
        self.log("\n" + "="*60)
        self.log("PHASE 4: DOMAIN GRAPH CONSTRUCTION")
        self.log("="*60)

        results = self.structured_agent.construct_domain_graph(construction_plan)

        # Log results
        if results['nodes_created']:
            self.log(f"✅ Nodes created: {', '.join(results['nodes_created'])}")
        if results['relationships_created']:
            self.log(f"✅ Relationships created: {', '.join(results['relationships_created'])}")

        if results.get('statistics'):
            self.log("\n📊 Domain Graph Statistics:")
            for label, count in results['statistics'].get('nodes', {}).items():
                self.log(f"  {label}: {count} nodes")
            for rel_type, count in results['statistics'].get('relationships', {}).items():
                self.log(f"  {rel_type}: {count} relationships")

        return results

    async def phase5_build_subject_graph(
        self,
        text_files: List[str],
        extraction_plan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Phase 5: Build the subject graph from text files."""
        self.log("\n" + "="*60)
        self.log("PHASE 5: SUBJECT GRAPH CONSTRUCTION")
        self.log("="*60)

        # Extract entity types and fact types from plan
        entity_types = extraction_plan.get("entity_types", ["Product", "Issue", "Feature", "User"])
        fact_types = extraction_plan.get("fact_types", {})

        # Build the graph
        results = await self.unstructured_agent.construct_subject_graph(
            file_paths=text_files,
            entity_types=entity_types,
            fact_types=fact_types,
            import_dir=None  # Files already have full paths
        )

        # Log results
        self.log(f"✅ Files processed: {len(results['files_processed'])}")
        if results['files_failed']:
            self.log(f"⚠️ Files failed: {len(results['files_failed'])}", "WARNING")

        if results.get('entities_by_type'):
            self.log("\n📊 Entity Statistics:")
            for entity_type, count in results['entities_by_type'].items():
                self.log(f"  {entity_type}: {count} entities")

        self.log(f"  Chunks created: {results.get('chunk_count', 0)}")
        self.log(f"  Documents created: {results.get('document_count', 0)}")

        return results

    def phase6_resolve_entities(self, entity_types: List[str] = None) -> Dict[str, Any]:
        """Phase 6: Resolve entities between graphs."""
        self.log("\n" + "="*60)
        self.log("PHASE 6: ENTITY RESOLUTION")
        self.log("="*60)

        # Remove existing correspondences
        self.linkage_agent.remove_existing_correspondences()

        # Use entity types from extraction plan if available
        if entity_types is None and "extraction_plan" in self.generated_plans:
            # Get domain entities that might appear in both graphs
            entity_types = ["Product", "Supplier", "Part", "Assembly"]

        # Perform resolution
        results = self.linkage_agent.resolve_all_entities(entity_types=entity_types)

        # Log results
        self.log(f"✅ Total relationships created: {results['total_relationships']}")

        if results.get('entities_resolved'):
            self.log("\n📊 Resolution by Type:")
            for entity_type, count in results['entities_resolved'].items():
                self.log(f"  {entity_type}: {count} correspondences")

        return results

    def save_all_plans(self, output_dir: str = "generated_plans") -> str:
        """Save all generated plans to a single JSON file."""
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(output_dir, "all_generated_plans.json")

        with open(output_file, 'w') as f:
            json.dump(self.generated_plans, f, indent=2, default=str)

        self.log(f"💾 All plans saved to: {output_file}")
        return output_file

    def get_final_statistics(self) -> Dict[str, Any]:
        """Get final statistics about the constructed graph."""
        stats_query = """
        MATCH (n)
        WITH labels(n) as node_labels, count(n) as node_count
        UNWIND node_labels as label
        WITH label, sum(node_count) as total
        RETURN label, total
        ORDER BY total DESC
        """

        result = graphdb.send_query(stats_query)

        stats = {"nodes_by_label": {}, "total_nodes": 0}

        if result['status'] == 'success':
            for row in result['query_result']:
                stats["nodes_by_label"][row['label']] = row['total']
                stats["total_nodes"] += row['total']

        # Count relationships
        rel_query = """
        MATCH ()-[r]->()
        RETURN type(r) as type, count(r) as count
        ORDER BY count DESC
        """

        rel_result = graphdb.send_query(rel_query)

        stats["relationships_by_type"] = {}
        stats["total_relationships"] = 0

        if rel_result['status'] == 'success':
            for row in rel_result['query_result']:
                stats["relationships_by_type"][row['type']] = row['count']
                stats["total_relationships"] += row['count']

        return stats

    async def build_complete_graph(
        self,
        reset: bool = True,
        force_regenerate_plans: bool = False,
        limit_text_files: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Build the complete knowledge graph with dynamic plan generation.

        Args:
            reset: Whether to reset the graph first
            force_regenerate_plans: Force regeneration of all plans
            limit_text_files: Limit number of text files to process

        Returns:
            Dictionary with complete build results
        """
        start_time = datetime.now()
        self.execution_log = []
        self.generated_plans = {}

        results = {
            "start_time": start_time.isoformat(),
            "data_directory": self.data_dir
        }

        try:
            self.log("\n" + "🚀 "*20)
            self.log("DYNAMIC KNOWLEDGE GRAPH PIPELINE")
            self.log("🚀 "*20 + "\n")

            # Reset if requested
            if reset:
                results['reset'] = self.reset_graph(confirm=True)

            # Discover files
            csv_files, text_files = self.discover_files()
            results['discovered_files'] = {
                "csv_count": len(csv_files),
                "text_count": len(text_files)
            }

            # Phase 1: Determine goal
            goal = self.phase1_determine_goal(
                csv_files,
                text_files,
                force_regenerate=force_regenerate_plans
            )
            results['goal'] = goal

            # Phase 2: Select files
            file_selection = self.phase2_select_files(
                csv_files,
                text_files,
                goal,
                force_reselect=force_regenerate_plans
            )
            results['file_selection'] = {
                "selected_csv": len(file_selection['approved_csv_files']),
                "selected_text": len(file_selection['approved_text_files'])
            }

            # Get selected files
            selected_csv = file_selection['approved_csv_files']
            selected_text = file_selection['approved_text_files']

            # Apply text file limit if specified
            if limit_text_files and len(selected_text) > limit_text_files:
                selected_text = selected_text[:limit_text_files]
                self.log(f"ℹ️ Limiting to {limit_text_files} text files")

            # Phase 3: Generate schema
            construction_plan, extraction_plan = self.phase3_generate_schema(
                selected_csv,
                selected_text,
                goal,
                force_regenerate=force_regenerate_plans
            )
            results['schema_generation'] = {
                "nodes_planned": len([v for v in construction_plan.values() if v.get('construction_type') == 'node']),
                "relationships_planned": len([v for v in construction_plan.values() if v.get('construction_type') == 'relationship']),
                "entity_types": len(extraction_plan.get('entity_types', [])),
                "fact_types": len(extraction_plan.get('fact_types', {}))
            }

            # Phase 4: Build domain graph
            results['domain'] = self.phase4_build_domain_graph(construction_plan)

            # Phase 5: Build subject graph
            if selected_text:
                results['subject'] = await self.phase5_build_subject_graph(
                    selected_text,
                    extraction_plan
                )

            # Phase 6: Entity resolution
            results['resolution'] = self.phase6_resolve_entities()

            # Save all generated plans
            self.save_all_plans()

            # Final Statistics
            self.log("\n" + "="*60)
            self.log("KNOWLEDGE GRAPH CONSTRUCTION COMPLETE")
            self.log("="*60)

            stats = self.get_final_statistics()
            results['final_statistics'] = stats

            self.log("\n📊 Final Graph Statistics:")
            self.log(f"  Total Nodes: {stats['total_nodes']:,}")
            self.log(f"  Total Relationships: {stats['total_relationships']:,}")

            for label, count in list(stats['nodes_by_label'].items())[:10]:
                self.log(f"    {label:20} {count:8,} nodes")

            # Calculate execution time
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            results['execution_time_seconds'] = execution_time
            results['end_time'] = end_time.isoformat()

            self.log(f"\n⏱️ Execution time: {execution_time:.2f} seconds")
            self.log("\n✅ Dynamic pipeline execution completed successfully!")

            results['status'] = 'success'

        except Exception as e:
            self.log(f"\n❌ Pipeline failed: {str(e)}", "ERROR")
            results['status'] = 'error'
            results['error'] = str(e)
            import traceback
            results['traceback'] = traceback.format_exc()

        finally:
            results['execution_log'] = self.execution_log
            results['generated_plans'] = self.generated_plans

        return results


async def create_and_run_dynamic_pipeline(
    reset: bool = True,
    force_regenerate_plans: bool = False,
    limit_text_files: Optional[int] = None,
    data_dir: Optional[str] = None
) -> DynamicKnowledgeGraphBuilder:
    """
    Convenience function to create and run the dynamic pipeline.

    Args:
        reset: Whether to reset the graph first
        force_regenerate_plans: Force regeneration of all plans
        limit_text_files: Limit number of text files to process
        data_dir: Data directory (uses default if not provided)

    Returns:
        DynamicKnowledgeGraphBuilder instance with completed graph
    """
    builder = DynamicKnowledgeGraphBuilder(data_dir=data_dir)
    await builder.build_complete_graph(
        reset=reset,
        force_regenerate_plans=force_regenerate_plans,
        limit_text_files=limit_text_files
    )
    return builder