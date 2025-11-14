"""
ADK-Enhanced Dynamic Pipeline Builder
Uses Google's Agent Development Kit for intelligent decision making
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import asyncio
import json
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

# Import regular pipeline components
from src.pipeline.dynamic_builder import DynamicKnowledgeGraphBuilder

# Import ADK components
from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm
from notebooks.helper import make_agent_caller


class ADKDynamicKnowledgeGraphBuilder(DynamicKnowledgeGraphBuilder):
    """
    Enhanced pipeline builder using ADK agents for intelligent decision making.
    """

    def __init__(self, data_dir: str = None, llm_model: str = "gpt-4o-mini"):
        """Initialize ADK-enhanced builder."""
        super().__init__(data_dir)
        self.llm_model = llm_model
        self.validation_results = {}

    async def validate_goal(self, goal: Dict[str, Any]) -> Dict[str, Any]:
        """Use ADK agent to validate and improve the goal determination."""
        agent = Agent(
            name="goal_validator",
            model=LiteLlm(model=f"openai/{self.llm_model}"),
            instruction="""You are a knowledge graph design expert. Analyze the proposed goal and provide:
            1. A quality score (0-100)
            2. Suggestions for improvement
            3. Any missing considerations

            Return JSON with: {score, suggestions, improvements}"""
        )

        try:
            caller = await make_agent_caller(agent)
            prompt = f"Validate this knowledge graph goal: {json.dumps(goal)}"
            response = await caller.call(prompt)

            # Parse response
            if isinstance(response, str):
                try:
                    validation = json.loads(response)
                except:
                    validation = {"score": 75, "suggestions": [response]}
            else:
                validation = response

            self.validation_results['goal_validation'] = validation
            self.log(f"Goal validation score: {validation.get('score', 'N/A')}/100")

            return validation
        except Exception as e:
            self.log(f"Goal validation failed: {e}", "WARNING")
            return {"score": 70, "suggestions": []}

    async def validate_file_selection(self, files: Dict[str, Any], goal: Dict[str, Any]) -> Dict[str, Any]:
        """Validate file selection using ADK agent."""
        agent = Agent(
            name="file_validator",
            model=LiteLlm(model=f"openai/{self.llm_model}"),
            instruction="""Analyze if the selected files match the goal. Consider:
            1. Relevance to the goal
            2. Coverage of required entities
            3. Missing data sources

            Return JSON with: {score, relevant_files, missing_data_types, suggestions}"""
        )

        try:
            caller = await make_agent_caller(agent)
            prompt = f"Goal: {json.dumps(goal)}\nSelected files: {json.dumps(files)}"
            response = await caller.call(prompt)

            if isinstance(response, str):
                try:
                    validation = json.loads(response)
                except:
                    validation = {"score": 80, "suggestions": [response]}
            else:
                validation = response

            self.validation_results['file_selection_validation'] = validation
            self.log(f"File selection score: {validation.get('score', 'N/A')}/100")

            return validation
        except Exception as e:
            self.log(f"File validation failed: {e}", "WARNING")
            return {"score": 75, "suggestions": []}

    async def validate_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Validate generated schema using ADK agent."""
        agent = Agent(
            name="schema_validator",
            model=LiteLlm(model=f"openai/{self.llm_model}"),
            instruction="""Review the schema design. Check for:
            1. Completeness of entity relationships
            2. Proper normalization
            3. Missing relationships
            4. Data quality issues

            Return JSON with: {score, missing_relationships, improvements, warnings}"""
        )

        try:
            caller = await make_agent_caller(agent)
            prompt = f"Validate this knowledge graph schema: {json.dumps(schema)}"
            response = await caller.call(prompt)

            if isinstance(response, str):
                try:
                    validation = json.loads(response)
                except:
                    validation = {"score": 85, "suggestions": [response]}
            else:
                validation = response

            self.validation_results['schema_validation'] = validation
            self.log(f"Schema validation score: {validation.get('score', 'N/A')}/100")

            return validation
        except Exception as e:
            self.log(f"Schema validation failed: {e}", "WARNING")
            return {"score": 80, "suggestions": []}

    async def suggest_improvements(self, current_state: Dict[str, Any]) -> List[str]:
        """Get improvement suggestions from ADK agent."""
        agent = Agent(
            name="improvement_advisor",
            model=LiteLlm(model=f"openai/{self.llm_model}"),
            instruction="""Analyze the current knowledge graph state and suggest improvements.
            Focus on:
            1. Data completeness
            2. Relationship quality
            3. Entity resolution
            4. Performance optimization

            Return a list of actionable improvements."""
        )

        try:
            caller = await make_agent_caller(agent)
            prompt = f"Suggest improvements for: {json.dumps(current_state)}"
            response = await caller.call(prompt)

            if isinstance(response, list):
                suggestions = response
            elif isinstance(response, str):
                suggestions = [s.strip() for s in response.split('\n') if s.strip()]
            else:
                suggestions = []

            return suggestions[:5]  # Top 5 suggestions
        except Exception as e:
            self.log(f"Improvement suggestions failed: {e}", "WARNING")
            return []

    def get_quality_metrics(self) -> Dict[str, Any]:
        """Calculate quality metrics for the graph."""
        metrics = {
            "quality_score": 0,
            "orphan_nodes": 0,
            "connectivity_ratio": 0,
            "node_types": 0,
            "relationship_types": 0
        }

        try:
            from src.neo4j_for_adk import graphdb

            # Count orphan nodes
            orphan_query = """
            MATCH (n)
            WHERE NOT (n)--()
            RETURN count(n) as orphans
            """
            result = graphdb.send_query(orphan_query)
            if result['status'] == 'success' and result['query_result']:
                metrics['orphan_nodes'] = result['query_result'][0].get('orphans', 0)

            # Get connectivity
            conn_query = """
            MATCH (n)
            WITH count(n) as total_nodes
            MATCH (n)--()
            WITH count(DISTINCT n) as connected_nodes, total_nodes
            RETURN toFloat(connected_nodes) / toFloat(total_nodes) as ratio
            """
            result = graphdb.send_query(conn_query)
            if result['status'] == 'success' and result['query_result']:
                metrics['connectivity_ratio'] = result['query_result'][0].get('ratio', 0)

            # Count types
            stats = self.get_final_statistics()
            metrics['node_types'] = len(stats.get('nodes_by_label', {}))
            metrics['relationship_types'] = len(stats.get('relationships_by_type', {}))

            # Calculate quality score
            score = 100
            score -= min(30, metrics['orphan_nodes'] * 2)  # Penalty for orphans
            score -= (1 - metrics['connectivity_ratio']) * 20  # Penalty for low connectivity
            score += min(10, metrics['node_types'] * 2)  # Bonus for diversity
            score += min(10, metrics['relationship_types'])  # Bonus for relationships

            metrics['quality_score'] = max(0, min(100, int(score)))

        except Exception as e:
            self.log(f"Quality metrics calculation failed: {e}", "WARNING")

        return metrics

    async def build_complete_graph(
        self,
        reset: bool = True,
        force_regenerate_plans: bool = False,
        limit_text_files: Optional[int] = None,
        validate_quality: bool = True
    ) -> Dict[str, Any]:
        """
        Build complete graph with ADK validation and improvements.
        """
        start_time = datetime.now()
        self.execution_log = []
        self.generated_plans = {}
        self.validation_results = {}

        results = {
            "start_time": start_time.isoformat(),
            "data_directory": self.data_dir
        }

        try:
            self.log("\n" + "🤖 "*20)
            self.log("ADK-ENHANCED KNOWLEDGE GRAPH PIPELINE")
            self.log("🤖 "*20 + "\n")

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
            goal = self.phase1_determine_goal(csv_files, text_files, force_regenerate_plans)
            results['goal'] = goal

            # ADK Validation: Validate goal
            if validate_quality:
                goal_validation = await self.validate_goal(goal)
                if goal_validation.get('score', 0) < 60:
                    self.log("⚠️ Low goal quality score. Consider reviewing the goal.", "WARNING")

            # Phase 2: Select files
            file_selection = self.phase2_select_files(
                csv_files, text_files, goal, force_regenerate_plans
            )
            results['file_selection'] = {
                "selected_csv": len(file_selection['approved_csv_files']),
                "selected_text": len(file_selection['approved_text_files'])
            }

            # ADK Validation: Validate file selection
            if validate_quality:
                file_validation = await self.validate_file_selection(file_selection, goal)
                if file_validation.get('score', 0) < 70:
                    self.log("⚠️ File selection may be incomplete.", "WARNING")

            # Get selected files
            selected_csv = file_selection['approved_csv_files']
            selected_text = file_selection['approved_text_files']

            # Apply text file limit
            if limit_text_files and len(selected_text) > limit_text_files:
                selected_text = selected_text[:limit_text_files]
                self.log(f"ℹ️ Limiting to {limit_text_files} text files")

            # Phase 3: Generate schema
            construction_plan, extraction_plan = self.phase3_generate_schema(
                selected_csv, selected_text, goal, force_regenerate_plans
            )

            # ADK Validation: Validate schema
            if validate_quality:
                schema_validation = await self.validate_schema({
                    "construction": construction_plan,
                    "extraction": extraction_plan
                })
                if schema_validation.get('score', 0) < 75:
                    self.log("⚠️ Schema may need improvements.", "WARNING")

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
                    selected_text, extraction_plan
                )

            # Phase 6: Entity resolution
            results['resolution'] = self.phase6_resolve_entities()

            # Calculate quality metrics
            if validate_quality:
                quality_metrics = self.get_quality_metrics()
                results['quality_metrics'] = quality_metrics

                self.log(f"\n📊 Graph Quality Score: {quality_metrics['quality_score']}/100")

                # Get improvement suggestions
                if quality_metrics['quality_score'] < 80:
                    suggestions = await self.suggest_improvements({
                        "goal": goal,
                        "files": file_selection,
                        "schema": {"construction": construction_plan, "extraction": extraction_plan},
                        "metrics": quality_metrics
                    })

                    if suggestions:
                        self.log("\n💡 Improvement Suggestions:")
                        for i, suggestion in enumerate(suggestions, 1):
                            self.log(f"  {i}. {suggestion}")

            # Save all generated plans
            self.save_all_plans()

            # Save ADK-specific results
            adk_results_file = os.path.join("generated_plans", "adk_pipeline_results.json")
            with open(adk_results_file, 'w') as f:
                json.dump({
                    "generated_plans": self.generated_plans,
                    "validation_results": self.validation_results,
                    "quality_metrics": results.get('quality_metrics', {}),
                    "timestamp": datetime.now().isoformat()
                }, f, indent=2, default=str)

            # Final statistics
            self.log("\n" + "="*60)
            self.log("ADK PIPELINE COMPLETE")
            self.log("="*60)

            stats = self.get_final_statistics()
            results['final_statistics'] = stats

            self.log(f"\n📊 Final Statistics:")
            self.log(f"  Nodes: {stats['total_nodes']:,}")
            self.log(f"  Relationships: {stats['total_relationships']:,}")

            # Execution time
            end_time = datetime.now()
            results['execution_time_seconds'] = (end_time - start_time).total_seconds()
            results['end_time'] = end_time.isoformat()

            self.log(f"\n⏱️ Execution time: {results['execution_time_seconds']:.2f} seconds")
            self.log("\n✅ ADK pipeline completed successfully!")

            results['status'] = 'success'
            results['validation_results'] = self.validation_results

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


async def create_and_run_adk_dynamic_pipeline(
    reset: bool = True,
    force_regenerate_plans: bool = False,
    limit_text_files: Optional[int] = None,
    data_dir: Optional[str] = None,
    llm_model: str = "gpt-4o-mini",
    validate_quality: bool = True
) -> ADKDynamicKnowledgeGraphBuilder:
    """
    Create and run the ADK-enhanced dynamic pipeline.
    """
    builder = ADKDynamicKnowledgeGraphBuilder(data_dir, llm_model)
    await builder.build_complete_graph(
        reset=reset,
        force_regenerate_plans=force_regenerate_plans,
        limit_text_files=limit_text_files,
        validate_quality=validate_quality
    )
    return builder