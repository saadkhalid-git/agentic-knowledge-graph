#!/usr/bin/env python3
"""
Run the ADK-Enhanced Dynamic Knowledge Graph Pipeline
Always regenerates the complete graph with full validation
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from datetime import datetime

# Import the ADK-enhanced builder
from src.pipeline.adk_dynamic_builder import ADKDynamicKnowledgeGraphBuilder


def print_banner():
    """Print a banner for the ADK pipeline."""
    print("\n" + "🤖 "*30)
    print("ADK-ENHANCED KNOWLEDGE GRAPH PIPELINE")
    print("Powered by LLM Intelligence & Validation")
    print("🤖 "*30 + "\n")


def print_results(results: dict):
    """Print formatted results."""
    print("\n" + "="*60)
    print("PIPELINE RESULTS")
    print("="*60)

    # Status
    status_emoji = "✅" if results.get('status') == 'success' else "❌"
    print(f"\n{status_emoji} Status: {results.get('status', 'unknown').upper()}")

    # Execution time
    if 'execution_time_seconds' in results:
        print(f"⏱️  Execution Time: {results['execution_time_seconds']:.2f} seconds")

    # Files discovered
    if 'discovered_files' in results:
        files = results['discovered_files']
        print(f"\n📁 Files Discovered:")
        print(f"   CSV: {files.get('csv_count', 0)}")
        print(f"   Text: {files.get('text_count', 0)}")

    # Goal
    if 'goal' in results:
        goal = results['goal']
        print(f"\n🎯 Goal: {goal.get('kind_of_graph', 'Unknown')}")
        desc = goal.get('description', '')[:150]
        if desc:
            print(f"   {desc}...")

    # File selection
    if 'file_selection' in results:
        selection = results['file_selection']
        print(f"\n📂 Files Selected:")
        print(f"   CSV: {selection.get('selected_csv', 0)}")
        print(f"   Text: {selection.get('selected_text', 0)}")

    # Schema generation
    if 'schema_generation' in results:
        schema = results['schema_generation']
        print(f"\n🏗️  Schema Generated:")
        print(f"   Nodes: {schema.get('nodes_planned', 0)}")
        print(f"   Relationships: {schema.get('relationships_planned', 0)}")
        print(f"   Entity Types: {schema.get('entity_types', 0)}")
        print(f"   Fact Types: {schema.get('fact_types', 0)}")

    # Final statistics
    if 'final_statistics' in results:
        stats = results['final_statistics']
        print(f"\n📊 Final Graph Statistics:")
        print(f"   Total Nodes: {stats.get('total_nodes', 0):,}")
        print(f"   Total Relationships: {stats.get('total_relationships', 0):,}")

        # Top node types
        if 'nodes_by_label' in stats:
            print(f"\n   Top Node Types:")
            for label, count in list(stats['nodes_by_label'].items())[:5]:
                print(f"     {label:20} {count:8,}")

    # Quality metrics
    if 'quality_metrics' in results:
        quality = results['quality_metrics']
        score = quality.get('quality_score', 0)
        score_emoji = "🏆" if score >= 80 else "⚠️" if score >= 60 else "❌"
        print(f"\n{score_emoji} Graph Quality Score: {score}/100")

        if 'orphan_nodes' in quality:
            print(f"   Orphan Nodes: {quality['orphan_nodes']}")
        if 'connectivity_ratio' in quality:
            print(f"   Connectivity: {quality['connectivity_ratio']:.1%}")
        if 'relationship_types' in quality:
            print(f"   Relationship Types: {quality['relationship_types']}")

    # Validation results
    if 'validation_results' in results:
        print(f"\n🔍 Validation Scores:")
        for val_name, val_result in results['validation_results'].items():
            if isinstance(val_result, dict) and 'score' in val_result:
                score = val_result['score']
                emoji = "✅" if score >= 80 else "⚠️" if score >= 60 else "❌"
                clean_name = val_name.replace('_', ' ').title()
                print(f"   {emoji} {clean_name}: {score}/100")

    # Error information
    if results.get('status') == 'error':
        print(f"\n❌ Error: {results.get('error', 'Unknown error')}")


async def main():
    """Main entry point for the ADK pipeline - always regenerates complete graph."""

    # Always show banner
    print_banner()

    print("🔄 Starting complete graph regeneration...")
    print("This will:")
    print("1. Reset the existing graph")
    print("2. Analyze all data files")
    print("3. Generate new intelligent plans using LLM")
    print("4. Build complete knowledge graph")
    print("5. Validate quality at each step\n")

    # Run the pipeline with fixed settings
    try:
        # Create builder and run pipeline
        # Always: reset=True, force_regenerate=True, validate_quality=True
        builder = ADKDynamicKnowledgeGraphBuilder(
            data_dir=None,  # Use default data directory
            llm_model="gpt-4o-mini"  # Use gpt-4o-mini for efficiency
        )

        results = await builder.build_complete_graph(
            reset=True,  # Always reset the graph
            force_regenerate_plans=True,  # Always regenerate all plans
            limit_text_files=None,  # Process all text files
            validate_quality=True  # Always validate quality
        )

        # Display results
        print_results(results)

        print("\n✅ Pipeline completed successfully!")
        print(f"📋 Results saved to: generated_plans/adk_pipeline_results.json")

    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())