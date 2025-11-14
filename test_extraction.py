"""
Test entity extraction directly on a review file
"""

import sys
import os
sys.path.insert(0, '.')

# Test with the SimpleKGPipeline directly
from neo4j_graphrag.experimental.pipeline.kg_builder import SimpleKGPipeline
from neo4j_graphrag.llm import OpenAILLM
from neo4j_graphrag.embeddings import OpenAIEmbeddings

print("Testing entity extraction from review file...")

# Read a sample review file
review_file = "data/product_reviews/malmo_desk_reviews.md"
with open(review_file, 'r') as f:
    content = f.read()

print(f"\nSample content from {review_file}:")
print(content[:500])
print("...")

# Check what the extraction plan says
import json
with open('generated_plans/extraction_plan.json', 'r') as f:
    extraction_plan = json.load(f)

print("\n\nExtraction plan configuration:")
print(f"Entity types: {extraction_plan.get('entity_types', [])}")
print(f"Fact types: {list(extraction_plan.get('fact_types', {}).keys())}")

# The issue might be with how the unstructured agent processes markdown
print("\n\nPotential issues:")
print("1. The SimpleKGPipeline might not be configured with the right entity types")
print("2. The markdown format might not be parsed correctly")
print("3. The LLM might not be extracting entities with the expected labels")

# Check if there are any __Entity__ nodes (generic extraction)
from src.neo4j_for_adk import graphdb

generic_query = """
MATCH (n:__Entity__)
RETURN count(n) as count, collect(DISTINCT n.label) as labels
LIMIT 10
"""
result = graphdb.send_query(generic_query)
if result['status'] == 'success' and result['query_result']:
    data = result['query_result'][0]
    print(f"\n\nGeneric __Entity__ nodes: {data['count']}")
    if data['labels']:
        print(f"Labels found: {data['labels'][:10]}")

# Check what __KGBuilder__ nodes contain
builder_query = """
MATCH (n:__KGBuilder__)
RETURN DISTINCT keys(n) as properties
LIMIT 1
"""
result = graphdb.send_query(builder_query)
if result['status'] == 'success' and result['query_result']:
    print(f"\n__KGBuilder__ node properties: {result['query_result'][0]['properties']}")

print("\n\nDIAGNOSIS:")
print("The SimpleKGPipeline is creating __KGBuilder__ and __Entity__ nodes")
print("but NOT creating the specific entity types (User, Rating, Issue, etc.)")
print("This suggests the LLM extraction is not working as expected.")
print("\nSOLUTION: The unstructured agent needs to be fixed to properly")
print("extract and label entities according to the extraction plan.")