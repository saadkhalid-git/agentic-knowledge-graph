"""
Test if review data is now properly accessible in the graph
"""

import sys
import os
sys.path.insert(0, '.')

from src.neo4j_for_adk import graphdb

print("="*60)
print("TESTING REVIEW DATA IN GRAPH")
print("="*60)

# 1. Check what entities were extracted from reviews
print("\n1. Checking review-related entities:")
entity_query = """
MATCH (n)
WHERE n:User OR n:Rating OR n:Issue OR n:Feature OR
      (n:Product AND NOT n:__KGBuilder__)
RETURN labels(n)[0] as type, count(*) as count
ORDER BY count DESC
"""
result = graphdb.send_query(entity_query)
if result['status'] == 'success':
    print("  Entity counts:")
    for item in result['query_result']:
        print(f"    {item['type']:15} {item['count']:5} entities")
else:
    print(f"  Error: {result}")

# 2. Check specific products extracted from text
print("\n2. Products found in review text:")
product_query = """
MATCH (p:Product)
WHERE NOT p:__KGBuilder__
RETURN p.id as name
LIMIT 20
"""
result = graphdb.send_query(product_query)
if result['status'] == 'success':
    products = [item['name'] for item in result['query_result']]
    for p in products:
        print(f"    - {p}")
else:
    print(f"  Error: {result}")

# 3. Check relationships from reviews
print("\n3. Review relationships in graph:")
rel_query = """
MATCH ()-[r]->()
WHERE type(r) IN ['reviewed_by', 'has_rating', 'has_issue', 'includes_feature']
RETURN type(r) as relationship, count(r) as count
"""
result = graphdb.send_query(rel_query)
if result['status'] == 'success':
    if result['query_result']:
        for item in result['query_result']:
            print(f"    {item['relationship']:20} {item['count']:5} relationships")
    else:
        print("    ⚠️ No review relationships found!")
else:
    print(f"  Error: {result}")

# 4. Check for specific products
print("\n4. Searching for specific products:")
test_products = ["Malmo Desk", "Helsingborg Dresser", "Stockholm Chair"]
for product_name in test_products:
    # Check in CSV-loaded products
    csv_query = """
    MATCH (p:Product)
    WHERE toLower(p.product_name) CONTAINS toLower($product_name)
    RETURN p.product_name as name, 'CSV' as source
    """
    csv_result = graphdb.send_query(csv_query, {"product_name": product_name})

    # Check in text-extracted products
    text_query = """
    MATCH (p:Product)
    WHERE toLower(p.id) CONTAINS toLower($product_name)
    AND NOT p:__KGBuilder__
    RETURN p.id as name, 'Text' as source
    """
    text_result = graphdb.send_query(text_query, {"product_name": product_name})

    found = []
    if csv_result['status'] == 'success' and csv_result['query_result']:
        found.extend(csv_result['query_result'])
    if text_result['status'] == 'success' and text_result['query_result']:
        found.extend(text_result['query_result'])

    if found:
        print(f"  ✅ {product_name}: Found in {', '.join([f['source'] for f in found])}")
    else:
        print(f"  ❌ {product_name}: Not found")

# 5. Sample review data
print("\n5. Sample review connections:")
sample_query = """
MATCH (p:Product)-[r]-(n)
WHERE type(r) IN ['reviewed_by', 'has_rating', 'has_issue', 'includes_feature']
AND NOT p:__KGBuilder__
RETURN p.id as product, type(r) as relationship,
       labels(n)[0] as connected_to, n.id as entity_id
LIMIT 10
"""
result = graphdb.send_query(sample_query)
if result['status'] == 'success':
    if result['query_result']:
        for item in result['query_result']:
            print(f"  {item['product']} -{item['relationship']}-> {item['connected_to']}: {item['entity_id']}")
    else:
        print("  ⚠️ No product-review connections found!")

print("\n" + "="*60)
print("ANALYSIS COMPLETE")
print("="*60)