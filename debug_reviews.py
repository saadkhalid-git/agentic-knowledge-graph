"""
Debug script to investigate why Malmo Desk reviews aren't being found
"""

import sys
import os

# Add the current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from src.neo4j_for_adk import graphdb

print("="*60)
print("DEBUGGING MALMO DESK REVIEW QUERY ISSUE")
print("="*60)

# 1. Check if Malmo Desk exists as a Product node
print("\n1. Checking for Malmo Desk in Product nodes:")
query1 = """
MATCH (p:Product)
WHERE toLower(p.product_name) CONTAINS 'malmo'
RETURN p.product_name as name, p.product_id as id
"""
result = graphdb.send_query(query1)
if result['status'] == 'success' and result['query_result']:
    for item in result['query_result']:
        print(f"   Found: {item['name']} (ID: {item['id']})")
else:
    print("   ❌ No Product node found with 'malmo' in name")

# 2. Check all Product nodes from text extraction (different label)
print("\n2. Checking Product entities extracted from text:")
query2 = """
MATCH (p)
WHERE 'Product' IN labels(p) AND NOT p:__KGBuilder__
RETURN p.id as name, labels(p) as labels
LIMIT 10
"""
result = graphdb.send_query(query2)
if result['status'] == 'success' and result['query_result']:
    print(f"   Found {len(result['query_result'])} Product entities from text:")
    for item in result['query_result']:
        print(f"   - {item['name']}")
else:
    print("   ❌ No Product entities found from text extraction")

# 3. Check if there are any reviews/users/ratings in the system
print("\n3. Checking for review-related entities:")
query3 = """
MATCH (n)
WHERE n:User OR n:Rating OR n:Issue OR n:Feature
RETURN labels(n)[0] as type, count(*) as count
"""
result = graphdb.send_query(query3)
if result['status'] == 'success' and result['query_result']:
    for item in result['query_result']:
        print(f"   {item['type']}: {item['count']} entities")
else:
    print("   ❌ No review entities found")

# 4. Check what review files were processed
print("\n4. Checking which review files were processed:")
query4 = """
MATCH (d:Document)
RETURN d.path as path
"""
result = graphdb.send_query(query4)
if result['status'] == 'success' and result['query_result']:
    print(f"   Found {len(result['query_result'])} documents processed:")
    for item in result['query_result']:
        if 'malmo' in item['path'].lower():
            print(f"   ✓ {item['path']}")
        else:
            print(f"   - {item['path']}")
else:
    print("   ❌ No Document nodes found")

# 5. Check for any entities with 'malmo' in their ID or properties
print("\n5. Searching for any entity with 'malmo' in properties:")
query5 = """
MATCH (n)
WHERE ANY(key IN keys(n) WHERE toLower(toString(n[key])) CONTAINS 'malmo')
RETURN labels(n) as labels, n.id as id, n.name as name
LIMIT 10
"""
result = graphdb.send_query(query5)
if result['status'] == 'success' and result['query_result']:
    print(f"   Found {len(result['query_result'])} entities mentioning 'malmo':")
    for item in result['query_result']:
        print(f"   - {item['labels']}: {item.get('id', item.get('name', 'Unknown'))}")
else:
    print("   ❌ No entities found with 'malmo' in any property")

# 6. Check entity resolution - are products linked between graphs?
print("\n6. Checking entity resolution (CORRESPONDS_TO relationships):")
query6 = """
MATCH (p1:Product)-[r:CORRESPONDS_TO]-(p2)
RETURN p1.product_name as csv_product, p2.id as text_product, type(r) as rel
LIMIT 5
"""
result = graphdb.send_query(query6)
if result['status'] == 'success' and result['query_result']:
    print(f"   Found {len(result['query_result'])} correspondences:")
    for item in result['query_result']:
        print(f"   - {item['csv_product']} <-> {item['text_product']}")
else:
    print("   ❌ No entity correspondences found between CSV products and text products")

# 7. Try alternative query to find reviews
print("\n7. Alternative query - looking for any product-review relationships:")
query7 = """
MATCH (p)-[r]-(n)
WHERE (p:Product OR 'Product' IN labels(p))
  AND (n:User OR n:Rating OR n:Issue)
RETURN p.product_name as product, p.id as product_id,
       type(r) as relationship, labels(n)[0] as entity_type
LIMIT 10
"""
result = graphdb.send_query(query7)
if result['status'] == 'success' and result['query_result']:
    print(f"   Found {len(result['query_result'])} product-review relationships:")
    for item in result['query_result']:
        print(f"   - {item.get('product', item.get('product_id', 'Unknown'))} -{item['relationship']}-> {item['entity_type']}")
else:
    print("   ❌ No relationships found between products and review entities")

# 8. Check the actual content of review entities
print("\n8. Sample review entities to see their structure:")
query8 = """
MATCH (n)
WHERE n:User OR n:Rating OR n:Issue
RETURN labels(n)[0] as type, n
LIMIT 3
"""
result = graphdb.send_query(query8)
if result['status'] == 'success' and result['query_result']:
    for item in result['query_result']:
        print(f"\n   {item['type']} entity:")
        node = item['n']
        for key, value in node.items():
            print(f"     {key}: {value}")
else:
    print("   ❌ No review entities to inspect")

print("\n" + "="*60)
print("DIAGNOSIS COMPLETE")
print("="*60)

# Final diagnosis
print("\nLIKELY ISSUES:")
print("1. Entity resolution failed - Products from CSV not linked to products in reviews")
print("2. Review extraction used different entity names/IDs than CSV products")
print("3. Relationships between products and reviews not created properly")
print("\nSOLUTION:")
print("Need to improve entity resolution or adjust query to handle unlinked entities")