"""
Workaround to query review data using the __Entity__ nodes
"""

import sys
import os
sys.path.insert(0, '.')

from src.neo4j_for_adk import graphdb

def search_reviews_workaround(product_name):
    """Search for reviews using the generic __Entity__ nodes"""

    print(f"\n🔍 Searching for reviews about: {product_name}")
    print("="*60)

    # Method 1: Search in __KGBuilder__ nodes (they have product field)
    query1 = """
    MATCH (n:__KGBuilder__)
    WHERE toLower(n.product) CONTAINS toLower($product_name)
       OR toLower(n.title) CONTAINS toLower($product_name)
    RETURN n.product as product, n.title as title, n.path as source_file
    LIMIT 5
    """

    result = graphdb.send_query(query1, {"product_name": product_name})
    if result['status'] == 'success' and result['query_result']:
        print(f"✅ Found review data for {product_name}:")
        for item in result['query_result']:
            print(f"  Product: {item['product']}")
            print(f"  Title: {item['title']}")
            print(f"  Source: {item['source_file']}")

        # Method 2: Find related entities
        query2 = """
        MATCH (kb:__KGBuilder__)
        WHERE toLower(kb.product) CONTAINS toLower($product_name)
        MATCH (kb)-[r]-(e:__Entity__)
        RETURN kb.product as product,
               type(r) as relationship,
               e.id as entity,
               e.description as description
        LIMIT 20
        """

        result2 = graphdb.send_query(query2, {"product_name": product_name})
        if result2['status'] == 'success' and result2['query_result']:
            print(f"\n  Related entities found:")
            seen = set()
            for item in result2['query_result']:
                entity_key = f"{item['entity']}: {item.get('description', 'N/A')[:50]}"
                if entity_key not in seen:
                    print(f"    • {entity_key}")
                    seen.add(entity_key)
    else:
        print(f"❌ No review data found for {product_name}")
        print("\n  Note: The entity extraction didn't properly label entities.")
        print("  Review data exists but is stored in generic __Entity__ nodes.")

    # Show what's actually in the graph for this product
    query3 = """
    MATCH (p:Product)
    WHERE toLower(p.product_name) CONTAINS toLower($product_name)
    RETURN p.product_name as name, p.price as price, p.description as description
    """

    result3 = graphdb.send_query(query3, {"product_name": product_name})
    if result3['status'] == 'success' and result3['query_result']:
        print(f"\n📦 Product from CSV:")
        for item in result3['query_result']:
            print(f"  Name: {item['name']}")
            print(f"  Price: ${item['price']}")
            print(f"  Description: {item['description'][:100]}...")

# Test with different products
test_products = [
    "Malmo Desk",
    "Helsingborg Dresser",
    "Stockholm Chair"
]

for product in test_products:
    search_reviews_workaround(product)

print("\n" + "="*60)
print("WORKAROUND ANALYSIS")
print("="*60)
print("\n📌 The review data WAS loaded into the graph but:")
print("  1. Entities are stored as generic __Entity__ nodes")
print("  2. Review metadata is in __KGBuilder__ nodes")
print("  3. Specific labels (User, Rating, Issue) were not applied")
print("\n💡 To fix this permanently:")
print("  1. The unstructured agent needs to be updated to properly use entity_types")
print("  2. The SimpleKGPipeline needs explicit configuration for entity extraction")
print("  3. Or we need to post-process __Entity__ nodes to add proper labels")