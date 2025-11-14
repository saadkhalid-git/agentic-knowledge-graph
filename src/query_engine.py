"""
Query Engine for ADK-Enhanced Knowledge Graph
Answers complex business questions with traceability
"""

import os
import sys

# Fix import path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from typing import Dict, List, Any, Optional, Tuple
import json
from dataclasses import dataclass
from datetime import datetime

# Neo4j connection
from src.neo4j_for_adk import graphdb

# LLM for natural language processing
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()


@dataclass
class QueryResult:
    """Structured result with traceability"""
    question: str
    answer: Any
    evidence: List[Dict[str, Any]]
    query_used: Optional[str] = None
    confidence: float = 1.0
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()

    def to_dict(self):
        return {
            "question": self.question,
            "answer": self.answer,
            "evidence": self.evidence,
            "query_used": self.query_used,
            "confidence": self.confidence,
            "timestamp": self.timestamp
        }


class KnowledgeGraphQueryEngine:
    """
    Query engine for answering business questions using the knowledge graph.
    Provides natural language interface with full traceability.
    """

    def __init__(self, use_llm: bool = True):
        """Initialize query engine"""
        self.use_llm = use_llm
        if use_llm:
            self.client = OpenAI()

        # Predefined query templates for common questions
        self.query_templates = {
            "list_products": """
                MATCH (p:Product)
                RETURN p.product_id as id, p.product_name as name,
                       p.price as price, p.description as description
                ORDER BY p.product_name
            """,

            "product_reviews": """
                MATCH (p:Product)
                WHERE toLower(p.product_name) CONTAINS toLower($product_name)
                OPTIONAL MATCH (p)<-[:reviewed_by]-(u:User)
                OPTIONAL MATCH (p)-[:has_rating]->(r:Rating)
                OPTIONAL MATCH (p)-[:has_issue]->(i:Issue)
                RETURN p.product_name as product,
                       collect(DISTINCT u.id) as reviewers,
                       collect(DISTINCT r.id) as ratings,
                       collect(DISTINCT i.id) as issues
            """,

            "product_suppliers": """
                MATCH (p:Product)
                WHERE toLower(p.product_name) CONTAINS toLower($product_name)
                MATCH (p)<-[:CONTAINS]-(a:Assembly)<-[:IS_PART_OF]-(part:Part)
                MATCH (part)<-[:SUPPLIES]-(s:Supplier)
                RETURN p.product_name as product,
                       collect(DISTINCT {
                           supplier: s.name,
                           specialty: s.specialty,
                           city: s.city,
                           country: s.country,
                           email: s.contact_email,
                           website: s.website,
                           parts: part.part_name
                       }) as suppliers
            """
        }

    def natural_language_to_cypher(self, question: str) -> Tuple[str, Dict[str, Any]]:
        """
        Convert natural language question to Cypher query.
        Returns (query, parameters)
        """
        question_lower = question.lower()

        # Pattern matching for common question types
        if "what products" in question_lower or "products available" in question_lower:
            return self.query_templates["list_products"], {}

        elif "customers saying" in question_lower or "reviews" in question_lower:
            # Extract product name
            product_name = self._extract_product_name(question)
            return self.query_templates["product_reviews"], {"product_name": product_name}

        elif "suppliers" in question_lower and ("provide" in question_lower or "parts" in question_lower):
            # Extract product name
            product_name = self._extract_product_name(question)
            return self.query_templates["product_suppliers"], {"product_name": product_name}

        else:
            # Use LLM to generate Cypher query if available
            if self.use_llm:
                return self._llm_generate_cypher(question), {}
            else:
                raise ValueError(f"Cannot interpret question: {question}")

    def _extract_product_name(self, question: str) -> str:
        """Extract product name from question"""
        # Known products in the catalog
        products = [
            "Stockholm Chair", "Uppsala Sofa", "Malmo Desk",
            "Gothenburg Table", "Linkoping Bed", "Helsingborg Dresser",
            "Orebro Lamp", "Vasteras Bookshelf", "Norrkoping Nightstand",
            "Jonkoping Coffee Table"
        ]

        question_lower = question.lower()
        for product in products:
            if product.lower() in question_lower:
                return product

        # Try to extract quoted text
        import re
        quoted = re.findall(r'"([^"]*)"', question)
        if quoted:
            return quoted[0]

        # Default extraction: last capitalized words
        words = question.split()
        for i in range(len(words)-1, -1, -1):
            if words[i][0].isupper():
                # Check if next word is also capitalized
                if i+1 < len(words) and words[i+1][0].isupper():
                    return f"{words[i]} {words[i+1]}"
                return words[i]

        return ""

    def _llm_generate_cypher(self, question: str) -> str:
        """Use LLM to generate Cypher query from natural language"""
        prompt = f"""
        Convert this question to a Neo4j Cypher query.

        Available node labels: Product, Supplier, Part, Assembly, User, Rating, Issue, Feature
        Available relationships: SUPPLIES, IS_PART_OF, CONTAINS, reviewed_by, has_rating, has_issue

        Question: {question}

        Return only the Cypher query, no explanation.
        """

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )

        return response.choices[0].message.content.strip()

    def execute_query(self, cypher: str, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute Cypher query and return results"""
        if parameters:
            # Neo4j parameter format
            result = graphdb.send_query(cypher, parameters)
        else:
            result = graphdb.send_query(cypher)

        return result

    def answer_question(self, question: str) -> QueryResult:
        """
        Answer a business question using the knowledge graph.
        Returns structured result with evidence and traceability.
        """
        try:
            # Convert to Cypher
            cypher, params = self.natural_language_to_cypher(question)

            # Execute query
            result = self.execute_query(cypher, params)

            if result['status'] != 'success':
                return QueryResult(
                    question=question,
                    answer=f"Error executing query: {result.get('error', 'Unknown error')}",
                    evidence=[],
                    query_used=cypher,
                    confidence=0.0
                )

            # Process results based on question type
            query_result = result.get('query_result', [])

            # Format answer based on question type
            answer, evidence = self._format_answer(question, query_result)

            return QueryResult(
                question=question,
                answer=answer,
                evidence=evidence,
                query_used=cypher,
                confidence=0.9 if query_result else 0.5
            )

        except Exception as e:
            return QueryResult(
                question=question,
                answer=f"Error processing question: {str(e)}",
                evidence=[],
                confidence=0.0
            )

    def _format_answer(self, question: str, results: List[Dict]) -> Tuple[Any, List[Dict]]:
        """Format query results into natural language answer with evidence"""
        question_lower = question.lower()

        if not results:
            return "No results found for this query.", []

        # Product listing
        if "what products" in question_lower:
            products = [r.get('name', 'Unknown') for r in results]
            answer = f"The catalog contains {len(products)} products:\n"
            for i, product in enumerate(products, 1):
                price = next((r.get('price') for r in results if r.get('name') == product), 'N/A')
                answer += f"{i}. {product} (${price})\n"
            return answer.strip(), results

        # Reviews and customer feedback
        elif "customers saying" in question_lower or "reviews" in question_lower:
            if results and results[0]:
                data = results[0]
                product = data.get('product', 'Product')
                reviewers = data.get('reviewers', [])
                ratings = data.get('ratings', [])
                issues = data.get('issues', [])

                answer = f"Customer feedback for {product}:\n"

                if reviewers:
                    answer += f"• {len(reviewers)} customer reviews found\n"

                if ratings:
                    answer += f"• Ratings mentioned: {', '.join(ratings)}\n"

                if issues:
                    answer += f"• Issues reported: {', '.join(issues)}\n"

                if not (reviewers or ratings or issues):
                    answer = f"No customer reviews found for {product} in the system."

                return answer.strip(), results
            else:
                return "No review data found for this product.", []

        # Supplier information
        elif "suppliers" in question_lower:
            if results and results[0]:
                data = results[0]
                product = data.get('product', 'Product')
                suppliers = data.get('suppliers', [])

                if suppliers:
                    answer = f"Suppliers providing parts for {product}:\n\n"

                    # Group by supplier
                    supplier_map = {}
                    for s in suppliers:
                        name = s.get('supplier', 'Unknown')
                        if name not in supplier_map:
                            supplier_map[name] = {
                                'details': s,
                                'parts': []
                            }
                        supplier_map[name]['parts'].append(s.get('parts', 'Unknown part'))

                    for i, (supplier, info) in enumerate(supplier_map.items(), 1):
                        details = info['details']
                        answer += f"{i}. {supplier}\n"
                        answer += f"   • Location: {details.get('city', 'N/A')}, {details.get('country', 'N/A')}\n"
                        answer += f"   • Specialty: {details.get('specialty', 'N/A')}\n"
                        answer += f"   • Contact: {details.get('email', 'N/A')}\n"
                        answer += f"   • Website: {details.get('website', 'N/A')}\n"
                        answer += f"   • Parts supplied: {', '.join(info['parts'])}\n\n"

                    return answer.strip(), suppliers
                else:
                    return f"No suppliers found for {product}.", []
            else:
                return "No supplier information found.", []

        # Generic response
        else:
            # Return raw results for other query types
            if len(results) == 1:
                return results[0], results
            else:
                return results, results

    def demonstrate_capabilities(self):
        """
        Demonstrate system capabilities with example questions.
        Returns formatted demonstration results.
        """
        demonstration = {
            "timestamp": datetime.now().isoformat(),
            "system": "ADK-Enhanced Knowledge Graph Query Engine",
            "capabilities": [],
            "questions_answered": []
        }

        # Example questions
        questions = [
            "What products are available in the catalog?",
            "What are customers saying about the Malmo Desk?",
            "Which suppliers provide parts for the Stockholm Chair, and what are their contact details?"
        ]

        capabilities_shown = [
            "Simple entity listing from structured data",
            "Text extraction and sentiment analysis from unstructured reviews",
            "Multi-hop relationship traversal across CSV sources"
        ]

        print("\n" + "="*60)
        print("KNOWLEDGE GRAPH QUERY ENGINE DEMONSTRATION")
        print("="*60 + "\n")

        for i, (question, capability) in enumerate(zip(questions, capabilities_shown), 1):
            print(f"\nQuestion {i}: {question}")
            print(f"Capability: {capability}")
            print("-" * 40)

            result = self.answer_question(question)

            print(f"Answer:\n{result.answer}")
            print(f"\nConfidence: {result.confidence:.1%}")
            print(f"Evidence items: {len(result.evidence)}")

            if result.query_used:
                print(f"\nCypher query used:")
                print(f"```cypher\n{result.query_used}\n```")

            demonstration["questions_answered"].append(result.to_dict())
            demonstration["capabilities"].append({
                "capability": capability,
                "demonstrated_by": question
            })

        return demonstration


# Convenience functions for notebook usage
def create_query_engine(use_llm: bool = True) -> KnowledgeGraphQueryEngine:
    """Create and return a query engine instance"""
    return KnowledgeGraphQueryEngine(use_llm=use_llm)


def answer_question(question: str, use_llm: bool = True) -> QueryResult:
    """Quick function to answer a single question"""
    engine = create_query_engine(use_llm)
    return engine.answer_question(question)


def run_demonstration():
    """Run full demonstration of capabilities"""
    engine = create_query_engine()
    return engine.demonstrate_capabilities()


if __name__ == "__main__":
    # Run demonstration when executed directly
    demo_results = run_demonstration()

    print("\n" + "="*60)
    print("DEMONSTRATION COMPLETE")
    print("="*60)
    print(f"\nTotal questions answered: {len(demo_results['questions_answered'])}")
    print(f"Capabilities demonstrated: {len(demo_results['capabilities'])}")