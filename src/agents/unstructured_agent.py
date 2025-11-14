"""
Unstructured Data Agent for Automated Pipeline
Handles text extraction from markdown files without human intervention
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import re
from pathlib import Path
from typing import Dict, Any, List, Optional
from src.neo4j_for_adk import graphdb
from neo4j_graphrag.experimental.pipeline.kg_builder import SimpleKGPipeline
from neo4j_graphrag.experimental.components.text_splitters.base import TextSplitter
from neo4j_graphrag.experimental.components.types import TextChunk, TextChunks, PdfDocument, DocumentInfo
from neo4j_graphrag.experimental.components.pdf_loader import DataLoader
from neo4j_graphrag.llm import OpenAILLM
from neo4j_graphrag.embeddings import OpenAIEmbeddings


class RegexTextSplitter(TextSplitter):
    """Custom text splitter using regex patterns."""

    def __init__(self, pattern: str = "---"):
        self.pattern = pattern

    async def run(self, text: str) -> TextChunks:
        """Split text using the regex pattern."""
        texts = re.split(self.pattern, text)
        chunks = [TextChunk(text=str(text), index=i) for (i, text) in enumerate(texts) if text.strip()]
        return TextChunks(chunks=chunks)


class MarkdownDataLoader(DataLoader):
    """Custom data loader for markdown files."""

    def extract_title(self, markdown_text: str) -> str:
        """Extract title from markdown text."""
        pattern = r'^# (.+)$'
        match = re.search(pattern, markdown_text, re.MULTILINE)
        return match.group(1) if match else "Untitled"

    def extract_product_name(self, filepath: Path) -> str:
        """Extract product name from filename."""
        filename = os.path.basename(filepath)
        product_name = filename.replace('.md', '').replace('_reviews', '')
        product_name = product_name.replace('_', ' ').title()
        return product_name

    async def run(self, filepath: Path, metadata: Dict = {}) -> PdfDocument:
        """Load and process a markdown file."""
        with open(filepath, "r") as f:
            markdown_text = f.read()

        doc_headline = self.extract_title(markdown_text)
        product_name = self.extract_product_name(filepath)

        markdown_info = DocumentInfo(
            path=str(filepath),
            metadata={
                "title": doc_headline,
                "product": product_name,
                **metadata
            }
        )
        return PdfDocument(text=markdown_text, document_info=markdown_info)


class AutomatedUnstructuredAgent:
    """
    Agent for constructing subject graph from unstructured text data.
    Automated version - no human intervention required.
    """

    def __init__(self, llm_model: str = "gpt-4o", embedding_model: str = "text-embedding-3-large"):
        self.name = "AutomatedUnstructuredAgent"
        self.description = "Extracts entities and relationships from text automatically"
        self.llm = OpenAILLM(model_name=llm_model, model_params={"temperature": 0})
        self.embedder = OpenAIEmbeddings(model=embedding_model)
        self.text_splitter = RegexTextSplitter("---")
        self.data_loader = MarkdownDataLoader()

    def create_entity_extraction_prompt(self, context: str = "") -> str:
        """Create a prompt for entity extraction."""
        general_instructions = """
        You are a top-tier algorithm designed for extracting
        information in structured formats to build a knowledge graph.

        Extract the entities (nodes) and specify their type from the following text.
        Also extract the relationships between these nodes.

        Return result as JSON using the following format:
        {{"nodes": [ {{"id": "0", "label": "Person", "properties": {{"name": "John"}} }}],
        "relationships": [{{"type": "KNOWS", "start_node_id": "0", "end_node_id": "1", "properties": {{"since": "2024-08-01"}} }}] }}

        Use only the following node and relationship types (if provided):
        {schema}

        Assign a unique ID (string) to each node, and reuse it to define relationships.
        Do respect the source and target node types for relationship and
        the relationship direction.

        Make sure you adhere to the following rules to produce valid JSON objects:
        - Do not return any additional information other than the JSON in it.
        - Omit any backticks around the JSON - simply output the JSON on its own.
        - The JSON object must not wrapped into a list - it is its own JSON object.
        - Property names must be enclosed in double quotes
        """

        if context:
            context_section = f"""
            Consider the following context to help identify entities and relationships:
            <context>
            {context}
            </context>"""
        else:
            context_section = ""

        input_section = """
        Input text:
        {text}
        """

        return general_instructions + "\n" + context_section + "\n" + input_section

    def create_entity_schema(
        self,
        entity_types: List[str],
        fact_types: Dict[str, Dict[str, str]]
    ) -> Dict[str, Any]:
        """Create an entity schema for extraction."""
        # Convert fact types to relationship types
        relationship_types = [key.upper() for key in fact_types.keys()]

        # Convert fact types to patterns
        patterns = [
            [fact['subject_label'], fact['predicate_label'].upper(), fact['object_label']]
            for fact in fact_types.values()
        ]

        schema = {
            "node_types": entity_types,
            "relationship_types": relationship_types,
            "patterns": patterns,
            "additional_node_types": False  # Strict mode
        }

        return schema

    def create_kg_pipeline(
        self,
        file_path: str,
        entity_schema: Dict[str, Any]
    ) -> SimpleKGPipeline:
        """Create a knowledge graph extraction pipeline for a file."""
        prompt = self.create_entity_extraction_prompt()

        # Create pipeline
        pipeline = SimpleKGPipeline(
            llm=self.llm,
            driver=graphdb.get_driver(),
            embedder=self.embedder,
            from_pdf=True,  # Using custom loader
            pdf_loader=self.data_loader,
            text_splitter=self.text_splitter,
            schema=entity_schema,
            prompt_template=prompt
        )

        return pipeline

    async def process_file(
        self,
        file_path: str,
        entity_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process a single file to extract entities and relationships."""
        try:
            print(f"    📄 Processing: {os.path.basename(file_path)}")

            # Create pipeline for this file
            kg_pipeline = self.create_kg_pipeline(file_path, entity_schema)

            # Run the pipeline
            results = await kg_pipeline.run_async(file_path=str(file_path))

            print(f"      ✅ Extracted entities and relationships")
            return {
                "status": "success",
                "file": file_path,
                "result": results.result if hasattr(results, 'result') else "Processed"
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

        # Create entity schema
        entity_schema = self.create_entity_schema(entity_types, fact_types)

        results = {
            "files_processed": [],
            "files_failed": [],
            "total_entities": 0,
            "total_relationships": 0,
            "errors": []
        }

        print(f"  Processing {len(file_paths)} markdown files...")

        for file_path in file_paths:
            # Add import_dir if provided
            full_path = os.path.join(import_dir, file_path) if import_dir else file_path

            result = await self.process_file(full_path, entity_schema)

            if result["status"] == "success":
                results["files_processed"].append(file_path)
            else:
                results["files_failed"].append(file_path)
                results["errors"].append(result["error"])

        # Get statistics
        stats = self.get_graph_statistics()
        results.update(stats)

        # Create indexes
        self.create_text_indexes()

        return results

    def get_graph_statistics(self) -> Dict[str, Any]:
        """Get statistics about the constructed subject graph."""
        entity_stats = graphdb.send_query("""
            MATCH (n:`__Entity__`)
            WITH labels(n) as node_labels
            UNWIND node_labels as label
            WITH label
            WHERE NOT label STARTS WITH "__"
            RETURN label, count(*) as count
            ORDER BY count DESC
        """)

        chunk_stats = graphdb.send_query("""
            MATCH (c:Chunk)
            RETURN count(c) as chunk_count
        """)

        doc_stats = graphdb.send_query("""
            MATCH (d:Document)
            RETURN count(d) as document_count
        """)

        stats = {
            "entities_by_type": {},
            "chunk_count": 0,
            "document_count": 0
        }

        if entity_stats['status'] == 'success':
            for row in entity_stats['query_result']:
                stats['entities_by_type'][row['label']] = row['count']

        if chunk_stats['status'] == 'success' and chunk_stats['query_result']:
            stats['chunk_count'] = chunk_stats['query_result'][0].get('chunk_count', 0)

        if doc_stats['status'] == 'success' and doc_stats['query_result']:
            stats['document_count'] = doc_stats['query_result'][0].get('document_count', 0)

        return stats

    def create_text_indexes(self) -> Dict[str, Any]:
        """Create text indexes for efficient search on chunks."""
        print("  🔍 Creating text indexes...")

        results = {}

        # Create vector index
        vector_index_query = """
        CREATE VECTOR INDEX `chunk_embedding_index` IF NOT EXISTS
        FOR (c:Chunk)
        ON (c.embedding)
        OPTIONS {
            indexConfig: {
                `vector.dimensions`: 3072,
                `vector.similarity_function`: 'cosine'
            }
        }
        """
        vector_result = graphdb.send_query(vector_index_query)
        results['vector_index'] = vector_result['status']

        # Create full-text index
        fulltext_index_query = """
        CREATE FULLTEXT INDEX `chunk_text_index` IF NOT EXISTS
        FOR (c:Chunk)
        ON EACH [c.text]
        """
        fulltext_result = graphdb.send_query(fulltext_index_query)
        results['fulltext_index'] = fulltext_result['status']

        print(f"    ✅ Indexes created")

        return results


# Default entity types and fact types for product reviews
DEFAULT_ENTITY_TYPES = ['Product', 'Issue', 'Feature', 'User', 'Location']

DEFAULT_FACT_TYPES = {
    'has_issue': {
        'subject_label': 'Product',
        'predicate_label': 'has_issue',
        'object_label': 'Issue'
    },
    'includes_feature': {
        'subject_label': 'Product',
        'predicate_label': 'includes_feature',
        'object_label': 'Feature'
    },
    'used_in_location': {
        'subject_label': 'Product',
        'predicate_label': 'used_in_location',
        'object_label': 'Location'
    },
    'reviewed_by': {
        'subject_label': 'Product',
        'predicate_label': 'reviewed_by',
        'object_label': 'User'
    }
}