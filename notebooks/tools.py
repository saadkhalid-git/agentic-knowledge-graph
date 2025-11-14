
import logging
from pathlib import Path
from itertools import islice

from typing import Dict, Any

from google.adk.tools import ToolContext

from src.neo4j_for_adk import graphdb, tool_success, tool_error
from notebooks.helper import get_neo4j_import_dir

def get_approved_user_goal(tool_context: ToolContext):
    """Returns the user's goal, which is a dictionary containing the kind of graph and its description."""
    if "approved_user_goal" not in tool_context.state:
        return tool_error("approved_user_goal not set. Ask the user to clarify their goal (kind of graph and description).")  
    
    user_goal_data = tool_context.state["approved_user_goal"]

    return tool_success("approved_user_goal", user_goal_data)

def get_approved_files(tool_context: ToolContext):
    """Returns the files that have been approved for import."""
    if "approved_files" not in tool_context.state:
        return tool_error("approved_files not set. Ask the user to approve the file suggestions.")
    
    files = tool_context.state["approved_files"]
    
    return tool_success("approved_files", files)
    
# Tool: Sample File
def sample_file(file_path: str) -> dict:
    """Samples a file by reading its content as text.
    
    Treats any file as text and reads up to a maximum of 100 lines.
    
    Args:
      file_path: file to sample, relative to the import directory
      
    Returns:
        dict: A dictionary containing metadata about the content,
              along with a sampling of the file.
              Includes a 'status' key ('success' or 'error').
              If 'success', includes a 'content' key with textual file content.
              If 'error', includes an 'error_message' key.
    """
    import_dir = Path(get_neo4j_import_dir())

    full_path_to_file = import_dir / file_path

    if not full_path_to_file.exists():
        return tool_error(f"File does not exist in import directory: {file_path}")
    
    try:
        # Treat all files as text
        with open(full_path_to_file, 'r', encoding='utf-8') as file:
            # Read up to 100 lines
            lines = list(islice(file, 100))
            content = ''.join(lines)
            return tool_success("content", content)
    
    except Exception as e:
        return tool_error(f"Error reading or processing file {file_path}: {e}")


### Neo4j Tools ###
def neo4j_is_ready():
    return graphdb.send_query("RETURN 'Neo4j is Ready!' as message")

def drop_neo4j_indexes() -> Dict[str, Any]:
    """Drops and constraints and indexes present on the neo4j graph database

    Returns:
        Success or an error.
    """
    # remove all constraints
    list_constraints = graphdb.send_query(
        """SHOW CONSTRAINTS YIELD name"""
    )
    if (list_constraints == "error"):
        return list_constraints
    constraint_names = [row["name"] for row in list_constraints["query_result"]]
    for constraint_name in constraint_names:
        dropped_constraint = graphdb.send_query("""DROP CONSTRAINT $constraint_name""", {"constraint_name": constraint_name})
        if (dropped_constraint["status"] == "error"):
            return dropped_constraint

    # remove all indexes
    list_indexes = graphdb.send_query(
        """SHOW INDEXES YIELD name"""
    )
    if (list_indexes == "error"):
        return list_indexes
    index_names = [row["name"] for row in list_indexes["query_result"]]
    for index_name in index_names:
        dropped_index = graphdb.send_query("""DROP INDEX $index_name""", {"index_name": index_name})
        if (dropped_index["status"] == "error"):
            return dropped_index

    return tool_success("message", "Neo4j constraints and indexes have been dropped.")

def clear_neo4j_data() -> Dict[str, Any]:
    """Clears all data from the neo4j graph database.

    Use with caution! Confirm with the user
    that they know this will completely reset the database.

    Returns:
        Success or an error.
    """
    # First, remove all nodes and relationships in batches
    data_removed = graphdb.send_query("""MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS""")
    if (data_removed["status"] == "error") :
        return data_removed

    return tool_success("message", "Neo4j graph has been reset.")
