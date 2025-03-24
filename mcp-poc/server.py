from mcp.server.fastmcp import FastMCP
import time
import signal
import sys
import os
from langchain_community.document_loaders import TextLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
from typing import Union, BinaryIO
from pydantic import BaseModel
import tempfile

# Handle SIGINT (Ctrl+C) gracefully
def signal_handler(sig, frame):
    print("Shutting down server gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Create an MCP server with increased timeout
mcp = FastMCP(
    name="mcp-poc",
    host="127.0.0.1",
    port=5000,
    # Add this to make the server more resilient
    timeout=30  # Increase timeout to 30 seconds
)

def load_vector_store(name : str, doc : Union[BinaryIO, bytes], embedding_model : str):
     # embedding
    print("Creating Embedding", file=sys.stderr)
    embeddings = HuggingFaceEmbeddings(model_name=embedding_model)
    print("Finish creating Embedding", file=sys.stderr)

    # Create a temporary file to store the document
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as temp_file:
        if isinstance(doc, bytes):
            temp_file.write(doc.decode('utf-8'))
        else:
            temp_file.write(doc.read().decode('utf-8'))
        temp_file_path = temp_file.name

    try:
        # Load document from temporary file
        loader = TextLoader(temp_file_path)
        documents = loader.load()
    finally:
        # Clean up the temporary file
        os.unlink(temp_file_path)

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
    docs = text_splitter.split_documents(documents)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    persistent_directory = os.path.join(current_dir, "db", f"chroma_db_{name}")

    if not os.path.exists(persistent_directory):
        print(f"Creating vector store : {persistent_directory}", file=sys.stderr)
        Chroma.from_documents(docs, embeddings, persist_directory=persistent_directory)
        print(f"Finish creating vector store : {persistent_directory}", file=sys.stderr)
    else:
        print(f"Vector store : {persistent_directory} already exists", file=sys.stderr)
        print(f"Loading the existing vector store from {persistent_directory}", file=sys.stderr)
        vector_store = Chroma(persist_directory=persistent_directory, embedding_function=embeddings)
    
    return {"status": "success", "message": f"Vector store {name} processed successfully"}

# Define our tool
@mcp.tool()
def count_r(word: str) -> int:
    """Count the number of 'r' letters in a given word."""
    try:
        # Add robust error handling
        if not isinstance(word, str):
            return 0
        return word.lower().count("r")
    except Exception as e:
        # Return 0 on any error
        return 0
    
@mcp.tool()
def create_chroma_database(name: str, document: Union[BinaryIO, bytes]) -> str:
    """Create a chroma vector database from a given name and document data.
    
    Args:
        name: The name of the database to create
        document: The document data, either as bytes or a file-like object
    
    Returns:
        A success message or error description
    """
    try:
        if not isinstance(name, str):
            return "Error: Name must be a string"
        
        # Handle the document based on its type
        if hasattr(document, 'read'):  # File-like object
            document_data = document.read()
        elif isinstance(document, bytes):
            document_data = document
        else:
            return "Error: Document must be either bytes or a file-like object"
        
        # Process the document data directly instead of loading from URL
        embedding_model = "sentence-transformers/all-MiniLM-L6-v2"
        load_vector_store(name=name, doc=document_data, embedding_model=embedding_model)
        return "Chroma database created successfully"
    except Exception as e:
        return f"Error: Failed to create chroma database - {str(e)}"
            

if __name__ == "__main__":
    try:
        print("Starting MCP server 'mcp-poc' on 127.0.0.1:5000")
        # Use this approach to keep the server running
        mcp.run()
        print('...', file=sys.stderr)
    except Exception as e:
        print(f"Error: {e}")
        # Sleep before exiting to give time for error logs
        time.sleep(5)