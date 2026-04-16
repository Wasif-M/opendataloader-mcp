"""
opendataloader-pdf MCP Server

A lightweight wrapper that exposes PDF processing tools to Claude via the Model Context Protocol.
Delegates all operations to the modular opendataloader_mcp package for clean separation of concerns.

Key Capabilities:
- Batch processing of multiple PDFs
- Robust error handling with detailed error messages
- Performance tracking and logging
- Intelligent caching for frequently accessed PDFs
- Format conversion (Markdown, JSON, HTML, Text)
- Full-text search with regex support
- Automatic retry with exponential backoff for resilience
"""

from opendataloader_mcp.server import mcp
if __name__ == "__main__":
    mcp.run(transport="stdio")