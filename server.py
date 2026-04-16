"""
opendataloader-pdf MCP Server
Exposes parse_pdf and extract_tables as Claude tools via stdio transport.

Features:
- Batch processing of multiple PDFs
- Enhanced error handling with detailed messages
- Logging and performance metrics
- Caching layer for repeated requests
- Data validation and format conversion
- Content search within PDFs
- Retry logic for failed operations

This is a wrapper that delegates to the modular opendataloader_mcp package.
"""

from opendataloader_mcp.server import mcp

if __name__ == "__main__":
