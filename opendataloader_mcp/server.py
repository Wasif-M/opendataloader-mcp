"""
Main entry point for opendataloader-mcp MCP server.
"""

from mcp.server.fastmcp import FastMCP
from .tools import (
    parse_pdf,
    batch_parse_pdfs,
    extract_tables,
    pdf_info,
    search_pdf_content,
    convert_pdf_format,
    get_server_config,
    clear_cache
)

# Initialize FastMCP server
mcp = FastMCP("opendataloader-pdf")

# Register all tools
mcp.tool()(parse_pdf)
mcp.tool()(batch_parse_pdfs)
mcp.tool()(extract_tables)
mcp.tool()(pdf_info)
mcp.tool()(search_pdf_content)
mcp.tool()(convert_pdf_format)
mcp.tool()(get_server_config)
mcp.tool()(clear_cache)


if __name__ == "__main__":
    mcp.run(transport="stdio")
