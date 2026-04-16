# opendataloader-pdf MCP Server

An MCP server that gives Claude the ability to read and analyse any PDF —
extract Markdown, structured JSON, tables with bounding boxes, and
accessibility metadata — all 100 % locally, with no cloud calls.

---

## Tools exposed to Claude

| Tool | What it does |
|---|---|
| `parse_pdf` | Convert a PDF to Markdown / JSON / HTML / text |
| `extract_tables` | Pull every table out as structured JSON with bounding boxes |
| `pdf_info` | Quick overview: page count, element types, heading outline, tagged? |

---

## Requirements

- Python 3.10+
- Java 11+ (required by opendataloader-pdf — install from [Adoptium](https://adoptium.net/))

Verify Java is available:
```bash
java -version
```

---

## Installation

### Option A — with `uv` (recommended)

```bash
# 1. Clone / copy this folder
cd opendataloader-mcp

# 2. Create the environment and install
uv venv
uv pip install -e .
```

### Option B — with plain pip

```bash
pip install mcp opendataloader-pdf
```

---

## Add to Claude Desktop

Open `claude_desktop_config.json`:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

Add the following block (replace the path with your actual location):

```json
{
  "mcpServers": {
    "opendataloader-pdf": {
      "command": "python",
      "args": ["/ABSOLUTE/PATH/TO/opendataloader-mcp/server.py"]
    }
  }
}
```

Or if you used `uv`:

```json
{
  "mcpServers": {
    "opendataloader-pdf": {
      "command": "uv",
      "args": [
        "run",
        "--project", "/ABSOLUTE/PATH/TO/opendataloader-mcp",
        "python", "server.py"
      ]
    }
  }
}
```

Restart Claude Desktop. You will see the tools listed in the tool panel.

---

## Usage Examples

Once installed, simply ask Claude to process your PDFs:

### Parse PDF to Markdown
```
Parse this PDF to markdown: C:\path\to\file.pdf
```

### Extract Tables
```
Extract tables from this PDF: C:\path\to\file.pdf
```

### Get PDF Info
```
Give me a quick overview of this PDF: C:\path\to\file.pdf
```

### Remote URLs
```
Parse this PDF from the web: https://example.com/document.pdf
```

---

## Add to Claude Code (CLI)

```bash
claude mcp add opendataloader-pdf python /ABSOLUTE/PATH/TO/opendataloader-mcp/server.py
```

---

## Project structure

```
opendataloader-mcp/
├── server.py              # Entry point wrapper
├── pyproject.toml         # Dependencies
├── README.md              # This file
└── opendataloader_mcp/    # Modular package
    ├── __init__.py        # Package initialization
    ├── config.py          # Configuration & logger
    ├── decorators.py      # Retry, metrics, cache decorators
    ├── validators.py      # Input validation functions
    ├── helpers.py         # Core PDF processing helpers
    ├── tools.py           # All 8 MCP tool implementations
    └── server.py          # FastMCP initialization & entry point
```

---

