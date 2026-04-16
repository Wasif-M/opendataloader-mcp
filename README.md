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

# For complex/borderless table support (hybrid mode):
uv pip install -e ".[hybrid]"
```

### Option B — with plain pip

```bash
pip install mcp opendataloader-pdf

# For hybrid mode:
pip install "opendataloader-pdf[hybrid]"
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

## Add to Claude Code (CLI)

```bash
claude mcp add opendataloader-pdf python /ABSOLUTE/PATH/TO/opendataloader-mcp/server.py
```

---

## Usage examples (inside Claude)

```
Parse this PDF and summarise it:
  parse_pdf("/path/to/report.pdf")

Get structured JSON with bounding boxes:
  parse_pdf("/path/to/invoice.pdf", format="json")

Extract all tables from a research paper URL:
  extract_tables("https://example.com/paper.pdf")

Use hybrid AI mode for complex tables:
  extract_tables("/path/to/financial_report.pdf", use_hybrid=True)

Get a quick structural overview:
  pdf_info("/path/to/document.pdf")
```

---

## Hybrid mode (complex tables & scanned PDFs)

For borderless tables, scanned documents, formulas, or non-English text,
start the hybrid backend in a separate terminal **before** using Claude:

```bash
# Standard
opendataloader-pdf-hybrid --port 5002

# Scanned / image PDFs
opendataloader-pdf-hybrid --port 5002 --force-ocr

# Non-English scanned (e.g. Korean + English)
opendataloader-pdf-hybrid --port 5002 --force-ocr --ocr-lang "ko,en"
```

Then ask Claude to call `extract_tables(..., use_hybrid=True)`.

---

## Project structure

```
opendataloader-mcp/
├── server.py          # MCP server — all tools defined here
├── pyproject.toml     # Dependencies
└── README.md          # This file
```

---

## License

Apache 2.0 — same as opendataloader-pdf.
