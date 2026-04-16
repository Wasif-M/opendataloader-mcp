"""
opendataloader-pdf MCP Server
Exposes parse_pdf and extract_tables as Claude tools via stdio transport.
"""

import json
import os
import tempfile
import urllib.request
from pathlib import Path
from typing import Optional

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("opendataloader-pdf")


# ─── helpers ────────────────────────────────────────────────────────────────

def _resolve_input(source: str) -> tuple[list[str], Optional[tempfile.TemporaryDirectory]]:
    """
    Accept a local path, a directory, or an HTTP(S) URL.
    Returns (list_of_paths, tmpdir_or_None).
    The caller must keep tmpdir alive until the convert() call completes.
    """
    if source.startswith("http://") or source.startswith("https://"):
        tmpdir = tempfile.TemporaryDirectory()
        dest = os.path.join(tmpdir.name, "input.pdf")
        urllib.request.urlretrieve(source, dest)
        return [dest], tmpdir

    p = Path(source)
    if not p.exists():
        raise FileNotFoundError(f"Path not found: {source}")
    return [str(p)], None


def _run_convert(sources: list[str], outdir: str, fmt: str, **kwargs) -> None:
    """Thin wrapper so import errors surface as a clean tool error."""
    try:
        import opendataloader_pdf  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "opendataloader-pdf is not installed. "
            "Run: pip install opendataloader-pdf"
        ) from exc
    opendataloader_pdf.convert(
        input_path=sources,
        output_dir=outdir,
        format=fmt,
        **kwargs,
    )


def _collect_outputs(outdir: str, ext: str) -> dict[str, str]:
    """Gather all files with the given extension from the output directory."""
    results: dict[str, str] = {}
    for f in Path(outdir).rglob(f"*.{ext}"):
        results[f.name] = f.read_text(encoding="utf-8", errors="replace")
    return results


# ─── tools ──────────────────────────────────────────────────────────────────

@mcp.tool()
def parse_pdf(
    source: str,
    format: str = "markdown",
    pages: Optional[str] = None,
    use_struct_tree: bool = False,
    sanitize: bool = False,
) -> str:
    """
    Parse a PDF and return its content as Markdown, JSON (with bounding boxes),
    HTML, or plain text.

    Args:
        source: Local file path, directory of PDFs, or an HTTP/HTTPS URL.
        format: Output format — one of 'markdown', 'json', 'html', 'text',
                or a comma-separated combination e.g. 'markdown,json'.
        pages: Optional page range to extract, e.g. '1-3' or '2,4,6'.
               Leave empty to process all pages.
        use_struct_tree: If True, use native PDF structure tags (better accuracy
                         for well-tagged PDFs).
        sanitize: If True, redact sensitive data (emails, URLs, phone numbers)
                  and strip hidden prompt-injection text.

    Returns:
        A JSON string mapping output filename → content for every generated file.
        For a single-format request this is usually one entry.
    """
    sources, tmpdir = _resolve_input(source)

    try:
        with tempfile.TemporaryDirectory() as outdir:
            kwargs: dict = {}
            if use_struct_tree:
                kwargs["use_struct_tree"] = True
            if pages:
                kwargs["pages"] = pages

            _run_convert(sources, outdir, format, **kwargs)

            # Collect by whichever extensions were requested
            all_files: dict[str, str] = {}
            for fmt_part in format.split(","):
                fmt_part = fmt_part.strip()
                ext_map = {
                    "markdown": "md",
                    "json": "json",
                    "html": "html",
                    "text": "txt",
                }
                ext = ext_map.get(fmt_part, fmt_part)
                all_files.update(_collect_outputs(outdir, ext))

            if not all_files:
                return json.dumps({"error": "No output files were produced."})

            return json.dumps(all_files, ensure_ascii=False, indent=2)
    finally:
        if tmpdir:
            tmpdir.cleanup()


@mcp.tool()
def extract_tables(
    source: str,
    use_hybrid: bool = False,
    pages: Optional[str] = None,
) -> str:
    """
    Extract all tables from a PDF as structured JSON.

    Each table entry includes:
      - type        : "table"
      - page_number : 1-indexed page
      - bounding_box: [left, bottom, right, top] in PDF points
      - content     : Markdown representation of the table rows

    Args:
        source: Local file path, directory of PDFs, or an HTTP/HTTPS URL.
        use_hybrid: If True, enable hybrid AI mode for complex/borderless tables
                    (~0.93 TEDS accuracy vs 0.49 in local mode).
                    Requires the hybrid server to be running:
                      opendataloader-pdf-hybrid --port 5002
        pages: Optional page range, e.g. '1-5'. Leave empty for all pages.

    Returns:
        A JSON string with a 'tables' list and 'summary' counts per file.
    """
    sources, tmpdir = _resolve_input(source)

    try:
        with tempfile.TemporaryDirectory() as outdir:
            kwargs: dict = {}
            if use_hybrid:
                kwargs["hybrid"] = "docling-fast"
            if pages:
                kwargs["pages"] = pages

            _run_convert(sources, outdir, "json", **kwargs)

            json_files = _collect_outputs(outdir, "json")
            if not json_files:
                return json.dumps({"error": "No JSON output produced."})

            output: dict = {"tables": [], "summary": {}}

            for filename, raw in json_files.items():
                try:
                    elements = json.loads(raw)
                except json.JSONDecodeError:
                    # Some versions wrap output differently
                    elements = []

                # Normalise: could be a list or {"elements": [...]}
                if isinstance(elements, dict):
                    elements = elements.get("elements", [])

                tables = [e for e in elements if e.get("type") == "table"]
                output["tables"].extend(tables)
                output["summary"][filename] = {
                    "total_elements": len(elements),
                    "tables_found": len(tables),
                }

            output["total_tables"] = len(output["tables"])
            return json.dumps(output, ensure_ascii=False, indent=2)
    finally:
        if tmpdir:
            tmpdir.cleanup()


@mcp.tool()
def pdf_info(source: str) -> str:
    """
    Return a structural overview of a PDF: page count, element types,
    heading hierarchy, and whether the file is accessibility-tagged.

    Useful as a quick 'what's in this PDF?' check before calling parse_pdf
    or extract_tables.

    Args:
        source: Local file path, directory of PDFs, or an HTTP/HTTPS URL.

    Returns:
        A JSON string with page count, element type counts, headings list,
        and accessibility status.
    """
    sources, tmpdir = _resolve_input(source)

    try:
        with tempfile.TemporaryDirectory() as outdir:
            _run_convert(sources, outdir, "json", use_struct_tree=True)

            json_files = _collect_outputs(outdir, "json")
            if not json_files:
                return json.dumps({"error": "Could not parse PDF."})

            result: dict = {}

            for filename, raw in json_files.items():
                try:
                    elements = json.loads(raw)
                except json.JSONDecodeError:
                    elements = []

                if isinstance(elements, dict):
                    is_tagged = elements.get("tagged", False)
                    elements = elements.get("elements", [])
                else:
                    is_tagged = False

                type_counts: dict[str, int] = {}
                headings: list[dict] = []
                pages: set[int] = set()

                for el in elements:
                    t = el.get("type", "unknown")
                    type_counts[t] = type_counts.get(t, 0) + 1
                    if el.get("page number"):
                        pages.add(el["page number"])
                    if t == "heading":
                        headings.append({
                            "level": el.get("heading level", "?"),
                            "text": (el.get("content", "")[:80] + "…")
                                    if len(el.get("content", "")) > 80
                                    else el.get("content", ""),
                            "page": el.get("page number"),
                        })

                result[filename] = {
                    "page_count": max(pages) if pages else "unknown",
                    "is_accessibility_tagged": is_tagged,
                    "element_type_counts": type_counts,
                    "heading_outline": headings[:30],  # cap at 30
                    "total_elements": len(elements),
                }

            return json.dumps(result, ensure_ascii=False, indent=2)
    finally:
        if tmpdir:
            tmpdir.cleanup()


# ─── entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run(transport="stdio")
