"""
MCP Tools for opendataloader-mcp PDF processing.
"""

import json
import tempfile
import re
from datetime import datetime
from typing import Optional

from .config import logger, SERVER_VERSION
from .validators import (
    validate_pdf_source,
    validate_format,
    validate_page_range,
    validate_input_list
)
from .decorators import (
    track_metrics,
    cache_result,
    get_cache_stats,
    clear_cache_storage
)
from .helpers import _resolve_input, _run_convert, _collect_outputs

# ─── Parse PDF Tool ─────────────────────────────────────────────────────────

@track_metrics
@cache_result
def parse_pdf(
    source: str,
    format: str = "markdown",
    pages: Optional[str] = None,
    use_struct_tree: bool = False,
    sanitize: bool = False,
) -> str:
    """
    Parse a PDF and return its content as Markdown, JSON, HTML, or plain text.

    Args:
        source: Local file path, directory of PDFs, or HTTP/HTTPS URL
        format: Output format - 'markdown', 'json', 'html', 'text', or comma-separated
        pages: Optional page range (e.g., '1-3' or '2,4,6')
        use_struct_tree: Use native PDF structure tags for better accuracy
        sanitize: Redact sensitive data (emails, URLs, phone numbers)

    Returns:
        JSON string with parsed content and metrics
    """
    # Validate inputs
    is_valid, error_msg = validate_pdf_source(source)
    if not is_valid:
        logger.error(f"Invalid PDF source: {error_msg}")
        return json.dumps({"error": error_msg, "status": "validation_failed"})
    
    is_valid, error_msg = validate_format(format)
    if not is_valid:
        logger.error(f"Invalid format: {error_msg}")
        return json.dumps({"error": error_msg, "status": "validation_failed"})
    
    is_valid, error_msg = validate_page_range(pages)
    if not is_valid:
        logger.error(f"Invalid page range: {error_msg}")
        return json.dumps({"error": error_msg, "status": "validation_failed"})

    sources = None
    tmpdir = None
    
    try:
        logger.info(f"Parsing PDF from {source} with format {format}")
        sources, tmpdir = _resolve_input(source)

        with tempfile.TemporaryDirectory() as outdir:
            kwargs: dict = {}
            if use_struct_tree:
                kwargs["use_struct_tree"] = True
            if pages:
                kwargs["pages"] = pages
            if sanitize:
                kwargs["sanitize"] = True

            _run_convert(sources, outdir, format, **kwargs)

            # Collect outputs
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
                logger.warning("No output files produced")
                return json.dumps({"error": "No output files were produced.", "status": "no_output"})

            logger.info(f"Successfully parsed PDF, generated {len(all_files)} file(s)")
            return json.dumps({
                "status": "success",
                "files": all_files,
                "file_count": len(all_files)
            }, ensure_ascii=False, indent=2)
    
    except Exception as e:
        logger.error(f"PDF parsing failed: {str(e)}")
        return json.dumps({
            "error": str(e),
            "status": "failed",
            "error_type": type(e).__name__
        })
    finally:
        if tmpdir:
            tmpdir.cleanup()


# ─── Batch Process Tool ─────────────────────────────────────────────────────

@track_metrics
def batch_parse_pdfs(
    sources: list[str],
    format: str = "markdown",
    pages: Optional[str] = None,
) -> str:
    """
    Process multiple PDFs efficiently in batch.

    Args:
        sources: List of file paths or URLs
        format: Output format for all PDFs
        pages: Optional page range

    Returns:
        JSON with results for each PDF and batch metrics
    """
    is_valid, error_msg = validate_input_list(sources)
    if not is_valid:
        logger.error(f"Invalid batch input: {error_msg}")
        return json.dumps({"error": error_msg, "status": "validation_failed"})
    
    logger.info(f"Starting batch processing of {len(sources)} PDFs")
    
    results = {
        "batch_summary": {
            "total_pdfs": len(sources),
            "successful": 0,
            "failed": 0,
            "timestamp": datetime.now().isoformat()
        },
        "results": {}
    }
    
    for idx, source in enumerate(sources, 1):
        try:
            logger.info(f"Processing PDF {idx}/{len(sources)}: {source}")
            result = parse_pdf(source, format, pages)
            result_data = json.loads(result)
            
            if result_data.get("status") == "success":
                results["results"][source] = result_data
                results["batch_summary"]["successful"] += 1
            else:
                results["results"][source] = {"error": "Processing failed", "status": result_data.get("status")}
                results["batch_summary"]["failed"] += 1
        except Exception as e:
            logger.error(f"Error processing {source}: {str(e)}")
            results["results"][source] = {"error": str(e), "status": "exception"}
            results["batch_summary"]["failed"] += 1
    
    logger.info(f"Batch processing complete: {results['batch_summary']['successful']} successful, {results['batch_summary']['failed']} failed")
    return json.dumps(results, ensure_ascii=False, indent=2)


# ─── Extract Tables Tool ────────────────────────────────────────────────────

@track_metrics
def extract_tables(
    source: str,
    use_hybrid: bool = False,
    pages: Optional[str] = None,
) -> str:
    """
    Extract all tables from a PDF as structured JSON with bounding boxes.

    Args:
        source: Local file path, directory, or HTTP/HTTPS URL
        use_hybrid: Enable AI mode for complex tables (~93% accuracy)
        pages: Optional page range

    Returns:
        JSON with tables list and summary counts
    """
    is_valid, error_msg = validate_pdf_source(source)
    if not is_valid:
        logger.error(f"Invalid PDF source: {error_msg}")
        return json.dumps({"error": error_msg, "status": "validation_failed"})

    sources = None
    tmpdir = None

    try:
        logger.info(f"Extracting tables from {source}")
        sources, tmpdir = _resolve_input(source)

        with tempfile.TemporaryDirectory() as outdir:
            kwargs: dict = {}
            if use_hybrid:
                kwargs["hybrid"] = "docling-fast"
                logger.info("Using hybrid mode for table extraction")
            if pages:
                kwargs["pages"] = pages

            _run_convert(sources, outdir, "json", **kwargs)

            json_files = _collect_outputs(outdir, "json")
            if not json_files:
                logger.warning("No JSON output produced for table extraction")
                return json.dumps({"error": "No JSON output produced.", "status": "no_output"})

            output: dict = {"tables": [], "summary": {}, "status": "success"}

            for filename, raw in json_files.items():
                try:
                    elements = json.loads(raw)
                except json.JSONDecodeError:
                    elements = []

                if isinstance(elements, dict):
                    elements = elements.get("elements", [])

                tables = [e for e in elements if e.get("type") == "table"]
                output["tables"].extend(tables)
                output["summary"][filename] = {
                    "total_elements": len(elements),
                    "tables_found": len(tables),
                }

            output["total_tables"] = len(output["tables"])
            logger.info(f"Extracted {output['total_tables']} tables")
            return json.dumps(output, ensure_ascii=False, indent=2)
    
    except Exception as e:
        logger.error(f"Table extraction failed: {str(e)}")
        return json.dumps({
            "error": str(e),
            "status": "failed",
            "error_type": type(e).__name__
        })
    finally:
        if tmpdir:
            tmpdir.cleanup()


# ─── PDF Info Tool ──────────────────────────────────────────────────────────

@track_metrics
def pdf_info(source: str) -> str:
    """
    Get structural overview: page count, element types, headings, accessibility status.

    Args:
        source: Local file path, directory, or HTTP/HTTPS URL

    Returns:
        JSON with PDF metadata and structure info
    """
    is_valid, error_msg = validate_pdf_source(source)
    if not is_valid:
        logger.error(f"Invalid PDF source: {error_msg}")
        return json.dumps({"error": error_msg, "status": "validation_failed"})

    sources = None
    tmpdir = None

    try:
        logger.info(f"Getting info for PDF: {source}")
        sources, tmpdir = _resolve_input(source)

        with tempfile.TemporaryDirectory() as outdir:
            _run_convert(sources, outdir, "json", use_struct_tree=True)

            json_files = _collect_outputs(outdir, "json")
            if not json_files:
                logger.warning(f"Could not analyze PDF: {source}")
                return json.dumps({"error": "Could not parse PDF.", "status": "parse_failed"})

            result: dict = {"files": {}, "status": "success"}

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

                result["files"][filename] = {
                    "page_count": max(pages) if pages else "unknown",
                    "is_accessibility_tagged": is_tagged,
                    "element_type_counts": type_counts,
                    "heading_outline": headings[:30],
                    "total_elements": len(elements),
                }

            logger.info(f"PDF info retrieved successfully")
            return json.dumps(result, ensure_ascii=False, indent=2)
    
    except Exception as e:
        logger.error(f"PDF info extraction failed: {str(e)}")
        return json.dumps({
            "error": str(e),
            "status": "failed",
            "error_type": type(e).__name__
        })
    finally:
        if tmpdir:
            tmpdir.cleanup()


# ─── Search Tool ────────────────────────────────────────────────────────────

@track_metrics
def search_pdf_content(
    source: str,
    query: str,
    context_lines: int = 2,
) -> str:
    """
    Search for text within a PDF and return matches with context.

    Args:
        source: PDF file path or URL
        query: Search term (supports regex patterns)
        context_lines: Number of surrounding lines to include

    Returns:
        JSON with search results and metadata
    """
    is_valid, error_msg = validate_pdf_source(source)
    if not is_valid:
        logger.error(f"Invalid PDF source: {error_msg}")
        return json.dumps({"error": error_msg, "status": "validation_failed"})

    sources = None
    tmpdir = None

    try:
        logger.info(f"Searching PDF for: {query}")
        sources, tmpdir = _resolve_input(source)

        with tempfile.TemporaryDirectory() as outdir:
            _run_convert(sources, outdir, "markdown")

            md_files = _collect_outputs(outdir, "md")
            results = {"matches": [], "summary": {}, "status": "success"}

            for filename, content in md_files.items():
                try:
                    pattern = re.compile(query, re.IGNORECASE)
                    matches = list(pattern.finditer(content))
                    
                    file_results = []
                    for match in matches:
                        lines = content[:match.start()].split('\n')
                        line_num = len(lines)
                        
                        file_results.append({
                            "matched_text": match.group(),
                            "line_number": line_num,
                            "position": match.start(),
                            "context_before": '\n'.join(lines[-context_lines:] if context_lines > 0 else []),
                        })
                    
                    if file_results:
                        results["matches"].extend(file_results)
                        results["summary"][filename] = len(file_results)
                    
                except re.error as e:
                    logger.error(f"Regex error: {str(e)}")
                    return json.dumps({
                        "error": f"Invalid regex pattern: {str(e)}",
                        "status": "regex_error"
                    })

            results["total_matches"] = len(results["matches"])
            logger.info(f"Found {results['total_matches']} matches")
            return json.dumps(results, ensure_ascii=False, indent=2)
    
    except Exception as e:
        logger.error(f"PDF search failed: {str(e)}")
        return json.dumps({
            "error": str(e),
            "status": "failed"
        })
    finally:
        if tmpdir:
            tmpdir.cleanup()


# ─── Format Conversion Tool ─────────────────────────────────────────────────

@track_metrics
def convert_pdf_format(
    source: str,
    from_format: str = "markdown",
    to_format: str = "json",
) -> str:
    """
    Convert parsed PDF content between formats.

    Args:
        source: PDF file path or URL
        from_format: Source format (markdown, json, html, text)
        to_format: Target format (markdown, json, html, text)

    Returns:
        JSON with converted content
    """
    is_valid, error_msg = validate_pdf_source(source)
    if not is_valid:
        return json.dumps({"error": error_msg, "status": "validation_failed"})

    logger.info(f"Converting PDF format from {from_format} to {to_format}")

    try:
        parsed = parse_pdf(source, f"{from_format},{to_format}")
        parsed_data = json.loads(parsed)
        
        if parsed_data.get("status") != "success":
            return json.dumps({"error": "Failed to parse PDF", "status": "parse_failed"})

        logger.info(f"Successfully converted PDF format")
        return json.dumps({
            "status": "success",
            "from_format": from_format,
            "to_format": to_format,
            "files": parsed_data.get("files", {})
        }, ensure_ascii=False, indent=2)
    
    except Exception as e:
        logger.error(f"Format conversion failed: {str(e)}")
        return json.dumps({
            "error": str(e),
            "status": "failed"
        })


# ─── Server Configuration Tools ─────────────────────────────────────────────

def get_server_config() -> str:
    """Get current server configuration and capabilities."""
    config = {
        "status": "operational",
        "version": SERVER_VERSION,
        "supported_formats": ["markdown", "json", "html", "text"],
        "features": [
            "batch_processing",
            "error_handling",
            "logging",
            "caching",
            "validation",
            "format_conversion",
            "content_search",
            "performance_metrics",
            "retry_logic"
        ],
        "cache_status": get_cache_stats(),
    }
    logger.info("Server config requested")
    return json.dumps(config, indent=2)


def clear_cache() -> str:
    """Clear the PDF cache to free memory."""
    cache_size = clear_cache_storage()
    logger.info(f"Cache cleared - freed {cache_size} entries")
    return json.dumps({
        "status": "success",
        "message": f"Cleared {cache_size} cached items",
        "cache_size_now": 0
    })
