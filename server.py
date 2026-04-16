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
"""

import json
import os
import tempfile
import urllib.request
import logging
import re
import time
import hashlib
from pathlib import Path
from typing import Optional, Dict, List, Any, Callable
from functools import wraps, lru_cache
from datetime import datetime

from mcp.server.fastmcp import FastMCP

# ─── Configuration ──────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("opendataloader-mcp")

# Configuration constants
MAX_RETRIES = 3
RETRY_BACKOFF = 1  # seconds
CACHE_SIZE = 10
REQUEST_TIMEOUT = 300  # seconds
SUPPORTED_FORMATS = ["markdown", "json", "html", "text"]

mcp = FastMCP("opendataloader-pdf")

# ─── Cache Storage ──────────────────────────────────────────────────────────

_pdf_cache: Dict[str, Dict[str, Any]] = {}
_cache_metadata: Dict[str, Dict[str, Any]] = {}

# ─── Decorators & Utilities ──────────────────────────────────────────────────

def retry_operation(max_retries: int = MAX_RETRIES, backoff: float = RETRY_BACKOFF):
    """Decorator to retry failed operations with exponential backoff."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    logger.info(f"Attempt {attempt + 1}/{max_retries} for {func.__name__}")
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_retries - 1:
                        wait_time = backoff * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"{func.__name__} failed: {str(e)}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"{func.__name__} failed after {max_retries} attempts: {str(e)}")
            raise last_error
        return wrapper
    return decorator

def track_metrics(func: Callable) -> Callable:
    """Decorator to track performance metrics."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_memory = 0
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            
            # Add metrics to result if JSON
            if isinstance(result, str):
                try:
                    data = json.loads(result)
                    if isinstance(data, dict):
                        data["_metrics"] = {
                            "execution_time_ms": round(elapsed * 1000, 2),
                            "timestamp": datetime.now().isoformat(),
                            "function": func.__name__
                        }
                        return json.dumps(data, ensure_ascii=False, indent=2)
                except:
                    pass
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"{func.__name__} failed in {elapsed:.2f}s: {str(e)}")
            raise
    return wrapper

def get_file_hash(filepath: str) -> str:
    """Generate hash of file for caching."""
    try:
        with open(filepath, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()[:16]
    except:
        return hashlib.md5(filepath.encode()).hexdigest()[:16]

def cache_result(func: Callable) -> Callable:
    """Decorator to cache PDF parsing results."""
    @wraps(func)
    def wrapper(source: str, format: str = "markdown", *args, **kwargs):
        # Create cache key
        cache_key = f"{source}_{format}_{str(args)}_{str(sorted(kwargs.items()))}"
        cache_hash = hashlib.md5(cache_key.encode()).hexdigest()
        
        # Check cache
        if cache_hash in _pdf_cache:
            logger.info(f"Cache hit for {source} with format {format}")
            return _pdf_cache[cache_hash]
        
        # Process and cache
        result = func(source, format, *args, **kwargs)
        
        # Store in cache (limit to CACHE_SIZE)
        if len(_pdf_cache) >= CACHE_SIZE:
            # Remove oldest entry
            oldest_key = min(_cache_metadata.keys(), key=lambda k: _cache_metadata[k]['timestamp'])
            del _pdf_cache[oldest_key]
            del _cache_metadata[oldest_key]
        
        _pdf_cache[cache_hash] = result
        _cache_metadata[cache_hash] = {'timestamp': time.time(), 'source': source}
        logger.info(f"Cached result for {source}")
        
        return result
    return wrapper

# ─── Validation Functions ───────────────────────────────────────────────────

def validate_pdf_source(source: str) -> tuple[bool, str]:
    """Validate PDF source path or URL."""
    if not source:
        return False, "Source cannot be empty"
    
    # Check if URL
    if source.startswith("http://") or source.startswith("https://"):
        if not source.lower().endswith(".pdf"):
            return False, "URL must point to a PDF file (ends with .pdf)"
        return True, ""
    
    # Check if local path
    path = Path(source)
    if not path.exists():
        return False, f"File not found: {source}"
    
    if not str(source).lower().endswith(".pdf"):
        return False, f"File must be a PDF: {source}"
    
    return True, ""

def validate_format(format_str: str) -> tuple[bool, str]:
    """Validate output format specification."""
    if not format_str:
        return False, "Format cannot be empty"
    
    formats = [f.strip() for f in format_str.split(",")]
    for fmt in formats:
        if fmt not in SUPPORTED_FORMATS:
            return False, f"Invalid format '{fmt}'. Supported: {', '.join(SUPPORTED_FORMATS)}"
    
    return True, ""

def validate_page_range(pages: Optional[str]) -> tuple[bool, str]:
    """Validate page range specification."""
    if not pages:
        return True, ""
    
    # Check format: should be like "1-5" or "1,3,5"
    if not re.match(r'^[\d\-,\s]+$', pages):
        return False, "Invalid page format. Use '1-5' or '1,3,5'"
    
    return True, ""



# ─── Core Helper Functions ──────────────────────────────────────────────────

@retry_operation(max_retries=MAX_RETRIES)
def _resolve_input(source: str) -> tuple[list[str], Optional[tempfile.TemporaryDirectory]]:
    """
    Accept a local path, a directory, or an HTTP(S) URL.
    Returns (list_of_paths, tmpdir_or_None).
    The caller must keep tmpdir alive until the convert() call completes.
    
    Raises:
        FileNotFoundError: If local path doesn't exist
        ValueError: If URL is invalid
        urllib.error.URLError: If URL cannot be accessed
    """
    try:
        if source.startswith("http://") or source.startswith("https://"):
            logger.info(f"Downloading PDF from URL: {source}")
            tmpdir = tempfile.TemporaryDirectory()
            dest = os.path.join(tmpdir.name, "input.pdf")
            try:
                urllib.request.urlretrieve(source, dest, timeout=REQUEST_TIMEOUT)
                logger.info(f"Successfully downloaded PDF to {dest}")
            except urllib.error.URLError as e:
                raise RuntimeError(f"Failed to download PDF from URL: {str(e)}")
            except Exception as e:
                raise RuntimeError(f"Download error: {str(e)}")
            return [dest], tmpdir

        p = Path(source)
        if not p.exists():
            raise FileNotFoundError(f"Path not found: {source}. Please check the file path.")
        
        if p.is_dir():
            pdfs = list(p.glob("**/*.pdf"))
            if not pdfs:
                raise FileNotFoundError(f"No PDF files found in directory: {source}")
            logger.info(f"Found {len(pdfs)} PDF files in {source}")
            return [str(pdf) for pdf in pdfs], None
        
        if not str(p).lower().endswith(".pdf"):
            raise ValueError(f"File is not a PDF: {source}")
        
        logger.info(f"Using local PDF: {source}")
        return [str(p)], None
    
    except Exception as e:
        logger.error(f"Error resolving input {source}: {str(e)}")
        raise

def _run_convert(sources: list[str], outdir: str, fmt: str, **kwargs) -> None:
    """Wrapper for PDF conversion with enhanced error handling."""
    try:
        import opendataloader_pdf  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "opendataloader-pdf is not installed. "
            "Install with: pip install opendataloader-pdf"
        ) from exc
    
    try:
        logger.info(f"Converting {len(sources)} PDF(s) to {fmt}")
        opendataloader_pdf.convert(
            input_path=sources,
            output_dir=outdir,
            format=fmt,
            **kwargs,
        )
        logger.info(f"Conversion completed successfully")
    except Exception as e:
        logger.error(f"Conversion failed: {str(e)}")
        raise RuntimeError(f"PDF conversion error: {str(e)}")

def _collect_outputs(outdir: str, ext: str) -> dict[str, str]:
    """Gather all files with the given extension from the output directory."""
    results: dict[str, str] = {}
    try:
        for f in Path(outdir).rglob(f"*.{ext}"):
            results[f.name] = f.read_text(encoding="utf-8", errors="replace")
        logger.info(f"Collected {len(results)} output files with extension .{ext}")
    except Exception as e:
        logger.error(f"Error collecting outputs: {str(e)}")
        raise
    return results



# ─── Core MCP Tools ─────────────────────────────────────────────────────────

@mcp.tool()
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
        format: Output format - 'markdown', 'json', 'html', 'text', or comma-separated combination
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


@mcp.tool()
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


@mcp.tool()
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
    # Validate inputs
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


@mcp.tool()
@track_metrics
def pdf_info(source: str) -> str:
    """
    Get structural overview: page count, element types, headings, accessibility status.

    Args:
        source: Local file path, directory, or HTTP/HTTPS URL

    Returns:
        JSON with PDF metadata and structure info
    """
    # Validate inputs
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


@mcp.tool()
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
                    # Use regex search
                    pattern = re.compile(query, re.IGNORECASE)
                    matches = list(pattern.finditer(content))
                    
                    file_results = []
                    for match in matches:
                        # Get line context
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


@mcp.tool()
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
        # Parse to intermediate format
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


@mcp.tool()
def get_server_config() -> str:
    """Get current server configuration and capabilities."""
    config = {
        "status": "operational",
        "version": "2.0.0",
        "supported_formats": SUPPORTED_FORMATS,
        "max_retries": MAX_RETRIES,
        "cache_size": CACHE_SIZE,
        "request_timeout": REQUEST_TIMEOUT,
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
        "cache_status": {
            "cached_items": len(_pdf_cache),
            "cache_capacity": CACHE_SIZE
        }
    }
    logger.info("Server config requested")
    return json.dumps(config, indent=2)


@mcp.tool()
def clear_cache() -> str:
    """Clear the PDF cache to free memory."""
    global _pdf_cache, _cache_metadata
    cache_size = len(_pdf_cache)
    _pdf_cache.clear()
    _cache_metadata.clear()
    logger.info(f"Cache cleared - freed {cache_size} entries")
    return json.dumps({
        "status": "success",
        "message": f"Cleared {cache_size} cached items",
        "cache_size_now": len(_pdf_cache)
    })



# ─── entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run(transport="stdio")
