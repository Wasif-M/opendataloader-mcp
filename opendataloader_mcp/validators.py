import re
from pathlib import Path
from typing import Tuple

from .config import logger, SUPPORTED_FORMATS



def validate_pdf_source(source: str) -> Tuple[bool, str]:
    """
    Validate PDF source path or URL.
    
    Args:
        source: PDF file path or URL
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not source:
        return False, "Source cannot be empty"
    
    # Check if URL
    if source.startswith("http://") or source.startswith("https://"):
        if not source.lower().endswith(".pdf"):
            return False, "URL must point to a PDF file (ends with .pdf)"
        logger.info(f"Valid PDF URL: {source}")
        return True, ""
    
    # Check if local path
    path = Path(source)
    if not path.exists():
        return False, f"File not found: {source}"
    
    if not str(source).lower().endswith(".pdf"):
        return False, f"File must be a PDF: {source}"
    
    logger.info(f"Valid local PDF: {source}")
    return True, ""


def validate_format(format_str: str) -> Tuple[bool, str]:
    """
    Validate output format specification.
    
    Args:
        format_str: Format specification (comma-separated list)
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not format_str:
        return False, "Format cannot be empty"
    
    formats = [f.strip() for f in format_str.split(",")]
    for fmt in formats:
        if fmt not in SUPPORTED_FORMATS:
            return False, f"Invalid format '{fmt}'. Supported: {', '.join(SUPPORTED_FORMATS)}"
    
    logger.debug(f"Valid format(s): {', '.join(formats)}")
    return True, ""


def validate_page_range(pages: str | None) -> Tuple[bool, str]:
    """
    Validate page range specification.
    
    Args:
        pages: Page range string (e.g., "1-5" or "1,3,5")
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not pages:
        return True, ""
    
    # Check format: should be like "1-5" or "1,3,5"
    if not re.match(r'^[\d\-,\s]+$', pages):
        return False, "Invalid page format. Use '1-5' or '1,3,5'"
    
    logger.debug(f"Valid page range: {pages}")
    return True, ""


def validate_input_list(sources: list) -> Tuple[bool, str]:
    """
    Validate batch input list.
    
    Args:
        sources: List of PDF sources
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not sources:
        return False, "Sources list cannot be empty"
    
    if not isinstance(sources, list):
        return False, "Sources must be a list"
    
    if len(sources) == 0:
        return False, "At least one PDF source is required"
    
    logger.info(f"Valid batch input: {len(sources)} sources")
    return True, ""
