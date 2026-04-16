import os
import tempfile
import urllib.request
import hashlib
from pathlib import Path
from typing import Tuple, Optional

from .config import logger, REQUEST_TIMEOUT
from .decorators import retry_operation



@retry_operation()
def _resolve_input(source: str) -> Tuple[list[str], Optional[tempfile.TemporaryDirectory]]:
    """
    Accept a local path, a directory, or an HTTP(S) URL.
    
    Returns:
        Tuple of (list_of_paths, tmpdir_or_None)
        
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
    """
    Wrapper for PDF conversion with enhanced error handling.
    
    Args:
        sources: List of PDF file paths
        outdir: Output directory for converted files
        fmt: Output format
        **kwargs: Additional parameters for conversion
        
    Raises:
        ImportError: If opendataloader-pdf is not installed
        RuntimeError: If conversion fails
    """
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
    """
    Gather all files with the given extension from the output directory.
    
    Args:
        outdir: Output directory path
        ext: File extension to search for
        
    Returns:
        Dictionary of {filename: content}
        
    Raises:
        Exception: If file reading fails
    """
    results: dict[str, str] = {}
    try:
        for f in Path(outdir).rglob(f"*.{ext}"):
            results[f.name] = f.read_text(encoding="utf-8", errors="replace")
        logger.info(f"Collected {len(results)} output files with extension .{ext}")
    except Exception as e:
        logger.error(f"Error collecting outputs: {str(e)}")
        raise
    return results


def get_file_hash(filepath: str) -> str:
    """
    Generate hash of file for caching purposes.
    
    Args:
        filepath: Path to file
        
    Returns:
        MD5 hash of file (first 16 chars)
    """
    try:
        with open(filepath, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()[:16]
    except:
        return hashlib.md5(filepath.encode()).hexdigest()[:16]
