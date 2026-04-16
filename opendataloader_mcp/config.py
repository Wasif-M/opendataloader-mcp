"""
Configuration and constants for opendataloader-mcp server.
"""

import logging

# ─── Logging Configuration ──────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("opendataloader-mcp")

# ─── Server Configuration ───────────────────────────────────────────────────

# Retry logic
MAX_RETRIES = 3
RETRY_BACKOFF = 1  # seconds

# Caching
CACHE_SIZE = 10

# Request handling
REQUEST_TIMEOUT = 300  # seconds

# Supported formats
SUPPORTED_FORMATS = ["markdown", "json", "html", "text"]

# Server info
SERVER_VERSION = "2.0.0"
SERVER_NAME = "opendataloader-pdf"

# Feature flags
ENABLE_LOGGING = True
ENABLE_CACHING = True
ENABLE_METRICS = True
ENABLE_RETRY = True
