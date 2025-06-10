import logging
from rich.logging import RichHandler
from rich.highlighter import NullHighlighter

# Configure logger with Rich integration
def create_logger():
    logger = logging.getLogger('sparse-ann')
    logger.setLevel(logging.DEBUG)
    
    # Remove existing handlers if any
    if logger.handlers:
        logger.handlers.clear()
    
    # Create Rich console handler
    rich_handler = RichHandler(rich_tracebacks=True)
    rich_handler.setLevel(logging.DEBUG)
    
    # Set formatter for non-Rich handlers if needed
    formatter = logging.Formatter('%(message)s')
    rich_handler.setFormatter(formatter)
    
    # Add the handler to the logger
    logger.addHandler(rich_handler)
    return logger

logger = create_logger()