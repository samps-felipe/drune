import logging
import sys

def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Configures and returns a logger instance.
    Avoids adding duplicate handlers if called multiple times.
    """
    logger = logging.getLogger(name)
    
    # Check if handlers are already configured to avoid duplication
    if not logger.handlers:
        logger.setLevel(level)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    # Prevent log messages from propagating to the root logger
    logger.propagate = False
    
    return logger
