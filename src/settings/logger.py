from datetime import datetime
from logging import Logger, Formatter, StreamHandler, FileHandler, DEBUG
from logging import getLogger
import os

def custom_logger(logger_name: str) -> Logger:
    """
    Returns a configured Logger instance with console and file handlers.

    Args:
        logger_name (str): The name of the logger.

    Returns:
        Logger: Configured logger.
    """

    # Ensure logs directory exists
    os.makedirs("data/logs", exist_ok=True)

    logger = getLogger(logger_name)
    logger.setLevel(DEBUG)

    if not logger.hasHandlers():
        # Console handler
        console_handler = StreamHandler()
        console_handler.setLevel(DEBUG)
        console_formatter = Formatter(
            "%(asctime)s.%(msecs)03d | %(levelname)s | %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(console_formatter)

        # File handler (daily log file)
        log_file = f"data/logs/{datetime.now().strftime('%Y-%m-%d')}.log"
        file_handler = FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(DEBUG)
        file_formatter = Formatter(
            "%(asctime)s.%(msecs)03d | %(levelname)s | %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)

        # Add both handlers
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger
