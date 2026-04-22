import logging


def setup_logger(name: str, log_file: str) -> logging.Logger:
    """Create and return a configured logger.

    The logger writes to both a file and standard output. Existing handlers
    are reused to avoid duplicate log entries when the function is called
    multiple times.

    Args:
        name: Logger name.
        log_file: Path to the log file.

    Returns:
        A configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

        file_handler = logging.FileHandler(log_file)
        stream_handler = logging.StreamHandler()

        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger
