from icecream import ic

## LOGGING RELATED
import logging

_LOGGER_PATH = "app.log"

def set_logger_path(logger_path: str):
    global _LOGGER_PATH
    _LOGGER_PATH = logger_path

def create_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(_LOGGER_PATH)
    file_handler.setFormatter(logging.Formatter(f'[%(asctime)s] {name} - %(levelname)s: %(message)s', datefmt="%Y-%m-%d %H:%M"))
    logger.addHandler(file_handler)
    return logger
