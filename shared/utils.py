## SHARED
import logging

def create_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler("app.log")
    file_handler.setFormatter(logging.Formatter(f'[%(asctime)s] {name} - %(levelname)s: %(message)s', datefmt="%Y-%m-%d %H:%M"))
    logger.addHandler(file_handler)
    return logger