import os
from dotenv import load_dotenv
from pybeansack import utils

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR+"/.env")

WORKING_DIR = os.getenv("WORKING_DIR", CURR_DIR)
LOG_FILE = os.getenv('LOG_FILE')
if LOG_FILE:
    utils.set_logger_path(WORKING_DIR+"/"+LOG_FILE)
