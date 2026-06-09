import os
import sys

from dotenv import load_dotenv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

load_dotenv()

from utils.logs import configure_logging

configure_logging()

os.makedirs(os.path.join(ROOT, ".test"), exist_ok=True)
