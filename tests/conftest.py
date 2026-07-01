import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from utils.env import load_coffeemaker_env

load_coffeemaker_env(ROOT)

from utils.logs import configure_logging

configure_logging()

os.makedirs(os.path.join(ROOT, ".test"), exist_ok=True)
