import os

from dotenv import load_dotenv


def load_coffeemaker_env(root_dir: str | None = None) -> None:
    root_dir = root_dir or os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    local_env = os.path.join(root_dir, ".env")
    if os.path.isfile(local_env):
        load_dotenv(local_env, override=True)
