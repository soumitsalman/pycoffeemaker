import os

from dotenv import load_dotenv


def load_coffeemaker_env(root_dir: str | None = None) -> None:
    root_dir = root_dir or os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    pipeline_env = os.path.join(root_dir, "factory", "pipeline-defaults.env")
    if os.path.isfile(pipeline_env):
        load_dotenv(pipeline_env)

    local_env = os.path.join(root_dir, ".env")
    if os.path.isfile(local_env):
        load_dotenv(local_env, override=True)
