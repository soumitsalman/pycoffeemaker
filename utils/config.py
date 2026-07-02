import os

VECTOR_LEN = int(os.getenv("VECTOR_LEN", 320))
CLUSTER_EPS = float(os.getenv("CLUSTER_EPS", 0.3))
BEANSACK_CLEANUP_WINDOW = os.getenv("BEANSACK_CLEANUP_WINDOW", "6 months")
