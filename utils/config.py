import os

VECTOR_LEN = int(os.getenv("VECTOR_LEN", 320))
CLUSTER_EPS = float(os.getenv("CLUSTER_EPS", 0.3))
CLEANUP_WINDOW = os.getenv("CLEANUP_WINDOW", "6 months")
