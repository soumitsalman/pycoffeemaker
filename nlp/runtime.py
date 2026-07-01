import os
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

LLAMACPP_PREFIX = "llamacpp://"
ONNX_PREFIX = "onnx://"
OPENVINO_PREFIX = "openvino://"
VLLM_PREFIX = "vllm://"
INFINITY_PREFIX = "infinity://"
NUM_THREADS = os.cpu_count()

REMOTE_RETRY_COUNT = 3
REMOTE_RETRY_JITTER = (60, 180)

TOKEN_MARGIN = 16

_ALLOWED_SPECIAL_TOKENS = {
    "<|endoftext|>",
    "<|im_start|>",
    "<|im_end|>",
    "<|assistant|>",
    "<|system|>",
    "<|human|>",
}


def run_batch(func: Callable, items, num_threads: int = os.cpu_count()):
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        return list(executor.map(func, items))


def clear_gpu_cache():
    """Clear GPU memory by running garbage collection and clearing CUDA cache if available."""
    import gc
    import torch

    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        torch.cuda.synchronize()


def is_cuda_oom(exc: BaseException) -> bool:
    if "OutOfMemoryError" in type(exc).__name__ or "AcceleratorError" in type(exc).__name__:
        return True
    msg = str(exc).lower()
    return "out of memory" in msg or "cudaerrormemoryallocation" in msg
