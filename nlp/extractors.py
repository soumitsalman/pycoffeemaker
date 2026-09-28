from functools import lru_cache
from typing import Type
from pydantic import BaseModel
from .models import Classifications, Entities
from .runtime import TOKEN_MARGIN, clear_gpu_cache
from itertools import chain
from collections import defaultdict
from .formatters import get_extraction_labels, get_classification_labels

try: 
    import torch
    from gliner2 import AutoExtractor
except: print("[WARNING] PyTorch Not Available. `EntityExtractor` will not work. Run `pip install torch`.")


class EntityExtractor:
    model_path: str
    confidence = 0.5
    _splitter = None

    def __init__(self, model_path: str, context_len: int, threshold=0.5, batch_size: int = 16) -> None:
        self.model_name = model_path
        self.context_len = context_len
        self.threshold = threshold
        self.batch_size = batch_size
        self._llm = None
        self._splitter = None
    
    def __enter__(self):
        if not self._llm:            
            from llama_index.core.text_splitter import TokenTextSplitter

            cuda_config = {"map_location": "cpu"}
            if torch.cuda.is_available():
                cuda_config = {
                    "map_location": "cuda",
                    "compile": True,
                    "quantize": True
                }

            self._llm = AutoExtractor.from_pretrained(self.model_name, **cuda_config)
            self._splitter = TokenTextSplitter(
                chunk_size=self.context_len - TOKEN_MARGIN,
                chunk_overlap=TOKEN_MARGIN<<1,
                include_metadata=False,
                include_prev_next_rel=False,
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._llm:
            del self._llm
            self._llm = None    
            del self._splitter
            self._splitter = None
            self._joint_schema.cache_clear()
        clear_gpu_cache()
        return False    

    @lru_cache(maxsize=8)
    def _joint_schema(self, entity_type: Type[BaseModel], class_type: Type[BaseModel]):
        schema = self._llm.create_schema().entities(get_extraction_labels(entity_type))
        for name, labels in get_classification_labels(class_type).items():
            schema.classification(name, labels)
        return schema

    @staticmethod
    def _label(value):
        if isinstance(value, dict):
            return value.get("label", value)
        return value

    @staticmethod
    def _texts(values):
        if values and isinstance(values, list) and isinstance(values[0], dict):
            return [v.get("text", v) for v in values]
        return values

    def _split(self, text: str):
        chunks = self._splitter.split_text(text)
        if len(chunks) > 1 and len(chunks[-1]) < (TOKEN_MARGIN<<2): chunks = chunks[:-1]
        return chunks

    def _create_chunks(self, texts: list[str]) -> tuple[list[str], list[int], list[int]]:
        texts = texts if isinstance(texts, list) else [texts]
        
        chunks = list(map(self._split, texts))
        counts = list[int](map(len, chunks))

        start_idx = [0]*len(chunks)
        for i in range(1,len(counts)):
            start_idx[i] = start_idx[i-1]+counts[i-1]
        return list(chain(*chunks)), start_idx, counts

    def _merge_chunks(self, entities: list[dict]):
        res = defaultdict(list)
        for e in entities:
            for field, values in e['entities'].items():
                res[field].extend(v['text'] for v in values)
        for k, v in res.items():
            res[k] = list({item.lower(): item for item in v}.values())
        return res

    def run_batch(
        self,
        input_messages: list[str],
        entity_type: Type[BaseModel] = Entities,
        class_type: Type[BaseModel] = Classifications,
    ) -> list[tuple[Entities, Classifications]]:
        results = self._llm.batch_extract(
            [msg[:self.context_len<<1] for msg in input_messages],
            self._joint_schema(entity_type, class_type),
            threshold=self.threshold,
            batch_size=self.batch_size,
            include_confidence=False,
            include_spans=False,
            overlap_policy="nested", # nested keeps a shorter span inside a longer one so it can belong to both fields
        )
        class_names = get_classification_labels(class_type)
        parsed = []
        for result in results:
            raw_entities = result.get("entities") or {}
            entities = entity_type(**{
                field: self._texts(values)
                for field, values in raw_entities.items()
                if field in entity_type.model_fields
            })
            classification = class_type(**{
                name: self._label(result[name]) for name in class_names
            })
            parsed.append((entities, classification))
        return parsed

