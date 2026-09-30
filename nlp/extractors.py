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

            cuda_config = dict(map_location="cpu")
            if torch.cuda.is_available():
                cuda_config = dict(
                    map_location="cuda",
                    compile=False,
                    quantize=True,
                    use_flashdeberta=True,
                )

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
        counts = list(map(len, chunks))

        start_idx = [0]*len(chunks)
        for i in range(1,len(counts)):
            start_idx[i] = start_idx[i-1]+counts[i-1]
        return list(chain(*chunks)), start_idx, counts

    def _merge_entities(self, chunk_results: list[dict]) -> dict:
        res = defaultdict(list)
        for result in chunk_results:
            for field, values in (result.get("entities") or {}).items():
                texts = self._texts(values)
                if texts:
                    res[field].extend(texts)
        for field, values in res.items():
            res[field] = list({item.lower(): item for item in values}.values())
        return res

    def _merge_chunks(self, results, start_idx, counts, entity_type, class_type):
        class_names = get_classification_labels(class_type)
        parsed = []
        for start, count in zip(start_idx, counts):
            group = results[start:start + count]
            if not group:
                parsed.append((entity_type(), class_type.model_construct()))
                continue
            raw_entities = self._merge_entities(group)
            entities = entity_type(**{
                field: values
                for field, values in raw_entities.items()
                if field in entity_type.model_fields
            })
            first = group[0]
            classification = class_type(**{
                name: self._label(self._first_of(first[name]))
                for name in class_names
            })
            parsed.append((entities, classification))
        return parsed

    def _parse_result(self, result: dict, entity_type: Type[BaseModel], class_type: Type[BaseModel]):
        raw_entities = {}
        for field, values in (result.get("entities") or {}).items():
            if field not in entity_type.model_fields:
                continue
            texts = self._texts(values)
            if texts:
                raw_entities[field] = list({item.lower(): item for item in texts}.values())
        entities = entity_type(**raw_entities)
        classification = class_type(**{
            name: self._label(self._first_of(result[name]))
            for name in get_classification_labels(class_type)
        })
        return entities, classification

    @staticmethod
    def _first_of(value):
        if isinstance(value, list):
            return value[0] if value else None
        return value

    def run_batch(
        self,
        input_messages: list[str],
        entity_type: Type[BaseModel] = Entities,
        class_type: Type[BaseModel] = Classifications,
    ) -> list[tuple[Entities, Classifications]]:
        if not input_messages:
            return []
        results = self._llm.batch_extract(
            input_messages,
            self._joint_schema(entity_type, class_type),
            threshold=self.threshold,
            batch_size=self.batch_size,
            include_confidence=False,
            include_spans=False,
            overlap_policy="nested", # nested keeps a shorter span inside a longer one so it can belong to both fields
            max_len=self.context_len,
        )
        return [
            self._parse_result(result, entity_type, class_type)
            for result in results
        ]

