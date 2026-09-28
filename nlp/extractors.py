from typing import Type
from pydantic import BaseModel
from .models import Classification, Entities
from .runtime import TOKEN_MARGIN, clear_gpu_cache
from itertools import chain
from collections import defaultdict
from .formatters import get_extraction_labels, get_classification_labels

try: import torch
except: print("[WARNING] PyTorch Not Available. `EntityExtractor` will not work. Run `pip install torch`.")

# class EntityExtractor:
#     model_path: str
#     confidence = 0.5
#     _splitter = None

#     _LABELS = [
#         "person",
#         "people",
#         "organization",
#         "company",
#         "institution",
#         "business",
#         "city",
#         "state",
#         "country",
#         "location",
#         "stock",
#         "ticker",
#         "stockticker",
#         "product",
#     ]
#     _LABEL_FIELD_MAPPINGS = {
#         "person": "people",
#         "people": "people",
#         "organization": "companies",
#         "company": "companies",
#         "institution": "companies",
#         "business": "companies",
#         "city": "regions",
#         "state": "regions",
#         "country": "regions",
#         "location": "regions",
#         "stock": "stock_tickers",
#         "ticker": "stock_tickers",
#         "stockticker": "stock_tickers",
#         "product": "products",
#     }   
        
#     def __init__(self, model_path: str, context_len: int, threshold=0.5, batch_size: int = 16) -> None:
#         self.model_name = model_path
#         self.context_len = context_len
#         self.threshold = threshold
#         self.batch_size = batch_size
#         self._llm = None
#         self._label_embeddings = None        
#         self._splitter = None
    
#     def __enter__(self):
#         if not self._llm:
#             import torch
#             from gliner import GLiNER
#             from llama_index.core.text_splitter import TokenTextSplitter

#             # config.fx_graph_cache = True
#             self._llm = GLiNER.from_pretrained(
#                 self.model_name,
#                 max_length=self.context_len,
#                 map_location="cuda" if torch.cuda.is_available() else "cpu",
#             )
#             self._label_embeddings = self._llm.encode_labels(
#                 self._LABELS, batch_size=len(self._LABELS)
#             )
#             self._splitter = TokenTextSplitter(
#                 chunk_size=self.context_len - TOKEN_MARGIN,
#                 chunk_overlap=TOKEN_MARGIN<<1,
#                 include_metadata=False,
#                 include_prev_next_rel=False,
#             )
#         return self

#     def __exit__(self, exc_type, exc_val, exc_tb):
#         if self._llm:
#             del self._llm
#             self._llm = None
#             del self._label_embeddings
#             self._label_embeddings = None        
#             del self._splitter
#             self._splitter = None        
#         clear_gpu_cache()
#         return False    

#     def parse_output(self, response):
#         res = defaultdict(list)
#         for ent in response:
#             res[self._LABEL_FIELD_MAPPINGS[ent["label"]]].append(ent["text"])
#         for k, v in res.items():
#             res[k] = list({item.lower(): item for item in v}.values())
#         return Entities(**res)

#     def _split(self, text: str):
#         chunks = self._splitter.split_text(text)
#         if len(chunks) > 1 and len(chunks[-1]) < (TOKEN_MARGIN<<2): chunks = chunks[:-1]
#         return chunks

#     def _create_chunks(self, texts: list[str]) -> tuple[list[str], list[int], list[int]]:
#         texts = texts if isinstance(texts, list) else [texts]
        
#         chunks = list(map(self._split, texts))
#         counts = list[int](map(len, chunks))

#         start_idx = [0]*len(chunks)
#         for i in range(1,len(counts)):
#             start_idx[i] = start_idx[i-1]+counts[i-1]
#         return list(chain(*chunks)), start_idx, counts

#     def _merge_chunks(self, entities: list[Entities]):
#         entities = [e for e in entities if e]
#         if entities:
#             return Entities(
#                 regions=merge_lists(*[e.regions for e in entities if e.regions]),
#                 people=merge_lists(*[e.people for e in entities if e.people]),
#                 products=merge_lists(*[e.products for e in entities if e.products]),
#                 companies=merge_lists(*[e.companies for e in entities if e.companies]),
#                 stock_tickers=merge_lists(*[e.stock_tickers for e in entities if e.stock_tickers]),
#             )

#     def run_batch(self, input_messages: list[str]):
#         chunks, start_idx, counts = self._create_chunks(input_messages)
#         entities = self._llm.batch_predict_with_embeds(
#             chunks,
#             labels_embeddings=self._label_embeddings,
#             labels=self._LABELS,
#             threshold=self.threshold,
#             batch_size=self.batch_size,
#         )
#         entities = [self.parse_output(group) if group else None for group in entities]        
#         return [self._merge_chunks(entities[start:start+count]) for start, count in zip(start_idx, counts)]


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
            from gliner2 import AutoExtractor
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
        clear_gpu_cache()
        return False    

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

    def run_batch_extract(self, input_messages: list[str], output_type: Type[BaseModel] = Entities):
        # chunks, start_idx, counts = self._create_chunks(input_messages)
        entities = self._llm.batch_extract_entities(
            # chunks,
            [msg[:self.context_len<<1] for msg in input_messages],
            get_extraction_labels(output_type),
            threshold=self.threshold,
            batch_size=self.batch_size,
            include_confidence=True,
            include_spans=False,            
            overlap_policy="nested", # nested keeps a shorter span inside a longer one so it can belong to both fields
        )
        return [
            output_type(**{
                field: [v['text'] for v in values] 
                for field, values in e['entities'].items()
            }) 
            for e in entities
        ]

    def run_batch_classify(self, input_messages: list[str], output_type: Type[BaseModel] = Classification):
        from icecream import ic
        classifications = self._llm.batch_classify_text(
            [msg[:self.context_len<<1] for msg in input_messages],
            get_classification_labels(output_type),
        )
        return [output_type(**c) for c in classifications]


# def _span_text(value) -> tuple[str, int | None, int | None]:
#     if isinstance(value, str):
#         return value.strip(), None, None
#     text = str(value.get("text") or "").strip()
#     start, end = value.get("start"), value.get("end")
#     return text, start, end


# def _contains(outer: tuple[str, int | None, int | None], inner: tuple[str, int | None, int | None]) -> bool:
#     outer_start, outer_end = outer[1], outer[2]
#     inner_start, inner_end = inner[1], inner[2]
#     if None in (outer_start, outer_end, inner_start, inner_end):
#         return False
#     return (
#         outer_start <= inner_start
#         and inner_end <= outer_end
#         and (outer_start, outer_end) != (inner_start, inner_end)
#     )


# def _with_contained_fields(entities: dict) -> dict[str, list[str]]:
#     """Copy an entity into every field whose span strictly contains it."""
#     parsed = {
#         field: [span for raw in values if (span := _span_text(raw))[0]]
#         for field, values in entities.items()
#     }
#     extras = defaultdict(list)
#     for inner_field, inners in parsed.items():
#         for outer_field, outers in parsed.items():
#             if inner_field == outer_field:
#                 continue
#             for inner in inners:
#                 if any(_contains(outer, inner) for outer in outers):
#                     extras[outer_field].append(inner[0])
#     return {
#         field: [span[0] for span in spans] + extras.get(field, [])
#         for field, spans in parsed.items()
#     }

