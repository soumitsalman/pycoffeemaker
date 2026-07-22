from .normalize import merge_lists
from .models import Entities
from .runtime import TOKEN_MARGIN, clear_gpu_cache
from itertools import batched, chain
from collections import defaultdict

try: import torch
except: print("[WARNING] PyTorch Not Available. `EntityExtractor` will not work. Run `pip install torch`.")

class EntityExtractor:
    model_path: str
    confidence = 0.5
    _splitter = None

    _LABELS = [
        "person",
        "people",
        "organization",
        "company",
        "institution",
        "business",
        "city",
        "state",
        "country",
        "location",
        "stock",
        "ticker",
        "stockticker",
        "product",
    ]
    _LABEL_FIELD_MAPPINGS = {
        "person": "people",
        "people": "people",
        "organization": "companies",
        "company": "companies",
        "institution": "companies",
        "business": "companies",
        "city": "regions",
        "state": "regions",
        "country": "regions",
        "location": "regions",
        "stock": "stock_tickers",
        "ticker": "stock_tickers",
        "stockticker": "stock_tickers",
        "product": "products",
    }   
        
    def __init__(self, model_path: str, context_len: int, threshold=0.5, batch_size: int = 16) -> None:
        self.model_name = model_path
        self.context_len = context_len
        self.threshold = threshold
        self.batch_size = batch_size
        self._llm = None
        self._label_embeddings = None        
        self._splitter = None
    
    def __enter__(self):
        if not self._llm:
            import torch
            from gliner import GLiNER
            from llama_index.core.text_splitter import TokenTextSplitter

            # config.fx_graph_cache = True
            self._llm = GLiNER.from_pretrained(
                self.model_name,
                max_length=self.context_len,
                map_location="cuda" if torch.cuda.is_available() else "cpu",
            )
            self._label_embeddings = self._llm.encode_labels(
                self._LABELS, batch_size=len(self._LABELS)
            )
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
            del self._label_embeddings
            self._label_embeddings = None        
            del self._splitter
            self._splitter = None        
        clear_gpu_cache()
        return False    

    def parse_output(self, response):
        res = defaultdict(list)
        for ent in response:
            res[self._LABEL_FIELD_MAPPINGS[ent["label"]]].append(ent["text"])
        for k, v in res.items():
            res[k] = list({item.lower(): item for item in v}.values())
        return Entities(**res)

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

    def _merge_chunks(self, entities: list[Entities]):
        entities = [e for e in entities if e]
        if entities:
            return Entities(
                regions=merge_lists(*[e.regions for e in entities if e.regions]),
                people=merge_lists(*[e.people for e in entities if e.people]),
                products=merge_lists(*[e.products for e in entities if e.products]),
                companies=merge_lists(*[e.companies for e in entities if e.companies]),
                stock_tickers=merge_lists(*[e.stock_tickers for e in entities if e.stock_tickers]),
            )

    def run_batch(self, input_messages: list[str]):
        chunks, start_idx, counts = self._create_chunks(input_messages)
        entities = self._llm.batch_predict_with_embeds(
            chunks,
            labels_embeddings=self._label_embeddings,
            labels=self._LABELS,
            threshold=self.threshold,
            batch_size=self.batch_size,
        )
        entities = [self.parse_output(group) if group else None for group in entities]        
        return [self._merge_chunks(entities[start:start+count]) for start, count in zip(start_idx, counts)]
