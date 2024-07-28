import __init__
from pybeansack.embedding import BeansackEmbeddings
from pybeansack.beansack import Localsack
import json

localsack = Localsack(".chromadb", BeansackEmbeddings(".models/gte-large-Q4.gguf", 4096))

def setup_categories():    
    with open("factory_settings.json", 'r') as file:
        categories = json.load(file)['categories']
    localsack.categorystore.upsert(
        ids=list(categories.keys()),
        documents=list(map(lambda keywords: ", ".join(keywords), categories.values())),
        metadatas=[{'source': "__SYSTEM__"}]*len(categories)
    )

setup_categories()