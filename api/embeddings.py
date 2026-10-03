"""Author:
"""
import hashlib, math, os, re

DIMS = 384
#fallback embedder
class HashingEmbedder:
    dims = DIMS
    def embed(self, text: str):
        vec=[0.0]*self.dims
        for token in re.findall(r"[A-Za-z0-9_\-]+", (text or '').lower()):
            digest=hashlib.sha256(token.encode()).digest()
            bucket=int.from_bytes(digest[:4], 'big') % self.dims
            sign=1.0 if digest[4] & 1 else -1.0
            vec[bucket]+=sign
        norm=math.sqrt(sum(x*x for x in vec)) or 1.0
        return [x/norm for x in vec]

class SentenceTransformerEmbedder:
    dims = DIMS
    def __init__(self):
        from sentence_transformers import SentenceTransformer
        self.model=SentenceTransformer(os.getenv('EMBEDDING_MODEL','sentence-transformers/all-MiniLM-L6-v2'))
    def embed(self, text: str):
        return self.model.encode(text or '', normalize_embeddings=True).tolist()


def get_embedder():
    if os.getenv('EMBEDDING_PROVIDER','hashing') == 'sentence_transformers':
        return SentenceTransformerEmbedder()
    return HashingEmbedder()
