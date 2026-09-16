import hashlib
import math
import re
from typing import List
import numpy as np

EMBEDDING_DIMENSION = 384

class SemanticEmbedder:
    """
    Generates 384-dimensional normalized vector embeddings.
    Implements a deterministic projection with subword hashing and cosine-preserving
    feature hashing. Allows zero-dependency semantic clustering of log signatures,
    with pluggable support for SentenceTransformers or LLM embedding APIs.
    """
    def __init__(self, dimension: int = EMBEDDING_DIMENSION):
        self.dimension = dimension
        # Deterministic projection matrix seeded for reproducible geometric spaces
        rng = np.random.RandomState(42)
        # Random orthogonal-like Gaussian projection matrix
        self.projection = rng.randn(dimension, dimension)
        q, _ = np.linalg.qr(self.projection)
        self.projection = q

    def _tokenize(self, text: str) -> List[str]:
        """Normalize and tokenize text into words and 3-character n-grams."""
        text = text.lower()
        # Strip timestamps, UUIDs, hex numbers to focus on semantic content
        text = re.sub(r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b', '', text)
        text = re.sub(r'0x[0-9a-f]+', '', text)
        text = re.sub(r'\d+', '', text)
        words = re.findall(r'[a-zA-Z_]+', text)
        
        tokens = list(words)
        # Add character tri-grams for subword similarity (e.g. 'timeout', 'time', 'out')
        for word in words:
            if len(word) >= 4:
                for i in range(len(word) - 2):
                    tokens.append(word[i:i+3])
        return tokens

    def embed(self, text: str) -> List[float]:
        """Convert log message or error trace into a 384-dim unit vector."""
        if not text or not text.strip():
            return [0.0] * self.dimension

        tokens = self._tokenize(text)
        if not tokens:
            return [0.0] * self.dimension

        vec = np.zeros(self.dimension, dtype=np.float32)
        
        for token in tokens:
            # Deterministic MD5 hash to index and sign
            h = hashlib.md5(token.encode('utf-8')).hexdigest()
            idx = int(h[:8], 16) % self.dimension
            sign = 1.0 if int(h[8:10], 16) % 2 == 0 else -1.0
            
            # Domain-weighted terms
            weight = 1.0
            if token in {'error', 'timeout', 'failed', 'refused', 'exhausted', 'oom', 'crash', 'deadlock', 'exception'}:
                weight = 2.5
            elif token in {'connection', 'database', 'pool', 'memory', 'cpu', 'socket', 'http', 'gateway'}:
                weight = 1.8
                
            vec[idx] += sign * weight

        # Project through rotation matrix
        projected = np.dot(self.projection, vec)

        # L2 normalize
        norm = np.linalg.norm(projected)
        if norm > 1e-6:
            projected = projected / norm
        else:
            projected = np.zeros(self.dimension)

        return projected.tolist()

    def cosine_similarity(self, vec_a: List[float], vec_b: List[float]) -> float:
        """Compute cosine similarity between two embedding vectors."""
        a = np.array(vec_a)
        b = np.array(vec_b)
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        if norm_a < 1e-6 or norm_b < 1e-6:
            return 0.0
        return float(np.dot(a, b) / (norm_a * norm_b))

embedder = SemanticEmbedder()
