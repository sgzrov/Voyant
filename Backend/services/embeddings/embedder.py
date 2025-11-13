import hashlib
import os
from typing import List

from dotenv import load_dotenv
from openai import OpenAI
import redis


load_dotenv()


def _redis_client() -> redis.Redis | None:
    url = os.getenv("REDIS_URL")
    if not url:
        return None
    return redis.from_url(url)


class Embedder:
    def __init__(self, model: str = "text-embedding-3-small") -> None:
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = model
        self.cache_ttl_seconds = int(os.getenv("EMBED_CACHE_TTL", "900"))
        self.redis = _redis_client()

    def _cache_key(self, text: str) -> str:
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        return f"embed:{self.model}:{digest}"

    def embed(self, text: str) -> List[float]:
        if self.redis:
            key = self._cache_key(text)
            cached = self.redis.get(key)
            if cached:
                try:
                    return [float(x) for x in cached.decode("utf-8").split(",")]
                except Exception:
                    pass

        response = self.client.embeddings.create(model=self.model, input=text)
        vector = response.data[0].embedding

        if self.redis:
            try:
                key = self._cache_key(text)
                self.redis.setex(key, self.cache_ttl_seconds, ",".join(str(x) for x in vector))
            except Exception:
                pass
        return vector


