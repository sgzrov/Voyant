from __future__ import annotations

from typing import Any, Dict
from sqlalchemy import text

from Backend.database import SessionLocal
from Backend.services.embeddings.embedder import Embedder


def vector_search(user_id: str, query_text: str, limit: int = 5) -> Dict[str, Any]:
    embedder = Embedder()
    query_embedding = embedder.embed(query_text)
    with SessionLocal() as session:
        res = session.execute(
            text(
                """
                SELECT summary_text, metrics
                FROM health_summaries
                WHERE user_id = :user_id
                ORDER BY embedding <=> :query_embedding
                LIMIT :limit
                """
            ),
            {"user_id": user_id, "query_embedding": query_embedding, "limit": limit},
        ).fetchall()
    return {
        "semantic_contexts": [row[0] for row in res],
        "metrics_list": [row[1] for row in res],
    }


