from __future__ import annotations

from typing import Any, Dict
import logging
from sqlalchemy import text

from Backend.database import SessionLocal
from Backend.services.embeddings.embedder import Embedder

logger = logging.getLogger(__name__)


def vector_search(user_id: str, query_text: str, limit: int = 3) -> Dict[str, Any]:
    embedder = Embedder()
    query_embedding = embedder.embed(query_text)
    # Pass embedding as text and cast to vector in SQL to avoid numeric[] type mismatch
    vector_text = "[" + ",".join(str(x) for x in query_embedding) + "]"
    with SessionLocal() as session:
        try:
            cnt = session.execute(
                text("SELECT count(*) FROM health_summaries WHERE user_id = :user_id"),
                {"user_id": user_id},
            ).scalar_one()
            logger.info("vector.precheck: user_id=%s summaries=%s", user_id, cnt)
        except Exception:
            pass
        res = session.execute(
            text(
                """
                SELECT summary_text, metrics
                FROM health_summaries
                WHERE user_id = :user_id
                ORDER BY embedding <=> CAST(:query_embedding_text AS vector)
                LIMIT :limit
                """
            ),
            {"user_id": user_id, "query_embedding_text": vector_text, "limit": limit},
        ).fetchall()
    return {
        "semantic_contexts": [row[0] for row in res],
        "metrics_list": [row[1] for row in res],
    }


