"""
semantic_retriever.py

岗位语义知识库本地检索器。

职责：
1. 读取本地 JSON + embedding 产物；
2. 对查询文本做向量化；
3. 返回 top-k 语义相关岗位知识片段；
4. 把结果压缩成适合传给 LLM 的 semantic_context。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

from .embedding_store import (
    DEFAULT_EMBEDDING_DIMENSION,
    DEFAULT_HASH_EMBEDDING_MODEL,
    clean_text,
    cosine_similarity_matrix,
    create_text_encoder,
    load_embedding_artifacts,
)


DEFAULT_KNOWLEDGE_OUTPUT_DIR = Path("outputs/knowledge")


def build_excerpt(text: str, max_chars: int = 220) -> str:
    normalized = clean_text(text)
    if len(normalized) <= max_chars:
        return normalized
    return f"{normalized[: max_chars - 1]}…"


class SemanticJobKnowledgeRetriever:
    """轻量岗位语义检索器。"""

    def __init__(self, knowledge_dir: str | Path) -> None:
        self.knowledge_dir = Path(knowledge_dir)
        self.metadata, self.embeddings, self.manifest = load_embedding_artifacts(self.knowledge_dir)
        model_name = clean_text(self.manifest.get("model_name")) or DEFAULT_HASH_EMBEDDING_MODEL
        dimension = int(self.manifest.get("dimension") or DEFAULT_EMBEDDING_DIMENSION)
        self.encoder = create_text_encoder(model_name, dimension=dimension)

    @classmethod
    def from_project_root(cls, project_root: str | Path) -> "SemanticJobKnowledgeRetriever":
        return cls(Path(project_root) / DEFAULT_KNOWLEDGE_OUTPUT_DIR)

    def search(
        self,
        query_text: str,
        top_k: int = 3,
        min_score: float = 0.0,
    ) -> List[Dict[str, Any]]:
        query = clean_text(query_text)
        if not query:
            return []

        query_vector = np.asarray(self.encoder.encode_text(query), dtype=np.float32)
        similarities = cosine_similarity_matrix(query_vector, self.embeddings)
        if similarities.size == 0:
            return []

        sorted_indices = np.argsort(similarities)[::-1]
        hits: List[Dict[str, Any]] = []
        for index in sorted_indices[: max(1, int(top_k or 1))]:
            score = float(similarities[index])
            if score < float(min_score):
                continue
            record = dict(self.metadata[index]) if index < len(self.metadata) else {}
            hits.append(
                {
                    "doc_id": clean_text(record.get("doc_id")),
                    "standard_job_name": clean_text(record.get("standard_job_name")),
                    "job_category": clean_text(record.get("job_category")),
                    "job_level": clean_text(record.get("job_level")),
                    "score": round(score, 4),
                    "hard_skills": list(record.get("hard_skills") or [])[:8],
                    "vertical_paths": list(record.get("vertical_paths") or [])[:4],
                    "transfer_paths": list(record.get("transfer_paths") or [])[:4],
                    "doc_text_excerpt": build_excerpt(record.get("doc_text")),
                    "source": "semantic_kb",
                }
            )
        return hits

    def build_semantic_context(
        self,
        query_text: str,
        top_k: int = 3,
        min_score: float = 0.0,
    ) -> Dict[str, Any]:
        hits = self.search(query_text=query_text, top_k=top_k, min_score=min_score)
        return {
            "query": clean_text(query_text),
            "top_k": len(hits),
            "hits": hits,
        }
