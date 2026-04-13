"""
embedding_store.py

本地语义知识库的轻量 embedding 存储与编码工具。

设计原则：
1. 优先支持本地 embedding 模型；
2. 若当前环境没有现成 embedding 依赖，则退回到依赖最少的本地哈希向量方案；
3. 向量结果保存为本地文件，便于离线交付和后续检索复用。
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np


DEFAULT_HASH_EMBEDDING_MODEL = "local-hash-embedding-v1"
DEFAULT_EMBEDDING_DIMENSION = 384
DEFAULT_EMBEDDING_METADATA_FILE = "job_knowledge_metadata.json"
DEFAULT_EMBEDDING_ARRAY_FILE = "job_knowledge_embeddings.npy"
DEFAULT_EMBEDDING_MANIFEST_FILE = "job_knowledge_manifest.json"


def clean_text(value: Any) -> str:
    """基础文本清洗。"""
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ").replace("\u3000", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text


def _tokenize_english_words(text: str) -> List[str]:
    return [match.group(0).lower() for match in re.finditer(r"[A-Za-z0-9_]{2,}", text)]


def _tokenize_chinese_terms(text: str) -> List[str]:
    chinese_segments = re.findall(r"[\u4e00-\u9fff]{2,}", text)
    tokens: List[str] = []
    for segment in chinese_segments:
        tokens.append(segment)
        segment_length = len(segment)
        for ngram_size in (2, 3):
            if segment_length < ngram_size:
                continue
            for index in range(segment_length - ngram_size + 1):
                tokens.append(segment[index : index + ngram_size])
    return tokens


def tokenize_text(text: str) -> List[str]:
    """把文本统一切成适合哈希编码的 token 序列。"""
    normalized = clean_text(text)
    if not normalized:
        return []
    tokens = []
    tokens.extend(_tokenize_english_words(normalized))
    tokens.extend(_tokenize_chinese_terms(normalized))
    return tokens


class LocalHashingEncoder:
    """依赖极少的本地哈希向量编码器。"""

    def __init__(
        self,
        dimension: int = DEFAULT_EMBEDDING_DIMENSION,
        model_name: str = DEFAULT_HASH_EMBEDDING_MODEL,
    ) -> None:
        self.dimension = max(64, int(dimension or DEFAULT_EMBEDDING_DIMENSION))
        self.model_name = clean_text(model_name) or DEFAULT_HASH_EMBEDDING_MODEL

    def _token_to_index_and_sign(self, token: str) -> Tuple[int, float]:
        digest = hashlib.md5(token.encode("utf-8")).hexdigest()
        index = int(digest[:8], 16) % self.dimension
        sign = 1.0 if int(digest[8:10], 16) % 2 == 0 else -1.0
        return index, sign

    def encode_text(self, text: str) -> np.ndarray:
        vector = np.zeros(self.dimension, dtype=np.float32)
        for token in tokenize_text(text):
            index, sign = self._token_to_index_and_sign(token)
            vector[index] += sign

        norm = float(np.linalg.norm(vector))
        if norm > 0:
            vector /= norm
        return vector

    def encode_texts(self, texts: Iterable[str]) -> np.ndarray:
        vectors = [self.encode_text(text) for text in texts]
        if not vectors:
            return np.zeros((0, self.dimension), dtype=np.float32)
        return np.stack(vectors).astype(np.float32)


class SentenceTransformerEncoder:
    """若环境已有 sentence-transformers，则优先使用本地模型。"""

    def __init__(self, model_name: str) -> None:
        from sentence_transformers import SentenceTransformer

        self.model_name = clean_text(model_name)
        self.model = SentenceTransformer(self.model_name)
        self.dimension = int(self.model.get_sentence_embedding_dimension())

    def encode_text(self, text: str) -> np.ndarray:
        vectors = self.encode_texts([text])
        return vectors[0] if len(vectors) else np.zeros(self.dimension, dtype=np.float32)

    def encode_texts(self, texts: Iterable[str]) -> np.ndarray:
        text_list = [clean_text(item) for item in texts]
        if not text_list:
            return np.zeros((0, self.dimension), dtype=np.float32)
        array = self.model.encode(
            text_list,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return np.asarray(array, dtype=np.float32)


def create_text_encoder(
    embedding_model_name: str = DEFAULT_HASH_EMBEDDING_MODEL,
    dimension: int = DEFAULT_EMBEDDING_DIMENSION,
) -> LocalHashingEncoder | SentenceTransformerEncoder:
    """
    创建文本编码器。

    优先尝试显式指定的本地 embedding 模型；
    若环境缺少依赖或未指定模型，则退回到内置哈希向量方案。
    """
    normalized_name = clean_text(embedding_model_name)
    if normalized_name and normalized_name != DEFAULT_HASH_EMBEDDING_MODEL:
        try:
            return SentenceTransformerEncoder(normalized_name)
        except Exception:
            pass
    return LocalHashingEncoder(dimension=dimension, model_name=DEFAULT_HASH_EMBEDDING_MODEL)


def save_embedding_artifacts(
    output_dir: str | Path,
    metadata: List[Dict[str, Any]],
    embeddings: np.ndarray,
    model_name: str,
    encoder_type: str,
) -> Dict[str, Any]:
    """统一保存 embedding 产物。"""
    base_dir = Path(output_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = base_dir / DEFAULT_EMBEDDING_METADATA_FILE
    embeddings_path = base_dir / DEFAULT_EMBEDDING_ARRAY_FILE
    manifest_path = base_dir / DEFAULT_EMBEDDING_MANIFEST_FILE

    with metadata_path.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    np.save(embeddings_path, embeddings.astype(np.float32))

    manifest = {
        "model_name": clean_text(model_name) or DEFAULT_HASH_EMBEDDING_MODEL,
        "encoder_type": clean_text(encoder_type) or "local_hash",
        "dimension": int(embeddings.shape[1]) if embeddings.ndim == 2 and embeddings.size else DEFAULT_EMBEDDING_DIMENSION,
        "document_count": int(len(metadata)),
        "metadata_file": metadata_path.name,
        "embeddings_file": embeddings_path.name,
    }
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    return manifest


def load_embedding_artifacts(base_dir: str | Path) -> Tuple[List[Dict[str, Any]], np.ndarray, Dict[str, Any]]:
    """加载本地 embedding 存储。"""
    root = Path(base_dir)
    manifest_path = root / DEFAULT_EMBEDDING_MANIFEST_FILE
    if not manifest_path.exists():
        raise FileNotFoundError(f"embedding manifest not found: {manifest_path}")

    with manifest_path.open("r", encoding="utf-8") as f:
        manifest = json.load(f)
    metadata_path = root / clean_text(manifest.get("metadata_file"))
    embeddings_path = root / clean_text(manifest.get("embeddings_file"))

    if not metadata_path.exists():
        raise FileNotFoundError(f"embedding metadata not found: {metadata_path}")
    if not embeddings_path.exists():
        raise FileNotFoundError(f"embedding array not found: {embeddings_path}")

    with metadata_path.open("r", encoding="utf-8") as f:
        metadata = json.load(f)
    embeddings = np.load(embeddings_path)
    if not isinstance(metadata, list):
        raise ValueError("embedding metadata must be a list")
    return metadata, np.asarray(embeddings, dtype=np.float32), manifest


def cosine_similarity_matrix(query_vector: np.ndarray, document_vectors: np.ndarray) -> np.ndarray:
    """计算 query 向量与文档向量矩阵的余弦相似度。"""
    if query_vector.ndim != 1:
        raise ValueError("query_vector must be 1-dimensional")
    if document_vectors.ndim != 2:
        raise ValueError("document_vectors must be 2-dimensional")
    if len(document_vectors) == 0:
        return np.zeros((0,), dtype=np.float32)
    query_norm = float(np.linalg.norm(query_vector))
    if query_norm <= 0:
        return np.zeros((len(document_vectors),), dtype=np.float32)
    query = query_vector / query_norm
    doc_norms = np.linalg.norm(document_vectors, axis=1)
    safe_doc_norms = np.where(doc_norms > 0, doc_norms, 1.0)
    normalized_docs = document_vectors / safe_doc_norms[:, None]
    return normalized_docs @ query

