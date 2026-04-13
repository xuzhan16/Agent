"""
build_embedding_index.py

基于岗位知识 JSON 文档构建本地 embedding 索引。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from semantic_retrieval.embedding_store import (
    DEFAULT_EMBEDDING_DIMENSION,
    DEFAULT_HASH_EMBEDDING_MODEL,
    clean_text,
    create_text_encoder,
    save_embedding_artifacts,
)


DEFAULT_KNOWLEDGE_JSON_PATH = Path("outputs/knowledge/job_knowledge.jsonl")
DEFAULT_KNOWLEDGE_OUTPUT_DIR = Path("outputs/knowledge")


def load_knowledge_documents(input_path: str | Path) -> List[Dict[str, Any]]:
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"knowledge file not found: {path}")

    if path.suffix.lower() == ".json":
        with path.open("r", encoding="utf-8") as f:
            loaded = json.load(f)
        if not isinstance(loaded, list):
            raise ValueError("knowledge json must be a list")
        return [item for item in loaded if isinstance(item, dict)]

    documents: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = clean_text(line)
            if not text:
                continue
            record = json.loads(text)
            if isinstance(record, dict):
                documents.append(record)
    return documents


def process_build_embedding_index(
    input_json_path: str | Path = DEFAULT_KNOWLEDGE_JSON_PATH,
    output_dir: str | Path = DEFAULT_KNOWLEDGE_OUTPUT_DIR,
    embedding_model_name: str = DEFAULT_HASH_EMBEDDING_MODEL,
) -> Dict[str, Any]:
    """主流程：构建岗位知识 embedding 索引。"""
    documents = load_knowledge_documents(input_json_path)
    texts = [clean_text(doc.get("doc_text")) for doc in documents]
    encoder = create_text_encoder(
        embedding_model_name=embedding_model_name,
        dimension=DEFAULT_EMBEDDING_DIMENSION,
    )
    embeddings = encoder.encode_texts(texts)
    manifest = save_embedding_artifacts(
        output_dir=output_dir,
        metadata=documents,
        embeddings=embeddings,
        model_name=getattr(encoder, "model_name", embedding_model_name),
        encoder_type=encoder.__class__.__name__,
    )
    return {
        "input_json_path": str(Path(input_json_path)),
        "output_dir": str(Path(output_dir)),
        "document_count": int(len(documents)),
        "model_name": manifest.get("model_name"),
        "encoder_type": manifest.get("encoder_type"),
        "dimension": manifest.get("dimension"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="构建岗位知识 embedding 索引")
    parser.add_argument("--input", default=str(DEFAULT_KNOWLEDGE_JSON_PATH), help="岗位知识 JSON/JSONL 文件路径")
    parser.add_argument("--output-dir", default=str(DEFAULT_KNOWLEDGE_OUTPUT_DIR), help="embedding 产物输出目录")
    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_HASH_EMBEDDING_MODEL,
        help="本地 embedding 模型名；若环境缺依赖则自动回退到本地哈希向量方案",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = process_build_embedding_index(
        input_json_path=args.input,
        output_dir=args.output_dir,
        embedding_model_name=args.embedding_model,
    )
    print("[build_embedding_index] finished.")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

