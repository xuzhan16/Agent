"""
岗位数据处理链：Excel 清洗、去重、LLM 抽取、导出 SQLite / Neo4j。

命令行入口见各子模块的 ``if __name__ == "__main__"``；整线调度见 ``demos.main``。

子模块按需导入（含 pandas）；仅 ``import job_data`` 不会立刻加载 pandas。
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "process_job_excel",
    "process_job_dedup",
    "process_job_extract",
    "process_export_to_sql",
    "process_export_to_neo4j",
]


def __getattr__(name: str) -> Any:
    if name == "process_job_excel":
        from .data_cleaning import process_job_excel

        return process_job_excel
    if name == "process_job_dedup":
        from .job_dedup import process_job_dedup

        return process_job_dedup
    if name == "process_job_extract":
        from .job_extract import process_job_extract

        return process_job_extract
    if name == "process_export_to_sql":
        from .export_to_sql import process_export_to_sql

        return process_export_to_sql
    if name == "process_export_to_neo4j":
        from .export_to_neo4j import process_export_to_neo4j

        return process_export_to_neo4j
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
