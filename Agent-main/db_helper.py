import sqlite3
import json
from pathlib import Path

def query_sqlite(db_path: str, query: str, parameters: tuple = ()) -> list[dict]:
    """通用 SQLite 查询辅助函数"""
    if not Path(db_path).exists():
        return []
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, parameters)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        print(f"SQLite 查询失败: {e}")
        return []

def query_neo4j(uri: str, user: str, password: str, query: str, parameters: dict = None) -> list[dict]:
    """通用 Neo4j 查询辅助函数 (需要安装 neo4j包: pip install neo4j)"""
    try:
        from neo4j import GraphDatabase
        with GraphDatabase.driver(uri, auth=(user, password)) as driver:
            with driver.session() as session:
                result = session.run(query, parameters or {})
                return [dict(record) for record in result]
    except ImportError:
        print("未安装 neo4j 驱动包，无法连接图数据库。请运行 pip install neo4j")
        return []
    except Exception as e:
        print(f"Neo4j 查询失败: {e}")
        return []
