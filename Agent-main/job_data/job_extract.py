"""
job_extract.py

岗位文本信息抽取模块。

功能：
1. 从岗位明细 DataFrame 中构造大模型输入；
2. 调用 llm_interface_layer.llm_service.call_llm("job_extract", ...)；
3. 清洗并解析大模型返回结果；
4. 把结构化岗位画像合并回原始 DataFrame；
5. 保存为新的 CSV 文件。
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from llm_interface_layer.llm_service import call_llm


DEFAULT_GROUP_SAMPLE_SIZE = 3
JOB_DESC_SAMPLE_MAX_CHARS = 800
COMPANY_DESC_SAMPLE_MAX_CHARS = 400

DEFAULT_JOB_PROFILE: Dict[str, Any] = {
    "standard_job_name": "",
    "job_category": "",
    "degree_requirement": "",
    "major_requirement": "",
    "experience_requirement": "",
    "hard_skills": [],
    "tools_or_tech_stack": [],
    "certificate_requirement": [],
    "soft_skills": [],
    "practice_requirement": "",
    "job_level": "",
    "suitable_student_profile": "",
    "raw_requirement_summary": "",
    "vertical_paths": [],
    "transfer_paths": [],
    "path_relation_details": [],
}

LIST_FIELDS = {
    "hard_skills",
    "tools_or_tech_stack",
    "certificate_requirement",
    "soft_skills",
    "vertical_paths",
    "transfer_paths",
}
OBJECT_LIST_FIELDS = {"path_relation_details"}


def clean_text(value: object) -> str:
    """基础文本清洗。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def get_first_existing_value(record: pd.Series, candidates: Sequence[str]) -> str:
    """从多个字段候选里取第一个非空值。"""
    for field in candidates:
        if field in record.index:
            text = clean_text(record.get(field, ""))
            if text:
                return text
    return ""


def parse_json_like_text(value: object) -> Dict[str, Any]:
    """
    兼容解析：
    - dict
    - 纯 JSON 字符串
    - ```json 代码块
    - 前后带少量解释文本
    """
    if isinstance(value, dict):
        return value

    text = clean_text(value)
    if not text:
        return {}

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    code_block_match = re.search(r"```json\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
    if code_block_match:
        try:
            return json.loads(code_block_match.group(1).strip())
        except json.JSONDecodeError:
            pass

    object_match = re.search(r"\{[\s\S]*\}", text)
    if object_match:
        try:
            return json.loads(object_match.group(0))
        except json.JSONDecodeError:
            pass

    return {}


def normalize_list_value(value: object) -> List[str]:
    """
    把列表字段统一转成字符串列表。
    支持：
    - list
    - JSON 数组字符串
    - 逗号/顿号/分号分隔字符串
    """
    if value is None:
        return []

    if isinstance(value, list):
        return sorted({clean_text(item) for item in value if clean_text(item)})

    text = clean_text(value)
    if not text:
        return []

    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return sorted({clean_text(item) for item in parsed if clean_text(item)})
        except json.JSONDecodeError:
            pass

    parts = re.split(r"[、,，;/；|]+", text)
    return sorted({clean_text(part) for part in parts if clean_text(part)})


def normalize_path_relation_details(
    value: object,
    source_job_name: str = "",
) -> List[Dict[str, Any]]:
    """
    统一解析岗位关系明细。

    支持：
    - list[dict]
    - JSON 字符串
    - 缺失时返回空列表
    """
    items: List[Any] = []
    if isinstance(value, list):
        items = value
    else:
        text = clean_text(value)
        if text.startswith("[") and text.endswith("]"):
            try:
                loaded = json.loads(text)
                if isinstance(loaded, list):
                    items = loaded
            except json.JSONDecodeError:
                items = []

    relation_rows: List[Dict[str, Any]] = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        source_job = clean_text(item.get("source_job") or source_job_name)
        target_job = clean_text(item.get("target_job") or item.get("to_job"))
        relation_type = clean_text(
            item.get("relation_type")
            or item.get("path_type")
            or item.get("type")
        ).upper()
        if relation_type not in {"PROMOTE_TO", "TRANSFER_TO"}:
            continue
        if not source_job or not target_job or source_job == target_job:
            continue
        row = {
            "source_job": source_job,
            "target_job": target_job,
            "relation_type": relation_type,
            "reason": clean_text(item.get("reason")),
            "confidence": clean_text(item.get("confidence")),
        }
        key = (
            row["source_job"],
            row["target_job"],
            row["relation_type"],
        )
        if key in seen:
            continue
        seen.add(key)
        relation_rows.append(row)
    return relation_rows


def build_path_relation_details(
    standard_job_name: str,
    vertical_paths: List[str],
    transfer_paths: List[str],
    existing_details: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """根据路径字段补齐路径关系明细。"""
    source_job = clean_text(standard_job_name)
    relation_rows = normalize_path_relation_details(existing_details or [], source_job_name=source_job)
    existing_keys = {
        (row["source_job"], row["target_job"], row["relation_type"])
        for row in relation_rows
    }

    def _append_from_path_values(path_values: List[str], relation_type: str) -> None:
        for raw_value in path_values:
            text = clean_text(raw_value)
            if not text:
                continue
            if "->" in text:
                parts = [clean_text(part) for part in text.split("->") if clean_text(part)]
                if len(parts) >= 2:
                    source = parts[0]
                    target = parts[-1]
                else:
                    continue
            else:
                source = source_job
                target = text
            key = (source, target, relation_type)
            if not source or not target or source == target or key in existing_keys:
                continue
            relation_rows.append(
                {
                    "source_job": source,
                    "target_job": target,
                    "relation_type": relation_type,
                    "reason": "",
                    "confidence": "",
                }
            )
            existing_keys.add(key)

    _append_from_path_values(vertical_paths, "PROMOTE_TO")
    _append_from_path_values(transfer_paths, "TRANSFER_TO")
    return relation_rows


def normalize_profile_fields(
    parsed_result: Dict[str, Any],
    record: pd.Series,
) -> Dict[str, Any]:
    """
    把大模型返回结果标准化到固定 schema。

    兼容两类情况：
    1. 模型直接按目标 schema 返回；
    2. 当前 mock 或旧接口返回其他字段名，需要映射到目标 schema。
    """
    profile = dict(DEFAULT_JOB_PROFILE)

    # 先填当前记录里的标准岗位名
    profile["standard_job_name"] = get_first_existing_value(
        record,
        ["standard_job_name", "normalized_job_title", "job_title_norm", "job_name", "job_title"],
    )

    # 直接字段映射
    for field in DEFAULT_JOB_PROFILE:
        if field in parsed_result and field != "standard_job_name":
            profile[field] = parsed_result[field]

    # 兼容旧字段名
    profile["job_category"] = clean_text(
        profile["job_category"]
        or parsed_result.get("job_family", "")
        or parsed_result.get("job_category", "")
    )
    profile["degree_requirement"] = clean_text(
        profile["degree_requirement"]
        or parsed_result.get("education_requirement", "")
        or parsed_result.get("degree_requirement", "")
    )
    profile["major_requirement"] = clean_text(
        profile["major_requirement"] or parsed_result.get("major_requirement", "")
    )
    profile["experience_requirement"] = clean_text(
        profile["experience_requirement"]
        or parsed_result.get("experience_requirement", "")
    )
    profile["hard_skills"] = normalize_list_value(
        profile["hard_skills"] or parsed_result.get("skills", [])
    )
    profile["tools_or_tech_stack"] = normalize_list_value(
        profile["tools_or_tech_stack"] or parsed_result.get("tools_or_tech_stack", [])
    )
    if not profile["tools_or_tech_stack"]:
        profile["tools_or_tech_stack"] = list(profile["hard_skills"])

    profile["certificate_requirement"] = normalize_list_value(
        profile["certificate_requirement"] or parsed_result.get("certificates", [])
    )
    profile["soft_skills"] = normalize_list_value(
        profile["soft_skills"] or parsed_result.get("soft_skills", [])
    )
    profile["practice_requirement"] = clean_text(
        profile["practice_requirement"] or parsed_result.get("practice_requirement", "")
    )
    profile["job_level"] = clean_text(
        profile["job_level"] or parsed_result.get("job_level", "")
    )
    profile["suitable_student_profile"] = clean_text(
        profile["suitable_student_profile"] or parsed_result.get("suitable_student_profile", "")
    )
    profile["raw_requirement_summary"] = clean_text(
        profile["raw_requirement_summary"]
        or parsed_result.get("summary", "")
        or parsed_result.get("raw_requirement_summary", "")
    )
    profile["vertical_paths"] = normalize_list_value(
        profile["vertical_paths"]
        or parsed_result.get("vertical_paths", [])
        or parsed_result.get("promote_paths", [])
        or parsed_result.get("promotion_paths", [])
    )
    profile["transfer_paths"] = normalize_list_value(
        profile["transfer_paths"]
        or parsed_result.get("transfer_paths", [])
        or parsed_result.get("lateral_paths", [])
        or parsed_result.get("transition_paths", [])
    )

    # 再兜底标准岗位名
    profile["standard_job_name"] = clean_text(
        parsed_result.get("standard_job_name", "") or profile["standard_job_name"]
    )
    profile["path_relation_details"] = build_path_relation_details(
        standard_job_name=profile["standard_job_name"],
        vertical_paths=profile["vertical_paths"],
        transfer_paths=profile["transfer_paths"],
        existing_details=normalize_path_relation_details(
            parsed_result.get("path_relation_details")
            or parsed_result.get("job_path_relations")
            or parsed_result.get("path_relations"),
            source_job_name=profile["standard_job_name"],
        ),
    )

    return profile


def validate_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    """保证所有字段存在且类型稳定。"""
    fixed = dict(DEFAULT_JOB_PROFILE)
    for field, default_value in DEFAULT_JOB_PROFILE.items():
        if field in LIST_FIELDS:
            fixed[field] = normalize_list_value(profile.get(field, default_value))
        elif field in OBJECT_LIST_FIELDS:
            fixed[field] = normalize_path_relation_details(
                profile.get(field, default_value),
                source_job_name=clean_text(profile.get("standard_job_name", "")),
            )
        else:
            fixed[field] = clean_text(profile.get(field, default_value))

    return fixed


def build_extraction_input(record: pd.Series) -> Dict[str, Any]:
    """构造发送给 LLM 的岗位输入。"""
    return {
        "job_name": get_first_existing_value(record, ["job_name", "job_title", "job_title_norm"]),
        "standard_job_name": get_first_existing_value(
            record,
            ["standard_job_name", "normalized_job_title", "job_title_norm", "job_name", "job_title"],
        ),
        "industry": get_first_existing_value(record, ["industry"]),
        "company_name": get_first_existing_value(record, ["company_name", "company_name_norm"]),
        "company_type": get_first_existing_value(record, ["company_type", "company_type_norm"]),
        "company_size": get_first_existing_value(record, ["company_size", "company_size_norm"]),
        "city": get_first_existing_value(record, ["city", "job_address_norm", "job_address"]),
        "salary_raw": get_first_existing_value(
            record,
            ["salary_raw", "salary_range_raw", "salary_range", "salary_range_clean"],
        ),
        "job_desc": get_first_existing_value(
            record,
            ["job_desc", "job_description_clean", "job_description_text", "job_description"],
        ),
        "company_desc": get_first_existing_value(
            record,
            ["company_desc", "company_description_clean", "company_description_text", "company_description"],
        ),
    }


def collect_distinct_sample_values(
    rows: Sequence[pd.Series],
    candidates: Sequence[str],
    max_samples: int,
    max_chars: Optional[int] = None,
) -> List[str]:
    """从同一岗位组里收集少量去重后的代表字段值。"""
    values: List[str] = []
    seen = set()
    sample_limit = max(1, int(max_samples or 1))

    for row in rows:
        text = get_first_existing_value(row, candidates)
        if max_chars is not None:
            text = text[:max_chars]
        if not text or text in seen:
            continue

        seen.add(text)
        values.append(text)
        if len(values) >= sample_limit:
            break

    return values


def merge_sample_texts(samples: Sequence[str], multiline: bool = False) -> str:
    """把多个代表样本拼成一个紧凑文本，交给 LLM 做组级抽取。"""
    cleaned_samples = [clean_text(item) for item in samples if clean_text(item)]
    if not cleaned_samples:
        return ""
    if len(cleaned_samples) == 1:
        return cleaned_samples[0]
    if multiline:
        return "\n".join(f"样本{index + 1}：{item}" for index, item in enumerate(cleaned_samples))
    return " / ".join(cleaned_samples)


def build_group_extraction_input(
    rows: Sequence[pd.Series],
    group_sample_size: int = DEFAULT_GROUP_SAMPLE_SIZE,
) -> Dict[str, Any]:
    """对同一岗位组聚合少量代表样本，减少重复 LLM 调用并保留多样性。"""
    if not rows:
        return {}

    first_input = build_extraction_input(rows[0])
    return {
        "job_name": merge_sample_texts(
            collect_distinct_sample_values(
                rows,
                ["job_name", "job_title", "job_title_norm"],
                max_samples=group_sample_size,
            )
        ) or first_input["job_name"],
        "standard_job_name": first_input["standard_job_name"],
        "industry": merge_sample_texts(
            collect_distinct_sample_values(rows, ["industry"], max_samples=group_sample_size)
        ) or first_input["industry"],
        "company_name": merge_sample_texts(
            collect_distinct_sample_values(
                rows,
                ["company_name", "company_name_norm"],
                max_samples=group_sample_size,
            )
        ) or first_input["company_name"],
        "company_type": merge_sample_texts(
            collect_distinct_sample_values(
                rows,
                ["company_type", "company_type_norm"],
                max_samples=group_sample_size,
            )
        ) or first_input["company_type"],
        "company_size": merge_sample_texts(
            collect_distinct_sample_values(
                rows,
                ["company_size", "company_size_norm"],
                max_samples=group_sample_size,
            )
        ) or first_input["company_size"],
        "city": merge_sample_texts(
            collect_distinct_sample_values(
                rows,
                ["city", "job_address_norm", "job_address"],
                max_samples=group_sample_size,
            )
        ) or first_input["city"],
        "salary_raw": merge_sample_texts(
            collect_distinct_sample_values(
                rows,
                ["salary_raw", "salary_range_raw", "salary_range", "salary_range_clean"],
                max_samples=group_sample_size,
            )
        ) or first_input["salary_raw"],
        "job_desc": merge_sample_texts(
            collect_distinct_sample_values(
                rows,
                ["job_desc", "job_description_clean", "job_description_text", "job_description"],
                max_samples=group_sample_size,
                max_chars=JOB_DESC_SAMPLE_MAX_CHARS,
            ),
            multiline=True,
        ) or first_input["job_desc"],
        "company_desc": merge_sample_texts(
            collect_distinct_sample_values(
                rows,
                [
                    "company_desc",
                    "company_description_clean",
                    "company_description_text",
                    "company_description",
                ],
                max_samples=group_sample_size,
                max_chars=COMPANY_DESC_SAMPLE_MAX_CHARS,
            ),
            multiline=True,
        ) or first_input["company_desc"],
    }


def build_profile_cache_key(input_data: Dict[str, Any]) -> str:
    """构造岗位组级缓存键，优先按标准岗位名复用 LLM 结果。"""
    title_key = clean_text(input_data.get("standard_job_name", "")) or clean_text(
        input_data.get("job_name", "")
    )
    if title_key:
        return title_key

    fallback_key = "||".join(
        [
            clean_text(input_data.get("job_desc", ""))[:200],
            clean_text(input_data.get("company_desc", ""))[:200],
        ]
    )
    return fallback_key or "__empty_job_profile_key__"


def call_job_extract_llm(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """调用统一 LLM 接口。"""
    extra_context = {
        "project": "基于AI的大学生职业规划智能体",
        "output_language": "zh-CN",
        "instruction": (
            "请严格按照输出 JSON 模板返回 JSON。"
            "如果某项信息无法确定，请返回空字符串或空列表。"
            "vertical_paths 和 transfer_paths 面向岗位知识本身，不要结合具体学生画像。"
        ),
        "expected_fields": [
            "standard_job_name",
            "job_category",
            "degree_requirement",
            "major_requirement",
            "experience_requirement",
            "hard_skills",
            "tools_or_tech_stack",
            "certificate_requirement",
            "soft_skills",
            "practice_requirement",
            "job_level",
            "suitable_student_profile",
            "raw_requirement_summary",
            "vertical_paths",
            "transfer_paths",
            "path_relation_details",
        ],
    }
    return call_llm(
        "job_extract",
        input_data=input_data,
        context_data=None,
        student_state=None,
        extra_context=extra_context,
    )


def extract_job_profile_from_input(
    input_data: Dict[str, Any],
    fallback_record: pd.Series,
) -> Dict[str, Any]:
    """
    基于已构造的 input_data 执行岗位抽取。

    返回固定结构，并附带：
    - extract_success
    - extract_error
    - job_extract_json
    """
    try:
        llm_response = call_job_extract_llm(input_data)
        parsed = parse_json_like_text(llm_response)
        normalized = normalize_profile_fields(parsed, fallback_record)
        validated = validate_profile(normalized)
        validated["extract_success"] = True
        validated["extract_error"] = ""
        validated["job_extract_json"] = json.dumps(validated, ensure_ascii=False)
        return validated
    except Exception as exc:
        fallback = validate_profile({"standard_job_name": input_data.get("standard_job_name", "")})
        fallback["extract_success"] = False
        fallback["extract_error"] = clean_text(str(exc))
        fallback["job_extract_json"] = json.dumps(fallback, ensure_ascii=False)
        return fallback


def extract_job_profile(record: pd.Series) -> Dict[str, Any]:
    """单条岗位抽取函数，保留原始调用方式。"""
    return extract_job_profile_from_input(build_extraction_input(record), record)


def timed_extract_job_profile(
    input_data: Dict[str, Any],
    fallback_record: pd.Series,
) -> Tuple[Dict[str, Any], float]:
    """返回岗位抽取结果和真实 LLM 调用耗时，不把线程池排队时间算进去。"""
    start_time = time.perf_counter()
    profile = extract_job_profile_from_input(input_data, fallback_record)
    elapsed = time.perf_counter() - start_time
    return profile, elapsed


def convert_profile_to_row(profile: Dict[str, Any]) -> Dict[str, Any]:
    """把抽取结果转换成适合 DataFrame/CSV 的行结构。"""
    row = {}
    for field in DEFAULT_JOB_PROFILE:
        if field in LIST_FIELDS or field in OBJECT_LIST_FIELDS:
            row[field] = json.dumps(profile.get(field, []), ensure_ascii=False)
        else:
            row[field] = clean_text(profile.get(field, ""))
    row["extract_success"] = bool(profile.get("extract_success", False))
    row["extract_error"] = clean_text(profile.get("extract_error", ""))
    row["job_extract_json"] = clean_text(profile.get("job_extract_json", ""))
    return row


def merge_extraction_results(
    df: pd.DataFrame,
    extracted_df: pd.DataFrame,
    index_col: str = "_source_index",
) -> pd.DataFrame:
    """将抽取结果合并回原始 DataFrame。"""
    return df.merge(extracted_df, on=index_col, how="left")


def batch_extract_job_profiles(
    df: pd.DataFrame,
    log_every: int = 50,
    max_workers: int = 4,
    group_sample_size: int = DEFAULT_GROUP_SAMPLE_SIZE,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    批处理岗位抽取。

    优化点：
    1. 先按标准岗位名+行业分组，只保留每组少量代表样本发给 LLM；
    2. 对唯一岗位组使用线程池并发请求真实大模型；
    3. 再按原始行顺序把抽取结果合并回 DataFrame。
    """
    working_df = df.copy().reset_index(drop=False).rename(columns={"index": "_source_index"})
    cache: Dict[str, Dict[str, Any]] = {}
    task_group_rows: Dict[str, List[pd.Series]] = {}
    task_group_sizes: Dict[str, int] = {}
    row_cache_keys: Dict[Any, str] = {}
    sample_limit = max(1, int(group_sample_size or 1))

    total_rows = len(working_df)
    for _, row in working_df.iterrows():
        input_data = build_extraction_input(row)
        cache_key = build_profile_cache_key(input_data)
        row_cache_keys[row["_source_index"]] = cache_key
        task_group_sizes[cache_key] = task_group_sizes.get(cache_key, 0) + 1

        group_rows = task_group_rows.setdefault(cache_key, [])
        if len(group_rows) < sample_limit:
            group_rows.append(row)

    total_tasks = len(task_group_rows)
    reused_rows = total_rows - total_tasks
    worker_count = max(1, int(max_workers or 1))
    print(
        f"[job_extract] Start extracting job profiles, total rows: {total_rows}, "
        f"unique llm tasks: {total_tasks}, cache reused rows: {reused_rows}, "
        f"max_workers: {worker_count}, group_sample_size: {sample_limit}"
    )

    if total_tasks == 0:
        extracted_df = pd.DataFrame(
            columns=[
                "_source_index",
                *DEFAULT_JOB_PROFILE.keys(),
                "extract_success",
                "extract_error",
                "job_extract_json",
            ]
        )
        merged_df = merge_extraction_results(working_df, extracted_df, index_col="_source_index")
        return merged_df, extracted_df

    task_items = [
        (
            cache_key,
            build_group_extraction_input(rows, group_sample_size=sample_limit),
            rows[0],
            task_group_sizes.get(cache_key, len(rows)),
        )
        for cache_key, rows in task_group_rows.items()
        if rows
    ]

    if worker_count == 1 or total_tasks == 1:
        for task_index, (cache_key, input_data, fallback_record, group_rows_count) in enumerate(
            task_items,
            start=1,
        ):
            job_name = clean_text(input_data.get("job_name") or input_data.get("standard_job_name"))
            profile, elapsed = timed_extract_job_profile(input_data, fallback_record)
            cache[cache_key] = profile
            print(
                f"[job_extract] Task {task_index}/{total_tasks} done in {elapsed:.2f}s | "
                f"job={job_name} | group_rows={group_rows_count} | "
                f"success={profile.get('extract_success', False)}"
            )
            if task_index == 1 or task_index % log_every == 0 or task_index == total_tasks:
                print(
                    f"[job_extract] Progress: unique_tasks={task_index}/{total_tasks}, "
                    f"rows={total_rows}, cache_reused_rows={reused_rows}"
                )
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(timed_extract_job_profile, input_data, fallback_record): (
                    cache_key,
                    input_data,
                    group_rows_count,
                )
                for cache_key, input_data, fallback_record, group_rows_count in task_items
            }
            completed_tasks = 0
            for future in as_completed(future_map):
                cache_key, input_data, group_rows_count = future_map[future]
                job_name = clean_text(input_data.get("job_name") or input_data.get("standard_job_name"))
                try:
                    profile, elapsed = future.result()
                except Exception as exc:
                    profile = validate_profile({"standard_job_name": input_data.get("standard_job_name", "")})
                    profile["extract_success"] = False
                    profile["extract_error"] = clean_text(str(exc))
                    profile["job_extract_json"] = json.dumps(profile, ensure_ascii=False)
                    elapsed = 0.0

                cache[cache_key] = profile
                completed_tasks += 1
                print(
                    f"[job_extract] Task {completed_tasks}/{total_tasks} done in {elapsed:.2f}s | "
                    f"job={job_name} | group_rows={group_rows_count} | "
                    f"success={profile.get('extract_success', False)}"
                )
                if completed_tasks == 1 or completed_tasks % log_every == 0 or completed_tasks == total_tasks:
                    print(
                        f"[job_extract] Progress: unique_tasks={completed_tasks}/{total_tasks}, "
                        f"rows={total_rows}, cache_reused_rows={reused_rows}"
                    )

    extracted_rows: List[Dict[str, Any]] = []
    for _, row in working_df.iterrows():
        cache_key = row_cache_keys[row["_source_index"]]
        profile = cache[cache_key]
        extracted_rows.append({"_source_index": row["_source_index"], **convert_profile_to_row(profile)})

    extracted_df = pd.DataFrame(extracted_rows)
    merged_df = merge_extraction_results(working_df, extracted_df, index_col="_source_index")
    return merged_df, extracted_df


def save_extraction_result(merged_df: pd.DataFrame, output_csv_path: str) -> None:
    """保存抽取后的完整结果。"""
    output_path = Path(output_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(output_path, index=False, encoding="utf-8-sig")


def load_table(input_path: str) -> pd.DataFrame:
    """支持从 CSV / Excel 加载输入数据。"""
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str).fillna("")
    return pd.read_csv(path, dtype=str).fillna("")


def process_job_extract(
    df: pd.DataFrame,
    output_csv_path: Optional[str] = None,
    log_every: int = 50,
    max_workers: int = 4,
    group_sample_size: int = DEFAULT_GROUP_SAMPLE_SIZE,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    主流程函数。

    返回：
    - merged_df: 原始数据 + 抽取字段
    - extracted_df: 抽取结果表
    """
    merged_df, extracted_df = batch_extract_job_profiles(
        df=df,
        log_every=log_every,
        max_workers=max_workers,
        group_sample_size=group_sample_size,
    )
    if output_csv_path:
        save_extraction_result(merged_df, output_csv_path)
    return merged_df, extracted_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="岗位文本信息抽取")
    parser.add_argument(
        "--input",
        default="outputs/jobs_dedup_result.csv",
        help="输入岗位数据文件路径，支持 CSV / Excel",
    )
    parser.add_argument(
        "--output",
        default="outputs/jobs_extracted.csv",
        help="输出抽取结果 CSV 文件路径",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=50,
        help="每处理多少个唯一岗位抽取任务打印一次进度",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="并发调用大模型的最大线程数；如果接口限流明显，可调小为 1 或 2",
    )
    parser.add_argument(
        "--group-sample-size",
        type=int,
        default=DEFAULT_GROUP_SAMPLE_SIZE,
        help="每个岗位组最多抽取多少条代表 JD 拼给 LLM；值越大越完整，但单次 prompt 更长",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = load_table(args.input)
    merged_df, extracted_df = process_job_extract(
        df=df,
        output_csv_path=args.output,
        log_every=args.log_every,
        max_workers=args.max_workers,
        group_sample_size=args.group_sample_size,
    )

    success_count = int(merged_df["extract_success"].fillna(False).sum()) if "extract_success" in merged_df.columns else 0
    print("[job_extract] Finished.")
    print(f"[job_extract] Input rows: {len(df)}")
    print(f"[job_extract] Extracted rows: {len(extracted_df)}")
    print(f"[job_extract] Success rows: {success_count}")
    print(f"[job_extract] Output file: {args.output}")


if __name__ == "__main__":
    main()



