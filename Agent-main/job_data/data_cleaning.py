"""
data_cleaning.py — 招聘岗位 Excel 清洗流水线

输入：各数据源导出的 Excel（列名可能是中文别名）。
输出：UTF-8-BOM CSV，列顺序见 clean_job_dataframe 末尾 ordered_columns。

流水线阶段（与 clean_job_dataframe 一致）：
    1. standardize_columns：中文/别名列 → 固定英文键；
    2. preserve_raw_columns：为每核心列保留 *_raw，便于对账；
    3. clean_text / strip_html_tags：去脏字符与 HTML；
    4. normalize_*：职位名、公司名、地址（拆 city/district）、规模、性质、日期；
    5. parse_salary_range：薪资字符串 → 数值 + 统一月薪；
    6. build_duplicate_key / build_record_id：去重键与稳定 ID；
    7. flag_abnormal_rows：质量标记，不丢弃；
    8. drop_obvious_duplicates：按 job_code / job_url / 组合键去重，保留信息更全的一条。

主入口：process_job_excel；命令行见 __main__。
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import html
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd

# ===========================================================================
# 列名映射与读取
# ===========================================================================

# 标准英文字段 → 允许出现的表头别名（含英文自身），命中第一个即映射到标准名
STANDARD_COLUMN_ALIASES: Dict[str, Iterable[str]] = {
    "job_title": ["职位名称", "岗位名称", "职位", "岗位", "job_title"],
    "job_address": ["工作地址", "工作地点", "地址", "城市", "job_address"],
    "salary_range": ["薪资范围", "薪资", "工资范围", "salary_range"],
    "company_name": ["公司全称", "公司名称", "企业名称", "company_name"],
    "industry": ["所属行业", "行业", "industry"],
    "company_size": ["人员规模", "公司规模", "规模", "company_size"],
    "company_type": ["企业性质", "公司类型", "融资阶段", "company_type"],
    "job_code": ["职位编码", "岗位编码", "job_code"],
    "job_description": ["职位描述", "岗位详情", "岗位描述", "工作内容", "job_description"],
    "company_description": ["公司简介", "公司详情", "company_description"],
    "updated_at": ["更新时间", "更新日期", "updated_at"],
    "job_url": ["岗位链接", "岗位来源地址", "职位链接", "job_url"],
}


# 清洗后 DataFrame 保证存在的逻辑列（缺失则在 standardize_columns 中补空列）
REQUIRED_COLUMNS = list(STANDARD_COLUMN_ALIASES.keys())


def load_excel(file_path: str, sheet_name: int | str = 0) -> pd.DataFrame:
    """读取 Excel，全部按字符串读入，避免首轮清洗时类型丢失。"""
    return pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)


# ===========================================================================
# 基础文本清洗
# ===========================================================================


def clean_text(value: object) -> str:
    """统一清洗文本中的空白字符、HTML 实体和常见异常空值。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""

    text = str(value)
    text = html.unescape(text)
    text = text.replace("\u00a0", " ")  # Excel 常见不间断空格
    text = text.replace("\r", " ")
    text = text.replace("\n", " ")
    text = text.replace("\t", " ")
    text = re.sub(r"\s+", " ", text).strip()

    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text


def strip_html_tags(value: object) -> str:
    """去掉 HTML 标签，并保留必要的文本分隔语义。"""
    text = clean_text(value)
    if not text:
        return ""

    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p>", "\n", text)
    text = re.sub(r"(?i)</li>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\r", " ").replace("\t", " ")
    text = re.sub(r"[ \u3000]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()


def standardize_columns(
    df: pd.DataFrame,
    column_aliases: Optional[Dict[str, Iterable[str]]] = None,
) -> pd.DataFrame:
    """将原始中文字段名统一映射为英文。"""
    column_aliases = column_aliases or STANDARD_COLUMN_ALIASES
    current_columns = {str(col).strip(): col for col in df.columns}
    rename_map: Dict[str, str] = {}

    for standard_name, aliases in column_aliases.items():
        for alias in aliases:
            if alias in current_columns:
                rename_map[current_columns[alias]] = standard_name
                break

    result = df.rename(columns=rename_map).copy()
    for col in REQUIRED_COLUMNS:
        if col not in result.columns:
            result[col] = ""
    return result


def preserve_raw_columns(df: pd.DataFrame) -> pd.DataFrame:
    """为核心字段保留原始版本，满足“保留原始字段和清洗后字段”的要求。"""
    result = df.copy()
    for col in REQUIRED_COLUMNS:
        result[f"{col}_raw"] = result[col]
    return result


# ===========================================================================
# 字段级规范化（职位 / 公司 / 地址）
# ===========================================================================


def normalize_company_name(value: object) -> str:
    """规范化公司名称，便于后续去重。"""
    text = clean_text(value)
    if not text:
        return ""

    text = re.sub(r"[()（）【】\[\]]", "", text)
    text = re.sub(r"\s+", "", text)
    return text


def normalize_job_title(value: object) -> str:
    """规范化职位名称，先做轻量规则处理，不做语义归一。"""
    text = clean_text(value)
    if not text:
        return ""

    text = re.sub(r"[()（）【】\[\]]", "", text)
    text = re.sub(r"(急聘|诚聘|直招|校招|社招|双休|五险一金|包吃住|可实习|接受小白)", "", text)
    text = re.sub(r"[\s_/|｜]+", "", text)
    text = text.replace("前端开发", "前端开发工程师")
    text = text.replace("后端开发", "后端开发工程师")
    text = text.replace("测试开发", "测试开发工程师")
    text = text.replace("Java开发", "Java开发工程师")
    text = text.replace("Python开发", "Python开发工程师")
    text = text.replace("实施工程", "实施工程师")
    text = text.replace("技术支持工程", "技术支持工程师")
    return text.strip()


def normalize_job_address(value: object) -> pd.Series:
    """
    规范化工作地址，并粗拆 city / district。
    示例：
    - 广州-天河区 -> city=广州, district=天河区
    - 深圳 / 南山区 -> city=深圳, district=南山区
    """
    text = clean_text(value)
    if not text:
        return pd.Series(
            {
                "job_address_norm": "",
                "city": "",
                "district": "",
            }
        )

    text = re.sub(r"[，,/｜|]+", "-", text)
    text = re.sub(r"\s*-\s*", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")

    parts = [part.strip() for part in text.split("-") if clean_text(part)]
    city = parts[0] if parts else ""
    district = ""
    if len(parts) > 1 and parts[1].lower() not in {"none", "null"}:
        district = parts[1]

    return pd.Series(
        {
            # 最多保留省-市-区三级，避免异常长串
            "job_address_norm": "-".join(parts[:3]),
            "city": city,
            "district": district,
        }
    )


# ===========================================================================
# 薪资解析与月薪折算
# ===========================================================================


def _convert_to_monthly(amount: float, salary_unit: str) -> float:
    """
    将不同计薪单位换算为月薪。
    约定：
    - 日薪：按 21.75 个工作日 / 月
    - 时薪：按 8 小时 / 天、21.75 天 / 月
    - 年薪：按 12 个月
    """
    if salary_unit == "month":
        return amount
    if salary_unit == "day":
        return amount * 21.75
    if salary_unit == "hour":
        return amount * 8 * 21.75
    if salary_unit == "year":
        return amount / 12
    return amount


def parse_salary_range(value: object) -> Dict[str, object]:
    """
    解析薪资范围。

    输出字段：
    - salary_min / salary_max：原计薪单位下的数值
    - salary_unit：month/day/hour/year/negotiable/unknown
    - salary_month_min / salary_month_max：统一折算后的月薪
    """
    text = clean_text(value)
    result = {
        "salary_range_clean": text,
        "salary_min": None,
        "salary_max": None,
        "salary_unit": "unknown",
        "salary_months": 12,
        "salary_month_min": None,
        "salary_month_max": None,
        "is_salary_negotiable": False,
    }

    if not text:
        return result

    if "面议" in text:
        result["salary_unit"] = "negotiable"
        result["is_salary_negotiable"] = True
        return result

    # 「14薪」等：影响年薪折算时的月数假设，先抽出再参与区间正则
    months_match = re.search(r"(\d{1,2})\s*薪", text)
    if months_match:
        result["salary_months"] = int(months_match.group(1))

    core_text = re.sub(r"[·•]?\s*\d{1,2}\s*薪", "", text)

    # 计薪单位推断顺序：日 / 小时 / 年，其余默认按月
    if re.search(r"元\s*/\s*(天|日)", core_text):
        salary_unit = "day"
    elif re.search(r"元\s*/\s*(小时|时)", core_text):
        salary_unit = "hour"
    elif re.search(r"(年薪|/年|元/年|万/年)", core_text):
        salary_unit = "year"
    else:
        salary_unit = "month"
    result["salary_unit"] = salary_unit

    range_match = re.search(
        r"(\d+(?:\.\d+)?)\s*[-~至]\s*(\d+(?:\.\d+)?)\s*(万|千|元)?",
        core_text,
    )
    single_match = re.search(r"(\d+(?:\.\d+)?)\s*(万|千|元)", core_text)

    low = None
    high = None
    unit_token = ""

    if range_match:
        low = float(range_match.group(1))
        high = float(range_match.group(2))
        unit_token = range_match.group(3) or ""
    elif single_match:
        low = float(single_match.group(1))
        high = low
        unit_token = single_match.group(2) or ""
    else:
        # 无明确「x-y」格式时，退化为按出现顺序取前两个数字
        number_matches = re.findall(r"\d+(?:\.\d+)?", core_text)
        if len(number_matches) >= 2:
            low = float(number_matches[0])
            high = float(number_matches[1])
        elif len(number_matches) == 1:
            low = float(number_matches[0])
            high = low

        if "万" in core_text:
            unit_token = "万"
        elif "千" in core_text:
            unit_token = "千"
        elif "元" in core_text:
            unit_token = "元"

    if low is None or high is None:
        return result

    multiplier = 1.0
    if unit_token == "万":
        multiplier = 10000.0
    elif unit_token == "千":
        multiplier = 1000.0

    low *= multiplier
    high *= multiplier

    result["salary_min"] = round(low, 2)
    result["salary_max"] = round(high, 2)
    result["salary_month_min"] = round(_convert_to_monthly(low, salary_unit), 2)
    result["salary_month_max"] = round(_convert_to_monthly(high, salary_unit), 2)
    return result


# ===========================================================================
# 公司规模 / 企业性质 / 更新时间
# ===========================================================================


def normalize_company_size(value: object) -> Dict[str, object]:
    """规范化公司规模。"""
    text = clean_text(value)
    result = {
        "company_size_norm": "",
        "company_size_min": None,
        "company_size_max": None,
    }
    if not text:
        return result

    if re.search(r"(\d+)\s*[-~至]\s*(\d+)\s*人", text):
        match = re.search(r"(\d+)\s*[-~至]\s*(\d+)\s*人", text)
        low = int(match.group(1))
        high = int(match.group(2))
        result["company_size_norm"] = f"{low}-{high}人"
        result["company_size_min"] = low
        result["company_size_max"] = high
        return result

    if re.search(r"(\d+)\s*人以上", text):
        match = re.search(r"(\d+)\s*人以上", text)
        low = int(match.group(1))
        result["company_size_norm"] = f"{low}人以上"
        result["company_size_min"] = low
        return result

    if re.search(r"少于\s*(\d+)\s*人", text):
        match = re.search(r"少于\s*(\d+)\s*人", text)
        high = int(match.group(1))
        result["company_size_norm"] = f"少于{high}人"
        result["company_size_max"] = high
        return result

    if re.search(r"(\d+)\s*人以下", text):
        match = re.search(r"(\d+)\s*人以下", text)
        high = int(match.group(1))
        result["company_size_norm"] = f"{high}人以下"
        result["company_size_max"] = high
        return result

    result["company_size_norm"] = text
    return result


def normalize_company_type(value: object) -> str:
    """
    规范化企业性质/公司类型。
    兼容常见“企业性质”和“融资阶段”混用的情况。
    """
    text = clean_text(value)
    if not text:
        return ""

    # 按列表顺序优先匹配：融资阶段与「国企/民营」等性质混写时先命中具体子串
    rules = [
        ("不需要融资", "不需要融资"),
        ("未融资", "未融资"),
        ("天使轮", "天使轮"),
        ("A轮", "A轮"),
        ("B轮", "B轮"),
        ("C轮", "C轮"),
        ("D轮", "D轮及以上"),
        ("上市", "上市公司"),
        ("国企", "国企"),
        ("央企", "央企"),
        ("民营", "民营"),
        ("民企", "民营"),
        ("私企", "民营"),
        ("合资", "合资"),
        ("外资", "外资"),
        ("外商独资", "外资"),
        ("事业单位", "事业单位"),
        ("政府", "政府/公共机构"),
        ("高校", "高校/院校"),
        ("学校", "高校/院校"),
    ]

    for keyword, normalized in rules:
        if keyword in text:
            return normalized
    return text


def normalize_updated_at(value: object, today: Optional[date] = None) -> str:
    """将更新时间尽量标准化为 YYYY-MM-DD。"""
    text = clean_text(value)
    if not text:
        return ""

    today = today or date.today()

    def build_safe_date(year: int, month: int, day: int) -> Optional[date]:
        """安全构造日期，避免 2月29日 这类无年份闰日数据在非闰年直接抛异常。"""
        if month < 1 or month > 12:
            return None
        max_day = calendar.monthrange(year, month)[1]
        safe_day = min(max(day, 1), max_day)
        return date(year, month, safe_day)

    if re.match(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$", text):
        parsed_ts = pd.to_datetime(text, errors="coerce")
        return parsed_ts.strftime("%Y-%m-%d") if pd.notna(parsed_ts) else ""

    year_month_day_match = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", text)
    if year_month_day_match:
        parsed = build_safe_date(
            int(year_month_day_match.group(1)),
            int(year_month_day_match.group(2)),
            int(year_month_day_match.group(3)),
        )
        return parsed.isoformat() if parsed else ""

    month_day_match = re.search(r"(\d{1,2})月(\d{1,2})日", text)
    if month_day_match:
        month = int(month_day_match.group(1))
        day = int(month_day_match.group(2))
        parsed = build_safe_date(today.year, month, day)
        if not parsed:
            return ""
        # 无年份时：若解析结果比「今天+30天」还晚，视为去年同日（处理跨年爬数）
        if parsed > today + timedelta(days=30):
            fallback_parsed = build_safe_date(today.year - 1, month, day)
            if fallback_parsed:
                parsed = fallback_parsed
        return parsed.isoformat()

    parsed_ts = pd.to_datetime(text, errors="coerce")
    if pd.notna(parsed_ts):
        return parsed_ts.strftime("%Y-%m-%d")
    return ""


# ===========================================================================
# 去重键、记录 ID、完整度评分
# ===========================================================================

def build_duplicate_key(row: pd.Series) -> str:
    """构造基于 公司名 + 职位名 + 地址 的去重键。"""
    parts = [
        clean_text(row.get("company_name_norm", "")),
        clean_text(row.get("job_title_norm", "")),
        clean_text(row.get("job_address_norm", "")),
    ]
    return "||".join(parts)


def build_record_id(row: pd.Series) -> str:
    """为每条数据生成稳定 record_id，便于后续入库。"""
    base_text = "||".join(
        [
            clean_text(row.get("job_code", "")),
            clean_text(row.get("job_url", "")),
            clean_text(row.get("company_name_norm", "")),
            clean_text(row.get("job_title_norm", "")),
            clean_text(row.get("job_address_norm", "")),
        ]
    )
    return hashlib.md5(base_text.encode("utf-8")).hexdigest()[:24]


def calculate_completeness_score(row: pd.Series) -> int:
    """用于去重时优先保留信息更完整的记录。"""
    fields = [
        "job_code",
        "job_url",
        "job_title",
        "job_address",
        "salary_range",
        "company_name",
        "industry",
        "job_description_clean",
        "company_description_clean",
        "updated_at_std",
    ]
    return sum(1 for field in fields if clean_text(row.get(field, "")))


# ===========================================================================
# 异常标记与分层去重
# ===========================================================================


def flag_abnormal_rows(df: pd.DataFrame) -> pd.DataFrame:
    """标记明显异常值，便于后续人工排查。"""
    result = df.copy()
    abnormal_reasons = []

    for _, row in result.iterrows():
        reasons = []

        if not clean_text(row.get("job_title")):
            reasons.append("missing_job_title")
        if not clean_text(row.get("company_name")):
            reasons.append("missing_company_name")
        if not clean_text(row.get("job_description_clean")):
            reasons.append("missing_job_description")
        if (
            row.get("salary_month_min") is not None
            and row.get("salary_month_max") is not None
            and float(row["salary_month_min"]) > float(row["salary_month_max"])
        ):
            reasons.append("salary_min_gt_salary_max")
        if clean_text(row.get("job_url")) and not re.match(r"^https?://", row["job_url"]):
            reasons.append("invalid_job_url")
        if len(clean_text(row.get("job_description_clean"))) < 10:
            reasons.append("job_description_too_short")

        abnormal_reasons.append(",".join(reasons))

    result["abnormal_reasons"] = abnormal_reasons
    result["is_abnormal"] = result["abnormal_reasons"].apply(bool)
    return result


def drop_obvious_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    去除明显重复记录。

    去重优先级：
    1. 职位编码重复
    2. 岗位链接重复
    3. 公司名 + 职位名 + 地址重复
    """
    result = df.copy()
    result["completeness_score"] = result.apply(calculate_completeness_score, axis=1)
    result = result.sort_values(
        by=["completeness_score", "updated_at_std"],
        ascending=[False, False],
        na_position="last",
    ).copy()

    result["duplicate_rule"] = ""
    result["is_duplicate_removed"] = False

    # 规则 1：职位编码重复（同一数据源内 job_code 常唯一）
    job_code_mask = result["job_code"].astype(str).str.strip() != ""
    duplicate_mask = result.loc[job_code_mask].duplicated(subset=["job_code"], keep="first")
    duplicate_index = result.loc[job_code_mask].index[duplicate_mask]
    result.loc[duplicate_index, "duplicate_rule"] = "duplicate_by_job_code"
    result.loc[duplicate_index, "is_duplicate_removed"] = True

    # 规则 2：岗位链接重复（跨表合并时常用）
    remaining = result["is_duplicate_removed"] == False
    job_url_mask = remaining & (result["job_url"].astype(str).str.strip() != "")
    duplicate_mask = result.loc[job_url_mask].duplicated(subset=["job_url"], keep="first")
    duplicate_index = result.loc[job_url_mask].index[duplicate_mask]
    result.loc[duplicate_index, "duplicate_rule"] = "duplicate_by_job_url"
    result.loc[duplicate_index, "is_duplicate_removed"] = True

    # 规则 3：规范化后的公司+职位+地址组合重复（编码/链接缺失时的兜底）
    remaining = result["is_duplicate_removed"] == False
    core_key_mask = remaining & (result["duplicate_core_key"].astype(str).str.strip() != "||")
    duplicate_mask = result.loc[core_key_mask].duplicated(subset=["duplicate_core_key"], keep="first")
    duplicate_index = result.loc[core_key_mask].index[duplicate_mask]
    result.loc[duplicate_index, "duplicate_rule"] = "duplicate_by_company_title_address"
    result.loc[duplicate_index, "is_duplicate_removed"] = True

    # 最终仅输出去重后的数据
    result = result[result["is_duplicate_removed"] == False].copy()
    result = result.sort_values(by="source_row_no").reset_index(drop=True)
    return result


# ===========================================================================
# 核心组装与对外 API
# ===========================================================================


def clean_job_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    核心清洗函数：输入原始 DataFrame（任意列名），输出列顺序固定的宽表。

    注意：is_abnormal 仅标记不删行；物理删除仅发生在 drop_obvious_duplicates。
    """
    result = standardize_columns(df)
    result = preserve_raw_columns(result)
    result = result.copy()
    result["source_row_no"] = range(1, len(result) + 1)

    # 核心列先做 clean_text（与 _raw 快照已分离）
    for col in REQUIRED_COLUMNS:
        result[col] = result[col].apply(clean_text)

    # 保留清洗后的纯文本字段
    result["job_description_clean"] = result["job_description"].apply(strip_html_tags)
    result["company_description_clean"] = result["company_description"].apply(strip_html_tags)

    # 规范化职位名称 / 公司名 / 地址
    result["job_title_norm"] = result["job_title"].apply(normalize_job_title)
    result["company_name_norm"] = result["company_name"].apply(normalize_company_name)
    address_df = result["job_address"].apply(normalize_job_address)
    result = pd.concat([result, address_df], axis=1)

    # 薪资解析
    salary_df = pd.DataFrame(result["salary_range"].apply(parse_salary_range).tolist())
    result = pd.concat([result, salary_df], axis=1)

    # 规范化公司规模 / 企业性质
    company_size_df = pd.DataFrame(result["company_size"].apply(normalize_company_size).tolist())
    result = pd.concat([result, company_size_df], axis=1)
    result["company_type_norm"] = result["company_type"].apply(normalize_company_type)

    # 更新时间标准化
    result["updated_at_std"] = result["updated_at"].apply(normalize_updated_at)

    # 去重辅助字段
    result["duplicate_core_key"] = result.apply(build_duplicate_key, axis=1)
    result["record_id"] = result.apply(build_record_id, axis=1)

    result = flag_abnormal_rows(result)

    # 去重会丢弃部分行；abnormal 信息在去重前已写入
    result = drop_obvious_duplicates(result)

    # 输出列顺序：主键与业务列在前，辅助/诊断列在后；未在表中的列追加在末尾
    ordered_columns = [
        "record_id",
        "source_row_no",
        "job_title_raw",
        "job_title",
        "job_title_norm",
        "job_address_raw",
        "job_address",
        "job_address_norm",
        "city",
        "district",
        "salary_range_raw",
        "salary_range",
        "salary_range_clean",
        "salary_min",
        "salary_max",
        "salary_unit",
        "salary_months",
        "salary_month_min",
        "salary_month_max",
        "is_salary_negotiable",
        "company_name_raw",
        "company_name",
        "company_name_norm",
        "industry_raw",
        "industry",
        "company_size_raw",
        "company_size",
        "company_size_norm",
        "company_size_min",
        "company_size_max",
        "company_type_raw",
        "company_type",
        "company_type_norm",
        "job_code_raw",
        "job_code",
        "job_description_raw",
        "job_description",
        "job_description_clean",
        "company_description_raw",
        "company_description",
        "company_description_clean",
        "updated_at_raw",
        "updated_at",
        "updated_at_std",
        "job_url_raw",
        "job_url",
        "duplicate_core_key",
        "duplicate_rule",
        "is_abnormal",
        "abnormal_reasons",
    ]

    existing_columns = [col for col in ordered_columns if col in result.columns]
    remaining_columns = [col for col in result.columns if col not in existing_columns]
    return result[existing_columns + remaining_columns]


def process_job_excel(
    input_excel_path: str,
    output_csv_path: str,
    sheet_name: int | str = 0,
) -> pd.DataFrame:
    """
    主处理函数：读 Excel → clean_job_dataframe → 写 CSV（utf-8-sig 便于 Excel 打开）。

    返回与写出内容一致的 DataFrame，便于脚本链式调用或检查行数。
    """
    raw_df = load_excel(input_excel_path, sheet_name=sheet_name)
    clean_df = clean_job_dataframe(raw_df)

    output_path = Path(output_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    clean_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    return clean_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="招聘岗位数据基础清洗脚本")
    parser.add_argument(
        "--input",
        default="20260226105856_457.xls",
        help="输入 Excel 文件路径",
    )
    parser.add_argument(
        "--output",
        default="outputs/jobs_cleaned.csv",
        help="输出清洗后 CSV 文件路径",
    )
    parser.add_argument(
        "--sheet-name",
        default=0,
        help="Excel sheet 名称或索引",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    cleaned_df = process_job_excel(
        input_excel_path=args.input,
        output_csv_path=args.output,
        sheet_name=args.sheet_name,
    )

    print("Data cleaning finished.")
    print(f"Input file: {args.input}")
    print(f"Output file: {args.output}")
    print(f"Row count after cleaning: {len(cleaned_df)}")
    print(f"Abnormal rows kept: {int(cleaned_df['is_abnormal'].sum())}")



