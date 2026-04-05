"""
student_profile_scorer.py

学生就业能力画像模块中的规则评分器。

职责：
1. 根据 student_profile_builder 输出的 profile_input_payload
   计算 profile_completeness_score；
2. 计算 competitiveness_score 的基础分；
3. 输出结构化评分结果和可解释评分原因；
4. 不依赖复杂机器学习框架。

输入数据来源：
- student_profile_builder.py 生成的 profile_input_payload JSON

输出结构：
{
  "profile_completeness_score": 0-100,
  "competitiveness_base_score": 0-100,
  "score_level": "...",
  "completeness_detail": {...},
  "competitiveness_detail": {...},
  "score_reasons": {
    "strengths": [...],
    "weaknesses": [...],
    "suggestions": [...]
  }
}
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_PROFILE_INPUT_PATH = Path("outputs/state/student_profile_input_payload.json")
DEFAULT_SCORE_OUTPUT_PATH = Path("outputs/state/student_profile_score_result.json")


@dataclass
class CompletenessDetail:
    """画像完整度评分明细。"""

    basic_info_score: float = 0.0
    education_score: float = 0.0
    skill_score: float = 0.0
    project_score: float = 0.0
    internship_score: float = 0.0
    qualification_score: float = 0.0
    intention_score: float = 0.0


@dataclass
class CompetitivenessDetail:
    """基础竞争力评分明细。"""

    education_base_score: float = 0.0
    skill_base_score: float = 0.0
    tool_base_score: float = 0.0
    project_base_score: float = 0.0
    internship_base_score: float = 0.0
    qualification_base_score: float = 0.0
    occupation_focus_score: float = 0.0
    domain_bonus_score: float = 0.0


@dataclass
class ScoreReasons:
    """评分解释文本。"""

    strengths: List[str] = field(default_factory=list)
    weaknesses: List[str] = field(default_factory=list)
    suggestions: List[str] = field(default_factory=list)


@dataclass
class StudentProfileScoreResult:
    """最终评分结果。"""

    profile_completeness_score: float = 0.0
    competitiveness_base_score: float = 0.0
    score_level: str = ""
    completeness_detail: CompletenessDetail = field(default_factory=CompletenessDetail)
    competitiveness_detail: CompetitivenessDetail = field(default_factory=CompetitivenessDetail)
    score_reasons: ScoreReasons = field(default_factory=ScoreReasons)


def load_profile_input_payload(input_path: str | Path) -> Dict[str, Any]:
    """读取 student_profile_builder 输出的中间特征 JSON。"""
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"profile_input_payload not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def _safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if value is None or value == "":
        return []
    return [value]


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _clip_score(value: float, min_value: float = 0.0, max_value: float = 100.0) -> float:
    return round(max(min_value, min(max_value, value)), 2)


def _ratio_score(valid_count: int, total_count: int, max_score: float) -> float:
    if total_count <= 0:
        return 0.0
    return round(max_score * valid_count / total_count, 2)


def score_basic_info_completeness(basic_info: Dict[str, Any]) -> Tuple[float, List[str], List[str]]:
    """评分基本信息完整度。"""
    key_fields = ["name", "phone", "email", "school", "major", "degree", "graduation_year"]
    valid_count = sum(1 for field_name in key_fields if _clean_text(basic_info.get(field_name)))
    score = _ratio_score(valid_count, len(key_fields), 20.0)

    strengths = []
    weaknesses = []
    if valid_count >= 6:
        strengths.append("基本信息较完整，已包含身份、教育背景和联系方式。")
    else:
        missing_fields = [
            field_name
            for field_name in key_fields
            if not _clean_text(basic_info.get(field_name))
        ]
        weaknesses.append(f"基本信息不完整，缺失字段: {', '.join(missing_fields)}。")
    return score, strengths, weaknesses


def score_education_completeness(normalized_education: Dict[str, Any]) -> Tuple[float, List[str], List[str]]:
    """评分教育信息完整度。"""
    key_fields = ["degree", "school", "major_std", "graduation_year"]
    valid_count = sum(1 for field_name in key_fields if _clean_text(normalized_education.get(field_name)))
    score = _ratio_score(valid_count, len(key_fields), 15.0)

    strengths = []
    weaknesses = []
    if valid_count == len(key_fields):
        strengths.append("教育信息完整，学历、学校、专业和毕业年份均已明确。")
    else:
        weaknesses.append("教育信息存在缺口，建议补充标准专业、学历层次或毕业时间。")
    return score, strengths, weaknesses


def score_skill_completeness(normalized_profile: Dict[str, Any]) -> Tuple[float, List[str], List[str]]:
    """评分技能信息完整度。"""
    hard_skills = [_clean_text(item) for item in _safe_list(normalized_profile.get("hard_skills")) if _clean_text(item)]
    tool_skills = [_clean_text(item) for item in _safe_list(normalized_profile.get("tool_skills")) if _clean_text(item)]
    total_skill_count = len(set(hard_skills + tool_skills))

    if total_skill_count >= 8:
        score = 20.0
    elif total_skill_count >= 5:
        score = 16.0
    elif total_skill_count >= 3:
        score = 12.0
    elif total_skill_count >= 1:
        score = 8.0
    else:
        score = 0.0

    strengths = []
    weaknesses = []
    if total_skill_count >= 5:
        strengths.append(f"技能信息较丰富，已识别 {total_skill_count} 项技能/工具。")
    elif total_skill_count >= 1:
        strengths.append(f"已有明确技能信息，当前识别 {total_skill_count} 项。")
        weaknesses.append("技能数量偏少，建议补充与目标岗位直接相关的工具栈和硬技能。")
    else:
        weaknesses.append("未识别到有效技能字段，画像完整度和竞争力都会受到影响。")
    return score, strengths, weaknesses


def score_practice_completeness(practice_profile: Dict[str, Any]) -> Tuple[Dict[str, float], List[str], List[str]]:
    """评分项目/实习/竞赛证书等实践信息完整度。"""
    project_count = int(practice_profile.get("project_count") or 0)
    internship_count = int(practice_profile.get("internship_count") or 0)
    award_count = int(practice_profile.get("award_count") or 0)

    project_score = 15.0 if project_count >= 2 else 12.0 if project_count == 1 else 0.0
    internship_score = 15.0 if internship_count >= 2 else 12.0 if internship_count == 1 else 0.0
    qualification_score = 10.0 if award_count >= 1 else 0.0

    strengths = []
    weaknesses = []
    if project_count > 0:
        strengths.append(f"已有项目经历 {project_count} 段，可作为能力证明材料。")
    else:
        weaknesses.append("缺少项目经历，建议补充课程项目、竞赛项目或个人作品。")

    if internship_count > 0:
        strengths.append(f"已有实习经历 {internship_count} 段，有助于证明岗位实践能力。")
    else:
        weaknesses.append("缺少实习经历，建议争取相关岗位实习或校企实践项目。")

    if award_count > 0:
        strengths.append("有竞赛/获奖记录，可增强简历背书。")
    else:
        weaknesses.append("暂未识别到竞赛/获奖信息，如有相关经历建议补充。")

    return {
        "project_score": project_score,
        "internship_score": internship_score,
        "qualification_score": qualification_score,
    }, strengths, weaknesses


def score_intention_completeness(
    explicit_profile: Dict[str, Any],
    normalized_profile: Dict[str, Any],
) -> Tuple[float, List[str], List[str]]:
    """评分求职意向明确度。"""
    target_job_intention = _clean_text(explicit_profile.get("target_job_intention"))
    occupation_hints = [
        _clean_text(item)
        for item in _safe_list(normalized_profile.get("occupation_hints"))
        if _clean_text(item)
    ]

    if target_job_intention and occupation_hints:
        score = 5.0
        strengths = [f"求职意向较明确，方向聚焦在 {', '.join(occupation_hints[:3])}。"]
        weaknesses = []
    elif target_job_intention or occupation_hints:
        score = 3.0
        strengths = ["已有一定职业方向信号，但表达还可以更明确。"]
        weaknesses = ["建议在简历中补充更明确的目标岗位或岗位方向。"]
    else:
        score = 0.0
        strengths = []
        weaknesses = ["求职意向不明确，建议补充目标岗位方向。"]
    return score, strengths, weaknesses


def calculate_profile_completeness_score(payload: Dict[str, Any]) -> Dict[str, Any]:
    """计算 profile_completeness_score 及明细。"""
    basic_info = _safe_dict(payload.get("basic_info"))
    normalized_education = _safe_dict(payload.get("normalized_education"))
    explicit_profile = _safe_dict(payload.get("explicit_profile"))
    normalized_profile = _safe_dict(payload.get("normalized_profile"))
    practice_profile = _safe_dict(payload.get("practice_profile"))

    basic_score, basic_strengths, basic_weaknesses = score_basic_info_completeness(basic_info)
    education_score, edu_strengths, edu_weaknesses = score_education_completeness(normalized_education)
    skill_score, skill_strengths, skill_weaknesses = score_skill_completeness(normalized_profile)
    practice_scores, practice_strengths, practice_weaknesses = score_practice_completeness(practice_profile)
    intention_score, intention_strengths, intention_weaknesses = score_intention_completeness(
        explicit_profile,
        normalized_profile,
    )

    detail = CompletenessDetail(
        basic_info_score=basic_score,
        education_score=education_score,
        skill_score=skill_score,
        project_score=practice_scores["project_score"],
        internship_score=practice_scores["internship_score"],
        qualification_score=practice_scores["qualification_score"],
        intention_score=intention_score,
    )
    total_score = _clip_score(
        detail.basic_info_score
        + detail.education_score
        + detail.skill_score
        + detail.project_score
        + detail.internship_score
        + detail.qualification_score
        + detail.intention_score
    )

    return {
        "profile_completeness_score": total_score,
        "completeness_detail": asdict(detail),
        "strengths": (
            basic_strengths
            + edu_strengths
            + skill_strengths
            + practice_strengths
            + intention_strengths
        ),
        "weaknesses": (
            basic_weaknesses
            + edu_weaknesses
            + skill_weaknesses
            + practice_weaknesses
            + intention_weaknesses
        ),
    }


def score_education_competitiveness(normalized_education: Dict[str, Any]) -> Tuple[float, List[str], List[str]]:
    """根据学历层次和专业清晰度给基础竞争力加分。"""
    degree = _clean_text(normalized_education.get("degree"))
    major_std = _clean_text(normalized_education.get("major_std"))

    degree_score_map = {
        "博士": 18.0,
        "硕士": 16.0,
        "本科": 13.0,
        "大专": 10.0,
        "专科": 10.0,
        "高中": 6.0,
    }
    score = 0.0
    for key, value in degree_score_map.items():
        if key in degree:
            score = value
            break

    if major_std:
        score += 2.0

    strengths = []
    weaknesses = []
    if score >= 15:
        strengths.append(f"学历背景具备一定竞争力，当前学历为{degree or '未注明'}。")
    elif score > 0:
        strengths.append(f"已具备明确学历背景：{degree}。")
    else:
        weaknesses.append("学历层次未明确，基础竞争力评分受影响。")

    if not major_std:
        weaknesses.append("专业方向未标准化成功，建议补充更明确的专业信息。")
    return _clip_score(score, 0.0, 20.0), strengths, weaknesses


def score_skill_competitiveness(normalized_profile: Dict[str, Any]) -> Tuple[Dict[str, float], List[str], List[str]]:
    """根据技能数量和工具栈数量给基础竞争力加分。"""
    hard_skills = [
        _clean_text(item)
        for item in _safe_list(normalized_profile.get("hard_skills"))
        if _clean_text(item)
    ]
    tool_skills = [
        _clean_text(item)
        for item in _safe_list(normalized_profile.get("tool_skills"))
        if _clean_text(item)
    ]

    hard_skill_count = len(set(hard_skills))
    tool_skill_count = len(set(tool_skills))

    hard_score = min(25.0, hard_skill_count * 4.0)
    tool_score = min(10.0, tool_skill_count * 2.5)

    strengths = []
    weaknesses = []
    if hard_skill_count >= 5:
        strengths.append(f"硬技能储备较好，已覆盖 {hard_skill_count} 项技能。")
    elif hard_skill_count >= 2:
        strengths.append(f"已有 {hard_skill_count} 项硬技能，但仍有扩展空间。")
        weaknesses.append("硬技能数量尚不充分，建议围绕目标岗位补强核心技能。")
    else:
        weaknesses.append("硬技能储备偏弱，建议优先补充岗位要求中的核心技能。")

    if tool_skill_count >= 3:
        strengths.append(f"工具栈较丰富，已覆盖 {tool_skill_count} 类工具。")
    elif tool_skill_count >= 1:
        strengths.append(f"已有工具技能 {tool_skill_count} 项。")
    else:
        weaknesses.append("工具栈信息不足，建议补充常用软件、开发工具或分析工具。")

    return {
        "skill_base_score": _clip_score(hard_score, 0.0, 25.0),
        "tool_base_score": _clip_score(tool_score, 0.0, 10.0),
    }, strengths, weaknesses


def score_practice_competitiveness(
    practice_profile: Dict[str, Any],
    normalized_profile: Dict[str, Any],
) -> Tuple[Dict[str, float], List[str], List[str]]:
    """根据项目/实习/证书奖项/职业聚焦度计算基础竞争力。"""
    project_count = int(practice_profile.get("project_count") or 0)
    internship_count = int(practice_profile.get("internship_count") or 0)
    award_count = int(practice_profile.get("award_count") or 0)
    has_target_job_intention = bool(practice_profile.get("has_target_job_intention"))
    practice_tags = [
        _clean_text(item)
        for item in _safe_list(practice_profile.get("practice_tags"))
        if _clean_text(item)
    ]

    qualification_tags = [
        _clean_text(item)
        for item in _safe_list(normalized_profile.get("qualification_tags"))
        if _clean_text(item)
    ]
    occupation_hints = [
        _clean_text(item)
        for item in _safe_list(normalized_profile.get("occupation_hints"))
        if _clean_text(item)
    ]
    domain_tags = [
        _clean_text(item)
        for item in _safe_list(normalized_profile.get("domain_tags"))
        if _clean_text(item)
    ]

    project_score = min(15.0, project_count * 8.0)
    if any("项目:建模算法" == tag for tag in practice_tags):
        project_score = min(15.0, project_score + 2.0)
    if any("项目:工程开发" == tag for tag in practice_tags):
        project_score = min(15.0, project_score + 2.0)

    internship_score = min(20.0, internship_count * 10.0)
    if any("实习:业务协作" == tag for tag in practice_tags):
        internship_score = min(20.0, internship_score + 2.0)

    cert_count = sum(1 for tag in qualification_tags if tag.startswith("证书:"))
    qualification_score = min(10.0, cert_count * 4.0 + award_count * 3.0)

    occupation_focus_score = 5.0 if has_target_job_intention and occupation_hints else 3.0 if occupation_hints else 0.0
    domain_bonus_score = min(10.0, len(set(domain_tags)) * 2.0)

    strengths = []
    weaknesses = []

    if project_count >= 2:
        strengths.append("项目经历数量较充足，对岗位能力证明较有帮助。")
    elif project_count == 1:
        strengths.append("已有项目经历，可作为能力展示基础。")
    else:
        weaknesses.append("项目实践不足，建议补充可量化成果的项目案例。")

    if internship_count >= 2:
        strengths.append("实习经历较丰富，具备一定岗位场景适应基础。")
    elif internship_count == 1:
        strengths.append("已有一段实习经历，对就业竞争力有正向帮助。")
    else:
        weaknesses.append("实习经验不足，建议优先争取与目标方向一致的实习。")

    if qualification_score >= 6:
        strengths.append("证书/获奖背书较好，可增强简历可信度。")
    elif qualification_score > 0:
        strengths.append("已有一定证书或获奖背书。")
    else:
        weaknesses.append("缺少证书或获奖背书，如目标岗位有门槛证书可优先补齐。")

    if occupation_focus_score >= 5:
        strengths.append(f"职业方向较聚焦，当前方向信号为 {', '.join(occupation_hints[:3])}。")
    elif occupation_focus_score > 0:
        strengths.append("已有职业方向信号，但求职目标仍可进一步收敛。")
    else:
        weaknesses.append("职业方向信号不明显，建议明确目标岗位并围绕该方向组织经历。")

    return {
        "project_base_score": _clip_score(project_score, 0.0, 15.0),
        "internship_base_score": _clip_score(internship_score, 0.0, 20.0),
        "qualification_base_score": _clip_score(qualification_score, 0.0, 10.0),
        "occupation_focus_score": _clip_score(occupation_focus_score, 0.0, 5.0),
        "domain_bonus_score": _clip_score(domain_bonus_score, 0.0, 10.0),
    }, strengths, weaknesses


def calculate_competitiveness_base_score(payload: Dict[str, Any]) -> Dict[str, Any]:
    """计算 competitiveness_score 的规则基础分。"""
    normalized_education = _safe_dict(payload.get("normalized_education"))
    normalized_profile = _safe_dict(payload.get("normalized_profile"))
    practice_profile = _safe_dict(payload.get("practice_profile"))

    education_score, edu_strengths, edu_weaknesses = score_education_competitiveness(normalized_education)
    skill_scores, skill_strengths, skill_weaknesses = score_skill_competitiveness(normalized_profile)
    practice_scores, practice_strengths, practice_weaknesses = score_practice_competitiveness(
        practice_profile,
        normalized_profile,
    )

    detail = CompetitivenessDetail(
        education_base_score=education_score,
        skill_base_score=skill_scores["skill_base_score"],
        tool_base_score=skill_scores["tool_base_score"],
        project_base_score=practice_scores["project_base_score"],
        internship_base_score=practice_scores["internship_base_score"],
        qualification_base_score=practice_scores["qualification_base_score"],
        occupation_focus_score=practice_scores["occupation_focus_score"],
        domain_bonus_score=practice_scores["domain_bonus_score"],
    )

    total_score = _clip_score(
        detail.education_base_score
        + detail.skill_base_score
        + detail.tool_base_score
        + detail.project_base_score
        + detail.internship_base_score
        + detail.qualification_base_score
        + detail.occupation_focus_score
        + detail.domain_bonus_score
    )

    return {
        "competitiveness_base_score": total_score,
        "competitiveness_detail": asdict(detail),
        "strengths": edu_strengths + skill_strengths + practice_strengths,
        "weaknesses": edu_weaknesses + skill_weaknesses + practice_weaknesses,
    }


def _infer_score_level(competitiveness_base_score: float) -> str:
    """根据基础竞争力分数给出等级标签。"""
    if competitiveness_base_score >= 85:
        return "A-竞争力较强"
    if competitiveness_base_score >= 70:
        return "B-具备一定竞争力"
    if competitiveness_base_score >= 55:
        return "C-基础可用但需补强"
    return "D-当前竞争力偏弱"


def _build_suggestions(
    completeness_result: Dict[str, Any],
    competitiveness_result: Dict[str, Any],
) -> List[str]:
    """根据扣分原因生成可操作建议。"""
    suggestions = []
    weaknesses = (
        _safe_list(completeness_result.get("weaknesses"))
        + _safe_list(competitiveness_result.get("weaknesses"))
    )
    weakness_text = " ".join(_clean_text(item) for item in weaknesses)

    if "基本信息" in weakness_text:
        suggestions.append("补全姓名、联系方式、学校、专业、学历和毕业年份等基础信息。")
    if "教育信息" in weakness_text or "学历" in weakness_text or "专业" in weakness_text:
        suggestions.append("完善教育背景描述，确保学历、学校、专业名称和毕业时间清晰可解析。")
    if "技能" in weakness_text or "工具栈" in weakness_text:
        suggestions.append("围绕目标岗位补充核心硬技能和常用工具，并在项目/实习中给出使用场景。")
    if "项目" in weakness_text:
        suggestions.append("补充 1-2 个可量化成果的课程项目、竞赛项目或个人作品项目。")
    if "实习" in weakness_text:
        suggestions.append("尽量争取与目标岗位方向一致的实习、校企实践或真实业务协作经历。")
    if "证书" in weakness_text or "获奖" in weakness_text:
        suggestions.append("如目标岗位看重英语、职业资格或竞赛奖项，可优先补充相关证书/获奖经历。")
    if "求职意向" in weakness_text or "职业方向" in weakness_text:
        suggestions.append("明确目标岗位方向，并让技能、项目和实习经历围绕该方向组织表达。")

    if not suggestions:
        suggestions.append("当前画像基础较完整，建议下一步结合目标岗位画像做更细粒度差距分析。")
    return list(dict.fromkeys(suggestions))


def score_student_profile_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """主评分函数：输入 builder payload，输出结构化评分结果。"""
    completeness_result = calculate_profile_completeness_score(payload)
    competitiveness_result = calculate_competitiveness_base_score(payload)

    score_reasons = ScoreReasons(
        strengths=list(
            dict.fromkeys(
                _safe_list(completeness_result.get("strengths"))
                + _safe_list(competitiveness_result.get("strengths"))
            )
        ),
        weaknesses=list(
            dict.fromkeys(
                _safe_list(completeness_result.get("weaknesses"))
                + _safe_list(competitiveness_result.get("weaknesses"))
            )
        ),
        suggestions=_build_suggestions(completeness_result, competitiveness_result),
    )

    result = StudentProfileScoreResult(
        profile_completeness_score=_clip_score(
            float(completeness_result.get("profile_completeness_score") or 0.0)
        ),
        competitiveness_base_score=_clip_score(
            float(competitiveness_result.get("competitiveness_base_score") or 0.0)
        ),
        score_level=_infer_score_level(
            float(competitiveness_result.get("competitiveness_base_score") or 0.0)
        ),
        completeness_detail=CompletenessDetail(
            **_safe_dict(completeness_result.get("completeness_detail"))
        ),
        competitiveness_detail=CompetitivenessDetail(
            **_safe_dict(competitiveness_result.get("competitiveness_detail"))
        ),
        score_reasons=score_reasons,
    )
    return asdict(result)


def score_student_profile_file(
    input_path: str | Path = DEFAULT_PROFILE_INPUT_PATH,
    output_path: Optional[str | Path] = DEFAULT_SCORE_OUTPUT_PATH,
) -> Dict[str, Any]:
    """文件级入口：读取 builder 输出 JSON，计算评分，按需保存结果。"""
    payload = load_profile_input_payload(input_path)
    score_result = score_student_profile_payload(payload)

    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open("w", encoding="utf-8") as f:
            json.dump(score_result, f, ensure_ascii=False, indent=2)

    return score_result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score student profile payload with rule-based scorer"
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_PROFILE_INPUT_PATH),
        help="student_profile_builder 输出的 profile_input_payload JSON 路径",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_SCORE_OUTPUT_PATH),
        help="评分结果 JSON 输出路径",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result_dict = score_student_profile_file(
        input_path=args.input,
        output_path=args.output,
    )
    print(json.dumps(result_dict, ensure_ascii=False, indent=2))
