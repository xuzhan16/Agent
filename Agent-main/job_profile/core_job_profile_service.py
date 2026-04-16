"""
core_job_profile_service.py

Compose job-side display assets for job_profile.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional

from job_match.match_asset_loader import MatchAssetLoader, clean_text, safe_dict, safe_list


def _requirement_distributions(stats: Dict[str, Any]) -> Dict[str, Any]:
    """Extract frontend chart-friendly requirement distributions."""
    stats = safe_dict(stats)
    return {
        "degree_distribution": deepcopy(safe_list(stats.get("degree_distribution"))),
        "major_distribution": deepcopy(safe_list(stats.get("major_distribution"))),
        "certificate_distribution": deepcopy(safe_list(stats.get("certificate_distribution"))),
        "no_certificate_requirement_ratio": stats.get("no_certificate_requirement_ratio", 0.0),
    }


def _skill_knowledge_snapshot(skill_assets: Dict[str, Any]) -> Dict[str, Any]:
    """Extract knowledge point fields for display."""
    skill_assets = safe_dict(skill_assets)
    return {
        "required_knowledge_points": deepcopy(safe_list(skill_assets.get("required_knowledge_points"))),
        "preferred_knowledge_points": deepcopy(safe_list(skill_assets.get("preferred_knowledge_points"))),
        "knowledge_source": clean_text(skill_assets.get("knowledge_source")),
    }


def build_core_job_profiles(
    loader: Optional[MatchAssetLoader] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Build the core job display list from post-processing assets."""
    asset_loader = loader or MatchAssetLoader()
    profiles: List[Dict[str, Any]] = []

    for core_job in asset_loader.core_jobs():
        job_name = clean_text(core_job.get("standard_job_name"))
        if not job_name:
            continue

        stats = asset_loader.get_requirement_stats(job_name)
        skill_assets = asset_loader.get_skill_assets(job_name)
        profile = {
            "standard_job_name": job_name,
            "sample_count": core_job.get("sample_count") or stats.get("sample_count", 0),
            "job_category": clean_text(core_job.get("job_category") or stats.get("job_category")),
            "job_level_summary": clean_text(core_job.get("job_level_summary") or stats.get("job_level_summary")),
            "display_order": core_job.get("display_order", len(profiles) + 1),
            "selection_reason": clean_text(core_job.get("selection_reason")),
            "mainstream_degree": clean_text(core_job.get("mainstream_degree") or stats.get("mainstream_degree")),
            "mainstream_majors_summary": core_job.get("mainstream_majors_summary")
            or deepcopy(safe_list(stats.get("mainstream_majors"))),
            "mainstream_cert_summary": core_job.get("mainstream_cert_summary")
            or deepcopy(safe_list(stats.get("mainstream_certificates"))),
            "top_skills": deepcopy(safe_list(core_job.get("top_skills"))),
            "degree_gate": clean_text(stats.get("degree_gate")),
            "major_gate_set": deepcopy(safe_list(stats.get("major_gate_set"))),
            "must_have_certificates": deepcopy(safe_list(stats.get("must_have_certificates"))),
            "preferred_certificates": deepcopy(safe_list(stats.get("preferred_certificates"))),
            "source_quality": deepcopy(safe_dict(stats.get("source_quality"))),
        }
        profile.update(_requirement_distributions(stats))
        profile.update(_skill_knowledge_snapshot(skill_assets))
        profiles.append(profile)

    profiles.sort(key=lambda item: int(item.get("display_order") or 9999))
    if limit is not None:
        return profiles[:limit]
    return profiles


def build_target_job_profile_assets(
    standard_job_name: Any,
    loader: Optional[MatchAssetLoader] = None,
) -> Dict[str, Any]:
    """Build target job assets used by job_profile frontend sections."""
    job_name = clean_text(standard_job_name)
    if not job_name:
        return {}

    asset_loader = loader or MatchAssetLoader()
    resolution = asset_loader.resolve_job_name(job_name)
    asset_job_name = clean_text(resolution.get("resolved_standard_job_name")) if resolution.get("asset_found") else job_name
    stats = asset_loader.get_requirement_stats_by_standard_name(asset_job_name)
    skill_assets = asset_loader.get_skill_assets_by_standard_name(asset_job_name)
    if not stats and not skill_assets:
        return {
            "requested_job_name": job_name,
            "standard_job_name": asset_job_name,
            "resolved_standard_job_name": clean_text(resolution.get("resolved_standard_job_name")) or job_name,
            "asset_found": False,
            "resolution_method": clean_text(resolution.get("resolution_method")),
            "resolution_confidence": resolution.get("resolution_confidence", 0.0),
            "asset_resolution": deepcopy(resolution),
            "evaluation_status": "insufficient_asset",
            "message": "当前目标岗位未命中标准岗位画像资产，无法进行完整画像展示。",
            "sample_count": 0,
            "degree_distribution": [],
            "major_distribution": [],
            "certificate_distribution": [],
            "no_certificate_requirement_ratio": 0.0,
            "degree_gate": "",
            "major_gate_set": [],
            "must_have_certificates": [],
            "preferred_certificates": [],
            "required_knowledge_points": [],
            "preferred_knowledge_points": [],
        }

    target_assets = {
        "requested_job_name": job_name,
        "standard_job_name": asset_job_name,
        "resolved_standard_job_name": asset_job_name,
        "asset_found": True,
        "resolution_method": clean_text(resolution.get("resolution_method")),
        "resolution_confidence": resolution.get("resolution_confidence", 0.0),
        "asset_resolution": deepcopy(resolution),
        "evaluation_status": "ok",
        "sample_count": stats.get("sample_count", 0),
        "job_category": clean_text(stats.get("job_category")),
        "job_level_summary": clean_text(stats.get("job_level_summary")),
        "mainstream_degree": clean_text(stats.get("mainstream_degree")),
        "mainstream_degree_ratio": stats.get("mainstream_degree_ratio", 0.0),
        "mainstream_majors": deepcopy(safe_list(stats.get("mainstream_majors"))),
        "mainstream_certificates": deepcopy(safe_list(stats.get("mainstream_certificates"))),
        "degree_gate": clean_text(stats.get("degree_gate")),
        "major_gate_set": deepcopy(safe_list(stats.get("major_gate_set"))),
        "must_have_certificates": deepcopy(safe_list(stats.get("must_have_certificates"))),
        "preferred_certificates": deepcopy(safe_list(stats.get("preferred_certificates"))),
        "source_quality": deepcopy(safe_dict(stats.get("source_quality"))),
    }
    target_assets.update(_requirement_distributions(stats))
    target_assets.update(_skill_knowledge_snapshot(skill_assets))
    return target_assets


def build_job_profile_asset_context(
    standard_job_name: Any,
    project_root: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """Build all job_profile post-processing asset fields with graceful fallback."""
    loader = MatchAssetLoader(project_root=project_root)
    return {
        "core_job_profiles": build_core_job_profiles(loader=loader),
        "target_job_profile_assets": build_target_job_profile_assets(standard_job_name, loader=loader),
        "asset_warnings": deepcopy(loader.warnings),
    }
