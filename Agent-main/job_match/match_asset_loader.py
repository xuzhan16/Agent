"""
match_asset_loader.py

Low-risk loader for post-processed match assets.
"""

from __future__ import annotations

import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional

from .job_name_normalizer import resolve_standard_job_name


DEFAULT_ASSET_DIR = Path("outputs/match_assets")
DEFAULT_ALIAS_PATH = Path("configs/job_name_aliases.json")


def clean_text(value: Any) -> str:
    """Normalize empty-ish values to an empty string."""
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ").replace("\u3000", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if text.lower() in {"", "nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text


def safe_dict(value: Any) -> Dict[str, Any]:
    """Return a dict or an empty dict."""
    return value if isinstance(value, dict) else {}


def safe_list(value: Any) -> List[Any]:
    """Return a list or an empty list."""
    return value if isinstance(value, list) else []


def normalize_lookup_key(value: Any) -> str:
    """Compact a job name for tolerant local lookup."""
    text = clean_text(value).lower()
    return re.sub(r"[\s\-_/\\|｜,，;；:：()（）\[\]【】<>《》]+", "", text)


class MatchAssetLoader:
    """Load and query post-processing assets under outputs/match_assets."""

    def __init__(
        self,
        project_root: Optional[str | Path] = None,
        asset_dir: Optional[str | Path] = None,
    ) -> None:
        self.project_root = Path(project_root) if project_root else Path(__file__).resolve().parent.parent
        self.asset_dir = Path(asset_dir) if asset_dir else self.project_root / DEFAULT_ASSET_DIR
        self._requirement_root: Optional[Dict[str, Any]] = None
        self._core_jobs_root: Optional[Dict[str, Any]] = None
        self._skill_assets_root: Optional[Dict[str, Any]] = None
        self._job_name_aliases: Optional[Dict[str, Any]] = None
        self.warnings: List[str] = []

    def _load_json(self, filename: str) -> Dict[str, Any]:
        path = self.asset_dir / filename
        if not path.exists():
            warning = f"后处理资产缺失: {path}"
            if warning not in self.warnings:
                self.warnings.append(warning)
            return {}
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:  # pragma: no cover - defensive fallback
            warning = f"后处理资产读取失败: {path}, {exc}"
            if warning not in self.warnings:
                self.warnings.append(warning)
            return {}
        if not isinstance(data, dict):
            warning = f"后处理资产格式异常: {path}"
            if warning not in self.warnings:
                self.warnings.append(warning)
            return {}
        return data

    @property
    def requirement_root(self) -> Dict[str, Any]:
        if self._requirement_root is None:
            self._requirement_root = self._load_json("job_requirement_stats.json")
        return self._requirement_root

    @property
    def core_jobs_root(self) -> Dict[str, Any]:
        if self._core_jobs_root is None:
            self._core_jobs_root = self._load_json("core_jobs.json")
        return self._core_jobs_root

    @property
    def skill_assets_root(self) -> Dict[str, Any]:
        if self._skill_assets_root is None:
            self._skill_assets_root = self._load_json("job_skill_knowledge_assets.json")
        return self._skill_assets_root

    @property
    def job_name_aliases(self) -> Dict[str, Any]:
        if self._job_name_aliases is None:
            path = self.project_root / DEFAULT_ALIAS_PATH
            if not path.exists():
                self._job_name_aliases = {}
            else:
                try:
                    with path.open("r", encoding="utf-8") as f:
                        loaded = json.load(f)
                    self._job_name_aliases = loaded if isinstance(loaded, dict) else {}
                except Exception as exc:  # pragma: no cover - defensive fallback
                    warning = f"岗位别名配置读取失败: {path}, {exc}"
                    if warning not in self.warnings:
                        self.warnings.append(warning)
                    self._job_name_aliases = {}
        return self._job_name_aliases

    def requirement_jobs(self) -> Dict[str, Any]:
        return safe_dict(self.requirement_root.get("jobs"))

    def skill_jobs(self) -> Dict[str, Any]:
        return safe_dict(self.skill_assets_root.get("jobs"))

    def core_jobs(self) -> List[Dict[str, Any]]:
        return [safe_dict(item) for item in safe_list(self.core_jobs_root.get("jobs"))]

    def all_standard_job_names(self) -> List[str]:
        names = (
            [clean_text(name) for name in self.requirement_jobs().keys() if clean_text(name)]
            + [clean_text(name) for name in self.skill_jobs().keys() if clean_text(name)]
            + [clean_text(item.get("standard_job_name")) for item in self.core_jobs() if clean_text(item.get("standard_job_name"))]
        )
        seen = set()
        result = []
        for name in names:
            if name not in seen:
                seen.add(name)
                result.append(name)
        return result

    def resolve_job_name(self, job_name: Any) -> Dict[str, Any]:
        """Resolve an arbitrary job name to a local standard asset job name."""
        return resolve_standard_job_name(
            raw_job_name=job_name,
            standard_job_names=self.all_standard_job_names(),
            aliases=self.job_name_aliases,
        )

    def _lookup_from_mapping(self, mapping: Dict[str, Any], job_name: Any) -> Dict[str, Any]:
        target = clean_text(job_name)
        if not target or not mapping:
            return {}
        if target in mapping:
            return deepcopy(safe_dict(mapping.get(target)))

        target_key = normalize_lookup_key(target)
        for candidate_name, candidate_value in mapping.items():
            if normalize_lookup_key(candidate_name) == target_key:
                return deepcopy(safe_dict(candidate_value))
        return {}

    def get_requirement_stats(self, job_name: Any) -> Dict[str, Any]:
        """Return requirement stats for a standard job name."""
        return self._lookup_from_mapping(self.requirement_jobs(), job_name)

    def get_skill_assets(self, job_name: Any) -> Dict[str, Any]:
        """Return skill knowledge assets for a standard job name."""
        return self._lookup_from_mapping(self.skill_jobs(), job_name)

    def get_requirement_stats_by_standard_name(self, standard_job_name: Any) -> Dict[str, Any]:
        """Return requirement stats without performing alias resolution."""
        return self._lookup_from_mapping(self.requirement_jobs(), standard_job_name)

    def get_skill_assets_by_standard_name(self, standard_job_name: Any) -> Dict[str, Any]:
        """Return skill assets without performing alias resolution."""
        return self._lookup_from_mapping(self.skill_jobs(), standard_job_name)

    def has_any_asset(self) -> bool:
        """Whether at least one asset file is available and has content."""
        return bool(self.requirement_jobs() or self.core_jobs() or self.skill_jobs())
