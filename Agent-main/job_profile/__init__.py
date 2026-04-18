"""岗位画像：岗位组特征、聚合统计与 LLM 语义补充。

Keep package import lightweight. Some endpoints only need submodules that do not
depend on pandas, so avoid eager imports from aggregator/builder here.
"""

__all__ = [
    "aggregate_job_profile_group",
    "build_job_profile_input_payload",
    "get_standard_job_group",
    "run_job_profile_service",
]


def __getattr__(name):
    if name == "aggregate_job_profile_group":
        from .job_profile_aggregator import aggregate_job_profile_group

        return aggregate_job_profile_group
    if name in {"build_job_profile_input_payload", "get_standard_job_group"}:
        from .job_profile_builder import build_job_profile_input_payload, get_standard_job_group

        return {
            "build_job_profile_input_payload": build_job_profile_input_payload,
            "get_standard_job_group": get_standard_job_group,
        }[name]
    if name == "run_job_profile_service":
        from .job_profile_service import run_job_profile_service

        return run_job_profile_service
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
