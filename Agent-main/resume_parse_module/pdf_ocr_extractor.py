"""
PDF 首页顶部 OCR 兜底抽取。

该模块只负责把 PDF 第一页顶部区域渲染为图片并执行可选 OCR。
OCR 结果只作为联系方式候选文本，具体 email/phone 校验仍由 resume_parser 负责。
"""

from __future__ import annotations

import tempfile
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

ORIGINAL_USER_HOME = Path.home()


def _project_ocr_cache_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "outputs" / "ocr_cache"


def _is_writable_directory(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".write_test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except Exception:
        return False


def _ensure_ocr_runtime_environment() -> None:
    """
    Paddle 在 Windows 下会写入用户目录的 .cache/paddle。
    部分机器该目录不可写，导致 OCR 依赖已安装但导入 paddle 失败。
    这里把 OCR 相关缓存收敛到项目 outputs/ocr_cache，避免污染或受限于用户目录。
    """
    cache_root = _project_ocr_cache_dir()
    cache_root.mkdir(parents=True, exist_ok=True)

    # PaddleOCR 2.x 默认使用 os.path.expanduser("~/.paddleocr") 作为模型目录。
    # 即使 PADDLEOCR_HOME 已设置，它仍可能回到 C:\Users\xxx\.paddleocr。
    # 因此 OCR 执行期间统一把 HOME/USERPROFILE 收敛到项目缓存目录，避免
    # 用户目录权限、半下载模型和多环境污染导致 OCR 初始化不稳定。
    home_dir = cache_root / "home"
    home_dir.mkdir(parents=True, exist_ok=True)
    os.environ["HOME"] = str(home_dir)
    os.environ["USERPROFILE"] = str(home_dir)

    os.environ.setdefault("PADDLE_HOME", str(cache_root / "paddle_home"))
    os.environ.setdefault("XDG_CACHE_HOME", str(cache_root / "xdg"))
    os.environ.setdefault("PADDLEOCR_HOME", str(home_dir / ".paddleocr"))
    os.environ.setdefault("TEMP", str(cache_root / "tmp"))
    os.environ.setdefault("TMP", str(cache_root / "tmp"))
    (cache_root / "tmp").mkdir(parents=True, exist_ok=True)
    tempfile.tempdir = str(cache_root / "tmp")
    # 避免无网络环境下每次 OCR 初始化都先做模型源联网探测。
    os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")


def _model_dir_has_inference_files(model_dir: Path) -> bool:
    if not model_dir.exists() or not model_dir.is_dir():
        return False
    expected_names = {
        "inference.pdmodel",
        "inference.pdiparams",
        "inference.json",
        "model.pdmodel",
        "model.pdiparams",
    }
    try:
        return any(child.name in expected_names for child in model_dir.iterdir())
    except Exception:
        return False


def _candidate_model_roots() -> List[Path]:
    roots: List[Path] = []
    env_root = os.environ.get("PADDLEOCR_MODEL_ROOT", "").strip()
    if env_root:
        roots.append(Path(env_root))

    cache_root = _project_ocr_cache_dir()
    roots.extend(
        [
            cache_root / "models",
            cache_root / "home" / ".paddleocr" / "whl",
            ORIGINAL_USER_HOME / ".paddleocr" / "whl",
        ]
    )
    deduped: List[Path] = []
    seen = set()
    for root in roots:
        key = str(root).lower()
        if key not in seen:
            deduped.append(root)
            seen.add(key)
    return deduped


def _find_model_dir(kind: str) -> Optional[str]:
    """查找 PaddleOCR 2.x 可直接使用的本地模型目录。"""
    name_candidates = {
        "det": (
            "ch_PP-OCRv4_det_infer",
            "ch_PP-OCRv3_det_infer",
            "ch_PP-OCRv2_det_infer",
            "ch_ppocr_mobile_v2.0_det_infer",
        ),
        "rec": (
            "ch_PP-OCRv4_rec_infer",
            "ch_PP-OCRv3_rec_infer",
            "ch_PP-OCRv2_rec_infer",
            "ch_ppocr_mobile_v2.0_rec_infer",
        ),
        "cls": (
            "ch_ppocr_mobile_v2.0_cls_infer",
            "ch_ppocr_mobile_v2.0_cls_slim_infer",
        ),
    }.get(kind, ())

    explicit_env = {
        "det": "PADDLEOCR_DET_MODEL_DIR",
        "rec": "PADDLEOCR_REC_MODEL_DIR",
        "cls": "PADDLEOCR_CLS_MODEL_DIR",
    }.get(kind)
    if explicit_env and os.environ.get(explicit_env):
        explicit_dir = Path(os.environ[explicit_env])
        if _model_dir_has_inference_files(explicit_dir):
            return str(explicit_dir)

    for root in _candidate_model_roots():
        if not root.exists():
            continue
        for name in name_candidates:
            direct = root / name
            if _model_dir_has_inference_files(direct):
                return str(direct)
        try:
            for child in root.rglob("*"):
                if child.is_dir() and child.name in name_candidates and _model_dir_has_inference_files(child):
                    return str(child)
        except Exception:
            continue
    return None


def _resolve_local_model_dirs() -> Dict[str, str]:
    model_dirs: Dict[str, str] = {}
    det_dir = _find_model_dir("det")
    rec_dir = _find_model_dir("rec")
    cls_dir = _find_model_dir("cls")
    if det_dir:
        model_dirs["det_model_dir"] = det_dir
    if rec_dir:
        model_dirs["rec_model_dir"] = rec_dir
    if cls_dir:
        model_dirs["cls_model_dir"] = cls_dir
    return model_dirs


def _format_ocr_init_error(errors: List[str]) -> str:
    joined = " | ".join(errors)
    if "paddleocr.bj.bcebos.com" in joined or "Max retries exceeded" in joined:
        cache_hint = _project_ocr_cache_dir() / "home" / ".paddleocr" / "whl"
        joined += (
            f" | PaddleOCR 模型下载失败。可稍后重试，或手动下载 det/rec 模型并解压到 {cache_hint}，"
            "也可通过环境变量 PADDLEOCR_MODEL_ROOT 指向本地模型根目录。"
        )
    return joined


def _base_ocr_result(
    *,
    enabled: bool = False,
    success: bool = False,
    ocr_text: str = "",
    ocr_confidence: float = 0.0,
    line_count: int = 0,
    error: str = "",
) -> Dict[str, Any]:
    return {
        "enabled": enabled,
        "success": success,
        "ocr_engine": "paddleocr",
        "ocr_text": ocr_text,
        "ocr_confidence": round(float(ocr_confidence or 0.0), 4),
        "line_count": int(line_count or 0),
        "error": error,
    }


def render_pdf_header_to_image(
    pdf_path: str | Path,
    output_path: str | Path | None = None,
    top_ratio: float = 0.32,
    zoom: float = 2.5,
) -> Dict[str, Any]:
    """渲染 PDF 第一页顶部区域，返回临时图片路径。"""
    _ensure_ocr_runtime_environment()
    try:
        import fitz
    except ImportError:
        return {
            "success": False,
            "image_path": "",
            "engine": "pymupdf",
            "top_ratio": top_ratio,
            "zoom": zoom,
            "error": "PyMuPDF/fitz not installed",
        }

    image_path = ""
    try:
        path = Path(pdf_path)
        if output_path:
            image_path = str(Path(output_path))
        else:
            with tempfile.NamedTemporaryFile(
                prefix="resume_header_",
                suffix=".png",
                delete=False,
            ) as tmp_file:
                image_path = tmp_file.name

        with fitz.open(str(path)) as document:
            if len(document) == 0:
                raise RuntimeError("PDF has no pages")
            page = document[0]
            page_rect = page.rect
            clipped_ratio = min(max(float(top_ratio or 0.32), 0.15), 0.5)
            clip_rect = fitz.Rect(
                page_rect.x0,
                page_rect.y0,
                page_rect.x1,
                page_rect.y0 + page_rect.height * clipped_ratio,
            )
            matrix = fitz.Matrix(float(zoom or 2.5), float(zoom or 2.5))
            pixmap = page.get_pixmap(matrix=matrix, clip=clip_rect, alpha=False)
            pixmap.save(image_path)

        return {
            "success": True,
            "image_path": image_path,
            "engine": "pymupdf",
            "top_ratio": min(max(float(top_ratio or 0.32), 0.15), 0.5),
            "zoom": float(zoom or 2.5),
            "error": "",
        }
    except Exception as exc:
        if image_path:
            try:
                Path(image_path).unlink(missing_ok=True)
            except Exception:
                pass
        return {
            "success": False,
            "image_path": "",
            "engine": "pymupdf",
            "top_ratio": top_ratio,
            "zoom": zoom,
            "error": str(exc),
        }


def _iter_paddleocr_lines(raw_result: Any) -> List[tuple[str, float]]:
    """兼容 PaddleOCR 常见返回结构，提取 (text, confidence)。"""
    lines: List[tuple[str, float]] = []

    def visit(node: Any) -> None:
        if node is None:
            return
        if isinstance(node, dict):
            text = node.get("text") or node.get("transcription")
            score = node.get("score") or node.get("confidence")
            if text:
                try:
                    lines.append((str(text), float(score or 0.0)))
                except Exception:
                    lines.append((str(text), 0.0))
            for value in node.values():
                if isinstance(value, (list, tuple, dict)):
                    visit(value)
            return
        if isinstance(node, (list, tuple)):
            if len(node) >= 2 and isinstance(node[1], (list, tuple)) and node[1]:
                maybe_text = node[1][0]
                maybe_score = node[1][1] if len(node[1]) > 1 else 0.0
                if isinstance(maybe_text, str):
                    try:
                        lines.append((maybe_text, float(maybe_score or 0.0)))
                    except Exception:
                        lines.append((maybe_text, 0.0))
                    return
            for item in node:
                visit(item)

    visit(raw_result)
    return lines


def _build_paddleocr_instance():
    _ensure_ocr_runtime_environment()
    try:
        import paddleocr as paddleocr_module
        from paddleocr import PaddleOCR
    except ImportError as exc:
        raise RuntimeError("paddleocr not installed") from exc

    local_model_dirs = _resolve_local_model_dirs()
    legacy_use_angle_cls = bool(local_model_dirs.get("cls_model_dir"))
    legacy_candidates = [
        {
            "use_angle_cls": legacy_use_angle_cls,
            "lang": "ch",
            "show_log": False,
            **local_model_dirs,
        },
        {
            "use_angle_cls": legacy_use_angle_cls,
            "lang": "ch",
            **local_model_dirs,
        },
    ]
    if not {"det_model_dir", "rec_model_dir"}.issubset(local_model_dirs):
        legacy_candidates.extend(
            [
                {"use_angle_cls": False, "lang": "ch", "show_log": False},
                {"use_angle_cls": False, "lang": "ch"},
            ]
        )
    modern_candidates = [
        # PaddleOCR 3.x 参数
        {
            "lang": "ch",
            "use_doc_orientation_classify": False,
            "use_doc_unwarping": False,
            "use_textline_orientation": False,
        },
        {"lang": "ch"},
    ]
    version = str(getattr(paddleocr_module, "__version__", "") or "")
    has_local_core_models = {"det_model_dir", "rec_model_dir"}.issubset(local_model_dirs)
    if version.startswith("2.") and has_local_core_models:
        init_candidates = legacy_candidates
    elif version.startswith("2."):
        init_candidates = legacy_candidates + modern_candidates
    else:
        init_candidates = modern_candidates + legacy_candidates
    errors: List[str] = []
    for kwargs in init_candidates:
        try:
            return PaddleOCR(**kwargs)
        except Exception as exc:
            errors.append(str(exc))
    raise RuntimeError("PaddleOCR initialization failed: " + _format_ocr_init_error(errors))


def extract_text_from_pdf_header_ocr(
    pdf_path: str | Path,
    top_ratio: float = 0.32,
    min_confidence: float = 0.45,
) -> Dict[str, Any]:
    """
    对 PDF 第一页顶部区域执行可选 OCR。

    依赖缺失或 OCR 失败时返回 success=False，不抛异常影响主链路。
    """
    image_info = render_pdf_header_to_image(pdf_path, top_ratio=top_ratio)
    image_path = image_info.get("image_path", "")
    if not image_info.get("success"):
        return _base_ocr_result(error=str(image_info.get("error", "")))

    try:
        ocr = _build_paddleocr_instance()
    except Exception as exc:
        try:
            Path(image_path).unlink(missing_ok=True)
        except Exception:
            pass
        return _base_ocr_result(error=str(exc))

    try:
        try:
            raw_result = ocr.ocr(image_path, cls=False)
        except TypeError:
            raw_result = ocr.ocr(image_path)

        lines = [
            (text.strip(), confidence)
            for text, confidence in _iter_paddleocr_lines(raw_result)
            if text and text.strip() and float(confidence or 0.0) >= min_confidence
        ]
        ocr_text = "\n".join(text for text, _ in lines)
        avg_confidence = (
            sum(float(confidence or 0.0) for _, confidence in lines) / len(lines)
            if lines
            else 0.0
        )
        return _base_ocr_result(
            enabled=True,
            success=bool(ocr_text.strip()),
            ocr_text=ocr_text,
            ocr_confidence=avg_confidence,
            line_count=len(lines),
            error="" if ocr_text.strip() else "no OCR text above confidence threshold",
        )
    except Exception as exc:
        return _base_ocr_result(enabled=True, success=False, error=str(exc))
    finally:
        try:
            Path(image_path).unlink(missing_ok=True)
        except Exception:
            pass
