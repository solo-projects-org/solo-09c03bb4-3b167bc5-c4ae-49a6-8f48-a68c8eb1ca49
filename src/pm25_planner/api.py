"""FastAPI 应用：读取计划的生成、快照核对、预览执行与统计验真。"""
from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import Settings
from .models import (
    PlanRequest,
    PlanResponse,
    PreviewResponse,
    SnapshotResponse,
    VerifyFileView,
    VerifyRecordOut,
    VerifyRequest,
    VerifyResponse,
    VerifyStatusResponse,
    VerifySubmitRequest,
    VerifySubmitResponse,
)
from .parquet_meta import duckdb_cross_check
from .planner import build_plan
from .reader import execute_preview
from .snapshot import Snapshot, SnapshotStore
from .verification import VerifyGate, VerifyStore

WEB_DIR = Path(__file__).resolve().parents[2] / "web"


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    store = SnapshotStore(settings)
    # 统计验真仅支持清单模式；扫描模式下开关自动失效
    verify_store = (
        VerifyStore(settings)
        if settings.verify_enabled and settings.manifest_path is not None
        else None
    )
    app = FastAPI(
        title="PM2.5 Parquet 档案读取计划服务",
        version="1.1.0",
        description="基于分区值与 min/max/null_count 元数据生成可核对的文件读取计划，"
                    "统计缺失/损坏一律保守保留，计划绑定档案快照；"
                    "清单模式可开启统计验真，实读 PM2.5 列核对 footer 统计。",
    )
    app.state.settings = settings
    app.state.store = store
    app.state.verify_store = verify_store

    def _snapshot_or_503() -> Snapshot:
        try:
            return store.current()
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"清单文件不可用（{exc.filename}），请先部署清单后重试",
            )
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"清单 JSON 不完整或损坏: {exc.msg}（请确认替换是否原子完成）",
            )

    def _gate_or_none(snap: Snapshot) -> VerifyGate | None:
        """验真开启且为清单模式时返回放行闸门，否则 None（行为与旧版一致）。"""
        if verify_store is None or snap.manifest_entries is None:
            return None
        return VerifyGate(verify_store, snap)

    @app.get("/api/health")
    def health() -> dict:
        try:
            snap = store.current()
            return {"status": "ok", "snapshot_id": snap.snapshot_id,
                    "file_count": len(snap.files), "source": snap.source,
                    "verify_enabled": verify_store is not None}
        except Exception as exc:  # noqa: BLE001 - 健康检查需要兜住全部启动期异常
            return {"status": "degraded", "error": f"{exc.__class__.__name__}: {exc}"}

    @app.get("/api/snapshot", response_model=SnapshotResponse)
    def get_snapshot() -> SnapshotResponse:
        snap = _snapshot_or_503()
        return SnapshotResponse(
            snapshot_id=snap.snapshot_id, source=snap.source,  # type: ignore[arg-type]
            file_count=len(snap.files),
            total_bytes=sum(f.size for f in snap.files),
            archive_dir=str(snap.archive_dir),
            manifest_path=str(snap.manifest_path) if snap.manifest_path else None,
            generated_at=snap.generated_at,
        )

    @app.post("/api/plan", response_model=PlanResponse)
    def create_plan(req: PlanRequest) -> PlanResponse:
        snap = _snapshot_or_503()
        if req.expected_snapshot_id and req.expected_snapshot_id != snap.snapshot_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "STALE_SNAPSHOT",
                    "message": "清单在计划生成前已替换，旧快照绑定失效，请重新查询",
                    "expected_snapshot_id": req.expected_snapshot_id,
                    "current_snapshot_id": snap.snapshot_id,
                },
            )
        return build_plan(snap, req, _gate_or_none(snap))

    @app.post("/api/plan/verify", response_model=VerifyResponse)
    def verify_snapshot(req: VerifyRequest) -> VerifyResponse:
        snap = _snapshot_or_503()
        if req.snapshot_id == snap.snapshot_id:
            return VerifyResponse(
                valid=True, current_snapshot_id=snap.snapshot_id,
                message="快照一致，计划仍然绑定当前档案",
            )
        return VerifyResponse(
            valid=False, current_snapshot_id=snap.snapshot_id,
            message="档案清单已替换，该计划绑定的快照已失效，请重新生成计划",
        )

    @app.post("/api/plan/preview", response_model=PreviewResponse)
    def preview_plan(req: PlanRequest) -> PreviewResponse:
        snap = _snapshot_or_503()
        bound_id = req.expected_snapshot_id or snap.snapshot_id
        # 预览内部再次按当前快照判定；快照变化时 valid_snapshot=False
        return execute_preview(snap, bound_id, req, settings, _gate_or_none(snap))

    # ------------------------------------------------------------------ #
    # 统计验真（仅清单模式）：实读 PM2.5 列核对 footer 统计
    # ------------------------------------------------------------------ #
    def _verify_available() -> None:
        if not settings.verify_enabled:
            raise HTTPException(
                status_code=400,
                detail="统计验真未开启（设置 PM25_VERIFY_ENABLED=1 后重启生效）",
            )
        if settings.manifest_path is None or verify_store is None:
            raise HTTPException(
                status_code=400,
                detail="统计验真仅支持清单模式（需配置 PM25_MANIFEST_PATH）",
            )

    @app.get("/api/verify/status", response_model=VerifyStatusResponse)
    def verify_status() -> VerifyStatusResponse:
        enabled = settings.verify_enabled
        manifest_mode = settings.manifest_path is not None
        store_path = (
            str(verify_store.store_path)
            if verify_store is not None and verify_store.store_path is not None
            else None
        )
        files: list[VerifyFileView] = []
        if enabled and manifest_mode and verify_store is not None:
            snap = _snapshot_or_503()
            for rel, (size, mtime_ns) in sorted((snap.manifest_entries or {}).items()):
                rec = verify_store.get(rel, size, mtime_ns)
                outdated = None if rec is not None else verify_store.latest_for_path(rel)
                files.append(VerifyFileView(
                    path=rel, size=size, mtime_ns=mtime_ns,
                    state=rec.status if rec is not None else "unverified",  # type: ignore[arg-type]
                    record=VerifyRecordOut(**rec.to_dict()) if rec is not None else None,
                    outdated_record=(
                        VerifyRecordOut(**outdated.to_dict()) if outdated is not None else None
                    ),
                ))
        return VerifyStatusResponse(
            enabled=enabled, manifest_mode=manifest_mode,
            store_path=store_path, files=files,
        )

    @app.post("/api/verify/stats", response_model=VerifySubmitResponse)
    def submit_verify(req: VerifySubmitRequest) -> VerifySubmitResponse:
        _verify_available()
        assert verify_store is not None
        snap = _snapshot_or_503()
        submitted, rejected = verify_store.submit(req.paths, snap, force=req.force)
        return VerifySubmitResponse(
            submitted=[VerifyRecordOut(**r.to_dict()) for r in submitted],
            rejected=rejected,
        )

    @app.get("/api/debug/duckdb-stats")
    def debug_duckdb_stats(path: str) -> dict:
        """调试接口：对单个文件用 DuckDB 独立重算统计，便于与 PyArrow 结果核对。"""
        root = settings.archive_dir.resolve()
        target = (root / path).resolve()
        if root not in target.parents and target != root:
            raise HTTPException(status_code=400, detail="路径越界，拒绝访问")
        if not target.exists():
            raise HTTPException(status_code=404, detail=f"文件不存在: {path}")
        result = duckdb_cross_check(target, settings.pm25_column)
        arrow_meta = store.current().by_relpath().get(path.replace("\\", "/").lstrip("./"))
        return {
            "path": path,
            "duckdb": result,
            "pyarrow": None if arrow_meta is None else {
                "pm25_min": arrow_meta.pm25_min,
                "pm25_max": arrow_meta.pm25_max,
                "null_count": arrow_meta.null_count,
                "row_count": arrow_meta.row_count,
                "stats_status": arrow_meta.stats_status,
            },
        }

    # 静态前端（index.html 在根路径提供）
    app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(str(WEB_DIR / "index.html"))

    return app


app = create_app()
