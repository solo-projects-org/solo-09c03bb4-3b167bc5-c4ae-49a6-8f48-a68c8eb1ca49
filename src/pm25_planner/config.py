"""运行配置：通过环境变量覆盖，全部带本地开发默认值。"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name)
    return Path(raw).expanduser().resolve() if raw else default.resolve()


@dataclass(frozen=True)
class Settings:
    # Parquet 档案根目录，内部按 station=.../date=... 分区
    archive_dir: Path
    # 清单（manifest）文件路径；为空表示直接扫描档案目录
    manifest_path: Path | None
    # PM2.5 数值列名（样例数据统一使用该名称）
    pm25_column: str
    # 预览读取接口最多允许读取的文件数，防止计划外的大查询
    preview_max_files: int
    # 单文件元数据缓存上限（按路径缓存 footer 解析结果）
    meta_cache_size: int
    # 统计验真总开关（仅清单模式生效）：开启后未通过验真的文件不做统计裁剪
    verify_enabled: bool = False
    # 验真结论持久化文件；为空时默认 <清单目录>/verification.json
    verify_store_path: Path | None = None
    # 验真后台线程数（实读 PM2.5 列，属于 IO 密集任务）
    verify_workers: int = 2

    @staticmethod
    def from_env() -> "Settings":
        manifest_raw = os.environ.get("PM25_MANIFEST_PATH", "").strip()
        verify_store_raw = os.environ.get("PM25_VERIFY_STORE_PATH", "").strip()
        verify_flag = os.environ.get("PM25_VERIFY_ENABLED", "").strip().lower()
        return Settings(
            archive_dir=_env_path(
                "PM25_ARCHIVE_DIR", Path(__file__).resolve().parents[2] / "data" / "archive"
            ),
            manifest_path=Path(manifest_raw).expanduser().resolve() if manifest_raw else None,
            pm25_column=os.environ.get("PM25_COLUMN", "pm25"),
            preview_max_files=int(os.environ.get("PM25_PREVIEW_MAX_FILES", "50")),
            meta_cache_size=int(os.environ.get("PM25_META_CACHE_SIZE", "4096")),
            verify_enabled=verify_flag in ("1", "true", "yes", "on"),
            verify_store_path=(
                Path(verify_store_raw).expanduser().resolve() if verify_store_raw else None
            ),
            verify_workers=int(os.environ.get("PM25_VERIFY_WORKERS", "2")),
        )

    def resolved_verify_store_path(self) -> Path | None:
        """验真结论库路径：显式配置优先，否则落在清单同目录（随数据卷持久化）。"""
        if self.verify_store_path is not None:
            return self.verify_store_path
        if self.manifest_path is not None:
            return self.manifest_path.parent / "verification.json"
        return None
