"""统计验真：实读 PM2.5 列核对 footer 统计，结论按清单版本持久化。

背景：转码可能把错误的 min/max/null_count 写进 Parquet footer，
按计划服务的统计裁剪会因此跳过本该读取的文件（漏读）。本模块提供
“验真”机制（仅清单模式）：

- 值班员在网页指定清单内的 Parquet 文件，提交后台验真；
- 后台线程**实读 PM2.5 列**（DuckDB），重算 min/max/null_count/row_count
  并与 footer 统计逐项比对，产出 通过 / 不符 / 读取失败 结论；
- 结论按清单登记的版本键 ``(path, size, mtime_ns)`` 持久化到
  ``verification.json``（写临时文件 + ``os.replace`` 原子落盘），
  重启后沿用；其他文件换版不影响已有结论；
- 同名文件换版（size/mtime 变化）后旧结论自然失配，必须重新验真；
  验真前后各 stat 一次磁盘文件，在途核验遇到换版立即作废，
  绝不为新版本放行；
- 计划生成始终只读元数据（footer + stat），验真在后台进行，
  验真期间照常出计划：未放行的文件一律保守保留。
"""
from __future__ import annotations

import json
import logging
import math
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import Settings
from .parquet_meta import FileMeta
from .snapshot import Snapshot

logger = logging.getLogger(__name__)

# 验真结论状态
STATUS_RUNNING = "running"      # 在途核验
STATUS_PASSED = "passed"        # 实读结果与 footer 统计一致
STATUS_MISMATCH = "mismatch"    # 实读结果与 footer 统计不符
STATUS_ERROR = "error"          # 实读失败 / 版本不一致 / 在途换版作废

# 计划层放行状态（GateVerdict.status，供前端与原因码核对）
GATE_PASSED = STATUS_PASSED
GATE_UNVERIFIED = "unverified"          # 从未验真（或换版后未重新验真）
GATE_VERSION_MISMATCH = "version_mismatch"  # 磁盘文件与清单登记版本不一致


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class VerifyRecord:
    """单文件单版本的验真结论。"""

    path: str
    size: int
    mtime_ns: int
    status: str  # running / passed / mismatch / error
    expected: dict[str, Any]          # footer 统计（提交时快照）
    actual: dict[str, Any] | None = None   # 实读重算结果
    mismatches: list[str] = field(default_factory=list)
    error: str | None = None
    started_at: str = ""
    finished_at: str | None = None

    @property
    def key(self) -> tuple[str, int, int]:
        return (self.path, self.size, self.mtime_ns)

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "status": self.status,
            "expected": self.expected,
            "actual": self.actual,
            "mismatches": list(self.mismatches),
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }

    @staticmethod
    def from_dict(payload: dict[str, Any]) -> "VerifyRecord":
        return VerifyRecord(
            path=str(payload["path"]),
            size=int(payload["size"]),
            mtime_ns=int(payload["mtime_ns"]),
            status=str(payload["status"]),
            expected=dict(payload.get("expected") or {}),
            actual=payload.get("actual"),
            mismatches=[str(m) for m in payload.get("mismatches") or []],
            error=payload.get("error"),
            started_at=str(payload.get("started_at") or ""),
            finished_at=payload.get("finished_at"),
        )


def read_column_stats(path: Path, pm25_column: str) -> dict[str, Any]:
    """实读 PM2.5 列（PyArrow 全列解码），重算 min/max/null_count/row_count。

    注意不能用 DuckDB 的 ``min()/max()`` 聚合：DuckDB 对 min/max 聚合有
    统计下推优化，会直接采信 footer（正是要防范的错误来源）。
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    try:
        table = pq.read_table(path, columns=[pm25_column])
    except Exception as exc:  # 损坏/缺列：如实返回失败原因
        return {"ok": False, "error": f"{exc.__class__.__name__}: {exc}"}
    arr = table.column(pm25_column).combine_chunks()
    non_null = arr.drop_null()
    return {
        "ok": True,
        "row_count": len(arr),
        "null_count": int(arr.null_count),
        "pm25_min": None if len(non_null) == 0 else float(pc.min(non_null).as_py()),
        "pm25_max": None if len(non_null) == 0 else float(pc.max(non_null).as_py()),
    }


def compare_stats(expected: dict[str, Any], actual: dict[str, Any]) -> list[str]:
    """逐项比对 footer 统计与实读结果，返回不符项说明（空列表即通过）。

    footer 未记录的项（expected 为 None 的 null_count/row_count）无从核对，
    直接跳过；min/max 为 None 表示“无非空值”，None 与数值不一致即不符。
    """
    mismatches: list[str] = []
    for field_name, label in (("pm25_min", "min"), ("pm25_max", "max")):
        e, a = expected.get(field_name), actual.get(field_name)
        if e is None and a is None:
            continue
        if e is None or a is None:
            mismatches.append(f"{label}: footer={e} 实读={a}")
            continue
        if not math.isclose(float(e), float(a), rel_tol=1e-9, abs_tol=1e-12):
            mismatches.append(f"{label}: footer={e} 实读={a}")
    for field_name in ("null_count", "row_count"):
        e = expected.get(field_name)
        if e is None:
            continue
        a = actual.get(field_name)
        if a is None or int(e) != int(a):
            mismatches.append(f"{field_name}: footer={e} 实读={a}")
    return mismatches


class VerifyStore:
    """验真任务调度与结论库（线程安全，结论持久化到 JSON）。"""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._store_path = settings.resolved_verify_store_path()
        self._records: dict[tuple[str, int, int], VerifyRecord] = {}
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(
            max_workers=max(settings.verify_workers, 1),
            thread_name_prefix="pm25-verify",
        )
        self._load()

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    @property
    def store_path(self) -> Path | None:
        return self._store_path

    def get(self, path: str, size: int, mtime_ns: int) -> VerifyRecord | None:
        with self._lock:
            return self._records.get((path, size, mtime_ns))

    def latest_for_path(self, path: str) -> VerifyRecord | None:
        """同一路径任意版本的最近一条结论（用于“换版须重新验真”提示）。"""
        with self._lock:
            candidates = [r for r in self._records.values() if r.path == path]
        if not candidates:
            return None
        return max(candidates, key=lambda r: r.started_at)

    def records(self) -> list[VerifyRecord]:
        with self._lock:
            return sorted(self._records.values(), key=lambda r: (r.path, r.started_at))

    # ------------------------------------------------------------------ #
    # 提交验真
    # ------------------------------------------------------------------ #
    def submit(
        self,
        paths: list[str],
        snapshot: Snapshot,
        *,
        force: bool = False,
    ) -> tuple[list[VerifyRecord], list[dict[str, str]]]:
        """对清单内文件提交后台验真；返回 (受理记录, 拒绝列表)。

        幂等：同一版本已有结论（非 force）或任务在途时直接返回现有记录。
        """
        entries = snapshot.manifest_entries or {}
        metas = snapshot.by_relpath()
        root = self._settings.archive_dir.resolve()
        submitted: list[VerifyRecord] = []
        rejected: list[dict[str, str]] = []

        for raw in paths:
            raw_str = str(raw)
            # 先用原始路径做越界检查（lstrip 会吃掉前导 ../，掩盖穿越意图）
            target = (self._settings.archive_dir / raw_str).resolve()
            if target != root and root not in target.parents:
                rejected.append({"path": raw_str, "reason": "路径越界，拒绝访问"})
                continue
            rel = raw_str.replace("\\", "/").lstrip("./")
            entry = entries.get(rel)
            if entry is None:
                rejected.append({"path": str(raw), "reason": "不在当前清单中"})
                continue
            size, mtime_ns = entry
            key = (rel, size, mtime_ns)
            with self._lock:
                existing = self._records.get(key)
            if existing is not None and (existing.status == STATUS_RUNNING or not force):
                submitted.append(existing)  # 在途或已有结论：直接沿用
                continue

            meta = metas.get(rel)
            expected = {
                "pm25_min": meta.pm25_min if meta else None,
                "pm25_max": meta.pm25_max if meta else None,
                "null_count": meta.null_count if meta else None,
                "row_count": meta.row_count if meta else None,
            }
            if meta is None or meta.stats_status != "ok":
                # footer 本身无完整统计：无统计可核对，直接落 error 结论
                detail = (
                    f"footer 统计不完整（stats_status="
                    f"{meta.stats_status if meta else 'missing'}），无统计可核对；"
                    "计划中本已保守保留"
                )
                rec = VerifyRecord(
                    path=rel, size=size, mtime_ns=mtime_ns,
                    status=STATUS_ERROR, expected=expected, error=detail,
                    started_at=_utcnow(), finished_at=_utcnow(),
                )
                with self._lock:
                    self._records[key] = rec
                self._persist()
                submitted.append(rec)
                continue

            rec = VerifyRecord(
                path=rel, size=size, mtime_ns=mtime_ns,
                status=STATUS_RUNNING, expected=expected, started_at=_utcnow(),
            )
            with self._lock:
                self._records[key] = rec
            self._pool.submit(self._run, key)
            submitted.append(rec)
        return submitted, rejected

    # ------------------------------------------------------------------ #
    # 后台核验
    # ------------------------------------------------------------------ #
    def _stat_signature(self, full: Path) -> tuple[int, int] | None:
        try:
            st = full.stat()
        except OSError:
            return None
        return (st.st_size, st.st_mtime_ns)

    def _run(self, key: tuple[str, int, int]) -> None:
        with self._lock:
            rec = self._records[key]
        rel, size, mtime_ns = key
        full = self._settings.archive_dir / rel

        # 读前核对：磁盘文件必须是清单登记的那个版本
        sig = self._stat_signature(full)
        if sig is None:
            self._finish(key, status=STATUS_ERROR,
                         error="磁盘文件不可访问，无法实读")
            return
        if sig != (size, mtime_ns):
            self._finish(
                key, status=STATUS_ERROR,
                error="磁盘文件与清单登记版本不一致（size/mtime 不符），无法按清单版本验真",
            )
            return

        # 实读 PM2.5 列，重算统计
        actual = read_column_stats(full, self._settings.pm25_column)
        if not actual.get("ok"):
            self._finish(key, status=STATUS_ERROR,
                         error=f"实读 PM2.5 列失败: {actual.get('error')}")
            return

        # 读后核对：在途期间换版则本次结果作废，绝不为新版放行
        sig2 = self._stat_signature(full)
        if sig2 != (size, mtime_ns):
            self._finish(
                key, status=STATUS_ERROR,
                error="验真期间文件被替换（版本变化），本次结果作废，请重新验真",
            )
            return

        mismatches = compare_stats(rec.expected, actual)
        self._finish(
            key,
            status=STATUS_PASSED if not mismatches else STATUS_MISMATCH,
            actual=actual, mismatches=mismatches,
        )

    def _finish(
        self,
        key: tuple[str, int, int],
        *,
        status: str,
        actual: dict[str, Any] | None = None,
        mismatches: list[str] | None = None,
        error: str | None = None,
    ) -> None:
        with self._lock:
            rec = self._records.get(key)
            if rec is None or rec.status != STATUS_RUNNING:
                return  # 记录已被替换（如 force 重验），丢弃过期结果
            rec.status = status
            rec.actual = actual
            rec.mismatches = list(mismatches or [])
            rec.error = error
            rec.finished_at = _utcnow()
        self._persist()

    # ------------------------------------------------------------------ #
    # 持久化（原子写；损坏时忽略并重建）
    # ------------------------------------------------------------------ #
    def _load(self) -> None:
        path = self._store_path
        if path is None or not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            for item in payload.get("records", []):
                rec = VerifyRecord.from_dict(item)
                if rec.status != STATUS_RUNNING:  # 在途记录不持久化
                    self._records[rec.key] = rec
            logger.info("已加载 %d 条验真结论 <- %s", len(self._records), path)
        except Exception:  # noqa: BLE001 - 结论库损坏不应拖垮服务
            logger.warning("验真结论库损坏，已忽略并从空库开始: %s", path, exc_info=True)

    def _persist(self) -> None:
        path = self._store_path
        if path is None:
            return
        with self._lock:
            completed = [r for r in self._records.values() if r.status != STATUS_RUNNING]
            payload = {
                "version": 1,
                "records": [r.to_dict() for r in
                            sorted(completed, key=lambda r: (r.path, r.started_at))],
            }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            # 临时文件名带线程标识：并发落盘互不覆盖，os.replace 保证原子性
            tmp = path.with_name(f"{path.name}.{threading.get_ident()}.tmp")
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                           encoding="utf-8")
            os.replace(tmp, path)  # 原子替换：读者只见旧版或新版完整内容
        except OSError:
            logger.warning("验真结论落盘失败: %s", path, exc_info=True)


@dataclass(frozen=True)
class GateVerdict:
    """计划层单文件放行结论。"""

    allow_stats_pruning: bool
    status: str  # passed / unverified / running / mismatch / error / version_mismatch
    detail: str


class VerifyGate:
    """把验真结论翻译成计划层的“是否允许统计裁剪”判定。

    每次判定都重新 stat 磁盘文件（仅元数据，不读数据行），
    确保“磁盘文件与清单所记版本一致”在出计划这一刻仍然成立。
    """

    def __init__(self, store: VerifyStore, snapshot: Snapshot) -> None:
        self._store = store
        self._archive_dir = snapshot.archive_dir
        self._entries = snapshot.manifest_entries or {}

    def check(self, meta: FileMeta) -> GateVerdict:
        entry = self._entries.get(meta.rel_path)
        if entry is None:
            return GateVerdict(
                False, GATE_UNVERIFIED,
                "文件不在当前清单中，无法核对登记版本，统计不可信，保守保留",
            )
        reg_size, reg_mtime = entry
        full = self._archive_dir / meta.rel_path
        try:
            st = full.stat()
            disk_sig = (st.st_size, st.st_mtime_ns)
        except OSError:
            disk_sig = None
        if disk_sig != (reg_size, reg_mtime):
            return GateVerdict(
                False, GATE_VERSION_MISMATCH,
                f"磁盘文件与清单登记版本不一致（登记 size={reg_size}/mtime_ns={reg_mtime}，"
                f"磁盘 {'不可访问' if disk_sig is None else f'size={disk_sig[0]}/mtime_ns={disk_sig[1]}'}），"
                "统计不可信，保守保留",
            )
        rec = self._store.get(meta.rel_path, reg_size, reg_mtime)
        if rec is None:
            return GateVerdict(
                False, GATE_UNVERIFIED,
                "统计未验真，为防止错误 footer 统计导致漏读，保守保留",
            )
        if rec.status == STATUS_RUNNING:
            return GateVerdict(False, STATUS_RUNNING, "统计验真进行中，结论未出前保守保留")
        if rec.status == STATUS_PASSED:
            return GateVerdict(True, STATUS_PASSED, "统计验真通过，按 footer 统计正常裁剪")
        if rec.status == STATUS_MISMATCH:
            return GateVerdict(
                False, STATUS_MISMATCH,
                "统计验真不符（" + "；".join(rec.mismatches) + "），保守保留",
            )
        return GateVerdict(
            False, STATUS_ERROR, f"统计验真读取失败（{rec.error}），保守保留",
        )
