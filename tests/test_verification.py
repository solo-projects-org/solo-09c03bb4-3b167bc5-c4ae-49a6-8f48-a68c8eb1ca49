"""统计验真：实读核对、保守保留、换版失效、在途作废与持久化测试。"""
from __future__ import annotations

import json
import os
import struct
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from pm25_planner import verification
from pm25_planner.api import create_app
from pm25_planner.config import Settings
from pm25_planner.planner import (
    PARTITION_STATION_EXCLUDED,
    RANGE_DISJOINT,
    STATS_UNVERIFIED_KEEP,
)
from pm25_planner.verification import compare_stats

LOW = "station=A/date=2026-01-01/part-low.parquet"
HIGH = "station=A/date=2026-01-02/part-high.parquet"
MID = "station=B/date=2026-01-01/part-mid.parquet"
BADMAX = "station=B/date=2026-01-02/part-badmax.parquet"
CORRUPT = "station=C/date=2026-01-01/part-corrupt.parquet"
NOSTATS = "station=C/date=2026-01-02/part-nostats.parquet"


def write_pq(path: Path, values) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"pm25": pa.array(values, type=pa.float64())}), path)
    return path


def write_wrong_footer_max(path: Path, values, fake_max: float) -> Path:
    """写入 parquet 后篡改 footer 统计中的 max（模拟转码写错最大值）。

    thrift compact 协议中 double 为小端字节序；footer 的 Statistics 同时
    带有 max 与 max_value 两处，位于文件末尾，只替换最后两处即可保证
    数据页不受影响。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"pm25": pa.array(values, type=pa.float64())}), path,
        compression="NONE", use_dictionary=False,
    )
    real_max = max(v for v in values if v is not None)
    raw = path.read_bytes()
    pat = struct.pack("<d", float(real_max))
    offs: list[int] = []
    i = raw.find(pat)
    while i != -1:
        offs.append(i)
        i = raw.find(pat, i + 1)
    assert len(offs) >= 2, "footer 中应至少包含 max/max_value 两处统计"
    buf = bytearray(raw)
    for off in offs[-2:]:
        buf[off:off + 8] = struct.pack("<d", float(fake_max))
    path.write_bytes(bytes(buf))
    # 自检：footer max 已被篡改、真实数据未受影响
    st = pq.ParquetFile(path).metadata.row_group(0).column(0).statistics
    assert st.max == fake_max, f"footer 篡改失败: {st.max}"
    return path


def write_manifest(archive: Path, manifest: Path) -> None:
    entries = []
    for p in sorted(archive.rglob("*.parquet")):
        st = p.stat()
        entries.append({
            "path": str(p.relative_to(archive)).replace("\\", "/"),
            "size": st.st_size,
            "mtime_ns": st.st_mtime_ns,
        })
    manifest.parent.mkdir(parents=True, exist_ok=True)
    tmp = manifest.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"version": 1, "pm25_column": "pm25", "files": entries}))
    os.replace(tmp, manifest)


@pytest.fixture()
def env(tmp_path: Path) -> SimpleNamespace:
    """清单模式环境：正常文件 + footer max 被篡改的文件 + 损坏文件。"""
    archive = tmp_path / "archive"
    write_pq(archive / LOW, [5.0, 10.0, 20.0])
    write_pq(archive / HIGH, [80.0, 90.0, None])
    write_pq(archive / MID, [40.0, 45.0])
    write_wrong_footer_max(archive / BADMAX, [10.0, 20.0, 30.0], fake_max=25.0)
    corrupt = write_pq(archive / CORRUPT, [50.0, 60.0])
    raw = corrupt.read_bytes()
    corrupt.write_bytes(raw[: len(raw) // 2])
    nostats = archive / NOSTATS
    nostats.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"pm25": pa.array([33.0, 66.0], type=pa.float64())}),
                   nostats, write_statistics=False)
    manifest = tmp_path / "manifest" / "manifest.json"
    write_manifest(archive, manifest)
    return SimpleNamespace(
        archive=archive, manifest=manifest,
        store_path=tmp_path / "manifest" / "verification.json",
    )


def make_settings(env: SimpleNamespace, *, verify_enabled: bool = True,
                  manifest: bool = True) -> Settings:
    return Settings(
        archive_dir=env.archive,
        manifest_path=env.manifest if manifest else None,
        pm25_column="pm25", preview_max_files=50, meta_cache_size=128,
        verify_enabled=verify_enabled,
        verify_store_path=env.store_path,
    )


def make_client(env: SimpleNamespace, **kwargs) -> TestClient:
    return TestClient(create_app(make_settings(env, **kwargs)))


def wait_verify_done(client: TestClient, timeout: float = 15.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = client.get("/api/verify/status").json()
        if all(f["state"] != "running" for f in body["files"]):
            return body
        time.sleep(0.05)
    raise AssertionError("验真超时未完成")


def file_view(body: dict, suffix: str) -> dict:
    for f in body["files"]:
        if f["path"].endswith(suffix):
            return f
    raise KeyError(suffix)


def plan_file(plan: dict, suffix: str) -> dict:
    for f in plan["files"]:
        if f["path"].endswith(suffix):
            return f
    raise KeyError(suffix)


# --------------------------------------------------------------------- #
# 比对函数单测
# --------------------------------------------------------------------- #
def test_compare_stats_unit():
    assert compare_stats(
        {"pm25_min": 1.0, "pm25_max": 2.0, "null_count": 0, "row_count": 3},
        {"pm25_min": 1.0, "pm25_max": 2.0, "null_count": 0, "row_count": 3},
    ) == []
    mm = compare_stats(
        {"pm25_min": 10.0, "pm25_max": 25.0, "null_count": 0, "row_count": 3},
        {"pm25_min": 10.0, "pm25_max": 30.0, "null_count": 1, "row_count": 3},
    )
    assert any("max" in m for m in mm) and any("null_count" in m for m in mm)
    # footer 未记录的项跳过；全 null 列 None==None 视为一致
    assert compare_stats(
        {"pm25_min": None, "pm25_max": None, "null_count": None, "row_count": None},
        {"pm25_min": None, "pm25_max": None, "null_count": 4, "row_count": 4},
    ) == []
    assert compare_stats(
        {"pm25_min": None, "pm25_max": None, "null_count": 2, "row_count": 2},
        {"pm25_min": 5.0, "pm25_max": 5.0, "null_count": 2, "row_count": 2},
    ) != []


# --------------------------------------------------------------------- #
# 开关与模式约束
# --------------------------------------------------------------------- #
def test_status_reports_disabled(env):
    client = make_client(env, verify_enabled=False)
    body = client.get("/api/verify/status").json()
    assert body["enabled"] is False and body["files"] == []
    r = client.post("/api/verify/stats", json={"paths": [LOW]})
    assert r.status_code == 400 and "未开启" in r.json()["detail"]


def test_verify_requires_manifest_mode(env):
    client = make_client(env, verify_enabled=True, manifest=False)
    body = client.get("/api/verify/status").json()
    assert body["enabled"] is True and body["manifest_mode"] is False
    r = client.post("/api/verify/stats", json={"paths": [LOW]})
    assert r.status_code == 400 and "清单模式" in r.json()["detail"]


def test_switch_off_keeps_legacy_stats_pruning(env):
    """开关关闭（默认）时行为与旧版一致：直接按 footer 统计裁剪。"""
    client = make_client(env, verify_enabled=False)
    plan = client.post("/api/plan", json={"pm25_min": 85}).json()
    assert plan["verify_enabled"] is False
    low = plan_file(plan, "part-low.parquet")
    assert low["decision"] == "EXCLUDE"
    assert RANGE_DISJOINT in low["reason_codes"]
    assert low["verify_status"] is None


# --------------------------------------------------------------------- #
# 开启后的保守保留与验真放行
# --------------------------------------------------------------------- #
def test_unverified_files_kept_but_partition_pruning_still_applies(env):
    client = make_client(env)
    plan = client.post("/api/plan", json={"pm25_min": 85}).json()
    assert plan["verify_enabled"] is True
    # 统计完整的文件因未验真而保守保留
    low = plan_file(plan, "part-low.parquet")
    assert low["decision"] == "KEEP"
    assert STATS_UNVERIFIED_KEEP in low["reason_codes"]
    assert low["verify_status"] == "unverified"
    assert any("未验真" in r for r in low["reasons"])
    # 分区裁剪（站点/日期）不受验真影响，照常排除
    plan_b = client.post("/api/plan", json={"stations": ["A"]}).json()
    mid = plan_file(plan_b, "part-mid.parquet")
    assert mid["decision"] == "EXCLUDE"
    assert PARTITION_STATION_EXCLUDED in mid["reason_codes"]


def test_verify_pass_enables_stats_pruning(env):
    client = make_client(env)
    r = client.post("/api/verify/stats", json={"paths": [LOW]})
    assert r.status_code == 200
    body = wait_verify_done(client)
    view = file_view(body, "part-low.parquet")
    assert view["state"] == "passed"
    rec = view["record"]
    assert rec["expected"]["pm25_max"] == 20.0
    assert rec["actual"] == {"ok": True, "row_count": 3, "null_count": 0,
                             "pm25_min": 5.0, "pm25_max": 20.0}
    assert rec["mismatches"] == []
    # 验真通过后照常按统计裁剪
    plan = client.post("/api/plan", json={"pm25_min": 85}).json()
    low = plan_file(plan, "part-low.parquet")
    assert low["decision"] == "EXCLUDE"
    assert RANGE_DISJOINT in low["reason_codes"]
    assert low["verify_status"] == "passed"


def test_verify_detects_wrong_footer_max(env):
    """footer max 被写小：验真不符；开启验真后该文件不再被统计裁剪误杀。"""
    client = make_client(env)
    # 未开启验真时：footer [10,25] 与 [28,+∞) 不相交 -> 错误排除（漏读 30.0）
    legacy = make_client(env, verify_enabled=False)
    legacy_plan = legacy.post("/api/plan", json={"pm25_min": 28}).json()
    assert plan_file(legacy_plan, "part-badmax.parquet")["decision"] == "EXCLUDE"

    r = client.post("/api/verify/stats", json={"paths": [BADMAX]})
    assert r.status_code == 200
    body = wait_verify_done(client)
    view = file_view(body, "part-badmax.parquet")
    assert view["state"] == "mismatch"
    rec = view["record"]
    assert rec["expected"]["pm25_max"] == 25.0   # footer 被篡改后的值
    assert rec["actual"]["pm25_max"] == 30.0     # 实读真实最大值
    assert any("max" in m for m in rec["mismatches"])
    # 验真不符 -> 保守保留，不再按错误的 footer 统计裁剪
    plan = client.post("/api/plan", json={"pm25_min": 28}).json()
    bad = plan_file(plan, "part-badmax.parquet")
    assert bad["decision"] == "KEEP"
    assert STATS_UNVERIFIED_KEEP in bad["reason_codes"]
    assert bad["verify_status"] == "mismatch"
    assert any("验真不符" in r_ for r_ in bad["reasons"])


def test_verify_error_on_unreadable_and_no_stats(env):
    client = make_client(env)
    client.post("/api/verify/stats", json={"paths": [CORRUPT, NOSTATS]})
    body = wait_verify_done(client)
    view = file_view(body, "part-corrupt.parquet")
    assert view["state"] == "error"
    assert view["record"]["error"]  # 能看到读取失败原因
    # footer 无统计可核对：直接给出失败结论与原因
    ns = file_view(body, "part-nostats.parquet")
    assert ns["state"] == "error"
    assert "无统计可核对" in ns["record"]["error"]
    # 损坏文件在计划中本就保守保留（FILE_UNREADABLE_KEEP），验真状态不适用
    plan = client.post("/api/plan", json={"pm25_min": 1}).json()
    corrupt = plan_file(plan, "part-corrupt.parquet")
    assert corrupt["decision"] == "KEEP"
    assert corrupt["verify_status"] is None


def test_gate_covers_null_based_exclusion(env):
    """未验真时，连“不包含缺测”的统计排除也一并停用。"""
    client = make_client(env)
    req = {"pm25_min": 1000, "include_nulls": False}
    plan = client.post("/api/plan", json=req).json()
    high = plan_file(plan, "part-high.parquet")
    assert high["decision"] == "KEEP"  # 未验真：不做 NULLS_PRESENT_BUT_EXCLUDED
    assert STATS_UNVERIFIED_KEEP in high["reason_codes"]
    # 验真通过后恢复统计排除
    client.post("/api/verify/stats", json={"paths": [HIGH]})
    wait_verify_done(client)
    plan2 = client.post("/api/plan", json=req).json()
    high2 = plan_file(plan2, "part-high.parquet")
    assert high2["decision"] == "EXCLUDE"
    assert high2["verify_status"] == "passed"


def test_plan_available_while_verification_running(env, monkeypatch):
    """验真在后台进行，期间照常出计划（只读元数据），在途文件保守保留。"""
    release = threading.Event()
    original = verification.read_column_stats

    def blocked(path, column):
        release.wait(10)
        return original(path, column)

    monkeypatch.setattr(verification, "read_column_stats", blocked)
    client = make_client(env)
    client.post("/api/verify/stats", json={"paths": [LOW]})
    try:
        r = client.post("/api/plan", json={"pm25_min": 85})
        assert r.status_code == 200
        low = plan_file(r.json(), "part-low.parquet")
        assert low["decision"] == "KEEP"
        assert low["verify_status"] == "running"
        assert STATS_UNVERIFIED_KEEP in low["reason_codes"]
    finally:
        release.set()
    body = wait_verify_done(client)
    assert file_view(body, "part-low.parquet")["state"] == "passed"
    # 验真完成后即可裁剪
    plan = client.post("/api/plan", json={"pm25_min": 85}).json()
    assert plan_file(plan, "part-low.parquet")["decision"] == "EXCLUDE"


def test_preview_respects_verification_gate(env):
    client = make_client(env)
    req = {"pm25_min": 85, "include_nulls": False}
    before = client.post("/api/plan/preview", json=req).json()
    assert LOW in {row["source_file"] for row in before["rows"]}  # 未验真被保留并实读
    client.post("/api/verify/stats", json={"paths": [LOW, MID, BADMAX]})
    wait_verify_done(client)
    after = client.post("/api/plan/preview", json=req).json()
    read_files = {row["source_file"] for row in after["rows"]}
    assert LOW not in read_files      # 验真通过 -> 统计裁剪生效
    assert BADMAX in read_files       # 验真不符 -> 保守保留仍被读取


# --------------------------------------------------------------------- #
# 版本与持久化
# --------------------------------------------------------------------- #
def test_version_change_requires_reverification(env):
    client = make_client(env)
    client.post("/api/verify/stats", json={"paths": [LOW]})
    wait_verify_done(client)
    plan = client.post("/api/plan", json={"pm25_min": 85}).json()
    assert plan_file(plan, "part-low.parquet")["decision"] == "EXCLUDE"

    # 同名文件换版（新内容 -> 新 mtime）并更新清单
    time.sleep(0.01)
    write_pq(env.archive / LOW, [1.0, 2.0])
    write_manifest(env.archive, env.manifest)
    client.app.state.store.invalidate()

    # 旧结论不能放行新版本：重新保守保留，状态提示换版
    plan2 = client.post("/api/plan", json={"pm25_min": 85}).json()
    low = plan_file(plan2, "part-low.parquet")
    assert low["decision"] == "KEEP"
    assert low["verify_status"] == "unverified"
    body = client.get("/api/verify/status").json()
    view = file_view(body, "part-low.parquet")
    assert view["state"] == "unverified"
    assert view["outdated_record"]["status"] == "passed"  # 旧版本结论仍在但已过期


def test_inflight_verification_cannot_clear_new_version(env, monkeypatch):
    """在途核验期间文件被替换：本次结果作废，绝不为新版本放行。"""
    original = verification.read_column_stats

    def replace_mid_read(path, column):
        actual = original(path, column)
        time.sleep(0.01)
        write_pq(Path(path), [999.0])  # 在途换版（同路径，新 mtime）
        return actual

    monkeypatch.setattr(verification, "read_column_stats", replace_mid_read)
    client = make_client(env)
    client.post("/api/verify/stats", json={"paths": [LOW]})
    body = wait_verify_done(client)
    view = file_view(body, "part-low.parquet")
    assert view["state"] == "error"
    assert "作废" in view["record"]["error"]

    # 清单登记新版本后：旧核验（即便读到过数据）不能放行新版
    write_manifest(env.archive, env.manifest)
    client.app.state.store.invalidate()
    plan = client.post("/api/plan", json={"pm25_min": 85}).json()
    low = plan_file(plan, "part-low.parquet")
    assert low["decision"] == "KEEP"
    assert low["verify_status"] == "unverified"


def test_results_persist_across_restart(env):
    """通过结论落盘，重启（新建应用）后可直接沿用。"""
    client = make_client(env)
    client.post("/api/verify/stats", json={"paths": [LOW]})
    wait_verify_done(client)
    assert env.store_path.exists()
    payload = json.loads(env.store_path.read_text())
    assert any(r["path"] == LOW and r["status"] == "passed"
               for r in payload["records"])

    # 模拟重启：同一 store_path 新建应用与 VerifyStore
    client2 = make_client(env)
    plan = client2.post("/api/plan", json={"pm25_min": 85}).json()
    low = plan_file(plan, "part-low.parquet")
    assert low["decision"] == "EXCLUDE"  # 无需重新验真即按统计裁剪
    assert low["verify_status"] == "passed"


def test_other_file_version_change_keeps_existing_results(env):
    """其他文件换版不影响已有通过结论。"""
    client = make_client(env)
    client.post("/api/verify/stats", json={"paths": [LOW, MID]})
    wait_verify_done(client)

    time.sleep(0.01)
    write_pq(env.archive / MID, [100.0, 200.0])  # 仅 MID 换版
    write_manifest(env.archive, env.manifest)
    client.app.state.store.invalidate()

    plan = client.post("/api/plan", json={"pm25_min": 85}).json()
    # LOW 结论沿用 -> 照常裁剪；MID 换版 -> 重新保守保留
    assert plan_file(plan, "part-low.parquet")["decision"] == "EXCLUDE"
    mid = plan_file(plan, "part-mid.parquet")
    assert mid["decision"] == "KEEP"
    assert mid["verify_status"] == "unverified"


def test_disk_differs_from_manifest_version(env):
    """磁盘文件与清单登记版本不一致：保守保留并拒绝按该版本验真。"""
    client = make_client(env)
    time.sleep(0.01)
    write_pq(env.archive / LOW, [7.0, 8.0, 9.0, 9.5])  # 磁盘换版，清单不更新
    plan = client.post("/api/plan", json={"pm25_min": 85}).json()
    low = plan_file(plan, "part-low.parquet")
    assert low["decision"] == "KEEP"
    assert low["verify_status"] == "version_mismatch"
    assert STATS_UNVERIFIED_KEEP in low["reason_codes"]

    r = client.post("/api/verify/stats", json={"paths": [LOW]})
    assert r.status_code == 200
    body = wait_verify_done(client)
    view = file_view(body, "part-low.parquet")
    assert view["state"] == "error"
    assert "版本不一致" in view["record"]["error"]


# --------------------------------------------------------------------- #
# 提交参数校验
# --------------------------------------------------------------------- #
def test_submit_rejections(env):
    client = make_client(env)
    r = client.post("/api/verify/stats", json={
        "paths": ["station=Z/date=2026-01-01/ghost.parquet", "../../etc/passwd"],
    })
    body = r.json()
    assert body["submitted"] == []
    reasons = {x["reason"] for x in body["rejected"]}
    assert "不在当前清单中" in reasons and "路径越界，拒绝访问" in reasons
    assert client.post("/api/verify/stats", json={"paths": []}).status_code == 422


def test_submit_idempotent_and_force(env):
    client = make_client(env)
    client.post("/api/verify/stats", json={"paths": [LOW]})
    wait_verify_done(client)
    again = client.post("/api/verify/stats", json={"paths": [LOW]}).json()
    assert again["submitted"][0]["status"] == "passed"  # 幂等：直接沿用结论
    forced = client.post("/api/verify/stats",
                         json={"paths": [LOW], "force": True}).json()
    assert forced["submitted"][0]["status"] == "running"  # force 重新实读
    body = wait_verify_done(client)
    assert file_view(body, "part-low.parquet")["state"] == "passed"
