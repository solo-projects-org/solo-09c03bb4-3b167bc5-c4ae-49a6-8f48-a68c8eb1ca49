# PM2.5 Parquet 档案读取计划服务

环境数据工程师在**按站点、日期分区的 PM2.5 Parquet 档案**中查找超标记录前，
先用本服务生成一份**可核对的文件读取计划**：服务只读 Parquet footer / 行组统计
（分区值、`min/max/null_count`），不扫描数据行，即给出

- 待读文件清单与**计划字节数**（保留文件字节求和）；
- 每个文件 **KEEP / EXCLUDE 的逐条原因**（中文说明 + 稳定原因码）；
- 档案**快照 ID**：清单替换后旧计划立即失效（HTTP 409）。

针对“**转码把错误的 min/max 写进 footer，超标查询因此漏读文件**”的风险，
清单模式可开启**统计验真**：值班员在网页勾选清单中的 Parquet 文件，服务在后台
**实读 PM2.5 列**重算 `min/max/null_count` 并与 footer 统计逐项核对；
未通过验真的文件一律保守保留，不再按统计裁剪。

技术栈：FastAPI + DuckDB + PyArrow，单页原生 HTML/JS 前端，Docker Compose 一键启动。

## 1. 目录结构

```
.
├── src/pm25_planner/
│   ├── config.py          # 环境变量配置
│   ├── models.py          # 请求/响应 Pydantic 模型
│   ├── parquet_meta.py    # PyArrow 元数据提取 + DuckDB 独立交叉核对
│   ├── snapshot.py        # 档案快照（目录扫描 / 清单 JSON 两种来源）+ LRU 缓存
│   ├── planner.py         # 逐文件保留/排除判定引擎（纯函数，含全部原因码）
│   ├── verification.py    # 统计验真：后台实读核对 + 按版本持久化的结论库
│   ├── reader.py          # DuckDB 预览执行（同样绑定快照）
│   ├── api.py             # FastAPI 路由
│   └── main.py            # uvicorn 入口
├── web/index.html         # 单页前端（无构建步骤，含统计验真面板）
├── scripts/
│   ├── generate_sample.py       # 生成本地样例档案（含各类异常文件）
│   ├── replace_manifest_demo.py # 清单原子替换 → 快照失效演示
│   └── entrypoint.sh            # 容器入口（按需自动造样例数据）
├── tests/                 # 43 项 pytest 自测
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 2. 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

Compose 默认启用**清单模式 + 统计验真**（`PM25_MANIFEST_PATH` 与
`PM25_VERIFY_ENABLED=1`）。容器首次启动时档案为空卷，entrypoint 自动生成
14 个覆盖全部分支的样例 Parquet 文件与清单（含一个 footer max 被写错的
station-E 演示文件）。之后重启不会重复生成；验真结论库
`/data/manifest/verification.json` 随数据卷持久化，重启后沿用。

## 3. 本地开发运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

python scripts/generate_sample.py           # 生成 data/ 样例
PYTHONPATH=src uvicorn pm25_planner.main:app --port 8000
# http://localhost:8000
```

切换为**清单模式**（由外部系统维护档案清单，原子换文件）：

```bash
PM25_MANIFEST_PATH=$(pwd)/data/manifest/manifest.json \
  PYTHONPATH=src uvicorn pm25_planner.main:app --port 8000
```

再开启**统计验真**（仅清单模式生效）：

```bash
PM25_MANIFEST_PATH=$(pwd)/data/manifest/manifest.json \
PM25_VERIFY_ENABLED=1 \
  PYTHONPATH=src uvicorn pm25_planner.main:app --port 8000
```

配置项（均有默认值）：`PM25_ARCHIVE_DIR`、`PM25_MANIFEST_PATH`、
`PM25_COLUMN`（默认 `pm25`）、`PM25_PREVIEW_MAX_FILES`、`PM25_META_CACHE_SIZE`、
`PM25_VERIFY_ENABLED`（默认关）、`PM25_VERIFY_STORE_PATH`
（默认 `<清单目录>/verification.json`）、`PM25_VERIFY_WORKERS`（默认 2）。

## 4. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/health` | 健康检查，含当前快照 ID 与验真开关状态 |
| GET | `/api/snapshot` | 当前快照：ID、来源、文件数、总字节 |
| POST | `/api/plan` | 生成读取计划；带 `expected_snapshot_id` 时做快照绑定校验 |
| POST | `/api/plan/verify` | 核对某快照 ID 是否仍为当前快照 |
| POST | `/api/plan/preview` | 用 DuckDB 实读“保留文件”做小批量预览 |
| GET | `/api/verify/status` | 验真开关状态 + 清单各文件当前版本的验真结论 |
| POST | `/api/verify/stats` | 提交统计验真：`{"paths": [...], "force": false}` |
| GET | `/api/debug/duckdb-stats?path=` | DuckDB 与 PyArrow 双引擎统计核对 |

请求体字段（全部可选）：

```json
{
  "stations": ["station-A"],
  "date_from": "2026-01-01",
  "date_to":   "2026-01-31",
  "pm25_min": 75.0,
  "pm25_max": null,
  "include_nulls": true,
  "expected_snapshot_id": "75c585af64ecfa17"
}
```

清单已替换而请求携带旧 `expected_snapshot_id` 时返回：

```
HTTP 409
{"detail":{"error":"STALE_SNAPSHOT",
           "expected_snapshot_id":"...", "current_snapshot_id":"..."}}
```

## 5. 判定规则（与原因码一一对应）

判定只依据**分区值**与 **footer 统计**，顺序如下：

1. **文件缺失**（清单引用了磁盘上不存在的文件）→ `FILE_MISSING_KEEP` 保守保留；
2. **分区不完整/非法**（无法解析 station 或 date）→
   `PARTITION_UNPARSEABLE_KEEP`，跳过一切分区裁剪，且即使统计不相交也强制保留；
3. 分区完整时做站点 `PARTITION_STATION_EXCLUDED`、
   日期 `PARTITION_DATE_EXCLUDED` 裁剪；
4. footer 损坏 `FILE_UNREADABLE_KEEP`、缺列 `MISSING_COLUMN_KEEP`、
   无统计 `NO_STATS_KEEP` → 一律保守保留；
5. 统计完整时比较 min/max 与请求区间（**闭区间**，边界相等算相交）：
   相交 `STATS_MATCH_KEEP`，不相交 `STATS_RANGE_DISJOINT` 排除；
6. 缺测策略：
   - `include_nulls=true` 且文件有 null → `NULLS_INCLUDED_KEEP` 保留；
   - `include_nulls=false` 且文件只有 null/无区间内非空值 →
     `NULLS_PRESENT_BUT_EXCLUDED` 排除（分区不完整时仍保守保留）。

> 保守保留只会产生**误保留**（多读），不会漏读；前端与 `notes` 字段会显式提示。

## 6. 统计验真（清单模式开关）

footer 统计由上游转码写入，**可能写错**（例如 max 写小），按统计裁剪会因此
跳过本该读取的文件。开启 `PM25_VERIFY_ENABLED=1` 后：

1. **网页提交验真**：值班员在“统计验真”面板勾选清单中的文件，服务在后台
   线程**实读 PM2.5 列**（PyArrow 全列解码；不用 DuckDB 的 `min()/max()`
   聚合，因其统计下推会直接采信 footer），重算 `min/max/null_count/row_count`
   与 footer 逐项比对，结论分为 通过 / 不符（逐项列出）/ 失败（附原因）；
2. **计划与预览联动**：仍先按站点、日期做分区裁剪；之后仅“**磁盘文件与
   清单登记版本一致且验真通过**”的文件照常按统计裁剪，其余一律保守保留
   （原因码 `STATS_UNVERIFIED_KEEP`，`verify_status` 字段标注
   未验真/验真中/不符/失败/版本不一致）；
3. **计划仍只读元数据**：验真在后台进行，验真期间照常出计划
   （在途文件按“验真中”保守保留）；
4. **版本即清单登记值**：版本标识 = 清单登记的 `(路径, size, mtime_ns)`；
   通过结论持久化到 `verification.json`（写临时文件 + `os.replace` 原子落盘），
   **重启后沿用**，其他文件换版不影响已有结论；
5. **换版必须重新验真**：同名文件换版（size/mtime 变化）后旧结论自然失配，
   网页会提示“须重新验真”；验真前后各 stat 一次磁盘文件，**在途核验遇到
   换版立即作废**，绝不为新版本放行。

footer 本身无完整统计的文件（no_stats / 缺列 / 损坏）提交验真会直接得到
失败结论（“无统计可核对”），它们在计划中本就已保守保留。

## 7. 快照与清单原子替换

- 快照 ID = 全部 `(相对路径, size, mtime_ns)` 有序集合的 SHA-256 前 16 位；
- 扫描模式按目录 mtime + 文件签名判断变化；清单模式把清单文件本身的
  size/mtime 一并纳入指纹；
- 清单写入采用“写临时文件 + `os.replace`”，读者只会看到旧版或新版完整 JSON，
  绝不会读到半截文件（若读到损坏 JSON，接口返回 503 而非产生错误计划）；
- 元数据按 `(size, mtime_ns)` 做 LRU 缓存，文件变动自动失效。

演示：

```bash
python scripts/replace_manifest_demo.py
# 输出替换前后两个不同的快照 ID，旧 ID 经 /api/plan/verify 判定失效
```

## 8. 样例档案覆盖的场景

| 文件 | 场景 |
|---|---|
| station-A 三天 | 全低于阈值 / 含缺测的超标日 / 多 row group 跨阈值 |
| station-B | 全低于阈值 / **全 null 列** / 跨阈值无 null |
| station-C | `write_statistics=False` 无统计 / 缺 pm25 列 / **截断损坏** / 非法分区孤儿目录 |
| station-D | min==max 边界值，验证闭区间包含 |
| station-E | **footer max 被转码写错**（真实 96.0 记为 42.0），供统计验真演示 |

## 9. 自测

```bash
PYTHONPATH=src pytest -q
# 43 passed
```

覆盖：分区裁剪、闭区间边界、null 两种策略、全 null 列、无统计/缺列/损坏/
缺失文件的保守保留、非法分区强制保留、计划字节求和、快照稳定性、
清单原子替换失效、409/503、预览绑定、PyArrow 与 DuckDB 统计一致性、
路径穿越拦截，以及统计验真全链路：实读核对通过/不符/失败、未验真保守保留、
验真期间照常出计划、预览联动、换版后须重新验真、在途核验遇换版作废、
结论重启沿用、其他文件换版不影响已有结论、磁盘与清单版本不一致拦截。
