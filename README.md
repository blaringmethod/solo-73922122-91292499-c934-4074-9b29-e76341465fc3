# Traffic PITR Recovery API（交通数仓误删恢复）

路口分钟计数被值班员误删后，需要从**物理备份 + WAL 归档**把删除前的记录找回来补数；
恢复期间主库采集不中断。本服务在启动时对主库做一次 `pg_basebackup` 物理基准备份，
主库持续归档 WAL；用户在任意时刻创建**命名恢复点**，只有当该点所需的 WAL 全部
**完整归档**后才允许选择它恢复；每次恢复都从基准备份**重建一个全新的隔离只读副本**，
重放 WAL 到目标恢复点即停，供查询补数。

## WAL 归档安全（短文件防护）

复制中断可能在归档卷上留下**短文件**（小于 16 MiB 的 WAL 段）。本栈对归档与受理
做了端到端加固（`db/archiver/safe_archive.sh` 与 `api/app/walarchive.py` 同语义）：

- **未写完整不发布**：归档先写临时文件并校验大小，确认恰好 16 MiB 后才原子
  `rename` 成最终文件名；短文件永远不会以有效归档的名字出现。
- **完整文件不覆盖**：同名完整段已存在时归档直接成功返回，源库重试不会冲掉好文件。
- **短文件可重归档**：残留的短文件会被删除并在源 WAL 仍在（`wal_keep_size`
  保留期）时重新归档；主库 `archive_command` 自身重试与 API 的 `POST /archive/repair`
  两条修复路径都遵守同一规则。
- **副本取 WAL 同样守门**：副本 `restore_command` 经 Python 助手取段，缺失或截短
  的段直接拒绝（PostgreSQL 随即 FATAL），绝不把半截文件喂给重放。
- **恢复点不搞“一劳永逸”**：就绪状态按归档实况持续重算；曾标记 `ready` 的点，
  若其所需段后来缺失或被截短，立即退回 `pending` 并记录问题段名。
- **恢复受理二次核验**：发起恢复前再次核对每一段都在且满长；不满足返回 409，
  **在退役任何旧副本之前拒绝**，因此健康旧副本继续按原恢复点供数。补齐归档后
  用**原恢复点**重新 POST 即可重试。

## 架构

```
┌──────────────┐  安全 WAL 归档(原子+大小校验)  ┌───────────────┐
│  db (主库)    │ ───────────────────────────▶ │  wal_archive  │
│  traffic 库   │  archive_command:            │  (共享卷)      │
│  单表:        │  safe_archive.sh %p %f       │  仅完整段可见   │
│  traffic_     │                            ┌─┴───────────────┘
│  minute_      │        ┌───────────────────┴──────────────┐
│  counts       │        │  api (FastAPI)                    │
└──────────────┘        │  · 启动时物理基准备份 (/backups)    │
       ▲                │  · 恢复点就绪检查 (存在性+16MiB)     │
       │ 主库采集不中断   │  · /archive 盘点、/archive/repair  │
       └────────────────│  · 恢复执行器: 每次从基准备份重建    │
                        │    隔离副本, 重放 WAL 到恢复点暂停   │
                        │    (/recovery_work/rN, 只读热备)     │
                        └─────────────────────────────────────┘
```

- **db**：`postgres:16`，开启 `archive_mode=on`、`archive_timeout=10s`，由挂载进
  容器的 `db/archiver/safe_archive.sh` 执行原子、带大小校验的 WAL 归档。
  源库只有一张业务表 `traffic_minute_counts(intersection_id, minute_utc, vehicle_count)`。
- **api**：`postgres:16` 镜像 + FastAPI。启动时执行 `pg_basebackup -X stream` 建立物理
  基准备份；恢复时把基准备份复制到全新目录，配置 `restore_command`（经
  `python -m app.walarchive restore` 守门）+ `recovery_target_name` +
  `recovery_target_action=pause`，以热备只读方式启动在独立端口（容器内 127.0.0.1:55433）。
  api 还以只读方式挂载主库数据目录（`/source_pgdata`），仅用于短文件修复时从
  `pg_wal` 重新归档，不写主库。
- API 自身的台账（恢复点、恢复任务）存在独立 SQLite（api_state 卷），**不污染源库**，
  源库始终只有一张业务表。

## 快速开始

```bash
docker compose up --build
# 等待 api 就绪后:
# 1) 打开演示入口 http://localhost:8000/docs （Swagger UI，根路径 / 会重定向到这里）
# 2) 或跑一键演示脚本（需要 curl 和 jq）:
./scripts/demo.sh
```

演示脚本完整走一遍：模拟采集写入 → 创建恢复点 `before_delete` → 等待 WAL 归档齐全 →
模拟误删最近 10 分钟 → 主库继续写入新路口数据 → 发起 PITR 恢复 → 从只读副本查回被删
记录 → 断言副本不含恢复点之后的提交、主库数据未受影响。

## API 一览（详见 /docs）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 服务与主库连通性 |
| GET | `/backup` | 启动基准备份状态（running/ready/failed + 错误） |
| GET | `/archive` | 归档盘点：完整段数、截短段名、残留临时文件 |
| POST | `/archive/repair` | 用源 `pg_wal` 重新归档残留短文件（完整段绝不覆盖），并重算就绪状态 |
| POST | `/demo/generate` | 模拟采集：生成 路口×分钟 计数（可指定前缀/数量） |
| POST | `/demo/delete` | 模拟误删：删除最近 N 分钟数据 |
| POST | `/demo/fault/truncate-archive` | 故障注入：把指定归档段截短（模拟复制中断留下的短文件） |
| POST | `/demo/fault/remove-archive` | 故障注入：删除一个已归档段（模拟归档后来缺失） |
| GET | `/primary/traffic` | 查主库当前数据（证明主库一直在采集） |
| GET | `/demo/overview` | 主库数据、备份、归档、恢复点、恢复任务总览 |
| POST | `/restore-points?name=...` | 创建命名恢复点（`pg_create_restore_point` + 强制切 WAL） |
| GET | `/restore-points` | 恢复点列表：`pending`（WAL 未齐/含短段）/ `ready`（可选）/ `expired`（早于基准备份）；附 `missing_wal`、`truncated_wal` 明细 |
| POST | `/recoveries` | 发起恢复 `{"restore_point": "name"}`，202 异步任务；同一时刻只允许一项在途；受理前二次核验，不足 409 且不动旧副本 |
| GET | `/recoveries` / `/recoveries/{id}` | 恢复任务状态：running/success/failed/superseded + 错误详情 |
| GET | `/recoveries/current` | 当前对外服务的恢复（无则 404） |
| GET | `/replica/traffic` | 查询已恢复只读副本（未就绪/已失败返回 409） |

## 恢复语义（对照需求）

- **启动即备份**：api 启动时后台执行 `pg_basebackup -X stream`（失败自动重试 10 次，
  状态见 `/backup`）；主库 WAL 由 `archive_command` 持续归档。
- **恢复点门控**：创建恢复点时记录其 LSN 并 `pg_switch_wal()`；后台任务持续校验
  「基准备份结束 LSN → 恢复点 LSN」之间每一个 WAL 段都已在归档目录中**且大小恰为
  16 MiB**，齐全才置 `ready`；未就绪的点发起恢复返回 409。早于基准备份重做起点的点
  标记 `expired`。`ready` 不是永久状态：任何所需段之后缺失或被截短，立即退回
  `pending`，问题段名记录在 `missing_wal` / `truncated_wal` 字段。
- **受理二次核验、旧副本不被误退役**：`POST /recoveries` 在退役当前副本之前，对归档
  再做一次存在性 + 大小核验；不通过直接 409（错误信息点名缺失/截短段），健康旧副本
  继续按原恢复点供数。补齐后用**同一个恢复点**重新提交即可重试。
- **物理恢复**：每次恢复都 `cp` 基准备份到全新目录 `recovery_work/rN`，写
  `recovery.signal` 与 PITR 配置后启动独立 postgres——不是逻辑留史/影子表。
  `restore_command` 走 `python -m app.walarchive restore`，取段时同样拒绝短段/缺段。
- **到达目标才报成功**：轮询副本直到 `pg_last_wal_replay_lsn() >= 目标 LSN`，
  并对业务表执行一次真实查询确认可查询，才置 `success` 并开放查询。
- **失败即报、不留残缺**：缺 WAL/短 WAL（`restore_command` 拒绝 → postgres FATAL）、
  恢复进程退出（pg_ctl 探活 + 后台看门狗每 3s 巡检）都会把任务置 `failed` 并记录
  postgres 日志尾部；失败任务的目录被清理，**不开放残缺副本，也不回退到上一次副本**
  （新恢复通过受理后才会退役并删除旧副本）。重试 = 再 POST 一次。
- **单一在途**：存在 running 任务时再发起返回 409。
- **不混数据**：新恢复启动即退役（停止+删除）旧副本并清空服务指针；每次恢复都是
  基准备份的全新拷贝，先后两个恢复点互不干扰。
- **主库不阻塞**：恢复只读归档卷与基准备份，对主库零写入、零锁。

## 环境变量（api 服务）

| 变量 | 默认 | 说明 |
|---|---|---|
| `PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE` | db/5432/postgres/postgres/traffic | 主库连接 |
| `REPLICA_PORT` | 55433 | 副本监听端口（容器内 127.0.0.1） |
| `RECOVERY_TIMEOUT_SEC` | 180 | 等待重放到目标点的超时 |
| `REPLICA_RUN_AS` | postgres | 副本进程运行用户（容器内 root 经 gosu 降权；本地裸跑可置空） |
| `BACKUP_ON_STARTUP` | 1 | 启动时是否做基准备份（预置备份的测试场景可置 0） |
| `AUTO_REPAIR` | 0 | 后台是否定期用源 `pg_wal` 重归档短/缺段（默认关：主库 `archive_command` 自身重试 + 手动 `/archive/repair` 已覆盖；置 1 开启自愈巡检） |
| `SOURCE_PG_WAL` | /source_pgdata/pg_wal | 源库 `pg_wal` 的只读挂载点，供短/缺段重新归档 |
| `PYTHON_BIN` | /opt/venv/bin/python | 副本 `restore_command` 调用的解释器（需能 `import app.walarchive`） |
| `WAL_ARCHIVE_DIR/BACKUP_DIR/RECOVERY_ROOT/STATE_DB` | /wal_archive /backups/base /recovery_work /state/api.db | 存储路径 |

## 已验证 / 未验证（诚实清单）

交付前在本沙箱用 PostgreSQL 16.16（PGDG 官方 .deb 解包，与 `postgres:16-bookworm`
同源）+ 本 API 真实运行验证，测试代码在 `api/tests/`，可用
`python3 tests/test_walarchive.py`、`python3 tests/test_integration.py`、
`bash tests/test_recovery_mechanics.sh` 复跑（后两者需要一个本地 PG 16 实例）。
**注意：本沙箱内核级禁止一切跨进程信号**（kill/pidfd 均 EPERM），导致两个环境性
限制：(1) `pg_basebackup` 的 checkpoint 信号被拒（`could not signal for checkpoint:
Permission denied`）；(2) 热备副本无法进入可连接状态（postmaster 收不到
PMSIGNAL）。这两点在正常 Linux / Docker 中不存在，属沙箱特有。

已验证（真实运行）：

- ✅ **安全归档（24 项断言，`test_walarchive.py`）**：临时文件+原子 rename 发布；
  完整段重试不覆盖（Python 助手与 `safe_archive.sh` 双实现同语义）；短文件从尚存
  源 WAL 重归档；短源段拒绝发布；路径穿越名拒绝；restore CLI 拒绝缺段/短段；
  残留 .tmp 清理
- ✅ **真实主库归档链路**：主库以 `safe_archive.sh` 为 `archive_command` 持续归档，
  全部段恰好 16 MiB；预置同名短文件后主库重试自动替换为完整段，`failed_count=0`
- ✅ **恢复点/受理（31 项断言，`test_integration.py`，FastAPI TestClient + 真 PG）**：
  归档齐全才 ready；截短后 ready→pending 且记录 `truncated_wal`；发起恢复 409 且
  **不退役旧副本、不动服务指针**；`/archive/repair` 修复后同点重新可选；删段后
  `missing_wal` + 409；未知点 404；过期点 expired/409；重名 409；旧版 SQLite 台账
  自动迁移出新列
- ✅ **物理恢复正确性（9 项断言，`test_recovery_mechanics.sh`，真实 postgres 重放）**：
  Python `restore_command` 驱动的 PITR 到 `before_delete` 后暂停，单用户模式核对
  **60 行全在（含被删 10 分钟）、恢复点后提交（IXB 6 行）为 0、3 个路口齐全**；
  归档含短段时 restore 助手拒绝、postgres FATAL、到不了目标；修复后用原恢复点
  重试成功且数据仍为 60 行
- ✅ API 启动、/health、/docs、/openapi.json（含新端点）、/ 重定向；演示写/删接口
- ✅ `docker-compose.yml` 与 demo.sh 语法校验、全部 Python 模块编译导入、OpenAPI 生成

未能在本环境验证（沙箱信号限制所致，逻辑已按 docker 场景设计并走查）：

- ⚠️ `docker compose up` 整体构建与联调（沙箱无 docker；已用同版本 PG 二进制与
  同版本 Python 依赖在本地等价运行）
- ⚠️ `pg_basebackup` 成功路径（沙箱禁止 checkpoint 信号；失败重试路径沿用原实现）
- ⚠️ 热备副本接受连接后的 API 成功闭环（`status=success` 与 `/replica/traffic`
  200）：沙箱中 postmaster 无法收到热备就绪信号。恢复本身已验证到达目标点
  （日志 `recovery stopping at restore point` + `pausing at the end of recovery`），
  数据正确性已用单用户模式验证（见上）
- ⚠️ `pg_ctl stop` 退役旧副本（沙箱无法发信号；docker 中正常）

## 限制

- 单 api 进程、单副本端口：同一时刻一个恢复、一个副本（需求即如此）。
- 基准备份为启动时一份；api 重启会重取备份，此前创建的恢复点会标记 `expired`。
- 恢复点名称唯一（重复创建返回 409）；WAL 归档卷只增不减，演示规模无清理策略。
- 无认证鉴权，仅限内网演示。
