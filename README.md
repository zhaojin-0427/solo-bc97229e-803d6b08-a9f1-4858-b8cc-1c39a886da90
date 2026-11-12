# 鸟类监测数据湖（Flask + PyArrow + SQLite）

把各监测站逐年新增鸟种的调查 CSV 入湖：每批一个独立 Parquet 文件，目录库（SQLite）
按列名合并出统一的逻辑结构，研究员可按站点预览 / 下载统一结构的 CSV。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000
```

可选：灌入示例数据（含一次「uint64 与 text 冲突 → 提升为 text」的演示）

```bash
docker compose exec web python scripts/seed_demo.py
```

运行端到端自检（解析校验 / 结构合并 / 类型提升 / 并发 / 重启持久化 / Web 流程）：

```bash
docker compose exec web python scripts/selftest.py
```

运行跨进程清理 / 发布竞态回归测试（真实 fork 子进程，可稳定控制交错与发布者退出）：

```bash
docker compose exec web python scripts/race_test.py
```

本地开发（不用 Docker）：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export DATA_DIR=./data
flask --app app.web run        # 或 gunicorn -b 0.0.0.0:8000 app.web:app
```

## 数据格式与入湖流程

1. **上传**：每批填写站点 + 选 CSV。CSV 首列为观测日（`YYYY-MM-DD`），其余每列一种鸟；
   列名去空白后非空且不得重复；空单元格一律按 NULL 处理。
2. **逐列声明类型**：每个鸟种列声明为 `uint64`（非负 64 位整数，0 ~ 2^64-1）或 `text`。
   首列观测日归一化为固定列名 `obs_date`（date 类型），鸟种列不得使用该保留名。
3. **发布**：
   - 为批次创建**发布租约**（见下节），再读取 / 解析 CSV；
   - 解析校验（坏整数、坏日期、字段数不齐 → **整批不发布**）；
   - 写 `parquet/<批次UUID>.parquet`：先写临时文件 + fsync，**解析 footer 校验完整与行数**
     后才原子 rename，**既有文件永不重写**；
   - 最后才开启**短暂的**目录库写事务：合并逻辑结构 + 登记批次。
   解析和写文件期间**不持有**目录库写事务；登记失败会删掉未发布的文件，**目录永不引用残件**。

## 逻辑结构合并规则

- 按列名合并；批次缺列在读出时补 NULL；新鸟种自动加列；
- 同鸟种类型冲突（一边 `uint64` 一边 `text`）→ 提升为 `text`；
- 提升后**不得退回**整数（`text` 是吸收态）；旧整数读出时按**十进制**转成文本；
- 并发上传新增不同鸟种、或对同列并发提升：发布事务经 `BEGIN IMMEDIATE` 串行化，
  结构变化与各批记录都完整保留（自检第 3 项覆盖）。

## 一致性读取

预览 / 下载在同**一个 SQLite 读事务**内取「逻辑结构 + 文件清单」快照（WAL 模式），
再按该快照读取不可变的 Parquet 文件——一次取数始终使用同一状态的结构与清单。
页面上的 `schema v<N>` 每次结构变化都会递增。

## 持久化与重启

`DATA_DIR`（Docker 卷 `lake_data`）内含 `catalog.db`（WAL）、全部 Parquet 文件与
`inflight/` 发布租约。重启后直接可用。

### 多进程共用 DATA_DIR 时的清理 / 发布竞态

Gunicorn 起多个 worker 时，每个 worker 启动都会跑一次清理。清理绝不能按文件年龄
误删另一进程仍在发布的文件：

- **发布租约**：每个发布者在写任何数据之前，先在 `DATA_DIR/inflight/<批次UUID>.lock`
  上持有排他 `flock`，租约内容宣告本批次的暂存 CSV 与最终 Parquet。`flock` 由内核在
  进程退出（含崩溃 / SIGKILL）时自动释放。
- **在途保护**：清理对租约做非阻塞加锁——加不上说明发布者仍活着，其文件（写了一半的
  临时 Parquet、已 rename 尚未登记的 Parquet、正在解析的暂存 CSV）一律保留，
  **与文件年龄无关**（即使超过 600 秒）。
- **登记竞态**：删除每个候选文件前，先确认没有活跃租约，再重读目录引用清单。
  发布者「先把文件 rename 完整 -> 再短事务登记 -> 最后释放租约」的顺序保证：
  清理读过清单之后才完成登记的文件不会被删。
- **复用与否**：批次 UUID 唯一，新发布者不会复用旧路径；发布者在解析 / 写文件 / 登记
  期间都不持有目录库写事务，慢发布不会阻塞其他批次发布，既有数据照常可取数。
- **崩溃回收**：发布者退出后租约锁自动释放，后续启动把其未发布的临时文件与 Parquet
  视为过期孤儿立即回收；若它已登记（登记后退租前崩溃），文件仍因目录引用而保留。
  有活跃发布者时，其他过期孤儿照样回收，不受影响。
- **完整性**：临时 Parquet 写完后先 `fsync` 并校验 footer 与行数，通过后才原子 rename；
  截断 / 损坏的写入不会被登记。登记失败会删除未发布文件，目录永不引用残件。

`scripts/race_test.py` 用真实子进程覆盖上述交错：写完后暂停再启动清理、解析中暂停、
清理读清单后才登记、发布者崩溃、活跃 + 孤儿并存、临时租约崩溃、截断写入。

## 目录结构

```
app/
  catalog.py    # SQLite 目录库：逻辑结构 + 批次清单，读写事务
  csvparse.py   # CSV 解析与校验（纯标准库）
  lease.py      # 跨进程发布租约（flock）：在途发布者 / 过期孤儿的判定
  store.py      # PyArrow：批次表构建、原子写 Parquet（footer 校验）、统一结构读取、CSV 导出
  service.py    # 入湖编排：租约 -> 校验 -> 写文件 -> 短事务登记；跨进程安全清理
  web.py        # Flask 路由：上传/声明/发布/预览/下载
  templates/    # 页面
scripts/
  selftest.py   # 端到端自检
  race_test.py  # 跨进程清理 / 发布竞态回归测试
  seed_demo.py  # 示例数据灌入
samples/        # 示例 CSV
```

## 主要 HTTP 入口

| 路由 | 说明 |
| --- | --- |
| `GET /` | 总览：逻辑结构、站点、批次 |
| `GET/POST /upload` | 上传 CSV（第一步） |
| `POST /publish` | 声明类型后发布（第二步） |
| `GET /preview?station=&limit=` | 按站点预览统一结构 |
| `GET /download.csv?station=` | 下载统一结构 CSV（可按站点过滤） |
| `GET /healthz` | 健康检查 |
