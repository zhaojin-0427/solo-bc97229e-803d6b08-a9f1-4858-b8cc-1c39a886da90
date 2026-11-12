"""入湖编排：暂存 -> 解析校验 -> 写 Parquet -> 短事务登记目录。

不变量：
1. 解析与写文件期间不持有目录库写事务；
2. 目录只引用已完整落盘的文件（先 rename 后登记；登记失败删文件）；
3. 坏整数 / 写盘失败 / 登记失败 => 整批不发布，不留残件引用；
4. 发布全程持有跨进程租约（app.lease），启动清理据此区分「在途」与「已遗弃」：
   在途发布者的暂存文件、临时 Parquet 与已落盘未登记的 Parquet 一律保留，
   无论文件年龄多大；发布者退出（锁自动释放）后才允许回收。
"""
import datetime
import os
import re
import shutil
import time
import uuid

from . import catalog, csvparse, lease, store

_TOKEN_RE = re.compile(r"[0-9a-f]{32}")
STAGING_MAX_AGE_SEC = 3600
ORPHAN_MAX_AGE_SEC = 600


def init_storage(cfg):
    os.makedirs(cfg.data_dir, exist_ok=True)
    os.makedirs(cfg.parquet_dir, exist_ok=True)
    os.makedirs(cfg.staging_dir, exist_ok=True)
    lease.ensure_dir(cfg)
    catalog.init_db(cfg.db_path)
    cleanup_staging(cfg)
    cleanup_orphan_files(cfg)


# ---------- 暂存（声明列类型的两步表单之间） ----------

def stage_upload(cfg, stream):
    token = uuid.uuid4().hex
    path = os.path.join(cfg.staging_dir, token + ".csv")
    with open(path, "wb") as out:
        shutil.copyfileobj(stream, out, 256 * 1024)
    return token


def staged_path(cfg, token):
    if not _TOKEN_RE.fullmatch(token or ""):
        raise ValueError("无效的暂存令牌")
    path = os.path.join(cfg.staging_dir, token + ".csv")
    if not os.path.isfile(path):
        raise ValueError("暂存文件不存在或已过期，请重新上传")
    return path


def staged_header(cfg, token):
    return csvparse.read_header(staged_path(cfg, token))


def discard_staged(cfg, token):
    if _TOKEN_RE.fullmatch(token or ""):
        try:
            os.unlink(os.path.join(cfg.staging_dir, token + ".csv"))
        except OSError:
            pass


# ---------- 发布 ----------

def publish(cfg, station, source_filename, csv_path, col_types):
    """校验并发布一个批次。任何失败都整批不发布。"""
    station = (station or "").strip()
    if not station:
        raise ValueError("站点不能为空")
    if len(station) > 200:
        raise ValueError("站点名过长")

    # 租约先于任何数据读取出现：暂存 CSV（可能正在被慢解析）与随后写入的
    # 临时/正式 Parquet 都宣告为「在途」，启动清理不会按文件年龄误删。
    batch_id = uuid.uuid4().hex
    final_name = batch_id + ".parquet"
    final_path = os.path.join(cfg.parquet_dir, final_name)
    lease_handle = lease.acquire(cfg, batch_id, [csv_path, final_path])
    try:
        header = csvparse.read_header(csv_path)
        species_names = header[1:]
        if len(species_names) != len(col_types):
            raise ValueError("列类型数量与 CSV 鸟种列数不一致")
        for t in col_types:
            if t not in catalog.COLUMN_TYPES:
                raise ValueError(f"非法列类型: {t!r}")
        if store.DATE_COLUMN in species_names:
            raise csvparse.CsvError(f"鸟种列不能使用保留列名 {store.DATE_COLUMN}")

        # 1) 解析与校验（不持有目录库写事务）；坏整数在此整批拒绝
        dates, values = csvparse.parse_rows(csv_path, col_types)
        columns = list(zip(species_names, col_types, values))
        table = store.build_batch_table(dates, columns)

        # 2) 写独立 Parquet 文件（不持有目录库写事务）；写盘失败整批不发布。
        #    临时文件校验 footer 后才 rename，目录只会看到完整文件。
        store.write_parquet_atomic(table, final_path)

        # 3) 短事务：合并逻辑结构 + 登记批次（并发发布者在此串行）
        conn = catalog.connect(cfg.db_path)
        try:
            with catalog.write_txn(conn):
                changes = catalog.merge_schema(conn, [(n, t) for n, t, _ in columns])
                catalog.insert_batch(conn, {
                    "id": batch_id,
                    "station": station,
                    "source_filename": source_filename or "",
                    "parquet_file": final_name,
                    "n_rows": table.num_rows,
                    "created_at": datetime.datetime.now(datetime.timezone.utc)
                                  .isoformat(timespec="seconds"),
                })
        except BaseException:
            try:
                os.unlink(final_path)  # 目录未登记 => 删除未发布文件，目录永不引用残件
            except OSError:
                pass
            raise
        finally:
            conn.close()
    finally:
        lease_handle.release()
    return {"batch_id": batch_id, "station": station,
            "n_rows": table.num_rows, "changes": changes}


# ---------- 启动清理 ----------

def _protected_now(cfg, path):
    """清理每个候选文件的最后一道检查：当前是否仍被存活发布者的租约保护。

    发布者「登记 -> 释放租约」的顺序保证：只要租约仍活跃，即使引用清单是在
    登记之前读的，也绝不能删该文件。批次 UUID 唯一，新发布者不会复用路径。
    """
    active, _abandoned = lease.scan_registry(cfg)
    return os.path.abspath(path) in active


def _referenced_files(cfg):
    conn = catalog.connect(cfg.db_path)
    try:
        return {r["parquet_file"] for r in conn.execute("SELECT parquet_file FROM batches")}
    finally:
        conn.close()


def cleanup_staging(cfg, max_age_sec=STAGING_MAX_AGE_SEC):
    """删除过期暂存文件；正在被某个存活发布者解析/发布的暂存文件一律保留。"""
    now = time.time()
    active, _abandoned = lease.scan_registry(cfg)
    for name in os.listdir(cfg.staging_dir):
        path = os.path.join(cfg.staging_dir, name)
        try:
            if now - os.path.getmtime(path) <= max_age_sec:
                continue
            if os.path.abspath(path) in active:
                continue
            if _protected_now(cfg, path):
                continue
            os.unlink(path)
        except OSError:
            pass


def cleanup_orphan_files(cfg, max_age_sec=ORPHAN_MAX_AGE_SEC):
    """清理崩溃遗留，跨进程安全。

    - 仍被存活发布者租约保护的文件（写了一半的临时文件、已 rename 尚未登记的
      Parquet、正在解析的暂存 CSV）一律保留，与文件年龄无关；
    - 发布者已退出（租约锁已释放）留下的未登记临时文件与 Parquet 立即回收；
    - 没有租约归属的孤儿：临时文件与未引用 Parquet 超过宽限期才删；
    - 每次 unlink 前重读引用清单与租约，避免误删「清单读完之后才完成登记」的文件。
    """
    now = time.time()
    active, abandoned = lease.scan_registry(cfg)
    # 初始清单只用于快筛；真正删除前还会再读一次（见 safe_to_delete），
    # 以便发现「清理读清单之后才完成登记」的批次。
    referenced = _referenced_files(cfg)

    def safe_to_delete(path):
        if os.path.abspath(path) in active:
            return False
        if _protected_now(cfg, path):
            return False
        # 顺序关键：先确认没有活跃租约，再读引用清单。发布者「登记 -> 退租」
        # 的顺序保证此刻若已登记，清单必然可见；若未登记，则它仍持租约或已退出。
        return os.path.basename(path) not in _referenced_files(cfg)
    for name in os.listdir(cfg.parquet_dir):
        path = os.path.join(cfg.parquet_dir, name)
        try:
            if not (name.endswith(".parquet") or
                    (name.startswith(".") and name.endswith(".tmp"))):
                continue
            if name.endswith(".parquet") and name in referenced:
                continue
            stale_abandoned = os.path.abspath(path) in abandoned
            if not stale_abandoned and now - os.path.getmtime(path) <= max_age_sec:
                continue
            if safe_to_delete(path):
                os.unlink(path)
        except OSError:
            pass
