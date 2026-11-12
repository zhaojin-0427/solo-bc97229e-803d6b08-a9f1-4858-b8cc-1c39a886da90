#!/usr/bin/env python3
"""跨进程清理 / 发布竞态回归测试（可稳定控制交错）。

覆盖：
1. 慢发布者在「Parquet 已落盘、尚未登记」期间（文件年龄超过 600s），另一进程
   启动清理不得删除其文件；期间其他批次仍可发布、既有数据仍可取数，
   且解析 / 写文件不持有目录库写事务。
2. 清理读过引用清单之后才完成登记的批次文件不得被删除。
3. 发布者退出（进程崩溃 / 未登记）留下的过期临时文件与 Parquet 能被后续启动回收；
   有活跃发布者时，仍能回收其他过期孤儿。
4. 崩溃在「登记之后、退租之前」的过期租约不得导致已登记文件被删。
5. 写盘截断（footer 不完整）不得登记残件，成功文件必须完整可读。

用法（仓库根目录，容器内或装好依赖的环境）：
    python scripts/race_test.py
"""
import datetime
import multiprocessing as mp
import os
import sys
import tempfile
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import catalog, lease, service, store  # noqa: E402
from app.config import Config  # noqa: E402


def check(name, cond, extra=""):
    if cond:
        print(f"  ✓ {name}")
    else:
        raise AssertionError(f"{name}  {extra}")


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)


def age(path, seconds=3600):
    t = time.time() - seconds
    os.utime(path, (t, t))


def snap(cfg, station=None):
    conn = catalog.connect(cfg.db_path)
    try:
        return catalog.snapshot(conn, station)
    finally:
        conn.close()


def referenced_names(cfg):
    return {b["parquet_file"] for b in snap(cfg)["batches"]}


def new_cfg(tmp):
    cfg = Config(tmp)
    service.init_storage(cfg)
    return cfg


# ---------- 子进程：慢发布者（在某一步暂停，等待父进程放行） ----------

def child_slow_publish(data_dir, station, csv_path, col_types, ready, go, errq,
                       pause_at):
    """在子进程中发布；pause_at='after_write' 或 'during_parse'。"""
    from app import csvparse as _csvparse
    from app import store as _store

    cfg = Config(data_dir)
    orig_write = _store.write_parquet_atomic
    orig_parse = _csvparse.parse_rows

    try:
        if pause_at == "after_write":
            def write_hook(table, path):
                orig_write(table, path)
                ready.set()
                if not go.wait(120):
                    raise RuntimeError("父进程未放行：cleanup 等待超时")
            _store.write_parquet_atomic = write_hook
        elif pause_at == "during_parse":
            def parse_hook(path, types):
                ready.set()
                if not go.wait(120):
                    raise RuntimeError("父进程未放行：cleanup 等待超时")
                return orig_parse(path, types)
            _csvparse.parse_rows = parse_hook
        service.publish(cfg, station, os.path.basename(csv_path), csv_path, col_types)
    except BaseException:
        errq.put(traceback.format_exc())
    finally:
        _store.write_parquet_atomic = orig_write
        _csvparse.parse_rows = orig_parse


def child_crash_with_lease(data_dir, batch_id, ready):
    """发布者在写文件途中崩溃：持有租约、留下临时与正式 Parquet、不登记。"""
    cfg = Config(data_dir)
    final_name = batch_id + ".parquet"
    final_path = os.path.join(cfg.parquet_dir, final_name)
    tmp_path = os.path.join(cfg.parquet_dir, "." + final_name + ".tmp")
    handle = lease.acquire(cfg, batch_id, [final_path])
    # 正式文件（已 rename 但未登记）+ 写了一半的临时文件
    write(final_path, "not-a-complete-parquet")
    write(tmp_path, "partial")
    age(final_path, 7200)
    age(tmp_path, 7200)
    ready.set()
    os._exit(9)  # 不等 finally：租约文件留在 inflight/，锁由内核自动释放


def run_child(target, args):
    ctx = mp.get_context("fork")
    errq = ctx.Queue()
    ready = ctx.Event()
    go = ctx.Event()
    p = ctx.Process(target=target, args=args + (ready, go, errq, "")
                    if target is child_slow_publish else args)
    p.start()
    return p, ready, go, errq


# ---------- 场景 1：慢发布期间启动清理 ----------

def test_cleanup_during_slow_publish(tmp, pause_at):
    print(f"  · 交错点：{pause_at}")
    cfg = new_cfg(tmp)
    f0 = os.path.join(tmp, "existing.csv")
    write(f0, "日期,麻雀\n2023-01-01,4\n")
    service.publish(cfg, "老站点", "existing.csv", f0, ["uint64"])
    old_snapshot = snap(cfg)

    f1 = os.path.join(tmp, "slow.csv")
    write(f1, "日期,麻雀,白鹭\n2024-01-01,5,6\n2024-01-02,7,8\n")
    ctx = mp.get_context("fork")
    errq, ready, go = ctx.Queue(), ctx.Event(), ctx.Event()
    p = ctx.Process(target=child_slow_publish,
                    args=(cfg.data_dir, "慢站点", f1, ["uint64", "uint64"],
                          ready, go, errq, pause_at))
    p.start()
    check("子进程到达暂停点", ready.wait(60))

    inflight = os.listdir(cfg.parquet_dir)
    before = referenced_names(cfg)
    check("没有未登记残件被提前登记", before == {b["parquet_file"] for b in old_snapshot["batches"]})

    # 发布者仍存活时：目录库写事务没被占用（解析/写文件期间不持有）
    t0 = time.time()
    conn = catalog.connect(cfg.db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("ROLLBACK")
    finally:
        conn.close()
    check("慢发布期间目录库写事务可用（未被占用）", time.time() - t0 < 5,
          f"{time.time() - t0:.2f}s")

    # 其他批次仍可发布（与慢发布者并发）
    f2 = os.path.join(tmp, "other.csv")
    write(f2, "日期,苍鹭\n2024-02-01,9\n")
    r = service.publish(cfg, "另一站点", "other.csv", f2, ["uint64"])
    check("慢发布期间其他批次仍可发布",
          r["batch_id"] + ".parquet" in referenced_names(cfg))

    # 既有数据仍可取数
    s = snap(cfg, "老站点")
    _, table = store.load_unified(cfg.parquet_dir, s["columns"], s["batches"])
    check("慢发布期间既有数据仍可取数", table.num_rows == 1 and table.to_pylist()[0]["麻雀"] == 4)

    # 把在途文件做旧到远超宽限期，模拟慢发布超过 600s（暂存为 3600s）
    if pause_at == "after_write":
        inflight_files = [n for n in os.listdir(cfg.parquet_dir)
                          if n.endswith(".parquet") and n not in referenced_names(cfg)]
        check("找到在途 Parquet", len(inflight_files) == 1, str(os.listdir(cfg.parquet_dir)))
        age(os.path.join(cfg.parquet_dir, inflight_files[0]), 7200)
    else:
        age(f1, 7200)

    # 另一个进程启动 -> 清理
    other = Config(cfg.data_dir)
    service.init_storage(other)

    after = os.listdir(cfg.parquet_dir)
    if pause_at == "after_write":
        check("在途 Parquet 未被清理误删", inflight_files[0] in after, str(after))
    check("既有文件仍在", old_snapshot["batches"][0]["parquet_file"] in after)
    # 活跃发布者的暂存 CSV 同样不得被清理（场景 1 里发布结束后会被移除）
    if pause_at == "during_parse":
        check("在途暂存 CSV 未被清理误删", os.path.isfile(f1))

    go.set()
    p.join(120)
    check("慢发布者正常完成", p.exitcode == 0 and errq.empty(), str(errq.get() if not errq.empty() else ""))

    final = snap(cfg)
    slow_batches = [b for b in final["batches"] if b["station"] == "慢站点"]
    check("慢发布批次已登记", len(slow_batches) == 1)
    _, slow_table = store.load_unified(cfg.parquet_dir, final["columns"], slow_batches)
    rows = slow_table.to_pylist()
    check("发布文件完整可读", len(rows) == 2 and [r["白鹭"] for r in rows] == [6, 8], str(rows))
    check("全部登记文件存在", all(os.path.isfile(os.path.join(cfg.parquet_dir, b["parquet_file"]))
                                  for b in final["batches"]))
    check("租约目录已清空", os.listdir(lease.lease_dir(cfg)) == [], str(os.listdir(lease.lease_dir(cfg))))


def test_slow_publish_after_write(tmp):
    test_cleanup_during_slow_publish(tmp, "after_write")


def test_slow_publish_during_parse(tmp):
    test_cleanup_during_slow_publish(tmp, "during_parse")


# ---------- 场景 2：清理读引用清单后才完成登记 ----------

def test_register_after_reference_read(tmp):
    cfg = new_cfg(tmp)
    batch_id = "0" * 16 + "beef" * 4
    final_name = batch_id + ".parquet"
    final_path = os.path.join(cfg.parquet_dir, final_name)
    store.write_parquet_atomic(
        store.build_batch_table([datetime.date(2024, 3, 1)],
                                [("麻雀", "uint64", [3])]), final_path)
    age(final_path, 7200)

    state = {"reads": 0}
    orig = service._referenced_files

    def racing_references(_cfg):
        state["reads"] += 1
        names = orig(_cfg)
        if state["reads"] == 1:
            # 第一次（清理开始时的引用清单）故意返回旧状态；
            # 随后另一个发布者完成登记——模拟「清理读过清单后才登记」。
            conn = catalog.connect(cfg.db_path)
            try:
                with catalog.write_txn(conn):
                    catalog.merge_schema(conn, [("麻雀", "uint64")])
                    catalog.insert_batch(conn, {
                        "id": batch_id, "station": "竞态站", "source_filename": "race.csv",
                        "parquet_file": final_name, "n_rows": 1,
                        "created_at": "2024-03-01T00:00:00+00:00"})
            finally:
                conn.close()
            names = set()
        return names

    service._referenced_files = racing_references
    try:
        service.cleanup_orphan_files(cfg)
    finally:
        service._referenced_files = orig

    check("清理确实先读到过时清单", state["reads"] >= 2, str(state["reads"]))
    check("登记在清理扫描之后完成的文件未被删除", os.path.isfile(final_path))
    s = snap(cfg, "竞态站")
    _, table = store.load_unified(cfg.parquet_dir, s["columns"], s["batches"])
    check("该批次仍可完整取数", table.num_rows == 1 and table.to_pylist()[0]["麻雀"] == 3)


# ---------- 场景 3：发布者退出 / 孤儿回收 ----------

def test_abandoned_publisher_reclaimed(tmp):
    cfg = new_cfg(tmp)
    ctx = mp.get_context("fork")
    ready = ctx.Event()
    batch_id = "dead" * 8
    p = ctx.Process(target=child_crash_with_lease,
                    args=(cfg.data_dir, batch_id, ready))
    p.start()
    check("发布者已就位", ready.wait(60))
    p.join(60)
    check("发布者异常退出", p.exitcode == 9)

    left = os.listdir(cfg.parquet_dir)
    check("崩溃留下未发布文件", batch_id + ".parquet" in left
          and "." + batch_id + ".parquet.tmp" in left, str(left))
    check("崩溃留下租约文件", any(n == batch_id + ".lock"
                                  for n in os.listdir(lease.lease_dir(cfg))))

    service.init_storage(Config(cfg.data_dir))
    left = os.listdir(cfg.parquet_dir)
    check("过期未发布 Parquet 被回收", batch_id + ".parquet" not in left, str(left))
    check("过期临时 Parquet 被回收", "." + batch_id + ".parquet.tmp" not in left, str(left))
    check("过期租约被回收", os.listdir(lease.lease_dir(cfg)) == [])
    check("目录未引用残件", all(os.path.isfile(os.path.join(cfg.parquet_dir, b["parquet_file"]))
                                for b in snap(cfg)["batches"]))


def test_active_publisher_does_not_block_other_gc(tmp):
    cfg = new_cfg(tmp)
    stale_id = "dead" * 8
    stale = os.path.join(cfg.parquet_dir, stale_id + ".parquet")
    store.write_parquet_atomic(store.build_batch_table(
        [datetime.date(2024, 1, 1)], [("麻雀", "uint64", [1])]), stale)
    age(stale, 7200)

    f = os.path.join(tmp, "slow2.csv")
    write(f, "日期,麻雀\n2024-04-01,2\n")
    ctx = mp.get_context("fork")
    errq, ready, go = ctx.Queue(), ctx.Event(), ctx.Event()
    p = ctx.Process(target=child_slow_publish,
                    args=(cfg.data_dir, "活跃站", f, ["uint64"],
                          ready, go, errq, "after_write"))
    p.start()
    check("活跃发布者到达暂停点", ready.wait(60))

    live = [n for n in os.listdir(cfg.parquet_dir)
            if n.endswith(".parquet") and n not in referenced_names(cfg)
            and n != stale_id + ".parquet"]
    check("找到活跃发布者的在途文件", len(live) == 1, str(os.listdir(cfg.parquet_dir)))

    service.init_storage(Config(cfg.data_dir))
    names = os.listdir(cfg.parquet_dir)
    check("有活跃发布者时仍回收过期孤儿", stale_id + ".parquet" not in names, str(names))
    check("活跃发布者的文件被保留", live[0] in names, str(names))

    go.set()
    p.join(120)
    check("活跃发布者完成", p.exitcode == 0 and errq.empty())
    check("活跃发布者批次已登记并可读",
          any(b["station"] == "活跃站" for b in snap(cfg)["batches"]))


def test_abandoned_temp_lease_reclaimed(tmp):
    cfg = new_cfg(tmp)
    batch_id = "cafe" * 8
    d = lease.ensure_dir(cfg)
    tmp_lease = os.path.join(d, "." + batch_id + ".lease.tmp")
    import json
    write(tmp_lease, json.dumps({
        "batch_id": batch_id, "pid": 999999, "created_at": 1,
        "paths": {"p0": os.path.join(cfg.parquet_dir, batch_id + ".parquet")},
    }))
    final = os.path.join(cfg.parquet_dir, batch_id + ".parquet")
    tmp_parquet = os.path.join(cfg.parquet_dir, "." + batch_id + ".parquet.tmp")
    write(final, "half-published")
    write(tmp_parquet, "partial")
    age(final, 7200)
    age(tmp_parquet, 7200)
    age(tmp_lease, 7200)

    service.init_storage(Config(cfg.data_dir))
    check("崩溃的临时租约被回收", not os.path.exists(tmp_lease))
    check("其宣告的正式文件被回收", not os.path.exists(final))
    check("其临时文件被回收", not os.path.exists(tmp_parquet))


def test_abandoned_lease_keeps_registered(tmp):
    """登记成功、退租前崩溃：租约过期，但目录已引用，文件必须保留。"""
    cfg = new_cfg(tmp)
    f = os.path.join(tmp, "reg.csv")
    write(f, "日期,麻雀\n2024-05-01,5\n")
    r = service.publish(cfg, "登记站", "reg.csv", f, ["uint64"])
    batch_id = r["batch_id"]
    d = lease.ensure_dir(cfg)
    write(os.path.join(d, batch_id + ".lock"), "{}")  # 未加锁的过期租约
    final_name = batch_id + ".parquet"
    age(os.path.join(cfg.parquet_dir, final_name), 7200)

    service.init_storage(Config(cfg.data_dir))
    check("已登记文件不被过期租约误删",
          os.path.isfile(os.path.join(cfg.parquet_dir, final_name)))
    s = snap(cfg, "登记站")
    _, table = store.load_unified(cfg.parquet_dir, s["columns"], s["batches"])
    check("登记数据仍完整可读", table.to_pylist()[0]["麻雀"] == 5)


# ---------- 场景 4：残件不得登记 ----------

def test_truncated_write_not_published(tmp):
    cfg = new_cfg(tmp)
    f = os.path.join(tmp, "ok.csv")
    write(f, "日期,麻雀\n2024-06-01,6\n")
    r = service.publish(cfg, "站A", "ok.csv", f, ["uint64"])
    check("正常发布成功", r["batch_id"] + ".parquet" in referenced_names(cfg))
    s = snap(cfg, "站A")
    _, t = store.load_unified(cfg.parquet_dir, s["columns"], s["batches"])
    check("正常发布文件完整可读", t.to_pylist()[0]["麻雀"] == 6)

    orig = store.pq.write_table

    def truncated(table, sink, **kw):
        sink.write(b"PAR1\x00\x00")  # 只有文件头，footer 缺失
        return None

    store.pq.write_table = truncated
    try:
        try:
            service.publish(cfg, "站B", "ok2.csv", f, ["uint64"])
            raise AssertionError("截断写入应失败")
        except Exception:
            print("  ✓ 截断写入被拒绝")
    finally:
        store.pq.write_table = orig

    names = os.listdir(cfg.parquet_dir)
    check("失败批次未登记", len(referenced_names(cfg)) == 1)
    check("失败批次无正式残件", all(n.endswith(".parquet") for n in names), str(names))
    check("失败批次无临时候选文件", not any(n.endswith(".tmp") for n in names), str(names))
    check("目录引用的文件全部存在", all(
        os.path.isfile(os.path.join(cfg.parquet_dir, b["parquet_file"]))
        for b in snap(cfg)["batches"]))


def main():
    with tempfile.TemporaryDirectory() as tmp:
        steps = [
            ("慢发布者已落盘未登记时启动清理（写完之后）", test_slow_publish_after_write),
            ("慢发布者在解析期间启动清理", test_slow_publish_during_parse),
            ("清理读过引用清单后才完成登记", test_register_after_reference_read),
            ("发布者崩溃遗留的过期文件被后续启动回收", test_abandoned_publisher_reclaimed),
            ("有活跃发布者时仍回收其他过期孤儿", test_active_publisher_does_not_block_other_gc),
            ("崩溃的临时租约及其文件被回收", test_abandoned_temp_lease_reclaimed),
            ("过期租约不误删已登记文件", test_abandoned_lease_keeps_registered),
            ("截断写入不登记残件，成功文件完整可读", test_truncated_write_not_published),
        ]
        for i, (title, fn) in enumerate(steps, 1):
            print(f"[{i}/{len(steps)}] {title}")
            fn(os.path.join(tmp, f"race{i}"))
    print("\n竞态回归测试全部通过 ✅")


if __name__ == "__main__":
    main()
