"""跨进程发布租约：把「正在发布」的批次及其文件宣告给启动清理。

背景：多个进程共用同一个 DATA_DIR。一个慢发布者在「Parquet 已落盘、目录尚未
登记」的窗口里，若另一个进程启动清理，仅凭文件年龄（600 秒）和一份过期的引用
清单会误删在途文件，留下无法取数的目录记录。

方案：发布前在 ``DATA_DIR/inflight/<批次UUID>.lock`` 上持有排他 ``flock``，
租约内容是本次发布将要创建/读取的文件路径。``flock`` 由内核在进程退出
（包括崩溃、SIGKILL）时自动释放，因此：

- 清理进程对租约做非阻塞加锁：加不上 => 发布者仍活着，其文件一律保留；
- 加得上 => 发布者已退出，租约过期，可以回收其未发布的临时文件与 Parquet；
- 租约文件只在数据文件创建之前出现（先锁临时名，再原子 rename 为正式名），
  所以正式租约一旦可见就已被发布者持有。
"""
import fcntl
import json
import os
import time
import uuid

LEASE_DIR_NAME = "inflight"
_LOCK_SUFFIX = ".lock"
_TMP_SUFFIX = ".lease.tmp"
# 临时租约在 rename 之前可能处于「已 open、尚未 flock」的极短窗口；
# 只有足够老的临时租约才可能是发布者崩溃的遗留，可以回收。
TEMP_LEASE_GRACE_SEC = 600


def lease_dir(cfg):
    return os.path.join(cfg.data_dir, LEASE_DIR_NAME)


def ensure_dir(cfg):
    os.makedirs(lease_dir(cfg), exist_ok=True)
    return lease_dir(cfg)


def _fsync_dir(dirpath):
    fd = os.open(dirpath, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _batch_id_from_name(name):
    if name.endswith(_LOCK_SUFFIX):
        return name[: -len(_LOCK_SUFFIX)]
    if name.startswith(".") and name.endswith(_TMP_SUFFIX):
        return name[1 : -len(_TMP_SUFFIX)]
    return None


def _derived_paths(cfg, batch_id):
    """即使租约内容不可读，也能由批次 UUID 推出它可能创建的文件。"""
    if not batch_id:
        return set()
    final_name = batch_id + ".parquet"
    return {
        os.path.abspath(os.path.join(cfg.parquet_dir, final_name)),
        os.path.abspath(os.path.join(cfg.parquet_dir, "." + final_name + ".tmp")),
    }


def _payload_paths(cfg, payload, batch_id):
    paths = _derived_paths(cfg, batch_id)
    if not isinstance(payload, dict):
        return paths
    raw = payload.get("paths")
    if isinstance(raw, dict):
        raw = list(raw.values())
    if not isinstance(raw, list):
        return paths
    data_dir = os.path.abspath(cfg.data_dir)
    for p in raw:
        if not isinstance(p, str):
            continue
        ap = os.path.abspath(p)
        # 仅保护 DATA_DIR 内的文件；清理只会动这些目录
        if ap == data_dir or ap.startswith(data_dir + os.sep):
            paths.add(ap)
    return paths


def _read_payload(fd):
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        data = os.read(fd, 256 * 1024)
        return json.loads(data.decode("utf-8")) if data else None
    except (OSError, ValueError, UnicodeDecodeError):
        return None


def _inspect_temp_lease(cfg, path, batch_id):
    """检查发布者崩溃后残留的、尚未 rename 成正式租约的临时租约文件。

    临时租约一旦可见，发布者可能正持锁（刚创建、还没 rename），此时不可删除；
    加锁成功说明没有活着的持有者，可回收并把它宣告过的文件视为已遗弃。
    返回 ``(active_paths, abandoned_paths)``。
    """
    try:
        age_sec = time.time() - os.path.getmtime(path)
    except OSError:
        return set(), set()
    if age_sec < TEMP_LEASE_GRACE_SEC:
        # 发布者可能刚 open 尚未 flock（极短窗口）：保守保护其可能创建的文件
        paths = _derived_paths(cfg, batch_id)
        payload = None
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return paths, set()
        try:
            payload = _read_payload(fd)
        finally:
            os.close(fd)
        return _payload_paths(cfg, payload, batch_id) | paths, set()

    paths = _derived_paths(cfg, batch_id)
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return set(), set()
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return _payload_paths(cfg, _read_payload(fd), batch_id), set()
        abandoned = _payload_paths(cfg, _read_payload(fd), batch_id)
        try:
            os.unlink(path)
        except OSError:
            pass
        return set(), abandoned | paths
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def scan_registry(cfg):
    """扫描租约目录。

    返回 ``(active_paths, abandoned_paths)``：

    - ``active_paths``：仍被存活发布者持有的租约所保护的文件（绝对路径）；
    - ``abandoned_paths``：过期租约（发布者已退出）宣告过的文件；调用方可立即回收，
      但仍须以目录引用清单为准（发布者可能在登记后、退租前退出）。

    扫描的同时会删除已过期的租约文件本身；无法加锁的临时租约一律不动。
    """
    active = set()
    abandoned = set()
    d = lease_dir(cfg)
    try:
        names = os.listdir(d)
    except OSError:
        return active, abandoned

    for name in names:
        path = os.path.join(d, name)
        batch_id = _batch_id_from_name(name)
        if batch_id is None:
            continue
        if name.startswith(".") and name.endswith(_TMP_SUFFIX):
            t_active, t_abandoned = _inspect_temp_lease(cfg, path, batch_id)
            active |= t_active
            abandoned |= t_abandoned
            continue
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            # 文件存在却打不开（如发布者正在 rename 前的极短窗口）：无法判断，
            # 保守保留其可能创建的文件，下次清理再回收。
            active |= _derived_paths(cfg, batch_id)
            continue
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            # 发布者仍存活：它宣告的文件（含尚未 rename 的临时文件）都保留
            active |= _payload_paths(cfg, _read_payload(fd), batch_id)
            os.close(fd)
            continue
        # 拿到锁 => 发布者已退出 => 租约过期，其文件可回收
        abandoned |= _payload_paths(cfg, _read_payload(fd), batch_id)
        try:
            os.unlink(path)
        except OSError:
            pass
        os.close(fd)
    return active, abandoned


class _Lease:
    def __init__(self, fd, path):
        self.fd = fd
        self.path = path
        self._closed = False

    def release(self):
        if self._closed:
            return
        self._closed = True
        # 先删路径再释放锁：其他进程要么看到完整租约（加锁失败 => 保留），
        # 要么看到路径不存在（视为已结束，不再保护）。
        try:
            os.unlink(self.path)
        except OSError:
            pass
        try:
            os.close(self.fd)
        except OSError:
            pass


def acquire(cfg, batch_id, protected_paths):
    """创建并持有本批次的发布租约。调用方必须 ``release()``（用 try/finally）。

    先写临时文件名并加锁、写内容、fsync，最后原子 rename 为
    ``inflight/<batch_id>.lock``；正式租约一旦可见就已经处于锁定状态。
    """
    d = ensure_dir(cfg)
    batch_id = batch_id or uuid.uuid4().hex
    final_name = batch_id + _LOCK_SUFFIX
    final_path = os.path.join(d, final_name)
    tmp_path = os.path.join(d, "." + batch_id + _TMP_SUFFIX)
    fd = os.open(tmp_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        payload = {
            "batch_id": batch_id,
            "pid": os.getpid(),
            "host": os.uname().nodename,
            "created_at": time.time(),
            "paths": {"p%d" % i: os.path.abspath(p) for i, p in enumerate(protected_paths)},
        }
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        os.write(fd, data)
        os.fsync(fd)
        os.replace(tmp_path, final_path)
        _fsync_dir(d)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass
        raise
    return _Lease(fd, final_path)
