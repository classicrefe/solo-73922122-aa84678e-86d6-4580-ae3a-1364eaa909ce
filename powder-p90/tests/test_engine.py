"""外存引擎测试：与旧核心随机交叉核对、外存落盘、内存线性增长、失败路径、快照。

运行：python -m unittest tests.test_engine -v
"""
import gc
import os
import random
import sys
import tempfile
import time
import tracemalloc
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import engine  # noqa: E402
from app.core import merge_p90, parse_table  # noqa: E402
from app.db import (  # noqa: E402
    connect,
    create_batch,
    init_db,
    insert_table,
    load_batch_tables,
)


def _make_table_text(rng, n_bins, total, width=9):
    rows = []
    lo = 0
    remaining = total
    for i in range(n_bins):
        hi = lo + width
        cnt = remaining if i == n_bins - 1 else rng.randint(0, min(remaining, max(1, total // n_bins * 3)))
        rows.append(f"{lo} {hi} {cnt}")
        remaining -= cnt
        lo = hi + 1
    return "\n".join(rows)


class EngineCrossCheckTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="engine-test-")
        self.db = os.path.join(self.tmp, "powder.db")
        init_db(self.db)
        self.jobs = os.path.join(self.tmp, "jobs")
        os.makedirs(self.jobs)

    def _run(self, ids, names):
        job_id = engine.new_job_id()
        work = os.path.join(self.jobs, job_id)
        os.makedirs(work)
        meta = engine.execute_job(
            job_id=job_id, work_dir=work, db_path=self.db, batch_ids=ids,
            batch_names=names, status_writer=lambda s, r="": None,
        )
        return meta, work

    def test_random_cross_check_with_core(self):
        rng = random.Random(20261124)
        for trial in range(40):
            # 每轮换一个独立库，避免批号冲突
            db = os.path.join(self.tmp, f"t{trial}.db")
            init_db(db)
            n_batches = rng.randint(1, 4)
            ids, names, bt = [], [], []
            with connect(db) as conn:
                for b in range(n_batches):
                    bid = create_batch(conn, f"B{trial}-{b}")
                    n_tables = rng.randint(1, 3)
                    n_bins = rng.randint(2, 12)
                    total = rng.randint(10, 400)
                    base = parse_table(_make_table_text(rng, n_bins, total))
                    insert_table(conn, bid, base, "base")
                    for _ in range(n_tables - 1):
                        # 用基础档表的重新切分（保证相容）：随机合并相邻档
                        bins = list(base.bins)
                        cut = rng.randint(1, len(bins) - 1)
                        merged_text = " ".join(map(str, (
                            bins[0].lo, bins[cut - 1].hi,
                            sum(x.count for x in bins[:cut])
                        ))) + "\n" + "\n".join(
                            f"{x.lo} {x.hi} {x.count}" for x in bins[cut:]
                        )
                        insert_table(conn, bid, parse_table(merged_text), "merge")
                    ids.append(bid)
                    names.append(f"B{trial}-{b}")
                    bt.append((bid, load_batch_tables(conn, bid)))
            name_map = dict(zip(ids, names))
            meta, work = self._run_with(db, ids, name_map)
            self.assertEqual(meta["status"], "ready", meta.get("reason"))
            ref = merge_p90(bt)
            self.assertEqual(meta["p90_low"], ref.p90_low, f"trial {trial} low")
            self.assertEqual(meta["p90_high"], ref.p90_high, f"trial {trial} high")
            self.assertEqual(meta["k"], ref.k)
            self.assertEqual(meta["total"], ref.total)
            # 两端完整 CSV 都存在且非空
            for f in ("witness_low_full.csv", "witness_high_full.csv"):
                p = os.path.join(work, f)
                self.assertTrue(os.path.exists(p))
                with open(p) as fh:
                    lines = [ln for ln in fh.read().splitlines() if ln]
                self.assertGreater(len(lines), 1)  # 表头 + 数据

    def _run_with(self, db, ids, names):
        job_id = engine.new_job_id()
        work = os.path.join(self.jobs, job_id)
        os.makedirs(work)
        meta = engine.execute_job(
            job_id=job_id, work_dir=work, db_path=db, batch_ids=ids,
            batch_names=names, status_writer=lambda s, r="": None,
        )
        return meta, work

    def test_spool_files_created_on_disk(self):
        with connect(self.db) as conn:
            bid = create_batch(conn, "BIG")
            insert_table(conn, bid, parse_table(_make_table_text(random.Random(1), 200, 10_000)), "")
        meta, work = self._run([bid], {bid: "BIG"})
        self.assertEqual(meta["status"], "ready")
        spools = [f for f in os.listdir(work) if f.endswith(".bin")]
        self.assertTrue(spools, "应当产生外存暂存文件")
        # 全局切割点文件确实承载了约 201 个点
        gc_path = os.path.join(work, "global_cuts.bin")
        self.assertTrue(os.path.exists(gc_path))
        self.assertGreaterEqual(os.path.getsize(gc_path), 201 * 8)

    def test_incompatible_stored_tables_fail_with_reason(self):
        # 绕过上传相容性校验，直接写入两张冲突档表，作业必须 failed 且带原因
        with connect(self.db) as conn:
            bid = create_batch(conn, "BAD")
            insert_table(conn, bid, parse_table("0 9 8\n10 19 2\n"), "")
            insert_table(conn, bid, parse_table("0 4 10\n5 19 0\n"), "")
        meta, _ = self._run([bid], {bid: "BAD"})
        self.assertEqual(meta["status"], "failed")
        self.assertIn("不相容", meta["reason"])

    def test_snapshot_independent_of_later_writes(self):
        with connect(self.db) as conn:
            bid = create_batch(conn, "S")
            insert_table(conn, bid, parse_table("0 99 400\n100 199 500\n"), "")
        # 先做一份快照
        work = os.path.join(self.jobs, engine.new_job_id())
        os.makedirs(work)
        snap = os.path.join(work, "snapshot.db")
        engine.snapshot_database(self.db, snap)
        # 主库再补报
        with connect(self.db) as conn:
            insert_table(conn, bid, parse_table(
                "0 49 100\n50 99 300\n100 149 400\n150 199 100\n"), "")
        # 快照里仍只有 1 张表
        with connect(snap) as sconn:
            n = sconn.execute("SELECT COUNT(*) FROM tables WHERE batch_id=?", (bid,)).fetchone()[0]
        self.assertEqual(n, 1)


class MemoryLinearTest(unittest.TestCase):
    """工作内存应随 B（批次数）和 M（最大单批档数）线性，而非随总档数暴涨。

    用 tracemalloc 测峰值；外存模式下增大 B 时，常驻增量主要是 B 路归并的 O(B)
    游标/堆，单批大档数 M 通过分块落盘不进常驻内存。
    """

    def _build_db(self, path, n_batches, n_bins, total):
        init_db(path)
        rng = random.Random(7)
        ids = []
        with connect(path) as conn:
            for b in range(n_batches):
                bid = create_batch(conn, f"B-{b}")
                insert_table(conn, bid, parse_table(
                    _make_table_text(rng, n_bins, total)), "")
                ids.append(bid)
        return ids

    def _peak_memory(self, db, ids):
        jobs = os.path.join(os.path.dirname(db), "jobs")
        os.makedirs(jobs, exist_ok=True)
        gc.collect()
        tracemalloc.start()
        job_id = engine.new_job_id()
        work = os.path.join(jobs, job_id)
        os.makedirs(work)
        meta = engine.execute_job(
            job_id=job_id, work_dir=work, db_path=db, batch_ids=ids,
            batch_names={i: f"B-{i}" for i in ids},
            status_writer=lambda s, r="": None,
        )
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        self.assertEqual(meta["status"], "ready", meta.get("reason"))
        return peak

    def test_memory_does_not_explode_with_total_bins(self):
        base = tempfile.mkdtemp(prefix="mem-")
        # 场景 A：4 批 × 500 档（总 2000 档）
        db_a = os.path.join(base, "a.db")
        ids_a = self._build_db(db_a, 4, 500, 100_000)
        peak_a = self._peak_memory(db_a, ids_a)
        # 场景 B：40 批 × 500 档（总 20000 档，10 倍总档数）
        db_b = os.path.join(base, "b.db")
        ids_b = self._build_db(db_b, 40, 500, 100_000)
        peak_b = self._peak_memory(db_b, ids_b)
        # 总档数扩大 10 倍，常驻峰值增长应远小于 10 倍（外存兜底，留宽松阈值）
        self.assertLess(peak_b, peak_a * 4,
                        f"peak_a={peak_a} peak_b={peak_b} 内存随总档数超线性增长")

    def test_large_single_batch_runs_external(self):
        base = tempfile.mkdtemp(prefix="mem2-")
        db = os.path.join(base, "big.db")
        ids = self._build_db(db, 2, 20_000, 500_000_000)  # 与旧性能测试同量级
        t0 = time.perf_counter()
        peak = self._peak_memory(db, ids)
        elapsed = time.perf_counter() - t0
        # 20k 档 × 2 批，外存暂存文件应存在且峰值内存远小于“全量 int 列表”规模
        self.assertLess(elapsed, 60)
        self.assertLess(peak, 200 * 1024 * 1024)  # 峰值 < 200MiB


if __name__ == "__main__":
    unittest.main(verbosity=2)
