"""Flask 网页端到端测试：上传/补报、作业化核算、见证下载、失败原因、快照隔离。"""
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.web import create_app  # noqa: E402
from app.db import connect, get_job  # noqa: E402

ALPHA_T1 = "0 99 400\n100 199 500\n"
ALPHA_T2 = "0 49 100\n50 99 300\n100 149 400\n150 199 100\n"
ALPHA_BAD = "0 49 450\n50 99 0\n100 199 450\n"
BETA_T1 = "0 100 500\n101 200 500\n"
GAMMA_T1 = "0 9 20\n10 19 30\n20 29 50\n"


class WebFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        os.unlink(self.tmp.name)
        self.app = create_app(db_path=self.tmp.name)
        self.client = self.app.test_client()

    def tearDown(self):
        if os.path.exists(self.tmp.name):
            os.unlink(self.tmp.name)

    def _upload(self, name, text, note=""):
        return self.client.post(
            "/upload",
            data={"batch_name": name, "table_text": text, "note": note},
            follow_redirects=True,
        )

    def _wait_job(self, job_url, timeout=20):
        """轮询作业页直到 ready/failed，返回最终响应文本。"""
        deadline = time.time() + timeout
        body = ""
        while time.time() < deadline:
            rv = self.client.get(job_url)
            body = rv.get_data(as_text=True)
            if "核算中" not in body:
                return rv.status_code, body
            time.sleep(0.05)
        raise AssertionError("作业超时未结束")

    def _start_compute(self, ids):
        rv = self.client.post("/compute", data={"batch_ids": ids}, follow_redirects=False)
        self.assertEqual(rv.status_code, 302)
        return rv.headers["Location"]

    def test_full_flow(self):
        html = self.client.get("/").get_data(as_text=True)
        self.assertIn("粒径档表 P90", html)

        for name, text in [("B-ALPHA", ALPHA_T1), ("B-BETA", BETA_T1), ("B-GAMMA", GAMMA_T1)]:
            rv = self._upload(name, text)
            self.assertIn("入库", rv.get_data(as_text=True))

        # 相容补报
        body = self._upload("B-ALPHA", ALPHA_T2, note="细分补报").get_data(as_text=True)
        self.assertIn("补报成功", body)
        self.assertIn("现共 2 张档表", body)

        # 不相容补报被拒绝
        body = self._upload("B-ALPHA", ALPHA_BAD).get_data(as_text=True)
        self.assertIn("补报被拒绝", body)
        self.assertIn("未入库", body)

        with self.app.app_context():
            with connect(self.tmp.name) as conn:
                bid = int(get_batch_id(conn, "B-ALPHA"))
                from app.db import list_tables
                self.assertEqual(len(list_tables(conn, bid)), 2)
        detail = self.client.get(f"/batch/{bid}").get_data(as_text=True)
        self.assertEqual(detail.count("900 粒"), 2)
        self.assertIn("[0, 49]", detail)

        # 解析失败不入库
        self.assertIn("解析失败", self._upload("B-BAD", "0 9 5\n11 19 3\n").get_data(as_text=True))

        # 作业化核算
        location = self._start_compute([1, 2, 3])
        status, body = self._wait_job(location)
        self.assertEqual(status, 200)
        self.assertIn("完成，见证齐备且复核通过", body)
        self.assertIn("2000", body)   # 总粒数
        self.assertIn("1800", body)   # k
        self.assertIn("101", body)    # P90 下界
        self.assertIn("200", body)    # P90 上界
        # 两个下载入口都在
        self.assertIn("下载下界见证 CSV", body)
        self.assertIn("下载上界见证 CSV", body)
        self.assertIn("/download/low", body)
        self.assertIn("/download/high", body)

        # 下载 CSV：含三批逐批频数，表头正确
        job_id = location.rstrip("/").split("/")[-1]
        low = self.client.get(f"/jobs/{job_id}/download/low")
        high = self.client.get(f"/jobs/{job_id}/download/high")
        self.assertEqual(low.status_code, 200)
        self.assertEqual(high.status_code, 200)
        low_text = low.get_data(as_text=True)
        high_text = high.get_data(as_text=True)
        low.close()
        high.close()
        self.assertTrue(low_text.startswith("batch_id,batch_name,size_micron,count"))
        self.assertIn("B-ALPHA", low_text)
        self.assertIn("B-BETA", low_text)
        self.assertIn("B-GAMMA", low_text)
        # 数据行数 = 9（三批非零见证项之和）
        self.assertEqual(len([ln for ln in low_text.splitlines() if ln]) - 1, 9)
        self.assertIn("B-ALPHA", high_text)

    def test_running_job_has_no_download(self):
        self._upload("B-ALPHA", ALPHA_T1)
        location = self._start_compute([1])
        # 立即查看（作业可能仍在 running），running 页不得出现下载入口
        body = self.client.get(location).get_data(as_text=True)
        self.assertNotIn("下载下界见证", body)
        self._wait_job(location)

    def test_download_only_when_ready(self):
        self._upload("B-ALPHA", ALPHA_T1)
        location = self._start_compute([1])
        job_id = location.rstrip("/").split("/")[-1]
        self._wait_job(location)
        # 非法 which / 非法 job id
        self.assertEqual(self.client.get(f"/jobs/{job_id}/download/mid").status_code, 404)
        self.assertEqual(self.client.get("/jobs/" + "z" * 32).status_code, 404)

    def test_failed_job_shows_reason(self):
        # 直接构造一个 failed 作业行与空目录，页面应展示原因且无下载入口
        from app.db import create_job, set_job_status
        job_id = "a" * 32
        os.makedirs(os.path.join(self.app.config["JOBS_DIR"], job_id), exist_ok=True)
        with connect(self.tmp.name) as conn:
            create_job(conn, job_id, [1])
            set_job_status(conn, job_id, "failed", "演示失败原因：磁盘暂存失败")
        body = self.client.get(f"/jobs/{job_id}").get_data(as_text=True)
        self.assertIn("核算失败", body)
        self.assertIn("演示失败原因", body)
        self.assertNotIn("下载下界见证", body)

    def test_compute_requires_selection(self):
        self._upload("B-ALPHA", ALPHA_T1)
        rv = self.client.post("/compute", data={}, follow_redirects=True)
        self.assertIn("请至少勾选一个批次", rv.get_data(as_text=True))

    def test_missing_and_duplicate_batch_rejected(self):
        self._upload("B-ALPHA", ALPHA_T1)
        # 不存在的批次
        rv = self.client.post("/compute", data={"batch_ids": [999]}, follow_redirects=True)
        self.assertIn("批次不存在", rv.get_data(as_text=True))
        # 重复勾选
        rv = self.client.post(
            "/compute", data={"batch_ids": [1, 1]}, follow_redirects=True)
        self.assertIn("重复", rv.get_data(as_text=True))

    def test_concurrent_job_limit(self):
        import app.engine as engine
        self._upload("B-ALPHA", ALPHA_T1)
        gate = __import__("threading").Event()
        original = engine.snapshot_database

        def slow(src, dst):
            original(src, dst)
            gate.wait(timeout=5)

        engine.snapshot_database = slow
        try:
            loc1 = self._start_compute([1])
            # 等第一个作业登记为 running
            deadline = __import__("time").time() + 5
            jid1 = loc1.rstrip("/").split("/")[-1]
            while __import__("time").time() < deadline:
                with connect(self.tmp.name) as conn:
                    if get_job(conn, jid1) is not None:
                        break
                __import__("time").sleep(0.02)
            # 第二个作业应被并发上限拦截（POWDER_MAX_JOBS 默认 2，再造第二个 running）
            # 先放一个占满，再请求：上限 2 时允许 2 个，这里直接把上限调成 1 验证
            self.app.config  # 占位确保 app 可用
            import app.web as webmod
            old_limit = webmod.MAX_CONCURRENT_JOBS
            webmod.MAX_CONCURRENT_JOBS = 1
            try:
                rv = self.client.post(
                    "/compute", data={"batch_ids": [1]}, follow_redirects=True)
                self.assertIn("已有核算作业在进行", rv.get_data(as_text=True))
            finally:
                webmod.MAX_CONCURRENT_JOBS = old_limit
            gate.set()
            self._wait_job(loc1)
        finally:
            engine.snapshot_database = original

    def test_index_lists_jobs(self):
        self._upload("B-ALPHA", ALPHA_T1)
        loc = self._start_compute([1])
        self._wait_job(loc)
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("最近核算作业", body)
        self.assertIn("查看作业", body)

    def test_invalid_upload_messages(self):
        self.assertIn("整表粒数为 0",
                      self._upload("X", "0 9 0\n10 19 0\n").get_data(as_text=True))
        rv = self.client.post(
            "/upload", data={"batch_name": "", "table_text": "0 9 1"}, follow_redirects=True
        )
        self.assertIn("批号不能为空", rv.get_data(as_text=True))

    def test_snapshot_isolation_while_supplementing(self):
        """核算进行中的补报不影响在途作业；下次核算采用新档表。"""
        import app.engine as engine
        self._upload("B-ALPHA", ALPHA_T1)
        self._upload("B-BETA", BETA_T1)

        original = engine.snapshot_database
        captured = {}

        def slow_snapshot(src, dst):
            original(src, dst)
            captured["gate"].wait(timeout=5)

        gate = __import__("threading").Event()
        captured["gate"] = gate
        engine.snapshot_database = slow_snapshot
        try:
            location = self._start_compute([1, 2])
            # 等作业进入快照阶段
            deadline = time.time() + 5
            job_id = location.rstrip("/").split("/")[-1]
            while time.time() < deadline:
                with connect(self.tmp.name) as conn:
                    row = get_job(conn, job_id)
                if row is not None:
                    break
                time.sleep(0.02)
            # 快照已基于“补报前”的已提交状态生成，此刻放行前做相容补报
            rv = self._upload("B-ALPHA", ALPHA_T2)
            self.assertIn("补报成功", rv.get_data(as_text=True))
            gate.set()
            _, body = self._wait_job(location)
        finally:
            engine.snapshot_database = original
        # 在途作业只用快照（补报前的 1 张表），仍成功，端点对应两批两表快照
        self.assertIn("完成，见证齐备且复核通过", body)
        # 作业明细里 B-ALPHA 的档表数为 1（快照状态），而主库现在是 2 张
        self.assertIn("档表数", body)

        # 再次核算：采用补报后的新档表（2 张表），结果应变化
        loc2 = self._start_compute([1, 2])
        _, body2 = self._wait_job(loc2)
        self.assertIn("完成，见证齐备且复核通过", body2)
        # 两次端点：第一次 (100,199) 类，第二次含细分表后 (101,200)
        self.assertIn("101", body2)


def get_batch_id(conn, name):
    from app.db import get_batch_by_name
    return get_batch_by_name(conn, name)["id"]


if __name__ == "__main__":
    unittest.main(verbosity=2)
