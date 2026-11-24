"""Flask 网页：按批号上传/补报档表，勾选批次后以外存作业核算合并 P90 最紧界。

核算语义
--------
* /compute 只负责登记作业并启动后台线程，立即跳转作业状态页；
* 作业开始时对主库做一致性快照，上下界、见证、复核全部只读快照，
  因此核算进行中的同批补报照常入库但不影响在途作业，下次核算才采用新档表；
* 作业状态 running/ready/failed 落 SQLite jobs 表，详情落作业目录 meta.json；
* 只有两份见证齐备且逐表/合并复核全部通过，状态才为 ready，页面才出现下载入口；
  核算或暂存失败时状态为 failed，页面展示原因。
"""
from __future__ import annotations

import json
import os
import re
import threading
from dataclasses import asdict

from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from . import engine
from .core import BinTableError, compatible
from .db import (
    connect,
    count_running_jobs,
    create_batch,
    create_job,
    get_batch_by_name,
    get_job,
    init_db,
    insert_table,
    list_batches,
    list_tables,
    load_batch_tables,
    load_table,
    parse_and_preview,
    set_job_status,
)

JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$")
MAX_CONCURRENT_JOBS = int(os.environ.get("POWDER_MAX_JOBS", "2"))


def create_app(db_path: str | None = None) -> Flask:
    app = Flask(__name__)
    app.secret_key = os.environ.get("SECRET_KEY", "powder-p90-demo-secret")
    app.config["DB_PATH"] = db_path or os.environ.get(
        "POWDER_DB", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "powder.db")
    )
    data_dir = os.path.dirname(app.config["DB_PATH"])
    app.config["JOBS_DIR"] = os.environ.get(
        "POWDER_JOBS", os.path.join(data_dir, "jobs")
    )
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(app.config["JOBS_DIR"], exist_ok=True)
    init_db(app.config["DB_PATH"])

    # 进程重启后，此前 running 的作业不可能再完成，标记失败并说明原因。
    with connect(app.config["DB_PATH"]) as conn:
        conn.execute(
            "UPDATE jobs SET status='failed', reason='服务进程重启，作业中断', "
            "updated_at=datetime('now') WHERE status='running'"
        )

    def _job_dir(job_id: str) -> str:
        return os.path.join(app.config["JOBS_DIR"], job_id)

    def _read_meta(job_id: str) -> dict | None:
        meta_path = os.path.join(_job_dir(job_id), "meta.json")
        if not os.path.exists(meta_path):
            return None
        try:
            with open(meta_path, encoding="utf-8") as fh:
                return json.load(fh)
        except (OSError, json.JSONDecodeError):
            return None

    def _run_job(job_id: str, ids: list[int], names: dict[int, str]) -> None:
        work_dir = _job_dir(job_id)

        def status_writer(status: str, reason: str = "") -> None:
            with connect(app.config["DB_PATH"]) as conn:
                set_job_status(conn, job_id, status, reason)

        try:
            engine.execute_job(
                job_id=job_id,
                work_dir=work_dir,
                db_path=app.config["DB_PATH"],
                batch_ids=ids,
                batch_names=names,
                status_writer=status_writer,
            )
        finally:
            engine.SpoolArray.close_all()

    @app.route("/")
    def index():
        with connect(app.config["DB_PATH"]) as conn:
            batches = [dict(r) for r in list_batches(conn)]
            jobs = [dict(r) for r in conn.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC, id DESC LIMIT 10"
            )]
        return render_template("index.html", batches=batches, jobs=jobs)

    @app.route("/upload", methods=["POST"])
    def upload():
        name = (request.form.get("batch_name") or "").strip()
        text = request.form.get("table_text") or ""
        note = (request.form.get("note") or "").strip()
        if not name:
            flash("批号不能为空", "error")
            return redirect(url_for("index"))
        try:
            table = parse_and_preview(text)
        except BinTableError as exc:
            flash(f"档表解析失败，未入库：{exc}", "error")
            return redirect(url_for("index"))

        with connect(app.config["DB_PATH"]) as conn:
            row = get_batch_by_name(conn, name)
            if row is None:
                batch_id = create_batch(conn, name)
                table_id = insert_table(conn, batch_id, table, note)
                flash(
                    f"新批 {name} 已创建，档表 #{table_id} 入库：{len(table.bins)} 档、"
                    f"{table.total} 粒",
                    "ok",
                )
            else:
                batch_id = int(row["id"])
                existing = load_batch_tables(conn, batch_id)
                ok, reason = compatible(existing + [table])
                if not ok:
                    # 补报失败：不写入，后续核算仍只按此前已入库档表
                    flash(
                        f"补报被拒绝（与批 {name} 已入库的 {len(existing)} 张档表不相容）："
                        f"{reason}。本次上传未入库，核算仍按原档表进行。",
                        "error",
                    )
                    return redirect(url_for("index"))
                table_id = insert_table(conn, batch_id, table, note)
                flash(
                    f"批 {name} 补报成功：档表 #{table_id} 与已有 {len(existing)} 张档表相容，"
                    f"现共 {len(existing) + 1} 张档表、{table.total} 粒。"
                    "进行中的核算不受影响，下一次核算采用新档表。",
                    "ok",
                )
        return redirect(url_for("index"))

    @app.route("/batch/<int:batch_id>")
    def batch_detail(batch_id: int):
        with connect(app.config["DB_PATH"]) as conn:
            trows = list_tables(conn, batch_id)
            tables = [
                {
                    "meta": dict(r),
                    "bins": [asdict(b) for b in load_table(conn, int(r["id"])).bins],
                }
                for r in trows
            ]
            batches = [dict(r) for r in list_batches(conn)]
            name = next((b["name"] for b in batches if b["id"] == batch_id), str(batch_id))
        return render_template("batch.html", name=name, batch_id=batch_id, tables=tables)

    @app.route("/compute", methods=["POST"])
    def compute():
        raw_ids = request.form.getlist("batch_ids")
        try:
            ids = [int(x) for x in raw_ids]
        except ValueError:
            flash("批次参数非法", "error")
            return redirect(url_for("index"))
        if not ids:
            flash("请至少勾选一个批次", "error")
            return redirect(url_for("index"))
        if len(ids) != len(set(ids)):
            flash("勾选的批次有重复，请重新选择", "error")
            return redirect(url_for("index"))

        with connect(app.config["DB_PATH"]) as conn:
            all_batches = {int(r["id"]): dict(r) for r in list_batches(conn)}
            missing = [i for i in ids if i not in all_batches]
            if missing:
                flash(f"批次不存在：{missing}", "error")
                return redirect(url_for("index"))
            empty = [
                all_batches[i]["name"] for i in ids
                if int(all_batches[i].get("table_count") or 0) <= 0
            ]
            if empty:
                flash(f"以下批次没有任何档表，无法核算：{empty}", "error")
                return redirect(url_for("index"))
            if count_running_jobs(conn) >= MAX_CONCURRENT_JOBS:
                flash(
                    "当前已有核算作业在进行，请等待其完成后再发起（作业在独立快照上运行，"
                    "补报不受影响）。",
                    "error",
                )
                return redirect(url_for("index"))
            names = {bid: all_batches[bid]["name"] for bid in ids}

            job_id = engine.new_job_id()
            work_dir = _job_dir(job_id)
            os.makedirs(work_dir, exist_ok=False)
            # 作业行先入库（running），后台线程只更新状态
            create_job(conn, job_id, ids)

        thread = threading.Thread(
            target=_run_job, args=(job_id, ids, names), name=f"job-{job_id}", daemon=True
        )
        thread.start()
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/jobs/<job_id>")
    def job_detail(job_id: str):
        if not JOB_ID_RE.fullmatch(job_id):
            abort(404)
        with connect(app.config["DB_PATH"]) as conn:
            row = get_job(conn, job_id)
            if row is None:
                abort(404)
            job = dict(row)
        meta = _read_meta(job_id) or {}
        # 见证文件齐备且作业 ready 才开放下载
        files_ready = False
        if job["status"] == "ready":
            files_ready = all(
                os.path.exists(os.path.join(_job_dir(job_id), meta.get("files", {}).get(k, "")))
                for k in ("low", "high")
            )
        return render_template(
            "job.html", job=job, meta=meta, files_ready=files_ready
        )

    @app.route("/jobs/<job_id>/download/<which>")
    def job_download(job_id: str, which: str):
        if not JOB_ID_RE.fullmatch(job_id) or which not in ("low", "high"):
            abort(404)
        with connect(app.config["DB_PATH"]) as conn:
            row = get_job(conn, job_id)
            if row is None:
                abort(404)
            if row["status"] != "ready":
                flash("见证尚未齐备，无法下载", "error")
                return redirect(url_for("job_detail", job_id=job_id))
        meta = _read_meta(job_id) or {}
        rel = meta.get("files", {}).get(which)
        if not rel:
            abort(404)
        path = os.path.join(_job_dir(job_id), rel)
        if not os.path.exists(path):
            abort(404)
        return send_file(
            path,
            as_attachment=True,
            download_name=f"p90_{which}_witness_{job_id[:8]}.csv",
            mimetype="text/csv",
        )

    return app
