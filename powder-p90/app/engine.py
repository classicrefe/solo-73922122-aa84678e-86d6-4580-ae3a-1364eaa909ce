"""
跨批合并 P90 核算的外存引擎：工作内存 O(B + M)，全量数据落在本地磁盘。

B = 所选批次数，M = 单张档表的最大档数。

背景
----
旧实现 app.core.merge_p90 会把所有批次的切割点并集、每批 L/U 走廊和两份见证
一次性放进内存，占用随「全部批次档数总和」增长，一次核算许多批次时档表和见证
会撑爆工作内存。本引擎改用分块暂存（spool）文件：

* SpoolArray：定长 int64 分块顺序写/顺序读，写满一块即落盘，内存里至多保留
  固定大小的读写缓冲（与数据总量无关）；文件描述符带 LRU，打开文件数也有常数上界；
* 切割点并集用二路外部归并逐张表并入，归并缓冲为常数内存；
* 单表 S/E 序列与走廊 L/U 的折叠全部走双指针流式扫描，不构造全量列表；
* 全局跨界判定用 B 路堆归并（堆大小 B）逐切割点推进，不物化全局切割点数组；
* 见证逐批流式写 CSV，逐表流式复核，再用 B 路堆归并复核合并第 k 粒。

快照语义
--------
作业开始时对主 SQLite 库做一次一致性在线备份（backup API），此后上下界计算、
见证生成、全部复核都只读该快照文件。补报写入主库不影响在途作业；下一次核算
重新备份，自然采用新档表。
"""
from __future__ import annotations

import array
import csv
import heapq
import json
import os
import sqlite3
import time
import uuid
import weakref
from dataclasses import dataclass

# 一块 32768 个 int64 = 256KiB。内存占用只与块大小、同时打开的数组个数有关，
# 与批次总数、档总数无关。
CHUNK = 32_768
# 只读句柄数上限：B 路归并每批至多 3 个 reader，上限 1024 可容纳数百批，
# 仍是与总档数无关的常数；超限给出明确报错而不是悄悄共享句柄。
MAX_OPEN_FDS = 1024


class JobError(RuntimeError):
    """作业执行失败（含暂存失败、快照失败、数据矛盾等），reason 直接展示给用户。"""


# ---------------------------------------------------------------------------
# int64 分块暂存数组：顺序写、多次顺序重读，内存只保留一块
# ---------------------------------------------------------------------------

class SpoolArray:
    """定长 int64 外存数组。

    写入：append/extend 累积到内存块，满块刷入 path 对应文件。
    重读：rewind() 后按迭代器顺序读出，读一块换一块，不全量载入。
    每个 _SpoolReader 独占一个只读文件句柄，读尽即关闭；同一文件可以被多个
    reader 同时打开（句柄相互独立，seek 不串扰）。打开句柄数有类级上限，
    归并结构保证同时存活的 reader 数为 O(B)。
    """

    _open_fds = 0
    # 存活的只读游标（弱引用，不阻止 GC 正常回收）；close_all 可兜底回收残留句柄
    _live_readers: "weakref.WeakSet[_SpoolReader]" = weakref.WeakSet()

    def __init__(self, path: str):
        self.path = path
        self._wfh = open(path, "w+b")
        self._wbuf = array.array("q")
        self.length = 0
        self._closed = False

    # -- 写入 ---------------------------------------------------------------
    def append(self, value: int) -> None:
        self._wbuf.append(value)
        self.length += 1
        if len(self._wbuf) >= CHUNK:
            self._flush()

    def extend(self, values) -> None:
        for v in values:
            self.append(v)

    def _flush(self) -> None:
        if self._wbuf:
            try:
                self._wfh.write(self._wbuf.tobytes())
                self._wfh.flush()
            except OSError as exc:  # 磁盘满 / 目录不可写等
                raise JobError(f"暂存失败（写入 {self.path}）：{exc}") from exc
            self._wbuf = array.array("q")

    def finish_write(self) -> None:
        self._flush()
        self._wfh.close()
        self._closed = True

    # -- 重读 ---------------------------------------------------------------
    def rewind(self):
        if not self._closed:
            self.finish_write()
        return _SpoolReader(self.path, self.length)

    def unlink(self) -> None:
        try:
            if not self._closed:
                self._wfh.close()
                self._closed = True
        finally:
            try:
                os.unlink(self.path)
            except FileNotFoundError:
                pass

    @classmethod
    def close_all(cls) -> None:
        # 作业结束兜底：关闭所有仍存活的只读游标（正常路径已读尽自关）
        for rd in list(cls._live_readers):
            rd.close()


class _SpoolReader:
    """SpoolArray 的顺序读游标，独占只读句柄，读缓冲固定一块。"""

    def __init__(self, path: str, length: int):
        self.path = path
        self.length = length
        self._left = length
        self._buf = array.array("q")
        self._pos = 0
        self._offset = 0
        if SpoolArray._open_fds >= MAX_OPEN_FDS:
            # 归并结构里同时存活的 reader 为 O(B)；达到上限说明 B 异常大，
            # 直接报错而不是悄悄共享句柄（共享会造成 seek 串扰）。
            raise JobError(
                f"同时打开的暂存文件数达到上限 {MAX_OPEN_FDS}，请减少一次核算的批次数"
            )
        try:
            self._fh = open(path, "rb")
        except OSError as exc:
            raise JobError(f"暂存文件无法打开：{path}：{exc}") from exc
        SpoolArray._open_fds += 1
        self._fh_open = True
        SpoolArray._live_readers.add(self)

    def __iter__(self):
        return self

    def __next__(self) -> int:
        if self._pos < len(self._buf):
            v = self._buf[self._pos]
            self._pos += 1
            self._left -= 1
            return v
        if self._left <= 0:
            self.close()
            raise StopIteration
        self._load_chunk()
        return next(self)

    def _load_chunk(self) -> None:
        fh = self._fh
        fh.seek(self._offset)
        n = min(CHUNK, self._left)
        data = fh.read(n * 8)
        if len(data) != n * 8:
            raise JobError(f"暂存文件损坏或被截断：{self.path}")
        self._offset += len(data)
        self._buf = array.array("q")
        self._buf.frombytes(data)
        self._pos = 0

    def close(self) -> None:
        if self._fh_open:
            self._fh.close()
            self._fh_open = False
            SpoolArray._open_fds -= 1
            SpoolArray._live_readers.discard(self)

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 流式小工具
# ---------------------------------------------------------------------------

class _PairStream:
    """把档行 (lo, hi, count) 流转成 (切割点, 该点累计 cum) 对的惰性序列。

    表 T 的 cuts/cum 为：
        (lo_0, 0), (hi_0+1, count_0), (hi_1+1, count_0+count_1), ...
    显式两帧状态机：每档先产出左边界对 (lo, cum_before)，再产出右边界对
    (hi+1, cum_after)。只保留当前行与累计值，内存 O(1)
    （行本身由 sqlite 游标分块拉取）。
    """

    def __init__(self, rows):
        self._rows = iter(rows)
        self._cum = 0          # 已处理各档的累计粒数
        self._row = next(self._rows, None)
        self._at_left = True   # 当前帧是否为当前档的左边界
        self._cur = None

    def peek(self):
        """返回当前 (cut, cum)，耗尽返回 None。"""
        if self._cur is not None:
            return self._cur
        row = self._row
        if row is None:
            return None
        if self._at_left:
            self._cur = (int(row[0]), self._cum)
        else:
            self._cum += int(row[2])
            self._cur = (int(row[1]) + 1, self._cum)
        return self._cur

    def advance(self):
        if self._cur is None:
            return
        self._cur = None
        if self._at_left:
            # 左帧走完，同一档的右帧还没产出
            self._at_left = False
        else:
            # 右帧走完，推进到下一档的左帧
            self._at_left = True
            self._row = next(self._rows, None)


def _bin_rows(conn: sqlite3.Connection, table_id: int):
    """按档顺序分块拉取一张表的档行，避免一次性 fetchall。"""
    cur = conn.execute(
        "SELECT lo, hi, count FROM bins WHERE table_id = ? ORDER BY id", (table_id,)
    )
    while True:
        chunk = cur.fetchmany(CHUNK)
        if not chunk:
            return
        for row in chunk:
            yield row


def _write_table_cuts(conn: sqlite3.Connection, table_id: int, out: SpoolArray) -> None:
    """一张表的切割点序列（已升序、连续档下天然不重复）直接落暂存。"""
    first = True
    for lo, hi, _ in _bin_rows(conn, table_id):
        if first:
            out.append(int(lo))
            first = False
        out.append(int(hi) + 1)
    if first:
        raise JobError(f"档表 #{table_id} 没有任何档数据")


def _merge_two_sorted(left_path: str, left_len: int, right_iter, work_path: str):
    """二路升序归并去重：left 暂存文件 + 右侧内存惰性序列 → 新暂存。常数内存。"""
    out = SpoolArray(work_path)
    reader = _SpoolReader(left_path, left_len) if left_path else None
    it = iter(right_iter)
    a = next(reader, None) if reader is not None else None
    b = next(it, None)
    last = None
    while a is not None or b is not None:
        if b is None or (a is not None and a <= b):
            v, a = a, next(reader, None)
        else:
            v, b = b, next(it, None)
        if v != last:
            out.append(v)
            last = v
    out.finish_write()
    return out


def _lo_cum_stream(conn: sqlite3.Connection, table_id: int):
    """惰性产出每张档的 (lo, 截至该档含本档的累计 cum)，内存 O(1)。

    用于 E_T(c)=cum[bisect_left(los, c)]：吸收所有 lo<c 的帧后，最近一帧携带的
    累计就是 E；没有任何 lo<c 时 E=0（由调用方初值保证）。
    """
    cum = 0
    for lo, _hi, count in _bin_rows(conn, table_id):
        cum += int(count)
        yield int(lo), cum


def _se_stream(conn: sqlite3.Connection, table_id: int, cuts: _SpoolReader):
    """表 T 在切割点序列 cuts 上的 (S, E) 流式序列。

    S：在完整 (cut, cum) 流上吸收所有 cut <= c，取最近一个累计；
    E：在 (lo, cum_before) 流上吸收所有 lo < c，取最近一个累计，
       与 app.core.e_at 的 bisect_left(los, c) 完全同义。
    两个流各自持有独立的 sqlite 行迭代器，内存只有常数个标量。
    """
    s_stream = _PairStream(_bin_rows(conn, table_id))
    e_pairs = iter(_lo_cum_stream(conn, table_id))
    s_cum = e_cum = 0
    e_next = next(e_pairs, None)

    for c in cuts:
        # S 游标：吸收所有 cut <= c 的对
        while True:
            pair = s_stream.peek()
            if pair is None or pair[0] > c:
                break
            s_cum = pair[1]
            s_stream.advance()
        # E 游标：吸收所有 lo < c 的档；最近一帧的累计即 E（lo>=c 的帧留给后面）
        while e_next is not None and e_next[0] < c:
            e_cum = e_next[1]
            e_next = next(e_pairs, None)
        yield s_cum, e_cum


# ---------------------------------------------------------------------------
# 单批走廊（全部落盘）
# ---------------------------------------------------------------------------

@dataclass
class BatchCorridor:
    batch_id: int
    total: int
    n_tables: int
    cuts_path: str
    low_path: str
    up_path: str
    n_cuts: int


def _build_batch_corridor(
    conn: sqlite3.Connection, work_dir: str, batch_id: int,
    global_cuts_path: str, n_global_cuts: int,
) -> BatchCorridor:
    """在全局切割点序列上构建一批的 L/U 走廊，三个数组全部落盘。

    必须用全局切割点而非批自身点：别的批插入的切割点会收紧本批的 E 上界
    （在该点重新对每张表求 E=min）。时间 O(表数 × (档数 + 全局切割点数))，
    内存 O(CHUNK)（S/E 流与折叠均流式，单表只保留常数个游标标量）。
    """
    trows = conn.execute(
        "SELECT id, total FROM tables WHERE batch_id = ? ORDER BY id", (batch_id,)
    ).fetchall()
    if not trows:
        raise JobError(f"批次 #{batch_id} 没有已入库档表，无法核算")
    table_ids = [int(r[0]) for r in trows]
    totals = {int(r[0]): int(r[1]) for r in trows}
    total = totals[table_ids[0]]
    if any(v != total for v in totals.values()):
        raise JobError(f"批次 #{batch_id} 的档表总粒数不一致，数据已损坏")

    # 该批走廊在全局点上的取值仍只在自身切割点处阶跃，但数组按全局点对齐落盘，
    # 长度与全局切割点数一致，便于后续 B 路归并按相同下标/流对齐。
    low_path = os.path.join(work_dir, f"low_{batch_id}.bin")
    up_path = os.path.join(work_dir, f"up_{batch_id}.bin")
    first_low = SpoolArray(low_path)
    first_up = SpoolArray(up_path)
    for s, e in _se_stream(
        conn, table_ids[0], _SpoolReader(global_cuts_path, n_global_cuts)
    ):
        first_low.append(s)
        first_up.append(e)
    first_low.finish_write()
    first_up.finish_write()
    for tid in table_ids[1:]:
        new_low_path = low_path + ".new"
        new_up_path = up_path + ".new"
        new_low = SpoolArray(new_low_path)
        new_up = SpoolArray(new_up_path)
        old_low = _SpoolReader(low_path, n_global_cuts)
        old_up = _SpoolReader(up_path, n_global_cuts)
        cuts_reader = _SpoolReader(global_cuts_path, n_global_cuts)
        for (s, e), old_l, old_u in zip(
            _se_stream(conn, tid, cuts_reader), old_low, old_up
        ):
            new_low.append(max(old_l, s))
            new_up.append(min(old_u, e))
        new_low.finish_write()
        new_up.finish_write()
        os.unlink(low_path)
        os.unlink(up_path)
        os.replace(new_low_path, low_path)
        os.replace(new_up_path, up_path)

    # 相容性与闭合校验（流式，不落额外数组）
    low_r = _SpoolReader(low_path, n_global_cuts)
    up_r = _SpoolReader(up_path, n_global_cuts)
    cuts_r = _SpoolReader(global_cuts_path, n_global_cuts)
    last_low = last_up = None
    for c, lv, uv in zip(cuts_r, low_r, up_r):
        if lv > uv:
            raise JobError(
                f"批次 #{batch_id} 的已入库档表不相容：切割点 c={c} 处"
                f"（粒径 <{c} 的粒数）至少 {lv} 粒、至多 {uv} 粒"
            )
        last_low, last_up = lv, uv
    if last_low != total or last_up != total:
        raise JobError(f"批次 #{batch_id} 的档表覆盖范围外仍有粒子")

    return BatchCorridor(
        batch_id=batch_id,
        total=total,
        n_tables=len(table_ids),
        cuts_path=global_cuts_path,  # 与 low/up 对齐的就是全局切割点序列
        low_path=low_path,
        up_path=up_path,
        n_cuts=n_global_cuts,
    )


def _cut_iter(conn: sqlite3.Connection, table_id: int):
    first = True
    for lo, hi, _ in _bin_rows(conn, table_id):
        if first:
            yield int(lo)
            first = False
        yield int(hi) + 1


def _write_batch_own_cuts(
    conn: sqlite3.Connection, work_dir: str, batch_id: int
) -> tuple[str, int]:
    """把一批所有档表的切割点并集（二路外部归并去重）落盘，返回 (路径, 点数)。

    这是全局切割点归并的每路输入；内存 O(CHUNK)，与档总数无关。
    """
    trows = conn.execute(
        "SELECT id FROM tables WHERE batch_id = ? ORDER BY id", (batch_id,)
    ).fetchall()
    if not trows:
        raise JobError(f"批次 #{batch_id} 没有已入库档表，无法核算")
    table_ids = [int(r[0]) for r in trows]
    union_path = os.path.join(work_dir, f"cuts_{batch_id}.bin.tmp0")
    first = SpoolArray(union_path)
    _write_table_cuts(conn, table_ids[0], first)
    first.finish_write()
    union_len = first.length
    round_no = 1
    for tid in table_ids[1:]:
        next_path = os.path.join(work_dir, f"cuts_{batch_id}.bin.tmp{round_no}")
        merged = _merge_two_sorted(
            union_path, union_len, _cut_iter(conn, tid), next_path
        )
        try:
            os.unlink(union_path)
        except FileNotFoundError:
            pass
        union_path = merged.path
        union_len = merged.length
        round_no += 1
    final_path = os.path.join(work_dir, f"cuts_{batch_id}.bin")
    os.replace(union_path, final_path)
    return final_path, union_len


# ---------------------------------------------------------------------------
# B 路堆归并：全局跨界判定，内存 O(B)
# ---------------------------------------------------------------------------

def _global_crossings(corridors: list[BatchCorridor], k: int):
    """返回 (p90_low, p90_high)。

    各批走廊在自身切割点之间阶跃不变，故把每批切割点作为一路有序流做 B 路
    堆归并；在每个全局切割点 c 上，把命中 c 的批次游标前进一步并求和。
    堆与各路游标状态占 O(B) 内存，不物化全局切割点。
    """
    # 每路维护“下一个待入账帧” (cut, low_at_cut, up_at_cut)；首个切割点处
    # L=U=0。切割点之前该批对“粒径 < c”的贡献为 0，故 cur_* 初值全 0。
    next_frame: list[tuple[int, int, int] | None] = []
    heap: list[tuple[int, int]] = []
    cur_low = [0] * len(corridors)
    cur_up = [0] * len(corridors)
    open_readers = []
    for idx, cor in enumerate(corridors):
        cuts = _SpoolReader(cor.cuts_path, cor.n_cuts)
        lows = _SpoolReader(cor.low_path, cor.n_cuts)
        ups = _SpoolReader(cor.up_path, cor.n_cuts)
        open_readers.extend((cuts, lows, ups))
        frame = (next(cuts), next(lows), next(ups))
        next_frame.append(frame)
        heapq.heappush(heap, (frame[0], idx))

    def advance(idx: int):
        """第 idx 路读下一帧；耗尽返回 None。"""
        cuts = open_readers[3 * idx]
        lows = open_readers[3 * idx + 1]
        ups = open_readers[3 * idx + 2]
        nxt_cut = next(cuts, None)
        if nxt_cut is None:
            next_frame[idx] = None
            return
        next_frame[idx] = (nxt_cut, next(lows), next(ups))
        heapq.heappush(heap, (nxt_cut, idx))

    p90_low = None
    p90_high = None
    try:
        # 跳过所有批的首个切割点（L=U=0，零贡献），锁定全局最小切割点为段起点
        first_c = heap[0][0]
        while heap and heap[0][0] == first_c:
            _, idx = heapq.heappop(heap)
            advance(idx)
        segment_left = first_c

        while heap:
            c = heap[0][0]
            # 同一全局切割点可能被多批共享，全部弹出并入账
            while heap and heap[0][0] == c:
                _, idx = heapq.heappop(heap)
                _, lv, uv = next_frame[idx]
                cur_low[idx] = lv
                cur_up[idx] = uv
                advance(idx)
            sum_u = sum(cur_up)
            sum_l = sum(cur_low)
            # 首个 U(c)>=k：跨界发生在段 [segment_left, c)，粒子可堆段左端
            if p90_low is None and sum_u >= k:
                p90_low = segment_left
            # 首个 L(c)>=k：跨界最迟发生在段右端 c-1
            if p90_high is None and sum_l >= k:
                p90_high = c - 1
            if p90_low is not None and p90_high is not None:
                break
            segment_left = c
    finally:
        for rd in open_readers:
            rd.close()

    if p90_low is None or p90_high is None:
        raise JobError("内部错误：找不到 P90 跨界段（总粒数或走廊数据异常）")
    if p90_low > p90_high:
        raise JobError("内部错误：P90 下界大于上界，走廊数据异常")
    return p90_low, p90_high


# ---------------------------------------------------------------------------
# 见证：在全局切割点上对每批走廊差分，逐批流式写 CSV，再逐表流式复核
# ---------------------------------------------------------------------------

def _merge_global_cuts(
    batch_cut_files: list[tuple[str, int]], out_path: str
):
    """把各批自身切割点暂存做 B 路堆归并去重，落一份全局切割点暂存。

    入参为每批 (切割点文件路径, 点数)。堆大小 O(B)，读游标 O(B)，不物化
    全局数组。返回 (SpoolArray, 全局切割点数)。
    """
    heap: list[tuple[int, int]] = []
    readers: list[_SpoolReader] = []
    for idx, (path, length) in enumerate(batch_cut_files):
        rd = _SpoolReader(path, length)
        readers.append(rd)
        heapq.heappush(heap, (next(rd), idx))
    out = SpoolArray(out_path)
    last = None
    try:
        while heap:
            c, idx = heapq.heappop(heap)
            if c != last:
                out.append(c)
                last = c
            nxt = next(readers[idx], None)
            if nxt is not None:
                heapq.heappush(heap, (nxt, idx))
    finally:
        for rd in readers:
            rd.close()
    out.finish_write()
    return out, out.length


def _write_witness_csvs(
    conn: sqlite3.Connection, cor: BatchCorridor, work_dir: str,
    global_cuts_path: str, n_global_cuts: int,
):
    """生成一批的两份逐批粒径频数见证 CSV（在全局切割点上差分）。

    走廊 low/up 已与全局切割点一一对齐（见 _build_batch_corridor）。
    下界见证：全局段 [a,a') 放 U(a')-U(a) 粒、堆段左端 a（CDF 贴 U）；
    上界见证：同段放 L(a')-L(a) 粒、堆段右端 a'-1（CDF 贴 L）。
    非零项数量 O(全局切割点数)，三条流同步推进，内存 O(CHUNK)。
    """
    low_csv = os.path.join(work_dir, f"witness_low_{cor.batch_id}.csv")
    high_csv = os.path.join(work_dir, f"witness_high_{cor.batch_id}.csv")
    cuts = _SpoolReader(global_cuts_path, n_global_cuts)
    lows = _SpoolReader(cor.low_path, n_global_cuts)
    ups = _SpoolReader(cor.up_path, n_global_cuts)
    low_rows = high_rows = 0
    low_total = high_total = 0
    prev_c = next(cuts)
    prev_l = next(lows)
    prev_u = next(ups)
    with open(low_csv, "w", newline="", encoding="utf-8") as fl, \
            open(high_csv, "w", newline="", encoding="utf-8") as fh:
        wl = csv.writer(fl)
        wh = csv.writer(fh)
        wl.writerow(["size_micron", "count"])
        wh.writerow(["size_micron", "count"])
        for c, lv, uv in zip(cuts, lows, ups):
            u_j = uv - prev_u
            l_j = lv - prev_l
            if u_j < 0 or l_j < 0:
                raise JobError(f"批次 #{cor.batch_id} 走廊非单调，见证无法构造")
            if u_j:
                wl.writerow([prev_c, u_j])
                low_rows += 1
                low_total += u_j
            if l_j:
                wh.writerow([c - 1, l_j])
                high_rows += 1
                high_total += l_j
            prev_c, prev_l, prev_u = c, lv, uv
    if low_total != cor.total or high_total != cor.total:
        raise JobError(
            f"批次 #{cor.batch_id} 见证总粒数不符：下界 {low_total}、上界 {high_total}、"
            f"应为 {cor.total}"
        )
    return low_csv, high_csv, low_rows, high_rows


def _verify_witness_file(csv_path: str, conn: sqlite3.Connection, batch_id: int) -> None:
    """流式断言一份见证 CSV 满足该批每张档表的逐档粒数。

    每张表只分配长度 = 该表档数的归并数组（即 O(M) 项），见证行按粒径有序，
    双指针线性归并，不逐粒展开、不载入整张见证。
    """
    trows = conn.execute(
        "SELECT id FROM tables WHERE batch_id = ? ORDER BY id", (batch_id,)
    ).fetchall()
    for (table_id,) in trows:
        bins = [tuple(r) for r in conn.execute(
            "SELECT lo, hi, count FROM bins WHERE table_id = ? ORDER BY id", (table_id,)
        ).fetchall()]
        per_bin = [0] * len(bins)
        idx = 0  # 每张表独立归并，游标必须从头开始
        total = 0
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            if header != ["size_micron", "count"]:
                raise JobError(f"见证文件 {os.path.basename(csv_path)} 表头损坏")
            for row in reader:
                if len(row) != 2:
                    raise JobError("见证文件存在字段数错误的行")
                size, cnt = int(row[0]), int(row[1])
                if cnt < 0:
                    raise JobError("见证出现负粒数")
                total += cnt
                # 跳过所有 hi 在 size 左侧的档，定位到唯一覆盖 size 的档；
                # 同一档可接收多个见证粒径，idx 不能因 size<=hi 而重复累加。
                while idx < len(bins) and size > bins[idx][1]:
                    idx += 1
                if idx >= len(bins) or size < bins[idx][0]:
                    raise JobError(
                        f"批次 #{batch_id} 见证粒径 {size} 落在档表 #{table_id} 覆盖外"
                    )
                per_bin[idx] += cnt
        table_total = sum(b[2] for b in bins)
        if total != table_total:
            raise JobError(
                f"批次 #{batch_id} 见证总粒数 {total} 与档表 #{table_id} 的 {table_total} 不符"
            )
        for i, (lo, hi, count) in enumerate(bins):
            if per_bin[i] != count:
                raise JobError(
                    f"批次 #{batch_id} 档 [{lo},{hi}] 归并粒数 {per_bin[i]} "
                    f"!= 档表粒数 {count}，见证不符合档表"
                )


def _kth_of_merged_csvs(csv_paths: list[str], k: int) -> int:
    """B 路有序 CSV 堆归并求合并后升序第 k 小粒径，内存 O(B)。"""
    heap: list[tuple[int, int, int]] = []
    files = [open(p, newline="", encoding="utf-8") for p in csv_paths]
    try:
        readers = [csv.reader(fh) for fh in files]
        for idx, reader in enumerate(readers):
            next(reader, None)  # 跳过表头
            row = next(reader, None)
            if row is not None:
                heapq.heappush(heap, (int(row[0]), int(row[1]), idx))
        running = 0
        while heap:
            size, cnt, idx = heapq.heappop(heap)
            running += cnt
            if running >= k:
                return size
            row = next(readers[idx], None)
            if row is not None:
                heapq.heappush(heap, (int(row[0]), int(row[1]), idx))
    finally:
        for fh in files:
            fh.close()
    raise JobError("内部错误：合并见证粒数不足 k")


# ---------------------------------------------------------------------------
# 快照与作业编排
# ---------------------------------------------------------------------------

def snapshot_database(src_path: str, dst_path: str) -> None:
    """对主库做一致性在线备份；补报事务未提交的内容不会进入快照。"""
    try:
        src = sqlite3.connect(src_path)
        dst = sqlite3.connect(dst_path)
        try:
            src.backup(dst)
        finally:
            dst.close()
            src.close()
    except sqlite3.Error as exc:
        raise JobError(f"快照失败：{exc}") from exc


def _assemble_full_csv(per_batch_paths: list[str], full_path: str,
                       header: list[str], id_name_rows: list[tuple[int, str]]) -> int:
    """把逐批 CSV 拼成一份完整 CSV，流式复制；返回数据行数。"""
    rows = 0
    with open(full_path, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(header)
        for (bid, bname), path in zip(id_name_rows, per_batch_paths):
            with open(path, newline="", encoding="utf-8") as fh:
                reader = csv.reader(fh)
                next(reader, None)  # 跳过逐批文件表头
                for row in reader:
                    writer.writerow([bid, bname, row[0], row[1]])
                    rows += 1
    return rows


def execute_job(
    *,
    job_id: str,
    work_dir: str,
    db_path: str,
    batch_ids: list[int],
    batch_names: dict[int, str],
    status_writer,
) -> dict:
    """执行一次完整核算作业，返回写入 meta.json 的结果字典。

    status_writer(status, reason="")：把状态同步到主库 jobs 表。
    任何异常都转成 JobError 并落 failed 状态，reason 可直接展示。
    """
    meta: dict = {
        "job_id": job_id,
        "status": "running",
        "reason": "",
        "batch_ids": list(batch_ids),
        "batch_names": {str(k): v for k, v in batch_names.items()},
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    meta_path = os.path.join(work_dir, "meta.json")

    def write_meta():
        tmp = meta_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(meta, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, meta_path)  # 原子替换，状态页读到的总是完整 JSON

    try:
        status_writer("running", "")
        write_meta()

        snapshot_path = os.path.join(work_dir, "snapshot.db")
        snapshot_database(db_path, snapshot_path)
        snap = sqlite3.connect(snapshot_path)
        try:
            # 快照后再核对一次：所选批次在核算开始的已提交状态中必须齐备
            placeholders = ",".join("?" * len(batch_ids))
            found = {
                int(r[0]): int(r[1])
                for r in snap.execute(
                    f"SELECT id, 1 FROM batches WHERE id IN ({placeholders})",
                    batch_ids,
                )
            }
            missing = [b for b in batch_ids if b not in found]
            if missing:
                raise JobError(f"批次不存在或已被删除：{missing}")

            # 1) 每批自身切割点落盘（外存二路归并）
            batch_cut_files: list[tuple[str, int]] = []
            batch_totals: dict[int, int] = {}
            for bid in batch_ids:
                status_writer("running", f"正在归并批次 #{bid} 的档表切割点")
                path, length = _write_batch_own_cuts(snap, work_dir, bid)
                batch_cut_files.append((path, length))
                row = snap.execute(
                    "SELECT MIN(total) FROM tables WHERE batch_id=?", (bid,)
                ).fetchone()
                batch_totals[bid] = int(row[0])

            total = sum(batch_totals.values())
            if total <= 0:
                raise JobError("合并总粒数为 0")
            k = (9 * total + 9) // 10

            # 2) 全局切割点暂存（B 路堆归并去重），走廊折叠/跨界/见证差分共用
            status_writer("running", "正在归并全局切割点")
            global_cuts_path = os.path.join(work_dir, "global_cuts.bin")
            gc_spool, n_global_cuts = _merge_global_cuts(
                batch_cut_files, global_cuts_path
            )
            del gc_spool  # 已 finish_write，后续只按路径重读

            # 3) 每批在全局切割点上折叠 L/U 走廊（外存，逐表流式）
            corridors: list[BatchCorridor] = []
            for bid in batch_ids:
                status_writer("running", f"正在构建批次 #{bid} 的累计走廊")
                corridors.append(
                    _build_batch_corridor(
                        snap, work_dir, bid, global_cuts_path, n_global_cuts
                    )
                )

            # 4) 全局跨界（B 路堆归并，O(B) 内存）
            status_writer("running", "正在判定 P90 最紧界")
            p90_low, p90_high = _global_crossings(corridors, k)

            # 5) 逐批见证（全局点差分）+ 逐表复核
            low_paths, high_paths = [], []
            low_row_total = high_row_total = 0
            per_batch_stats = []
            for cor in corridors:
                status_writer("running", f"正在生成并复核批次 #{cor.batch_id} 的见证")
                lp, hp, lr, hr = _write_witness_csvs(
                    snap, cor, work_dir, global_cuts_path, n_global_cuts
                )
                _verify_witness_file(lp, snap, cor.batch_id)
                _verify_witness_file(hp, snap, cor.batch_id)
                low_paths.append(lp)
                high_paths.append(hp)
                low_row_total += lr
                high_row_total += hr
                per_batch_stats.append({
                    "batch_id": cor.batch_id,
                    "total": cor.total,
                    "n_tables": cor.n_tables,
                    "low_rows": lr,
                    "high_rows": hr,
                })

            # 6) 合并第 k 粒复核：两端见证必须恰好取到对应 P90 端点
            status_writer("running", "正在复核合并见证的第 k 粒")
            if _kth_of_merged_csvs(low_paths, k) != p90_low:
                raise JobError("内部错误：下界见证合并后未取到 P90 下界")
            if _kth_of_merged_csvs(high_paths, k) != p90_high:
                raise JobError("内部错误：上界见证合并后未取到 P90 上界")

            # 7) 拼装两端完整 CSV（含全部所选批次的逐批频数）
            id_name_rows = [(bid, batch_names[bid]) for bid in batch_ids]
            full_low = os.path.join(work_dir, "witness_low_full.csv")
            full_high = os.path.join(work_dir, "witness_high_full.csv")
            full_low_rows = _assemble_full_csv(
                low_paths, full_low,
                ["batch_id", "batch_name", "size_micron", "count"], id_name_rows,
            )
            full_high_rows = _assemble_full_csv(
                high_paths, full_high,
                ["batch_id", "batch_name", "size_micron", "count"], id_name_rows,
            )
        finally:
            snap.close()

        meta.update({
            "status": "ready",
            "reason": "",
            "total": total,
            "k": k,
            "p90_low": p90_low,
            "p90_high": p90_high,
            "witness_low_rows": low_row_total,
            "witness_high_rows": high_row_total,
            "full_low_rows": full_low_rows,
            "full_high_rows": full_high_rows,
            "files": {
                "low": "witness_low_full.csv",
                "high": "witness_high_full.csv",
            },
            "per_batch": per_batch_stats,
            "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        status_writer("ready", "")
        write_meta()
        return meta
    except JobError as exc:
        meta.update({"status": "failed", "reason": str(exc),
                     "finished_at": time.strftime("%Y-%m-%d %H:%M:%S")})
        status_writer("failed", str(exc))
        write_meta()
        return meta
    except Exception as exc:  # 兜底：任何意外都要让页面看到原因
        reason = f"核算发生未预期错误：{type(exc).__name__}: {exc}"
        meta.update({"status": "failed", "reason": reason,
                     "finished_at": time.strftime("%Y-%m-%d %H:%M:%S")})
        status_writer("failed", reason)
        write_meta()
        return meta
    finally:
        SpoolArray.close_all()


def new_job_id() -> str:
    return uuid.uuid4().hex
