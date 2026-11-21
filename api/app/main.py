"""流域归属与观测服务 API.

所有写操作都在单事务中执行; 数据库在提交时统一校验:
  1. 同站归属时段不得重叠;
  2. 每条观测在采样时刻必须恰好落入本站一个归属时段.
违反即整笔回滚并返回 409.

归属时段带版本标识(version_id): 转属/拆分/合并/历史更正在与时段变更相同的事务内
写入接续关系(period_succession); 下游凭供数接口返回的 period_version_id 调用
/trace 追溯采样时刻的归属变迁. 其他入口(变更集裸 update/delete_period)或直连 SQL
改动/删除链上时段不产生接续, 追溯返回已确认的链并标明断点, 改回原值也不续接.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional
from uuid import UUID

import psycopg
from fastapi import FastAPI, HTTPException, Query
from pydantic import AwareDatetime

from .changeset import execute_changeset
from .db import close_pool, get_pool, init_pool
from .models import (
    ChangeSet,
    CorrectRequest,
    MergeRequest,
    ObservationIn,
    SplitRequest,
    StationIn,
    TraceBreak,
    TraceChainLink,
    TraceResponse,
    TransferRequest,
    to_utc,
)

DESCRIPTION = """维护站点-流域归属时段与站点观测。

* 时段左闭右开 `[valid_from, valid_to)`，终点可空（至今有效）
* 同站时段不得重叠；每条观测须恰有一段归属
* `/changesets` 可在一笔事务内拆分/合并/更正时段并补改观测，中途允许暂时违规，提交时统一校验
* 冲突（含并发冲突、死锁）整笔拒绝，返回 409
* 供数接口返回归属时段版本标识 `period_version_id`；`GET /trace` 凭它 + UTC 采样时刻追溯归属变迁链
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_pool()
    yield
    close_pool()


app = FastAPI(
    title="流域归属与观测服务",
    version="1.1.0",
    description=DESCRIPTION,
    lifespan=lifespan,
)

_INTEGRITY_ERRORS = (
    psycopg.errors.IntegrityError,        # 含提交时延迟约束触发器抛出的 23514
    psycopg.errors.RaiseException,        # P0001
    psycopg.errors.DeadlockDetected,      # 40P01
    psycopg.errors.SerializationFailure,  # 40001
)


def _to_409(exc: psycopg.Error) -> HTTPException:
    msg = exc.diag.message_primary if exc.diag and exc.diag.message_primary else str(exc).strip()
    return HTTPException(status_code=409, detail=msg)


def run_txn(fn):
    """在单事务中执行 fn(conn); 完整性/并发冲突 -> 409, 整笔回滚."""
    pool = get_pool()
    with pool.connection() as conn:
        try:
            with conn.transaction():
                return fn(conn)
        except HTTPException:
            raise
        except _INTEGRITY_ERRORS as exc:
            raise _to_409(exc) from exc


def _link_succession(conn, pred_period_id: int, pred_version_no: int,
                     successors: list[tuple[int, int]]) -> None:
    """写入接续关系 pred -> succ (与时段变更在同一事务, 一起提交/回滚)."""
    for succ_pid, succ_vno in successors:
        conn.execute(
            """
            INSERT INTO period_succession
                (pred_period_id, pred_version_no, succ_period_id, succ_version_no)
            VALUES (%s, %s, %s, %s)
            """,
            (pred_period_id, pred_version_no, succ_pid, succ_vno),
        )


# 追溯: 单条 SQL(递归 CTE)完成, 整条链读同一已提交快照.
# 从源版本出发, 每跳在接续后继中按左闭右开区间选择包含采样时刻的一版;
# 同时取出链尾时段的在线版本号, 用于判定断点(被改动/被删除).
TRACE_SQL = """
WITH RECURSIVE walk AS (
    SELECT v.period_id, v.version_no, v.version_id, v.watershed,
           v.valid_from, v.valid_to,
           0 AS depth, ARRAY[v.version_id] AS path
    FROM period_versions v
    WHERE v.version_id = %(version_id)s
    UNION ALL
    SELECT nv.period_id, nv.version_no, nv.version_id, nv.watershed,
           nv.valid_from, nv.valid_to,
           w.depth + 1, w.path || nv.version_id
    FROM walk w
    JOIN period_succession ps
      ON ps.pred_period_id = w.period_id
     AND ps.pred_version_no = w.version_no
    JOIN period_versions nv
      ON nv.period_id = ps.succ_period_id
     AND nv.version_no = ps.succ_version_no
     AND tstzrange(nv.valid_from, nv.valid_to, '[)') @> %(at)s::timestamptz
    WHERE NOT nv.version_id = ANY(w.path)
      AND w.depth < 1000
),
tail AS (
    SELECT w.period_id, w.version_no, ap.version_no AS live_version_no
    FROM (
        SELECT period_id, version_no, depth FROM walk
        ORDER BY depth DESC, period_id DESC LIMIT 1
    ) w
    LEFT JOIN attribution_periods ap ON ap.period_id = w.period_id
)
SELECT w.period_id, w.version_no, w.version_id, w.watershed,
       w.valid_from, w.valid_to, t.live_version_no
FROM walk w
CROSS JOIN tail t
ORDER BY w.depth, w.period_id
"""


# ---------------------------------------------------------------------------
# 基础
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"service": "watershed-attribution", "docs": "/docs", "health": "/health"}


@app.get("/health")
def health():
    pool = get_pool()
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# 站点
# ---------------------------------------------------------------------------

@app.post("/stations", status_code=201, tags=["stations"])
def create_station(body: StationIn):
    def fn(conn):
        conn.execute(
            "INSERT INTO stations (station_id, name) VALUES (%s, %s)",
            (body.station_id, body.name),
        )
        return {"station_id": body.station_id, "name": body.name}

    return run_txn(fn)


@app.get("/stations", tags=["stations"])
def list_stations():
    pool = get_pool()
    with pool.connection() as conn:
        return conn.execute(
            "SELECT station_id, name, created_at FROM stations ORDER BY station_id"
        ).fetchall()


@app.get("/stations/{station_id}/periods", tags=["stations"])
def list_periods(station_id: str):
    pool = get_pool()
    with pool.connection() as conn:
        return conn.execute(
            """
            SELECT period_id, station_id, watershed, valid_from, valid_to,
                   version_no, version_id
            FROM attribution_periods
            WHERE station_id = %s
            ORDER BY valid_from
            """,
            (station_id,),
        ).fetchall()


# ---------------------------------------------------------------------------
# 变更集(核心写入口)
# ---------------------------------------------------------------------------

@app.post("/changesets", status_code=201, tags=["write"])
def submit_changeset(cs: ChangeSet):
    """一笔事务执行全部操作; 提交时校验不通过则整笔回滚并返回 409."""

    def fn(conn):
        return {"committed": True, "results": execute_changeset(conn, cs)}

    return run_txn(fn)


@app.post("/observations", status_code=201, tags=["write"])
def create_observation(body: ObservationIn):
    """单条补录(等价于只含一个 insert_observation 的变更集)."""

    def fn(conn):
        row = conn.execute(
            """
            INSERT INTO observations (station_id, observed_at, value)
            VALUES (%s, %s, %s)
            RETURNING observation_id
            """,
            (body.station_id, to_utc(body.observed_at), body.value),
        ).fetchone()
        return {"observation_id": row["observation_id"]}

    return run_txn(fn)


# ---------------------------------------------------------------------------
# 便捷操作: 转属 / 拆分 / 合并 / 历史更正
# ---------------------------------------------------------------------------

@app.post("/stations/{station_id}/transfer", status_code=201, tags=["write"])
def transfer(station_id: str, body: TransferRequest):
    """转属: at 时刻截断当前归属时段, 同时开启归属 new_watershed 的新时段.

    原时段截断后生成新版本(沿用原编号), 并与新时段一起登记为旧版的接续;
    时段变更与接续关系在同一事务提交或回滚.
    """
    at = to_utc(body.at)

    def fn(conn):
        row = conn.execute(
            """
            SELECT period_id, watershed, valid_from, valid_to, version_no
            FROM attribution_periods
            WHERE station_id = %s AND tstzrange(valid_from, valid_to, '[)') @> %s
            FOR UPDATE
            """,
            (station_id, at),
        ).fetchone()
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"station {station_id}: no attribution period covers {at.isoformat()}",
            )
        if row["watershed"] == body.new_watershed:
            raise HTTPException(status_code=422, detail="new_watershed equals current watershed")
        if not at > row["valid_from"]:
            raise HTTPException(
                status_code=422,
                detail="transfer time must be later than the covering period start",
            )
        closed = conn.execute(
            """
            UPDATE attribution_periods SET valid_to = %s WHERE period_id = %s
            RETURNING version_no, version_id
            """,
            (at, row["period_id"]),
        ).fetchone()
        new = conn.execute(
            """
            INSERT INTO attribution_periods (station_id, watershed, valid_from, valid_to)
            VALUES (%s, %s, %s, %s)
            RETURNING period_id, version_no, version_id
            """,
            (station_id, body.new_watershed, at, row["valid_to"]),
        ).fetchone()
        _link_succession(
            conn, row["period_id"], row["version_no"],
            [(row["period_id"], closed["version_no"]),
             (new["period_id"], new["version_no"])],
        )
        return {
            "station_id": station_id,
            "closed_period_id": row["period_id"],
            "closed_version_no": closed["version_no"],
            "closed_version_id": closed["version_id"],
            "new_period_id": new["period_id"],
            "new_version_no": new["version_no"],
            "new_version_id": new["version_id"],
            "watershed": body.new_watershed,
            "valid_from": at,
            "valid_to": row["valid_to"],
        }

    return run_txn(fn)


@app.post("/periods/{period_id}/split", status_code=201, tags=["write"])
def split_period(period_id: int, body: SplitRequest):
    """拆分: 把时段在 at 处一分为二; 右半段可指定新流域.

    沿用原编号的左半段同样生成新版本(version_no+1, 换发 version_id);
    两半都登记为旧版的接续, 与时段变更在同一事务提交或回滚.
    """
    at = to_utc(body.at)

    def fn(conn):
        row = conn.execute(
            """
            SELECT period_id, station_id, watershed, valid_from, valid_to, version_no
            FROM attribution_periods
            WHERE period_id = %s
            FOR UPDATE
            """,
            (period_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail=f"period {period_id} not found")
        if not at > row["valid_from"]:
            raise HTTPException(status_code=422, detail="split point must be after valid_from")
        if row["valid_to"] is not None and not at < row["valid_to"]:
            raise HTTPException(status_code=422, detail="split point must be before valid_to")
        right_ws = body.right_watershed or row["watershed"]
        left = conn.execute(
            """
            UPDATE attribution_periods SET valid_to = %s WHERE period_id = %s
            RETURNING version_no, version_id
            """,
            (at, period_id),
        ).fetchone()
        right = conn.execute(
            """
            INSERT INTO attribution_periods (station_id, watershed, valid_from, valid_to)
            VALUES (%s, %s, %s, %s)
            RETURNING period_id, version_no, version_id
            """,
            (row["station_id"], right_ws, at, row["valid_to"]),
        ).fetchone()
        _link_succession(
            conn, period_id, row["version_no"],
            [(period_id, left["version_no"]),
             (right["period_id"], right["version_no"])],
        )
        return {
            "left_period_id": period_id,
            "left_version_no": left["version_no"],
            "left_version_id": left["version_id"],
            "right_period_id": right["period_id"],
            "right_version_no": right["version_no"],
            "right_version_id": right["version_id"],
            "right_watershed": right_ws,
            "split_at": at,
        }

    return run_txn(fn)


@app.post("/stations/{station_id}/merge", status_code=201, tags=["write"])
def merge_periods(station_id: str, body: MergeRequest):
    """合并: 把在 at 处首尾相接(左段 valid_to == at == 右段 valid_from)的两段并为一段.

    合并后的时段生成新版本; 左右两段的旧版都登记接续到合并版,
    与时段变更在同一事务提交或回滚.
    """
    at = to_utc(body.at)

    def fn(conn):
        left = conn.execute(
            """
            SELECT period_id, watershed, valid_from, valid_to, version_no
            FROM attribution_periods
            WHERE station_id = %s AND valid_to = %s
            FOR UPDATE
            """,
            (station_id, at),
        ).fetchone()
        right = conn.execute(
            """
            SELECT period_id, watershed, valid_from, valid_to, version_no
            FROM attribution_periods
            WHERE station_id = %s AND valid_from = %s
            FOR UPDATE
            """,
            (station_id, at),
        ).fetchone()
        if left is None or right is None:
            raise HTTPException(
                status_code=404,
                detail=f"station {station_id}: no adjacent period pair at {at.isoformat()}",
            )
        merged_ws = body.watershed or left["watershed"]
        conn.execute(
            "DELETE FROM attribution_periods WHERE period_id = %s",
            (right["period_id"],),
        )
        merged = conn.execute(
            """
            UPDATE attribution_periods SET valid_to = %s, watershed = %s
            WHERE period_id = %s
            RETURNING version_no, version_id
            """,
            (right["valid_to"], merged_ws, left["period_id"]),
        ).fetchone()
        _link_succession(
            conn, left["period_id"], left["version_no"],
            [(left["period_id"], merged["version_no"])],
        )
        _link_succession(
            conn, right["period_id"], right["version_no"],
            [(left["period_id"], merged["version_no"])],
        )
        return {
            "merged_period_id": left["period_id"],
            "merged_version_no": merged["version_no"],
            "merged_version_id": merged["version_id"],
            "removed_period_id": right["period_id"],
            "watershed": merged_ws,
            "valid_from": left["valid_from"],
            "valid_to": right["valid_to"],
        }

    return run_txn(fn)


@app.post("/stations/{station_id}/correct", status_code=201, tags=["write"])
def correct_period(station_id: str, body: CorrectRequest):
    """历史更正: 把 [valid_from, valid_to) 子区间改属另一流域.

    要求该子区间完整落在某一个现有时段内; 原时段被切分为最多三段,
    观测归属随采样时刻自动重排. 跨多段的更正请用 /changesets.
    原时段旧版到每个新分段都登记接续, 与时段变更在同一事务提交或回滚.
    """
    cfrom, cto = to_utc(body.valid_from), to_utc(body.valid_to)
    if not cfrom < cto:
        raise HTTPException(status_code=422, detail="valid_from must be before valid_to")

    def fn(conn):
        row = conn.execute(
            """
            SELECT period_id, watershed, valid_from, valid_to, version_no
            FROM attribution_periods
            WHERE station_id = %s
              AND valid_from <= %s
              AND (valid_to IS NULL OR %s <= valid_to)
            ORDER BY valid_from
            LIMIT 1
            FOR UPDATE
            """,
            (station_id, cfrom, cto),
        ).fetchone()
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"station {station_id}: no single period covers "
                       f"[{cfrom.isoformat()}, {cto.isoformat()}); use /changesets instead",
            )
        conn.execute(
            "DELETE FROM attribution_periods WHERE period_id = %s",
            (row["period_id"],),
        )
        segments = []
        if cfrom > row["valid_from"]:
            segments.append((row["watershed"], row["valid_from"], cfrom))
        segments.append((body.watershed, cfrom, cto))
        if row["valid_to"] is None or cto < row["valid_to"]:
            segments.append((row["watershed"], cto, row["valid_to"]))
        new_periods = []
        for ws, seg_from, seg_to in segments:
            new_periods.append(
                conn.execute(
                    """
                    INSERT INTO attribution_periods (station_id, watershed, valid_from, valid_to)
                    VALUES (%s, %s, %s, %s)
                    RETURNING period_id, version_no, version_id
                    """,
                    (station_id, ws, seg_from, seg_to),
                ).fetchone()
            )
        _link_succession(
            conn, row["period_id"], row["version_no"],
            [(p["period_id"], p["version_no"]) for p in new_periods],
        )
        return {
            "replaced_period_id": row["period_id"],
            "replaced_version_no": row["version_no"],
            "new_period_ids": [p["period_id"] for p in new_periods],
            "new_version_ids": [p["version_id"] for p in new_periods],
            "corrected_watershed": body.watershed,
            "valid_from": cfrom,
            "valid_to": cto,
        }

    return run_txn(fn)


# ---------------------------------------------------------------------------
# 供数: 按采样时刻关联流域(只见已提交状态); 每条记录带归属时段版本标识
# ---------------------------------------------------------------------------

_FEED_COLUMNS = (
    "observation_id, station_id, observed_at, value, watershed, period_id, "
    "period_version_no, period_version_id"
)


@app.get("/stations/{station_id}/observations", tags=["feed"])
def list_attributed_observations(
    station_id: str,
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = Query(None),
):
    sql = f"""
        SELECT {_FEED_COLUMNS}
        FROM observation_attribution
        WHERE station_id = %s
    """
    params: list = [station_id]
    if from_ is not None:
        sql += " AND observed_at >= %s"
        params.append(from_)
    if to is not None:
        sql += " AND observed_at < %s"
        params.append(to)
    sql += " ORDER BY observed_at"
    pool = get_pool()
    with pool.connection() as conn:
        return conn.execute(sql, params).fetchall()


@app.get("/attributed-observations", tags=["feed"])
def attributed_feed(
    station_id: Optional[str] = None,
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = Query(None),
):
    sql = f"""
        SELECT {_FEED_COLUMNS}
        FROM observation_attribution
        WHERE TRUE
    """
    params: list = []
    if station_id is not None:
        sql += " AND station_id = %s"
        params.append(station_id)
    if from_ is not None:
        sql += " AND observed_at >= %s"
        params.append(from_)
    if to is not None:
        sql += " AND observed_at < %s"
        params.append(to)
    sql += " ORDER BY station_id, observed_at"
    pool = get_pool()
    with pool.connection() as conn:
        return conn.execute(sql, params).fetchall()


# ---------------------------------------------------------------------------
# 追溯: 凭供数收到的版本标识 + UTC 采样时刻, 还原归属变迁链
# ---------------------------------------------------------------------------

@app.get("/trace", response_model=TraceResponse, tags=["trace"])
def trace_attribution(
    version_id: UUID = Query(..., description="供数接口返回的归属时段版本标识 period_version_id"),
    at: AwareDatetime = Query(..., description="UTC 采样时刻(带时区 ISO-8601, 归一化到 UTC)"),
):
    """从 (版本标识, 采样时刻) 出发沿接续关系追溯: 每跳按左闭右开区间
    `[valid_from, valid_to)` 选择包含采样时刻的后继, 返回链上各版的
    时段编号、范围与流域.

    * 源版本不覆盖该时刻 -> 422 明确拒绝; 版本标识不存在 -> 404
    * 链上时段被其他入口或直连 SQL 改动/删除 -> `complete=false`,
      返回已确认的链并在 `break` 中标明断点(事后改回原值也不续接)
    * 整条追溯在单条 SQL 内完成, 只读同一已提交快照
    """
    at_utc = to_utc(at)
    pool = get_pool()
    with pool.connection() as conn:
        rows = conn.execute(
            TRACE_SQL, {"version_id": version_id, "at": at_utc}
        ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"period version {version_id} not found")

    src = rows[0]
    if not (src["valid_from"] <= at_utc and
            (src["valid_to"] is None or at_utc < src["valid_to"])):
        to_str = src["valid_to"].isoformat() if src["valid_to"] is not None else "null(open)"
        raise HTTPException(
            status_code=422,
            detail=f"source version {version_id} covers "
                   f"[{src['valid_from'].isoformat()}, {to_str}), "
                   f"which does not contain {at_utc.isoformat()}",
        )

    tail = rows[-1]
    live_v = tail["live_version_no"]
    complete = live_v == tail["version_no"]

    chain = []
    for i, r in enumerate(rows):
        if i < len(rows) - 1:
            status = "superseded"
        else:
            status = "current" if complete else "last_confirmed"
        chain.append(TraceChainLink(
            period_id=r["period_id"],
            version_no=r["version_no"],
            version_id=r["version_id"],
            watershed=r["watershed"],
            valid_from=r["valid_from"],
            valid_to=r["valid_to"],
            status=status,
        ))

    break_point: Optional[TraceBreak] = None
    if not complete:
        if live_v is None:
            reason = "period_deleted"
            detail = (f"period {tail['period_id']} was deleted without a recorded succession "
                      f"(e.g. changeset delete_period or direct SQL); the chain cannot continue")
        else:
            reason = "period_modified"
            detail = (f"period {tail['period_id']} is now at version {live_v}, but no recorded "
                      f"succession leads from version {tail['version_no']} "
                      f"(modified outside transfer/split/merge/correct, e.g. changeset "
                      f"update_period or direct SQL); the chain cannot continue")
        break_point = TraceBreak(
            after_period_id=tail["period_id"],
            after_version_no=tail["version_no"],
            reason=reason,
            live_version_no=live_v,
            detail=detail,
        )

    return TraceResponse(
        version_id=version_id, at=at_utc, complete=complete,
        chain=chain, break_point=break_point,
    )
