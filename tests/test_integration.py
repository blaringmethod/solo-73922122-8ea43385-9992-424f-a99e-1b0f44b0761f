"""集成测试: 需要运行中的 API 与数据库(默认 localhost:8000 / localhost:5432).

运行: API_URL=... DATABASE_URL=... python -m pytest tests -v
"""
from __future__ import annotations

import os
import threading
import time
import uuid

import httpx
import psycopg
import pytest

API = os.environ.get("API_URL", "http://localhost:8000")
DSN = os.environ.get("DATABASE_URL", "postgresql://watershed:watershed@localhost:5432/watershed")


def new_sid() -> str:
    return "T-" + uuid.uuid4().hex[:12]


@pytest.fixture()
def client():
    with httpx.Client(base_url=API, timeout=30) as c:
        yield c


def mk_station(client: httpx.Client, sid: str) -> None:
    r = client.post("/stations", json={"station_id": sid, "name": "pytest"})
    assert r.status_code == 201, r.text


def changeset(client: httpx.Client, ops: list[dict]) -> httpx.Response:
    return client.post("/changesets", json={"operations": ops})


def periods(client: httpx.Client, sid: str) -> list[dict]:
    r = client.get(f"/stations/{sid}/periods")
    assert r.status_code == 200, r.text
    return r.json()


def observations(client: httpx.Client, sid: str) -> list[dict]:
    r = client.get(f"/stations/{sid}/observations")
    assert r.status_code == 200, r.text
    return r.json()


# ---------------------------------------------------------------------------
# 基础可用性
# ---------------------------------------------------------------------------

def test_health_and_docs(client):
    assert client.get("/health").status_code == 200
    r = client.get("/docs")
    assert r.status_code == 200 and "swagger" in r.text.lower()


def test_basic_attribution(client):
    sid = new_sid()
    mk_station(client, sid)
    r = changeset(client, [
        {"op": "insert_period", "station_id": sid, "watershed": "WS-A",
         "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2022-01-01T00:00:00Z"},
        {"op": "insert_observation", "station_id": sid, "observed_at": "2020-06-01T00:00:00Z", "value": 1.0},
        {"op": "insert_observation", "station_id": sid, "observed_at": "2021-06-01T00:00:00Z", "value": 2.0},
    ])
    assert r.status_code == 201, r.text
    obs = observations(client, sid)
    assert [o["watershed"] for o in obs] == ["WS-A", "WS-A"]


# ---------------------------------------------------------------------------
# 提交时校验 + 整笔回滚
# ---------------------------------------------------------------------------

def test_overlap_rejected_and_whole_changeset_rolled_back(client):
    sid = new_sid()
    mk_station(client, sid)
    r = changeset(client, [
        {"op": "insert_period", "station_id": sid, "watershed": "WS-A",
         "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2022-01-01T00:00:00Z"},
        {"op": "insert_period", "station_id": sid, "watershed": "WS-B",
         "valid_from": "2021-01-01T00:00:00Z", "valid_to": "2023-01-01T00:00:00Z"},
    ])
    assert r.status_code == 409, r.text
    assert "overlap" in r.json()["detail"]
    # 第一笔操作也必须回滚
    assert periods(client, sid) == []


def test_uncovered_observation_rejected(client):
    sid = new_sid()
    mk_station(client, sid)
    r = changeset(client, [
        {"op": "insert_period", "station_id": sid, "watershed": "WS-A",
         "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2021-01-01T00:00:00Z"},
        {"op": "insert_observation", "station_id": sid, "observed_at": "2022-06-01T00:00:00Z", "value": 1.0},
    ])
    assert r.status_code == 409, r.text
    assert "no covering attribution period" in r.json()["detail"]
    assert periods(client, sid) == []
    assert observations(client, sid) == []


def test_mid_transaction_violation_allowed_commit_satisfies(client):
    """事务中途观测无归属是允许的, 提交时满足即可."""
    sid = new_sid()
    mk_station(client, sid)
    r = changeset(client, [
        {"op": "insert_period", "station_id": sid, "watershed": "WS-A",
         "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2022-01-01T00:00:00Z"},
        {"op": "insert_observation", "station_id": sid, "observed_at": "2021-01-01T00:00:00Z", "value": 5.0},
    ])
    assert r.status_code == 201, r.text
    pid = periods(client, sid)[0]["period_id"]
    # 一笔事务: 先删时段(观测暂时无归属)再补回新流域时段
    r = changeset(client, [
        {"op": "delete_period", "period_id": pid},
        {"op": "insert_period", "station_id": sid, "watershed": "WS-B",
         "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2022-01-01T00:00:00Z"},
    ])
    assert r.status_code == 201, r.text
    assert observations(client, sid)[0]["watershed"] == "WS-B"


def test_boundary_semantics_left_closed_right_open(client):
    """左闭右开: 终点时刻的观测不属于该时段."""
    sid = new_sid()
    mk_station(client, sid)
    r = changeset(client, [
        {"op": "insert_period", "station_id": sid, "watershed": "WS-A",
         "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2021-01-01T00:00:00Z"},
        {"op": "insert_observation", "station_id": sid, "observed_at": "2021-01-01T00:00:00Z", "value": 1.0},
    ])
    assert r.status_code == 409, r.text  # 恰在终点, 无归属
    # 相邻两段 [a,b) [b,c) 不算重叠, 边界观测属右段
    r = changeset(client, [
        {"op": "insert_period", "station_id": sid, "watershed": "WS-A",
         "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2021-01-01T00:00:00Z"},
        {"op": "insert_period", "station_id": sid, "watershed": "WS-B",
         "valid_from": "2021-01-01T00:00:00Z", "valid_to": "2022-01-01T00:00:00Z"},
        {"op": "insert_observation", "station_id": sid, "observed_at": "2021-01-01T00:00:00Z", "value": 1.0},
    ])
    assert r.status_code == 201, r.text
    assert observations(client, sid)[0]["watershed"] == "WS-B"


# ---------------------------------------------------------------------------
# 便捷操作
# ---------------------------------------------------------------------------

def test_transfer(client):
    sid = new_sid()
    mk_station(client, sid)
    r = changeset(client, [
        {"op": "insert_period", "station_id": sid, "watershed": "WS-A",
         "valid_from": "2020-01-01T00:00:00Z", "valid_to": None},
        {"op": "insert_observation", "station_id": sid, "observed_at": "2020-06-01T00:00:00Z", "value": 1.0},
        {"op": "insert_observation", "station_id": sid, "observed_at": "2021-06-01T00:00:00Z", "value": 2.0},
    ])
    assert r.status_code == 201, r.text
    r = client.post(f"/stations/{sid}/transfer",
                    json={"at": "2021-01-01T00:00:00Z", "new_watershed": "WS-B"})
    assert r.status_code == 201, r.text
    ps = periods(client, sid)
    assert len(ps) == 2
    assert ps[0]["valid_to"] == "2021-01-01T00:00:00+00:00"
    assert ps[1]["valid_to"] is None
    obs = observations(client, sid)
    assert [o["watershed"] for o in obs] == ["WS-A", "WS-B"]


def test_split_and_merge(client):
    sid = new_sid()
    mk_station(client, sid)
    r = changeset(client, [
        {"op": "insert_period", "station_id": sid, "watershed": "WS-A",
         "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2023-01-01T00:00:00Z"},
    ])
    assert r.status_code == 201, r.text
    pid = periods(client, sid)[0]["period_id"]
    r = client.post(f"/periods/{pid}/split",
                    json={"at": "2021-01-01T00:00:00Z", "right_watershed": "WS-B"})
    assert r.status_code == 201, r.text
    ps = periods(client, sid)
    assert [(p["watershed"]) for p in ps] == ["WS-A", "WS-B"]
    r = client.post(f"/stations/{sid}/merge", json={"at": "2021-01-01T00:00:00Z"})
    assert r.status_code == 201, r.text
    ps = periods(client, sid)
    assert len(ps) == 1 and ps[0]["valid_to"] == "2023-01-01T00:00:00+00:00"


def test_correct_historical(client):
    sid = new_sid()
    mk_station(client, sid)
    r = changeset(client, [
        {"op": "insert_period", "station_id": sid, "watershed": "WS-OLD",
         "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2023-01-01T00:00:00Z"},
        {"op": "insert_observation", "station_id": sid, "observed_at": "2020-06-01T00:00:00Z", "value": 1.0},
        {"op": "insert_observation", "station_id": sid, "observed_at": "2021-06-01T00:00:00Z", "value": 2.0},
        {"op": "insert_observation", "station_id": sid, "observed_at": "2022-06-01T00:00:00Z", "value": 3.0},
    ])
    assert r.status_code == 201, r.text
    r = client.post(f"/stations/{sid}/correct", json={
        "valid_from": "2021-01-01T00:00:00Z",
        "valid_to": "2022-01-01T00:00:00Z",
        "watershed": "WS-NEW",
    })
    assert r.status_code == 201, r.text
    assert len(periods(client, sid)) == 3
    obs = observations(client, sid)
    assert [o["watershed"] for o in obs] == ["WS-OLD", "WS-NEW", "WS-OLD"]


# ---------------------------------------------------------------------------
# 直连 SQL(独立 READ COMMITTED 连接)同样受约束
# ---------------------------------------------------------------------------

def test_direct_sql_commit_time_enforcement():
    sid = new_sid()
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("INSERT INTO stations (station_id) VALUES (%s)", (sid,))

    # 事务中途违规、提交时满足 -> 成功
    with psycopg.connect(DSN) as c:
        c.execute(
            "INSERT INTO attribution_periods (station_id, watershed, valid_from, valid_to)"
            " VALUES (%s, 'WS-A', '2020-01-01 00:00+00', '2022-01-01 00:00+00')", (sid,))
        c.execute(
            "INSERT INTO observations (station_id, observed_at, value)"
            " VALUES (%s, '2021-01-01 00:00+00', 7.7)", (sid,))
        c.execute("DELETE FROM attribution_periods WHERE station_id = %s", (sid,))
        c.execute(
            "INSERT INTO attribution_periods (station_id, watershed, valid_from, valid_to)"
            " VALUES (%s, 'WS-B', '2020-01-01 00:00+00', '2022-01-01 00:00+00')", (sid,))
        c.commit()
    with psycopg.connect(DSN) as c:
        ws = c.execute(
            "SELECT watershed FROM observation_attribution WHERE station_id = %s", (sid,)
        ).fetchone()[0]
        assert ws == "WS-B"

    # 提交时仍违规 -> 整笔回滚
    with psycopg.connect(DSN) as c:
        c.execute("DELETE FROM attribution_periods WHERE station_id = %s", (sid,))
        with pytest.raises(psycopg.errors.CheckViolation):
            c.commit()
    with psycopg.connect(DSN) as c:
        n = c.execute(
            "SELECT count(*) FROM attribution_periods WHERE station_id = %s", (sid,)
        ).fetchone()[0]
        assert n == 1  # 时段仍在

    # 重叠时段 -> 拒绝
    with psycopg.connect(DSN) as c:
        c.execute(
            "INSERT INTO attribution_periods (station_id, watershed, valid_from, valid_to)"
            " VALUES (%s, 'WS-C', '2021-06-01 00:00+00', '2023-01-01 00:00+00')", (sid,))
        with pytest.raises(psycopg.errors.CheckViolation):
            c.commit()


def test_read_committed_visibility(client):
    """供数只见已提交状态: 未提交事务的观测不可见."""
    sid = new_sid()
    mk_station(client, sid)
    holder = psycopg.connect(DSN)  # 默认 READ COMMITTED, 非 autocommit
    try:
        holder.execute(
            "INSERT INTO attribution_periods (station_id, watershed, valid_from, valid_to)"
            " VALUES (%s, 'WS-A', '2020-01-01 00:00+00', '2022-01-01 00:00+00')", (sid,))
        holder.execute(
            "INSERT INTO observations (station_id, observed_at, value)"
            " VALUES (%s, '2021-01-01 00:00+00', 1.0)", (sid,))
        assert observations(client, sid) == []  # 未提交不可见
        holder.commit()
        assert len(observations(client, sid)) == 1
    finally:
        holder.close()


# ---------------------------------------------------------------------------
# 并发: 撤段 vs 补录
# ---------------------------------------------------------------------------

def _delete_period_commit(sid: str, out: dict, key: str, hold_seconds: float = 0.0):
    try:
        with psycopg.connect(DSN) as c:
            c.execute("DELETE FROM attribution_periods WHERE station_id = %s", (sid,))
            if hold_seconds:
                c.execute("SELECT pg_sleep(%s)", (hold_seconds,))
            c.commit()
        out[key] = "ok"
    except Exception as exc:  # noqa: BLE001
        out[key] = f"error: {exc}"


def _insert_observation_commit(sid: str, out: dict, key: str):
    try:
        with psycopg.connect(DSN, autocommit=True) as c:
            c.execute(
                "INSERT INTO observations (station_id, observed_at, value)"
                " VALUES (%s, '2020-06-01 00:00+00', 42.0)", (sid,))
        out[key] = "ok"
    except Exception as exc:  # noqa: BLE001
        out[key] = f"error: {exc}"


def _db_counts(sid: str) -> tuple[int, int]:
    with psycopg.connect(DSN) as c:
        p = c.execute("SELECT count(*) FROM attribution_periods WHERE station_id = %s", (sid,)).fetchone()[0]
        o = c.execute("SELECT count(*) FROM observations WHERE station_id = %s", (sid,)).fetchone()[0]
        return p, o


def test_revoke_then_backfill_rejected_sequential(client):
    """撤段先提交 -> 补录在提交时被拒.

    与并发串行化后的终态等价(锁保证后者看到前者已提交状态),
    但不依赖跨进程锁等待唤醒, 任何环境可跑.
    """
    sid = new_sid()
    mk_station(client, sid)
    r = changeset(client, [
        {"op": "insert_period", "station_id": sid, "watershed": "WS-A",
         "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2021-01-01T00:00:00Z"},
    ])
    assert r.status_code == 201, r.text

    out: dict = {}
    _delete_period_commit(sid, out, "delete")
    assert out["delete"] == "ok", out          # 无观测, 撤段合法
    _insert_observation_commit(sid, out, "insert")
    assert out["insert"].startswith("error"), out
    assert "no covering attribution period" in out["insert"]
    assert _db_counts(sid) == (0, 0)


@pytest.mark.skipif(
    os.environ.get("PG_LOCK_WAIT_OK") != "1",
    reason="需要正常的锁等待/唤醒语义(设 PG_LOCK_WAIT_OK=1 启用); 某些沙箱阻断进程间信号, 锁等待不返回",
)
def test_concurrent_revoke_then_backfill_threaded(client):
    """真并发: 撤段持锁期间补录被阻塞, 撤段提交后补录在提交时被拒."""
    sid = new_sid()
    mk_station(client, sid)
    r = changeset(client, [
        {"op": "insert_period", "station_id": sid, "watershed": "WS-A",
         "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2021-01-01T00:00:00Z"},
    ])
    assert r.status_code == 201, r.text

    out: dict = {}
    t = threading.Thread(target=_delete_period_commit, args=(sid, out, "delete", 2.0))
    t.start()
    time.sleep(0.7)  # 确保删除方先持锁
    _insert_observation_commit(sid, out, "insert")
    t.join(timeout=30)

    assert out["delete"] == "ok", out
    assert out["insert"].startswith("error"), out
    assert "no covering attribution period" in out["insert"]
    assert _db_counts(sid) == (0, 0)


def test_concurrent_backfill_then_revoke_rejected(client):
    """补录先提交 -> 撤段在提交时被拒; 归属保持完好."""
    sid = new_sid()
    mk_station(client, sid)
    r = changeset(client, [
        {"op": "insert_period", "station_id": sid, "watershed": "WS-A",
         "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2021-01-01T00:00:00Z"},
    ])
    assert r.status_code == 201, r.text

    out: dict = {}
    _insert_observation_commit(sid, out, "insert")
    assert out["insert"] == "ok", out
    _delete_period_commit(sid, out, "delete")
    assert out["delete"].startswith("error"), out
    assert "no covering attribution period" in out["delete"]
    assert _db_counts(sid) == (1, 1)


def test_unrelated_stations_both_commit(client):
    """无关站点互不阻塞: 两个并发变更集都能提交."""
    sid1, sid2 = new_sid(), new_sid()
    mk_station(client, sid1)
    mk_station(client, sid2)
    results: dict = {}

    def work(sid: str, key: str):
        c = httpx.Client(base_url=API, timeout=30)
        try:
            r = changeset(c, [
                {"op": "insert_period", "station_id": sid, "watershed": "WS-X",
                 "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2021-01-01T00:00:00Z"},
                {"op": "insert_observation", "station_id": sid,
                 "observed_at": "2020-06-01T00:00:00Z", "value": 1.0},
            ])
            results[key] = r.status_code
        finally:
            c.close()

    t1 = threading.Thread(target=work, args=(sid1, "s1"))
    t2 = threading.Thread(target=work, args=(sid2, "s2"))
    t1.start(); t2.start(); t1.join(timeout=30); t2.join(timeout=30)
    assert results == {"s1": 201, "s2": 201}


# ---------------------------------------------------------------------------
# 归属时段版本与追溯
# ---------------------------------------------------------------------------

def trace(client: httpx.Client, version_id: str, at: str) -> httpx.Response:
    return client.get("/trace", params={"version_id": version_id, "at": at})


def mk_station_with_period(client: httpx.Client, sid: str, ws: str = "WS-A",
                           vf: str = "2020-01-01T00:00:00Z",
                           vt: str = "2023-01-01T00:00:00Z") -> None:
    mk_station(client, sid)
    r = changeset(client, [
        {"op": "insert_period", "station_id": sid, "watershed": ws,
         "valid_from": vf, "valid_to": vt},
    ])
    assert r.status_code == 201, r.text


def only_period(client: httpx.Client, sid: str) -> dict:
    ps = periods(client, sid)
    assert len(ps) == 1, ps
    return ps[0]


def test_feeds_carry_period_version_identity(client):
    """两个供数接口都返回归属时段版本标识, 且与时段当前版一致."""
    sid = new_sid()
    mk_station_with_period(client, sid)
    r = changeset(client, [
        {"op": "insert_observation", "station_id": sid,
         "observed_at": "2020-06-01T00:00:00Z", "value": 1.0},
    ])
    assert r.status_code == 201, r.text

    p = only_period(client, sid)
    assert p["version_no"] == 1
    for url in (f"/stations/{sid}/observations", f"/attributed-observations?station_id={sid}"):
        resp = client.get(url)
        assert resp.status_code == 200, resp.text
        obs = resp.json()
        assert len(obs) == 1
        assert obs[0]["period_version_id"] == p["version_id"]
        assert obs[0]["period_version_no"] == 1


def test_split_generates_new_versions_and_trace(client):
    """拆分: 沿用原编号的一侧也生成新版本; 凭拆分前收到的版本标识可追溯两半."""
    sid = new_sid()
    mk_station_with_period(client, sid)
    r = changeset(client, [
        {"op": "insert_observation", "station_id": sid,
         "observed_at": "2020-06-01T00:00:00Z", "value": 1.0},
        {"op": "insert_observation", "station_id": sid,
         "observed_at": "2022-06-01T00:00:00Z", "value": 2.0},
    ])
    assert r.status_code == 201, r.text
    p0 = only_period(client, sid)
    vid0 = p0["version_id"]  # 下游拆分前收到的版本标识

    r = client.post(f"/periods/{p0['period_id']}/split",
                    json={"at": "2021-01-01T00:00:00Z", "right_watershed": "WS-B"})
    assert r.status_code == 201, r.text
    body = r.json()
    # 沿用原编号的一侧也生成了新版本
    assert body["left_period_id"] == p0["period_id"]
    assert body["left_version_no"] == 2
    assert body["left_version_id"] != vid0
    assert body["right_period_id"] != p0["period_id"]
    assert body["right_version_no"] == 1

    # 拆分后两条观测分属不同版本
    obs = observations(client, sid)
    assert obs[0]["period_version_id"] == body["left_version_id"]
    assert obs[1]["period_version_id"] == body["right_version_id"]

    # 追溯左半时刻: 链 = [旧版, 左半新版], 完整
    r = trace(client, vid0, "2020-06-01T00:00:00Z")
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["complete"] is True and d["break"] is None
    assert [(l["period_id"], l["version_no"], l["watershed"], l["status"]) for l in d["chain"]] == [
        (p0["period_id"], 1, "WS-A", "superseded"),
        (p0["period_id"], 2, "WS-A", "current"),
    ]
    assert d["chain"][0]["valid_to"] == "2023-01-01T00:00:00Z"
    assert d["chain"][1]["valid_to"] == "2021-01-01T00:00:00Z"

    # 追溯右半时刻: 链 = [旧版, 右半新版 WS-B], 完整
    r = trace(client, vid0, "2022-06-01T00:00:00Z")
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["complete"] is True
    assert [(l["period_id"], l["version_no"], l["watershed"], l["status"]) for l in d["chain"]] == [
        (p0["period_id"], 1, "WS-A", "superseded"),
        (body["right_period_id"], 1, "WS-B", "current"),
    ]

    # 源版本即当前版时, 链长为 1
    r = trace(client, body["right_version_id"], "2022-06-01T00:00:00Z")
    assert r.status_code == 200 and r.json()["complete"] is True
    assert len(r.json()["chain"]) == 1


def test_trace_rejects_uncovered_moment_and_unknown_version(client):
    """源版本不覆盖采样时刻 -> 422; 版本标识不存在 -> 404. 左闭右开边界."""
    sid = new_sid()
    mk_station_with_period(client, sid)
    p = only_period(client, sid)
    vid = p["version_id"]

    # 早于 valid_from
    r = trace(client, vid, "2019-12-31T23:59:59Z")
    assert r.status_code == 422 and "does not contain" in r.json()["detail"]
    # 恰在 valid_to (右开, 不覆盖)
    r = trace(client, vid, "2023-01-01T00:00:00Z")
    assert r.status_code == 422 and "does not contain" in r.json()["detail"]
    # 恰在 valid_from (左闭, 覆盖)
    r = trace(client, vid, "2020-01-01T00:00:00Z")
    assert r.status_code == 200 and r.json()["complete"] is True
    # 未知版本标识
    r = trace(client, "00000000-0000-0000-0000-000000000000", "2020-06-01T00:00:00Z")
    assert r.status_code == 404


def test_correct_then_trace(client):
    """历史更正: 凭更正前的版本标识追溯, 链经过更正段; 区间内外各归其段."""
    sid = new_sid()
    mk_station_with_period(client, sid, ws="WS-OLD")
    p0 = only_period(client, sid)
    vid0 = p0["version_id"]

    r = client.post(f"/stations/{sid}/correct", json={
        "valid_from": "2021-01-01T00:00:00Z",
        "valid_to": "2022-01-01T00:00:00Z",
        "watershed": "WS-NEW",
    })
    assert r.status_code == 201, r.text
    new_ids = r.json()["new_period_ids"]
    assert len(new_ids) == 3

    # 更正区间内 -> 中段 WS-NEW
    d = trace(client, vid0, "2021-06-01T00:00:00Z").json()
    assert d["complete"] is True
    assert [(l["period_id"], l["watershed"], l["status"]) for l in d["chain"]] == [
        (p0["period_id"], "WS-OLD", "superseded"),
        (new_ids[1], "WS-NEW", "current"),
    ]
    # 更正区间外 -> 左段 WS-OLD
    d = trace(client, vid0, "2020-06-01T00:00:00Z").json()
    assert d["complete"] is True
    assert [(l["period_id"], l["watershed"]) for l in d["chain"]] == [
        (p0["period_id"], "WS-OLD"), (new_ids[0], "WS-OLD"),
    ]
    # 右段
    d = trace(client, vid0, "2022-06-01T00:00:00Z").json()
    assert d["complete"] is True
    assert [(l["period_id"], l["watershed"]) for l in d["chain"]] == [
        (p0["period_id"], "WS-OLD"), (new_ids[2], "WS-OLD"),
    ]


def test_transfer_then_trace(client):
    """转属: 凭转属前的版本标识追溯, 跨过转属时刻归属变为新流域."""
    sid = new_sid()
    mk_station_with_period(client, sid, vt=None)
    p0 = only_period(client, sid)
    vid0 = p0["version_id"]

    r = client.post(f"/stations/{sid}/transfer",
                    json={"at": "2021-01-01T00:00:00Z", "new_watershed": "WS-B"})
    assert r.status_code == 201, r.text
    new_pid = r.json()["new_period_id"]

    d = trace(client, vid0, "2020-06-01T00:00:00Z").json()
    assert d["complete"] is True
    assert [(l["period_id"], l["version_no"], l["watershed"]) for l in d["chain"]] == [
        (p0["period_id"], 1, "WS-A"), (p0["period_id"], 2, "WS-A"),
    ]
    d = trace(client, vid0, "2021-06-01T00:00:00Z").json()
    assert d["complete"] is True
    assert [(l["period_id"], l["version_no"], l["watershed"]) for l in d["chain"]] == [
        (p0["period_id"], 1, "WS-A"), (new_pid, 1, "WS-B"),
    ]


def test_merge_then_trace_multi_hop(client):
    """合并: 从原始版出发经拆分右段跳到合并版, 多跳链完整."""
    sid = new_sid()
    mk_station_with_period(client, sid)
    p0 = only_period(client, sid)
    vid0 = p0["version_id"]
    r = client.post(f"/periods/{p0['period_id']}/split",
                    json={"at": "2021-01-01T00:00:00Z", "right_watershed": "WS-B"})
    assert r.status_code == 201, r.text
    right_pid = r.json()["right_period_id"]
    r = client.post(f"/stations/{sid}/merge", json={"at": "2021-01-01T00:00:00Z"})
    assert r.status_code == 201, r.text

    d = trace(client, vid0, "2022-06-01T00:00:00Z").json()
    assert d["complete"] is True
    assert [(l["period_id"], l["version_no"], l["watershed"], l["status"]) for l in d["chain"]] == [
        (p0["period_id"], 1, "WS-A", "superseded"),
        (right_pid, 1, "WS-B", "superseded"),
        (p0["period_id"], 3, "WS-A", "current"),
    ]


def test_direct_sql_modify_breaks_chain_and_no_reconnect(client):
    """直连 SQL 改动链上时段 -> 断点 period_modified; 改回原值也不续接."""
    sid = new_sid()
    mk_station_with_period(client, sid)
    p0 = only_period(client, sid)
    vid0 = p0["version_id"]
    r = client.post(f"/periods/{p0['period_id']}/split",
                    json={"at": "2021-01-01T00:00:00Z", "right_watershed": "WS-B"})
    assert r.status_code == 201, r.text
    right_pid = r.json()["right_period_id"]
    assert trace(client, vid0, "2022-06-01T00:00:00Z").json()["complete"] is True

    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("UPDATE attribution_periods SET watershed='WS-X' WHERE period_id=%s", (right_pid,))
    d = trace(client, vid0, "2022-06-01T00:00:00Z").json()
    assert d["complete"] is False
    assert d["break"]["reason"] == "period_modified"
    assert d["break"]["after_period_id"] == right_pid
    assert d["break"]["after_version_no"] == 1
    assert d["break"]["live_version_no"] == 2
    # 已确认的链仍然返回, 链尾标记 last_confirmed
    assert [(l["period_id"], l["version_no"], l["status"]) for l in d["chain"]] == [
        (p0["period_id"], 1, "superseded"),
        (right_pid, 1, "last_confirmed"),
    ]

    # 改回原值: 版本号继续递增, 链不续接
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("UPDATE attribution_periods SET watershed='WS-B' WHERE period_id=%s", (right_pid,))
    d = trace(client, vid0, "2022-06-01T00:00:00Z").json()
    assert d["complete"] is False
    assert d["break"]["reason"] == "period_modified"
    assert d["break"]["live_version_no"] == 3


def test_changeset_update_breaks_chain(client):
    """其他入口(变更集 update_period)改动链上时段 -> 断点 period_modified."""
    sid = new_sid()
    mk_station_with_period(client, sid)
    p0 = only_period(client, sid)
    vid0 = p0["version_id"]

    r = changeset(client, [
        {"op": "update_period", "period_id": p0["period_id"], "watershed": "WS-Z"},
    ])
    assert r.status_code == 201, r.text
    d = trace(client, vid0, "2020-06-01T00:00:00Z").json()
    assert d["complete"] is False
    assert d["break"]["reason"] == "period_modified"
    assert d["break"]["live_version_no"] == 2
    assert len(d["chain"]) == 1 and d["chain"][0]["status"] == "last_confirmed"


def test_direct_sql_delete_breaks_chain(client):
    """直连 SQL 删除链上时段 -> 断点 period_deleted."""
    sid = new_sid()
    mk_station_with_period(client, sid)  # 无观测, 删除不违反覆盖约束
    p0 = only_period(client, sid)
    vid0 = p0["version_id"]
    r = client.post(f"/periods/{p0['period_id']}/split",
                    json={"at": "2021-01-01T00:00:00Z", "right_watershed": "WS-B"})
    assert r.status_code == 201, r.text
    right_pid = r.json()["right_period_id"]

    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("DELETE FROM attribution_periods WHERE period_id=%s", (right_pid,))
    d = trace(client, vid0, "2022-06-01T00:00:00Z").json()
    assert d["complete"] is False
    assert d["break"]["reason"] == "period_deleted"
    assert d["break"]["live_version_no"] is None
    assert [(l["period_id"], l["status"]) for l in d["chain"]] == [
        (p0["period_id"], "superseded"), (right_pid, "last_confirmed"),
    ]
    # 左半时刻的链不受影响
    assert trace(client, vid0, "2020-06-01T00:00:00Z").json()["complete"] is True


def test_trace_reads_only_committed_state(client):
    """一次追溯只读同一已提交状态: 未提交的改动对追溯不可见."""
    sid = new_sid()
    mk_station_with_period(client, sid)
    p0 = only_period(client, sid)
    vid0 = p0["version_id"]

    holder = psycopg.connect(DSN)  # 默认 READ COMMITTED, 非 autocommit
    try:
        holder.execute("UPDATE attribution_periods SET watershed='WS-SNEAKY' "
                       "WHERE period_id=%s", (p0["period_id"],))
        # 未提交: 追溯仍看到已提交的完整链
        d = trace(client, vid0, "2020-06-01T00:00:00Z").json()
        assert d["complete"] is True
        assert d["chain"][0]["watershed"] == "WS-A"
        holder.commit()
    finally:
        holder.close()
    # 提交后: 断点可见
    d = trace(client, vid0, "2020-06-01T00:00:00Z").json()
    assert d["complete"] is False
    assert d["break"]["reason"] == "period_modified"


def test_rejected_split_leaves_no_versions_or_succession(client):
    """拆分被参数校验拒绝(422) -> 不产生任何新版本/接续, 原时段保持 v1."""
    sid = new_sid()
    mk_station_with_period(client, sid)
    p0 = only_period(client, sid)

    r = client.post(f"/periods/{p0['period_id']}/split",
                    json={"at": "2019-12-31T00:00:00Z"})  # 不在时段内
    assert r.status_code == 422, r.text

    p = only_period(client, sid)
    assert p["version_no"] == 1 and p["version_id"] == p0["version_id"]
    with psycopg.connect(DSN) as c:
        n_succ = c.execute(
            "SELECT count(*) FROM period_succession WHERE pred_period_id=%s", (p0["period_id"],)
        ).fetchone()[0]
        n_ver = c.execute(
            "SELECT count(*) FROM period_versions WHERE period_id=%s", (p0["period_id"],)
        ).fetchone()[0]
    assert n_succ == 0
    assert n_ver == 1


def test_succession_rolls_back_with_period_changes(client):
    """接续关系与时段变更同一事务: 回滚则两者都不留痕."""
    sid = new_sid()
    mk_station_with_period(client, sid)
    p0 = only_period(client, sid)
    with psycopg.connect(DSN) as c:
        before = c.execute(
            "SELECT (SELECT count(*) FROM period_versions), (SELECT count(*) FROM period_succession)"
        ).fetchone()

    conn = psycopg.connect(DSN)
    try:
        conn.execute("UPDATE attribution_periods SET valid_to='2021-01-01 00:00+00' "
                     "WHERE period_id=%s", (p0["period_id"],))
        conn.execute("INSERT INTO attribution_periods (station_id, watershed, valid_from, valid_to)"
                     " VALUES (%s, 'WS-B', '2021-01-01 00:00+00', '2023-01-01 00:00+00')", (sid,))
        conn.execute(
            "INSERT INTO period_succession (pred_period_id, pred_version_no,"
            " succ_period_id, succ_version_no) VALUES (%s, 1, %s, 2)",
            (p0["period_id"], p0["period_id"]),
        )
        conn.rollback()
    finally:
        conn.close()

    with psycopg.connect(DSN) as c:
        after = c.execute(
            "SELECT (SELECT count(*) FROM period_versions), (SELECT count(*) FROM period_succession)"
        ).fetchone()
        n = c.execute("SELECT count(*) FROM attribution_periods WHERE station_id=%s",
                      (sid,)).fetchone()[0]
    assert after == before
    assert n == 1


def test_audit_tables_append_only(client):
    """版本存档与接续关系表 append-only: UPDATE/DELETE/TRUNCATE 均被拒."""
    sid = new_sid()
    mk_station_with_period(client, sid)
    with psycopg.connect(DSN, autocommit=True) as c:
        with pytest.raises(psycopg.errors.RaiseException):
            c.execute("UPDATE period_versions SET watershed='X'")
        with pytest.raises(psycopg.errors.RaiseException):
            c.execute("DELETE FROM period_succession")
        with pytest.raises(psycopg.errors.RaiseException):
            c.execute("TRUNCATE period_succession")
