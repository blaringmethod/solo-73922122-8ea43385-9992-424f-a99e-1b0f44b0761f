-- ============================================================================
-- 流域归属与观测完整性约束
--
--   * 归属时段: [valid_from, valid_to) 左闭右开, valid_to 可空(=至今有效)
--   * 规则1: 同一站点任意两个时段不得重叠
--   * 规则2: 每条观测在采样时刻必须恰好落入本站的某一个归属时段
--            (不重叠保证"至多一段", 覆盖检查保证"至少一段", 合起来即"恰好一段")
--   * 约束以 DEFERRABLE INITIALLY DEFERRED 触发器在提交时统一校验:
--       事务中途允许暂时违规(便于拆分/合并/更正时段并补改观测),
--       提交时必须全部满足, 否则整笔回滚
--   * 修改时段/观测前按站点取行锁(SELECT ... FOR UPDATE), 使同站并发写
--     串行化; 不同站持不同锁行, 互不阻塞
--   * 约束保存在数据库目录中, 对 API 与任意直连 SQL 会话(READ COMMITTED)
--     同样生效, 重启后仍然有效
-- ============================================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE stations (
    station_id  text PRIMARY KEY,
    name        text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE attribution_periods (
    period_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id  text NOT NULL REFERENCES stations (station_id),
    watershed   text NOT NULL,
    valid_from  timestamptz NOT NULL,
    valid_to    timestamptz,                       -- NULL = 至今有效
    version_no  integer NOT NULL DEFAULT 1,        -- 每次 UPDATE 由触发器 +1
    version_id  uuid NOT NULL DEFAULT gen_random_uuid(),  -- 每版一个标识, 供数/追溯用
    CONSTRAINT period_bounds_ok CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE TABLE observations (
    observation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id  text NOT NULL REFERENCES stations (station_id),
    observed_at timestamptz NOT NULL,
    value       double precision NOT NULL
);

CREATE INDEX periods_station_range_idx
    ON attribution_periods USING gist (station_id, tstzrange(valid_from, valid_to, '[)'));
CREATE INDEX observations_station_time_idx
    ON observations (station_id, observed_at);

-- ----------------------------------------------------------------------------
-- 归属时段版本与接续关系(供下游追溯)
--
--   * period_versions: 每个时段每一版的不可变存档(append-only)。行触发器在
--     INSERT/UPDATE 时记录新版 —— 任何写路径(API/变更集/直连 SQL)都会留下版本;
--     存档与时段变更处于同一事务, 随之一起提交或回滚
--   * period_succession: 接续关系(append-only), 仅由 转属/拆分/合并/历史更正
--     端点在与时段变更相同的事务内写入。变更集裸 update/delete_period 或直连
--     SQL 改动时段不会产生接续 —— 追溯链在该版之后断开(断点), 即使事后改回
--     原值也不会续接(版本号只会单调递增, 不会回到旧版)
--   * 两张审计表禁止 UPDATE/DELETE/TRUNCATE
-- ----------------------------------------------------------------------------
CREATE TABLE period_versions (
    version_id  uuid PRIMARY KEY,
    period_id   bigint NOT NULL,        -- 逻辑关联, 不加外键: 时段删除后历史仍保留
    version_no  integer NOT NULL,
    station_id  text NOT NULL,
    watershed   text NOT NULL,
    valid_from  timestamptz NOT NULL,
    valid_to    timestamptz,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT period_versions_unique UNIQUE (period_id, version_no)
);

CREATE TABLE period_succession (
    pred_period_id  bigint NOT NULL,
    pred_version_no integer NOT NULL,
    succ_period_id  bigint NOT NULL,
    succ_version_no integer NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT period_succession_pk PRIMARY KEY
        (pred_period_id, pred_version_no, succ_period_id, succ_version_no),
    CONSTRAINT period_succession_pred_fk FOREIGN KEY (pred_period_id, pred_version_no)
        REFERENCES period_versions (period_id, version_no),
    CONSTRAINT period_succession_succ_fk FOREIGN KEY (succ_period_id, succ_version_no)
        REFERENCES period_versions (period_id, version_no)
);

CREATE INDEX period_succession_pred_idx
    ON period_succession (pred_period_id, pred_version_no);

-- UPDATE 时段: 版本号 +1 并换发 version_id (任何写路径都经过此触发器)
CREATE OR REPLACE FUNCTION bump_period_version() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.version_no := OLD.version_no + 1;
    NEW.version_id := gen_random_uuid();
    RETURN NEW;
END;
$$;

CREATE TRIGGER periods_bump_version
    BEFORE UPDATE ON attribution_periods
    FOR EACH ROW EXECUTE FUNCTION bump_period_version();

-- INSERT/UPDATE 之后把新版写入存档(与变更同事务)
CREATE OR REPLACE FUNCTION archive_period_version() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO period_versions
        (version_id, period_id, version_no, station_id, watershed, valid_from, valid_to)
    VALUES
        (NEW.version_id, NEW.period_id, NEW.version_no, NEW.station_id, NEW.watershed,
         NEW.valid_from, NEW.valid_to);
    RETURN NULL;
END;
$$;

CREATE TRIGGER periods_archive_version
    AFTER INSERT OR UPDATE ON attribution_periods
    FOR EACH ROW EXECUTE FUNCTION archive_period_version();

-- 版本存档与接续关系为 append-only 审计数据
CREATE OR REPLACE FUNCTION forbid_audit_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only: % is not allowed', TG_TABLE_NAME, TG_OP;
END;
$$;

CREATE TRIGGER versions_no_mutation
    BEFORE UPDATE OR DELETE ON period_versions
    FOR EACH ROW EXECUTE FUNCTION forbid_audit_mutation();
CREATE TRIGGER versions_no_truncate
    BEFORE TRUNCATE ON period_versions
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_audit_mutation();
CREATE TRIGGER succession_no_mutation
    BEFORE UPDATE OR DELETE ON period_succession
    FOR EACH ROW EXECUTE FUNCTION forbid_audit_mutation();
CREATE TRIGGER succession_no_truncate
    BEFORE TRUNCATE ON period_succession
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_audit_mutation();

-- ----------------------------------------------------------------------------
-- 单站完整性强检: 无重叠时段 + 每条观测都有归属
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION check_station_integrity(p_station_id text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_bad bigint;
BEGIN
    SELECT a.period_id INTO v_bad
    FROM attribution_periods a
    JOIN attribution_periods b
      ON b.station_id = a.station_id
     AND a.period_id < b.period_id
     AND tstzrange(a.valid_from, a.valid_to, '[)') && tstzrange(b.valid_from, b.valid_to, '[)')
    WHERE a.station_id = p_station_id
    LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'station %: attribution periods overlap (period_id=%)', p_station_id, v_bad
            USING ERRCODE = '23514';
    END IF;

    SELECT o.observation_id INTO v_bad
    FROM observations o
    WHERE o.station_id = p_station_id
      AND NOT EXISTS (
            SELECT 1
            FROM attribution_periods p
            WHERE p.station_id = o.station_id
              AND tstzrange(p.valid_from, p.valid_to, '[)') @> o.observed_at
          )
    LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'station %: observation % has no covering attribution period', p_station_id, v_bad
            USING ERRCODE = '23514';
    END IF;
END;
$$;

-- ----------------------------------------------------------------------------
-- BEFORE 行触发器: 按站点取排他行锁(升序加锁降低死锁概率),
-- 并使该站本事务内的"已检"标记失效(支持 SET CONSTRAINTS ... IMMEDIATE 后继续改)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION before_change_station() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_stations text[];
    s text;
BEGIN
    IF to_regclass('pg_temp.pending_station_checks') IS NULL THEN
        CREATE TEMP TABLE pending_station_checks (station_id text PRIMARY KEY) ON COMMIT DELETE ROWS;
    END IF;

    IF TG_OP = 'DELETE' THEN
        v_stations := ARRAY[OLD.station_id];
    ELSIF TG_OP = 'INSERT' THEN
        v_stations := ARRAY[NEW.station_id];
    ELSIF NEW.station_id IS DISTINCT FROM OLD.station_id THEN
        v_stations := ARRAY[OLD.station_id, NEW.station_id];
    ELSE
        v_stations := ARRAY[NEW.station_id];
    END IF;

    SELECT array_agg(x ORDER BY x) INTO v_stations FROM unnest(v_stations) AS x;

    FOREACH s IN ARRAY v_stations LOOP
        PERFORM 1 FROM stations WHERE station_id = s FOR UPDATE;
        DELETE FROM pg_temp.pending_station_checks WHERE station_id = s;
    END LOOP;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$;

-- ----------------------------------------------------------------------------
-- 提交时(DEFERRABLE INITIALLY DEFERRED)对受影响站点跑强检; 同一轮触发内去重
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION deferred_station_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_stations text[];
    s text;
BEGIN
    IF to_regclass('pg_temp.pending_station_checks') IS NULL THEN
        CREATE TEMP TABLE pending_station_checks (station_id text PRIMARY KEY) ON COMMIT DELETE ROWS;
    END IF;

    IF TG_OP = 'DELETE' THEN
        v_stations := ARRAY[OLD.station_id];
    ELSIF TG_OP = 'INSERT' THEN
        v_stations := ARRAY[NEW.station_id];
    ELSIF NEW.station_id IS DISTINCT FROM OLD.station_id THEN
        v_stations := ARRAY[OLD.station_id, NEW.station_id];
    ELSE
        v_stations := ARRAY[NEW.station_id];
    END IF;

    SELECT array_agg(x ORDER BY x) INTO v_stations FROM unnest(v_stations) AS x;

    FOREACH s IN ARRAY v_stations LOOP
        INSERT INTO pg_temp.pending_station_checks (station_id) VALUES (s) ON CONFLICT DO NOTHING;
        IF FOUND THEN
            PERFORM check_station_integrity(s);
        END IF;
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER periods_lock_station
    BEFORE INSERT OR UPDATE OR DELETE ON attribution_periods
    FOR EACH ROW EXECUTE FUNCTION before_change_station();
CREATE TRIGGER observations_lock_station
    BEFORE INSERT OR UPDATE OR DELETE ON observations
    FOR EACH ROW EXECUTE FUNCTION before_change_station();

CREATE CONSTRAINT TRIGGER periods_check_at_commit
    AFTER INSERT OR UPDATE OR DELETE ON attribution_periods
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION deferred_station_check();
CREATE CONSTRAINT TRIGGER observations_check_at_commit
    AFTER INSERT OR UPDATE OR DELETE ON observations
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION deferred_station_check();

-- ----------------------------------------------------------------------------
-- 禁止 TRUNCATE (它绕过行触发器, 会留下无归属观测)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION forbid_truncate() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'TRUNCATE is not allowed on %: integrity triggers must run per row', TG_TABLE_NAME;
END;
$$;

CREATE TRIGGER periods_no_truncate
    BEFORE TRUNCATE ON attribution_periods
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_truncate();
CREATE TRIGGER observations_no_truncate
    BEFORE TRUNCATE ON observations
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_truncate();

-- ----------------------------------------------------------------------------
-- 供数视图: 按采样时刻关联流域 (READ COMMITTED 下只见完整已提交状态);
-- 附带归属时段版本标识(period_version_id), 下游凭它调用 /trace 追溯归属变迁
-- ----------------------------------------------------------------------------
CREATE VIEW observation_attribution AS
SELECT o.observation_id,
       o.station_id,
       o.observed_at,
       o.value,
       p.period_id,
       p.version_no AS period_version_no,
       p.version_id AS period_version_id,
       p.watershed,
       p.valid_from AS period_valid_from,
       p.valid_to   AS period_valid_to
FROM observations o
JOIN attribution_periods p
  ON p.station_id = o.station_id
 AND tstzrange(p.valid_from, p.valid_to, '[)') @> o.observed_at;

COMMIT;
