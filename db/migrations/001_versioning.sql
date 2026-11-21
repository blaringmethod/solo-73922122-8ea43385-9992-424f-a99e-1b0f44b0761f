-- ============================================================================
-- 迁移 001: 归属时段版本与追溯
--
--   适用于由旧版 init.sql 建好的存量库; 全新部署无需执行(init.sql 已包含).
--   用法: psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/001_versioning.sql
--
--   * attribution_periods 增加 version_no / version_id
--   * 新建 period_versions(版本存档) 与 period_succession(接续关系), 均 append-only
--   * 为存量时段补登 v1 存档; 之后任何写路径的 INSERT/UPDATE 自动记录新版
--   * 重建供数视图 observation_attribution, 增加 period_version_* 列
-- ============================================================================
BEGIN;

ALTER TABLE attribution_periods
    ADD COLUMN version_no integer NOT NULL DEFAULT 1,
    ADD COLUMN version_id uuid NOT NULL DEFAULT gen_random_uuid();

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

-- 为存量时段补登 v1 存档(当前在线版)
INSERT INTO period_versions
    (version_id, period_id, version_no, station_id, watershed, valid_from, valid_to)
SELECT version_id, period_id, version_no, station_id, watershed, valid_from, valid_to
FROM attribution_periods;

-- 重建供数视图(新增 period_version_no / period_version_id 两列)
DROP VIEW observation_attribution;
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
