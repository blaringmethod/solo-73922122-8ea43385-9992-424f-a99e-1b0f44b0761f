#!/usr/bin/env bash
# 示例5: 拆分 + 追溯 —— 下游凭供数收到的版本标识, 查清采样时刻的归属后来怎样变了
#   1) 供数拿到 period_version_id
#   2) 拆分(沿用原编号的一侧也生成新版本)后, 凭旧版本标识追溯两半
#   3) 历史更正后, 凭同一标识追溯更正段
#   4) 直连 SQL 改动链上时段 -> 追溯返回已确认的链并标明断点; 改回原值也不续接
set -euo pipefail

API=${API_URL:-http://localhost:8000}
SID=${SID:-ST-TRACE-$(date +%Y%m%d%H%M%S)}
DSN=${DATABASE_URL:-postgresql://watershed:watershed@localhost:5432/watershed}
if [ -n "${PSQL_CMD:-}" ]; then
  :
elif command -v docker >/dev/null 2>&1; then
  PSQL_CMD="docker compose exec -T db psql $DSN"
else
  PSQL_CMD="psql $DSN"
fi

echo "== 站点: $SID =="

echo "-- 1. 建站 + 一段 [2020,2023) 归属 WS-A 的时段 + 两条观测"
curl -sS -X POST "$API/stations" -H 'Content-Type: application/json' \
  -d "{\"station_id\":\"$SID\",\"name\":\"追溯示例站\"}" | jq .
curl -sS -X POST "$API/changesets" -H 'Content-Type: application/json' -d "{
  \"operations\": [
    {\"op\":\"insert_period\",\"station_id\":\"$SID\",\"watershed\":\"WS-A\",
     \"valid_from\":\"2020-01-01T00:00:00Z\",\"valid_to\":\"2023-01-01T00:00:00Z\"},
    {\"op\":\"insert_observation\",\"station_id\":\"$SID\",\"observed_at\":\"2020-06-01T00:00:00Z\",\"value\":1.0},
    {\"op\":\"insert_observation\",\"station_id\":\"$SID\",\"observed_at\":\"2022-06-01T00:00:00Z\",\"value\":2.0}
  ]
}" | jq .

echo "-- 2. 供数: 每条观测带归属时段版本标识 period_version_id (下游存档)"
curl -sS "$API/stations/$SID/observations" | jq .
VID=$(curl -sS "$API/stations/$SID/observations" | jq -r '.[0].period_version_id')
PID=$(curl -sS "$API/stations/$SID/periods" | jq -r '.[0].period_id')
echo "   下游存档的版本标识: $VID (period_id=$PID)"

echo "-- 3. 2021-01-01 拆分: 左半沿用原编号但生成新版本, 右半改属 WS-B"
curl -sS -X POST "$API/periods/$PID/split" -H 'Content-Type: application/json' \
  -d '{"at":"2021-01-01T00:00:00Z","right_watershed":"WS-B"}' | jq .

echo "-- 4. 追溯: 凭拆分前收到的版本标识 + 采样时刻"
echo "   4a. 采样时刻 2020-06-01 (左半): 链 = [原v1, 左半v2], 完整"
curl -sS "$API/trace?version_id=$VID&at=2020-06-01T00:00:00Z" | jq .
echo "   4b. 采样时刻 2022-06-01 (右半): 链 = [原v1, 右半v1 WS-B], 完整"
curl -sS "$API/trace?version_id=$VID&at=2022-06-01T00:00:00Z" | jq .

echo "-- 5. 历史更正: 2022-01-01..2023-01-01 实际应属 WS-C"
curl -sS -X POST "$API/stations/$SID/correct" -H 'Content-Type: application/json' -d '{
  "valid_from":"2022-01-01T00:00:00Z",
  "valid_to":"2023-01-01T00:00:00Z",
  "watershed":"WS-C"
}' | jq .
echo "   再追溯 2022-06-01: 链 = [原v1, 右半v1, 更正段 WS-C], 完整"
curl -sS "$API/trace?version_id=$VID&at=2022-06-01T00:00:00Z" | jq .

echo "-- 6. 直连 SQL 改动链上时段(绕过 API): 把更正段流域改成 WS-X"
$PSQL_CMD -c "UPDATE attribution_periods SET watershed='WS-X' WHERE station_id='$SID' AND watershed='WS-C';"
echo "   再追溯 2022-06-01: complete=false, 返回已确认的链并标明断点 period_modified"
curl -sS "$API/trace?version_id=$VID&at=2022-06-01T00:00:00Z" | jq .

echo "-- 7. 直连 SQL 改回原值 WS-C: 版本号继续递增, 链不续接"
$PSQL_CMD -c "UPDATE attribution_periods SET watershed='WS-C' WHERE station_id='$SID' AND watershed='WS-X';"
curl -sS "$API/trace?version_id=$VID&at=2022-06-01T00:00:00Z" | jq '{complete: .complete, break: .break}'

echo "-- 8. 拒绝路径"
echo "   8a. 源版本不覆盖的时刻 -> 422"
curl -sS -w '\nHTTP %{http_code}\n' "$API/trace?version_id=$VID&at=2019-01-01T00:00:00Z"
echo "   8b. 不存在的版本标识 -> 404"
curl -sS -w '\nHTTP %{http_code}\n' \
  "$API/trace?version_id=00000000-0000-0000-0000-000000000000&at=2020-06-01T00:00:00Z"

echo "完成"
