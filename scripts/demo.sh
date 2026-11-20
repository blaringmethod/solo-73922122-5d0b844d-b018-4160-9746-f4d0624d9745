#!/usr/bin/env bash
# 端到端演示：发布 -> 幂等重试 -> 冲突拒绝 -> 取件 -> 两类证明 -> 离线核验 -> 篡改拒绝。
# 用法：BASE=http://127.0.0.1:8001 bash scripts/demo.sh
set -euo pipefail

BASE="${BASE:-http://127.0.0.1:8001}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cat > "$WORK/day1.csv" <<'CSV'
station,time,ph,do
ST01,2026-11-16T08:00,7.2,8.1
CSV
cat > "$WORK/day2.csv" <<'CSV'
station,time,ph,do
ST02,2026-11-16T08:00,7.0,8.4
CSV
# 含引号内换行的跨行 CSV：换行属于同一条记录，划片不得切断。
cat > "$WORK/multiline.csv" <<'CSV'
station,time,ph,note
STM,2026-11-16T08:00,7.2,"first line
second line"
STM,2026-11-16T09:00,7.0,plain
STM,2026-11-16T10:00,6.8,third
CSV

echo "== 1. 首次发布 =="
curl -s -X POST "$BASE/releases" \
  -F station_id=ST01 -F sample_date=2026-11-16 -F order_no=ORD-001 \
  -F file=@"$WORK/day1.csv" | python3 -m json.tool

echo "== 2. 同单同输入重试（期望 reused=true）=="
curl -s -X POST "$BASE/releases" \
  -F station_id=ST01 -F sample_date=2026-11-16 -F order_no=ORD-001 \
  -F file=@"$WORK/day1.csv" | python3 -m json.tool

echo "== 3. 同单异输入（期望 HTTP 409）=="
curl -s -w "\nHTTP %{http_code}\n" -X POST "$BASE/releases" \
  -F station_id=ST01 -F sample_date=2026-11-16 -F order_no=ORD-001 \
  -F file=@"$WORK/day2.csv"

echo "== 4. 第二件发布 =="
curl -s -X POST "$BASE/releases" \
  -F station_id=ST02 -F sample_date=2026-11-16 -F order_no=ORD-002 \
  -F file=@"$WORK/day2.csv" | python3 -m json.tool

echo "== 5. 下载并比对原件 =="
curl -s "$BASE/releases/ST01/2026-11-16/ORD-001/download" -o "$WORK/back.csv"
cmp "$WORK/day1.csv" "$WORK/back.csv" && echo "字节一致"

echo "== 6. 纳入证明与离线核验 =="
curl -s "$BASE/releases/ST01/2026-11-16/ORD-001/inclusion-proof" \
  > "$WORK/inclusion.json"
python3 -m app.verify inclusion \
  --proof "$WORK/inclusion.json" --file "$WORK/day1.csv"

echo "== 7. 追加证明（1 号 -> 最新）与离线核验 =="
curl -s "$BASE/checkpoints/1" > "$WORK/cp1.json"
curl -s "$BASE/checkpoints/latest" > "$WORK/cp-latest.json"
curl -s "$BASE/checkpoints/1/append-proof" > "$WORK/append.json"
python3 -m app.verify append \
  --proof "$WORK/append.json" \
  --old-checkpoint "$WORK/cp1.json" \
  --new-checkpoint "$WORK/cp-latest.json"

echo "== 8. 发布跨行 CSV 并制分片清单（每片至多 2 条记录）=="
curl -s -X POST "$BASE/releases" \
  -F station_id=STM -F sample_date=2026-11-16 -F order_no=ORD-ML \
  -F file=@"$WORK/multiline.csv" | python3 -m json.tool >/dev/null
ML_ID="$(curl -s "$BASE/releases/STM/2026-11-16/ORD-ML" | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')"
CP_SEQ="$(curl -s "$BASE/checkpoints/latest" | python3 -c 'import sys,json;print(json.load(sys.stdin)["seq"])')"
MANIFEST="$(curl -s -X POST "$BASE/manifests" -H 'Content-Type: application/json' \
  -d "{\"checkpoint_seq\":$CP_SEQ,\"release_ids\":[$ML_ID],\"max_records_per_shard\":2}")"
echo "$MANIFEST" | python3 -m json.tool
MID="$(echo "$MANIFEST" | python3 -c 'import sys,json;print(json.load(sys.stdin)["manifest_id"])')"

echo "== 9. 同请求重试（期望 reused=true、同一清单编号与片号）=="
curl -s -X POST "$BASE/manifests" -H 'Content-Type: application/json' \
  -d "{\"checkpoint_seq\":$CP_SEQ,\"release_ids\":[$ML_ID],\"max_records_per_shard\":2}" \
  | python3 -c 'import sys,json;d=json.load(sys.stdin);print("reused =",d["reused"],"manifest_id =",d["manifest_id"])'

echo "== 10. 按片号取回（仅读表头+该片字节，跨行记录完整，逐字节核对）=="
for SHARD in 1 2; do
  curl -s "$BASE/manifests/$MID/shards/$SHARD" -o "$WORK/shard-$SHARD.csv" \
    -D "$WORK/shard-$SHARD.headers"
  echo "--- 片 $SHARD ---"
  cat "$WORK/shard-$SHARD.csv"
  grep -i 'x-payload-sha256' "$WORK/shard-$SHARD.headers"
done
python3 - "$WORK" <<'PY'
import pathlib, sys
work = pathlib.Path(sys.argv[1])
original = (work / "multiline.csv").read_bytes()
header = original[: original.index(b"\n")]
segments = b"".join(
    (work / f"shard-{i}.csv").read_bytes().split(b"\n", 1)[1] for i in (1, 2)
)
assert header + b"\n" + segments == original, "分片拼回与原件不一致"
print("两片数据段拼回与原件逐字节一致")
assert b'"first line\nsecond line"' in (work / "shard-1.csv").read_bytes()
print("引号内换行的跨行记录完整保留在同一片")
PY

echo "== 11. 篡改原件（期望离线拒绝，退出码 1）=="
cp "$WORK/day1.csv" "$WORK/tampered.csv"
printf 'X' >> "$WORK/tampered.csv"
python3 -m app.verify inclusion \
  --proof "$WORK/inclusion.json" --file "$WORK/tampered.csv" || true
