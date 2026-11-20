"""河流水质站日 CSV 发布 API。

流程与不变量
------------
1. 上传先落共享卷（临时文件 fsync + 原子改名），写盘失败返回 507 且无库记录；
2. 数据库事务 BEGIN IMMEDIATE 串行化双实例并发：
   - 同 (站号, 日期, 单号) 同字节 -> 复用既有记录（幂等重试）；
   - 同键异字节 -> 409 拒绝，记录不可改；
   - 超累计上限 -> 409 拒绝；
3. 每次发布追加一个检查点（条数 + Merkle 根），只增不改；
4. 下载与证明只服务已提交记录；启动自检发现缺件则健康检查置为不可用。
"""
from __future__ import annotations

import hashlib
import json
import re
from contextlib import asynccontextmanager
from datetime import date

from fastapi import FastAPI, File, Form, HTTPException, Response, UploadFile
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from . import merkle, sharding
from .config import settings
from .csv_records import CsvFormatError, parse_csv
from .db import ConflictError, Database
from .schemas import (
    AppendProofOut,
    CheckpointOut,
    HealthOut,
    InclusionProofOut,
    LeafRecordOut,
    ManifestCreateIn,
    ManifestOut,
    PublishResult,
    ReleaseOut,
    ShardFetchOut,
    ShardOut,
)
from .storage import BlobError, BlobStore

blob_store = BlobStore(settings)
database = Database(settings)

# 启动自检结果；发现缺件时拒绝出具任何证明与下载。
_startup_problems: list[str] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _startup_problems
    _startup_problems = database.integrity_problems(blob_store.verify_present)
    yield


app = FastAPI(
    title="河流水质站日 CSV 发布服务",
    version="1.0.0",
    description=(
        "不可改的原件发布、SHA-256 Merkle 检查点、单件纳入证明与检查点追加证明。"
        "原件字节存共享卷，数据库只存目录与摘要。"
    ),
    lifespan=lifespan,
)

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ID_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")
_ORDER_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,128}$")


def _validate_identity(station_id: str, sample_date: str, order_no: str) -> None:
    if not _ID_RE.match(station_id or ""):
        raise HTTPException(422, "station_id 须为 1-64 位字母数字及 ._- 字符")
    if not _DATE_RE.match(sample_date or ""):
        raise HTTPException(422, "sample_date 须为 YYYY-MM-DD 格式")
    try:
        date.fromisoformat(sample_date)
    except ValueError:
        raise HTTPException(422, "sample_date 不是合法日历日期")
    if not _ORDER_RE.match(order_no or ""):
        raise HTTPException(422, "order_no 须为 1-128 位字母数字及 ._- 字符")


def _require_healthy() -> None:
    if _startup_problems:
        raise HTTPException(
            503,
            detail={
                "message": "存储自检未通过，存在缺件记录，服务拒绝出具证明",
                "problems": _startup_problems,
            },
        )


# --------------------------------------------------------------------------- #
# 发布
# --------------------------------------------------------------------------- #
@app.post(
    "/releases",
    response_model=PublishResult,
    status_code=201,
    summary="发布站日 CSV（同单同输入重试复用，异输入拒绝）",
)
async def publish_release(
    station_id: str = Form(..., description="站号"),
    sample_date: str = Form(..., description="采样日期 YYYY-MM-DD"),
    order_no: str = Form(..., description="发布单号"),
    file: UploadFile = File(..., description="原件，单文件不超过 1 MiB"),
):
    _require_healthy()
    _validate_identity(station_id, sample_date, order_no)

    # 先在请求内读取并强制大小上限，避免超限文件落盘。
    data = await file.read(settings.max_file_bytes + 1)
    if len(data) > settings.max_file_bytes:
        raise HTTPException(
            413, f"原件超过单件上限 {settings.max_file_bytes} 字节（1 MiB）"
        )
    if len(data) == 0:
        raise HTTPException(422, "原件为空文件，拒绝发布")

    try:
        digest, _path = await run_in_threadpool(blob_store.store, data)
    except BlobError as exc:
        # 写盘失败：无任何库记录，客户端可原样重试。
        return JSONResponse(status_code=507, content={"detail": str(exc)})

    try:
        record, reused = await run_in_threadpool(
            database.publish,
            station_id,
            sample_date,
            order_no,
            digest,
            len(data),
        )
    except ConflictError as exc:
        raise HTTPException(409, str(exc))

    checkpoint = await run_in_threadpool(database.latest_checkpoint)
    return PublishResult(
        reused=reused,
        release=ReleaseOut(**record),
        checkpoint=CheckpointOut(**checkpoint),
    )


# --------------------------------------------------------------------------- #
# 查询与取件
# --------------------------------------------------------------------------- #
@app.get("/releases", response_model=list[ReleaseOut], summary="列出全部发布记录")
async def list_releases():
    rows = await run_in_threadpool(database.list_releases)
    return [ReleaseOut(**r) for r in rows]


@app.get(
    "/releases/{station_id}/{sample_date}/{order_no}",
    response_model=ReleaseOut,
    summary="按站号/日期/单号查询发布目录记录",
)
async def get_release(station_id: str, sample_date: str, order_no: str):
    record = await run_in_threadpool(
        database.get_release, station_id, sample_date, order_no
    )
    if record is None:
        raise HTTPException(404, "未找到该站号/日期/发布单号对应的发布记录")
    return ReleaseOut(**record)


@app.get(
    "/releases/{station_id}/{sample_date}/{order_no}/download",
    summary="下载已提交原件（仅对应完整已提交字节）",
)
async def download_release(station_id: str, sample_date: str, order_no: str):
    _require_healthy()
    record = await run_in_threadpool(
        database.get_release, station_id, sample_date, order_no
    )
    if record is None:
        raise HTTPException(404, "未找到该站号/日期/发布单号对应的发布记录")
    present = await run_in_threadpool(
        blob_store.verify_present, record["blob_sha256"]
    )
    if not present:
        # 理论上启动自检已拦截；此处兜底，绝不返回缺件/错件。
        raise HTTPException(503, "原件缺失或摘要不符，拒绝下载")
    data = await run_in_threadpool(blob_store.read, record["blob_sha256"])
    filename = f"{station_id}_{sample_date}_{order_no}.csv"
    return Response(
        content=data,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# --------------------------------------------------------------------------- #
# 检查点
# --------------------------------------------------------------------------- #
@app.get(
    "/checkpoints/latest",
    response_model=CheckpointOut,
    summary="最新检查点（条数 + Merkle 根摘要）",
)
async def latest_checkpoint():
    return CheckpointOut(**(await run_in_threadpool(database.latest_checkpoint)))


@app.get(
    "/checkpoints/{seq}",
    response_model=CheckpointOut,
    summary="按条数序号取任意旧检查点",
)
async def get_checkpoint(seq: int):
    checkpoint = await run_in_threadpool(database.get_checkpoint, seq)
    if checkpoint is None:
        raise HTTPException(404, f"不存在序号为 {seq} 的检查点")
    return CheckpointOut(**checkpoint)


@app.get(
    "/checkpoints",
    response_model=list[CheckpointOut],
    summary="列出全部检查点（含零号空树检查点）",
)
async def list_checkpoints():
    rows = await run_in_threadpool(database.list_checkpoints)
    return [CheckpointOut(**c) for c in rows]


# --------------------------------------------------------------------------- #
# 证明
# --------------------------------------------------------------------------- #
@app.get(
    "/releases/{station_id}/{sample_date}/{order_no}/inclusion-proof",
    response_model=InclusionProofOut,
    summary="单件纳入证明（路径长度 = 树深度，O(log n)）",
)
async def inclusion_proof(station_id: str, sample_date: str, order_no: str):
    _require_healthy()
    record = await run_in_threadpool(
        database.get_release, station_id, sample_date, order_no
    )
    if record is None:
        raise HTTPException(404, "未找到该站号/日期/发布单号对应的发布记录")
    index = record["id"] - 1
    leaves = await run_in_threadpool(database.leaf_hashes)
    path = merkle.inclusion_path(index, leaves, settings.tree_depth)
    checkpoint = await run_in_threadpool(database.latest_checkpoint)
    leaf_record = LeafRecordOut(
        v=merkle.LEAF_VERSION,
        station_id=record["station_id"],
        sample_date=record["sample_date"],
        order_no=record["order_no"],
        blob_sha256=record["blob_sha256"],
        size=record["size"],
    )
    return InclusionProofOut(
        index=index,
        checkpoint_seq=checkpoint["seq"],
        checkpoint_root=checkpoint["root_hex"],
        tree_depth=settings.tree_depth,
        leaf_record=leaf_record,
        leaf_hash=leaves[index].hex(),
        path=[step.to_dict() for step in path],
    )


@app.get(
    "/checkpoints/{old_seq}/append-proof",
    response_model=AppendProofOut,
    summary="旧检查点到新检查点的追加证明（条目数 O(log n)）",
)
async def append_proof(old_seq: int):
    _require_healthy()
    old_cp = await run_in_threadpool(database.get_checkpoint, old_seq)
    if old_cp is None:
        raise HTTPException(404, f"不存在序号为 {old_seq} 的旧检查点")
    new_cp = await run_in_threadpool(database.latest_checkpoint)
    leaves = await run_in_threadpool(database.leaf_hashes)
    if new_cp["seq"] != len(leaves):
        raise HTTPException(500, "检查点与叶子条数不一致")
    entries = await run_in_threadpool(
        merkle.append_proof, leaves, old_cp["seq"], settings.tree_depth
    )
    return AppendProofOut(
        old_seq=old_cp["seq"],
        new_seq=new_cp["seq"],
        old_root=old_cp["root_hex"],
        new_root=new_cp["root_hex"],
        tree_depth=settings.tree_depth,
        entries=entries,
    )


# --------------------------------------------------------------------------- #
# 分片清单
# --------------------------------------------------------------------------- #
def _manifest_to_out(row: dict, reused: bool) -> ManifestOut:
    shards = [
        ShardOut(
            shard_no=s["shard_no"],
            release_id=s["release_id"],
            record_range=(s["record_lo"], s["record_hi"]),
            record_count=s["record_count"],
            byte_range=(s["byte_start"], s["byte_end"]),
            header_range=(s["header_start"], s["header_end"]),
            payload_sha256=s["payload_sha256"],
            payload_size=s["payload_size"],
        )
        for s in row["shards"]
    ]
    return ManifestOut(
        manifest_id=row["id"],
        request_key=row["request_key"],
        reused=reused,
        checkpoint_seq=row["checkpoint_seq"],
        max_records_per_shard=row["max_records_per_shard"],
        release_ids=json.loads(row["release_ids_json"]),
        shard_count=row["shard_count"],
        shards=shards,
        created_at=row["created_at"],
    )


def _create_manifest_sync(payload: ManifestCreateIn) -> ManifestOut:
    """制单主流程（线程池执行，阻塞 IO 不卡事件循环）。

    全部校验通过后才在单事务内提交清单；任一步失败均抛 HTTPException，
    数据库与清单表不产生任何写入（整份新清单不生效）。
    """
    release_ids = list(payload.release_ids)
    # 去重校验：同一发布序号在一份清单内只能出现一次。
    if len(set(release_ids)) != len(release_ids):
        raise HTTPException(422, "release_ids 存在重复发布序号")
    if len(release_ids) > settings.max_manifest_releases:
        raise HTTPException(
            422,
            f"单份清单至多包含 {settings.max_manifest_releases} 个发布序号",
        )

    # 幂等指纹与顺序无关：同请求不同排列仍命中同一清单。
    request_key = sharding.canonical_request_key(
        payload.checkpoint_seq, release_ids, payload.max_records_per_shard
    )
    existing = database.get_manifest_by_request_key(request_key)
    if existing is not None:
        return _manifest_to_out(existing, reused=True)

    checkpoint = database.get_checkpoint(payload.checkpoint_seq)
    if checkpoint is None:
        raise HTTPException(
            422, f"不存在序号为 {payload.checkpoint_seq} 的检查点"
        )

    plan_inputs: list[sharding.ReleasePlanInput] = []
    raw_by_digest: dict[str, bytes] = {}
    for release_id in sorted(set(release_ids)):
        # 检查点 seq 即截至该点的发布条数；发布序号必须落在该检查点范围内。
        if not 1 <= release_id <= payload.checkpoint_seq:
            raise HTTPException(
                422,
                f"发布序号 {release_id} 不在检查点 {payload.checkpoint_seq} "
                f"的覆盖范围 1..{payload.checkpoint_seq} 内",
            )
        record = database.get_release_by_id(release_id)
        if record is None:
            # 序号稠密不变量下理论不可达，仍显式拒绝。
            raise HTTPException(422, f"发布序号 {release_id} 不存在")
        digest = record["blob_sha256"]
        # 原件缺失或摘要不符：整单拒绝，不进入写事务。
        if not blob_store.verify_present(digest):
            raise HTTPException(
                409,
                f"发布 {release_id} 的原件缺失或摘要不符，整份清单不生效",
            )
        raw = blob_store.read(digest)
        if len(raw) != record["size"]:
            raise HTTPException(
                409,
                f"发布 {release_id} 的原件字节数与目录记录不符，整份清单不生效",
            )
        # 格式无效（编码、引用、缺表头等）：整单拒绝。
        try:
            layout = parse_csv(raw)
        except CsvFormatError as exc:
            raise HTTPException(
                422,
                f"发布 {release_id} 不是有效的带表头 UTF-8 CSV：{exc}",
            ) from exc
        raw_by_digest[digest] = raw
        plan_inputs.append(
            sharding.ReleasePlanInput(
                release_id=release_id,
                blob_sha256=digest,
                size=record["size"],
                layout=layout,
            )
        )

    # 纯函数划片：每条数据记录恰好一片，片号全局稠密。
    plans = sharding.build_shard_plans(
        plan_inputs, payload.max_records_per_shard
    )
    # 按真实字节复算每片载荷摘要，并做整文件连续覆盖自检。
    plans = sharding.finalize_shard_hashes(plans, raw_by_digest)

    saved, reused = database.insert_manifest(
        request_key=request_key,
        checkpoint_seq=payload.checkpoint_seq,
        max_records_per_shard=payload.max_records_per_shard,
        release_ids=sorted(set(release_ids)),
        shard_plans=plans,
    )
    return _manifest_to_out(saved, reused=reused)


@app.post(
    "/manifests",
    response_model=ManifestOut,
    status_code=201,
    summary="提交分片清单（检查点序号 + 至多 8 个发布序号 + 每片记录数上限）",
)
async def create_manifest(payload: ManifestCreateIn):
    _require_healthy()
    return await run_in_threadpool(_create_manifest_sync, payload)


@app.get(
    "/manifests",
    response_model=list[ManifestOut],
    summary="列出全部分片清单",
)
async def list_manifests():
    rows = await run_in_threadpool(database.list_manifests)
    out: list[ManifestOut] = []
    for row in rows:
        full = await run_in_threadpool(database.get_manifest, row["id"])
        out.append(_manifest_to_out(full, reused=False))
    return out


@app.get(
    "/manifests/{manifest_id}",
    response_model=ManifestOut,
    summary="按清单编号取回清单（含各片编号、发布序号、记录与字节范围）",
)
async def get_manifest(manifest_id: int):
    row = await run_in_threadpool(database.get_manifest, manifest_id)
    if row is None:
        raise HTTPException(404, f"不存在编号为 {manifest_id} 的分片清单")
    return _manifest_to_out(row, reused=False)


@app.get(
    "/manifests/{manifest_id}/shards/{shard_no}",
    summary="按片号取回：仅读表头与该片字节，原样返回，摘要不符即拒绝",
)
async def fetch_shard(manifest_id: int, shard_no: int):
    manifest = await run_in_threadpool(database.get_manifest, manifest_id)
    if manifest is None:
        raise HTTPException(404, f"不存在编号为 {manifest_id} 的分片清单")
    shard = await run_in_threadpool(
        database.get_shard, manifest_id, shard_no
    )
    if shard is None:
        raise HTTPException(
            404,
            f"清单 {manifest_id} 中不存在片号 {shard_no}"
            f"（合法范围 1..{manifest['shard_count']}）",
        )

    def _read_and_verify() -> bytes:
        # 只读取原件表头与该片所需字节，绝不全量加载整个原件。
        try:
            header_bytes = blob_store.read_range(
                shard["blob_sha256"], shard["header_start"], shard["header_end"]
            )
            segment = blob_store.read_range(
                shard["blob_sha256"], shard["byte_start"], shard["byte_end"]
            )
        except BlobError as exc:
            raise HTTPException(409, f"取片被拒绝：{exc}") from exc
        payload = sharding.assemble_shard_payload(header_bytes, segment)
        actual_digest = hashlib.sha256(payload).hexdigest()
        # 字节读不全或与制单时不符就拒绝，绝不返回损坏数据。
        if actual_digest != shard["payload_sha256"]:
            raise HTTPException(
                409,
                "取片被拒绝：该片载荷摘要与制单时不符，原件数据段可能已损坏",
            )
        if len(payload) != shard["payload_size"]:
            raise HTTPException(
                409, "取片被拒绝：载荷字节数与制单时不符"
            )
        return payload

    payload = await run_in_threadpool(_read_and_verify)
    fetch_meta = ShardFetchOut(
        manifest_id=manifest_id,
        shard_no=shard_no,
        release_id=shard["release_id"],
        record_range=(shard["record_lo"], shard["record_hi"]),
        byte_range=(shard["byte_start"], shard["byte_end"]),
        payload_sha256=shard["payload_sha256"],
        payload_size=shard["payload_size"],
    )
    # 原样字节通过 X-Shard-Meta 头附带核验元数据，响应体即“表头+该片数据段”。
    return Response(
        content=payload,
        media_type="text/csv",
        headers={
            "X-Shard-Meta": json.dumps(
                fetch_meta.model_dump(), separators=(",", ":")
            ),
            "X-Payload-SHA256": shard["payload_sha256"],
        },
    )


# --------------------------------------------------------------------------- #
# 健康检查
# --------------------------------------------------------------------------- #
@app.get("/health", response_model=HealthOut, summary="存储自检与健康状态")
async def health():
    problems = list(_startup_problems)
    rows = await run_in_threadpool(database.list_releases)
    return HealthOut(
        status="ok" if not problems else "degraded",
        release_count=len(rows),
        problems=problems,
    )


@app.get("/", include_in_schema=False)
def root():
    return {
        "service": "water-quality-release-service",
        "docs": "/docs",
        "limits": {
            "max_file_bytes": settings.max_file_bytes,
            "max_releases": settings.max_releases,
            "tree_depth": settings.tree_depth,
            "max_manifest_releases": settings.max_manifest_releases,
        },
    }
