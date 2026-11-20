"""分片清单的制单计划（纯函数）与取回载荷组装。

片号稳定性
----------
片号在一份清单内从 1 起、按发布序号升序、再按片在该发布内的顺序稠密分配。
清单一旦提交即冻结为快照：后续发布只追加新记录、既有记录不可改，
因此同一请求重试得到的片号、记录范围与字节范围永远一致，重启亦不变。

取回载荷
--------
按片号取回时返回“原件表头 + 该片数据段”的原样字节：
* 表头区间与数据段区间都在制单时按字节固定；
* 对每个片计算载荷 SHA-256，取回时只读取所需字节并复核，
  字节读不全或与制单时不符一律拒绝。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from .csv_records import CsvLayout, shard_record_ranges


@dataclass(frozen=True)
class ReleasePlanInput:
    """制单时一个发布的解析输入（已通过存在性、摘要与 CSV 格式校验）。"""

    release_id: int
    blob_sha256: str
    size: int
    layout: CsvLayout


@dataclass(frozen=True)
class ShardPlan:
    shard_no: int
    release_id: int
    blob_sha256: str
    # 数据记录序号区间（0 起，半开，不含表头）。
    record_lo: int
    record_hi: int
    # 该片数据段在原件中的半开字节区间 [byte_start, byte_end)。
    byte_start: int
    byte_end: int
    # 原件表头半开字节区间，所有片共用。
    header_start: int
    header_end: int
    record_count: int
    payload_sha256: str
    payload_size: int


def build_shard_plans(
    releases: list[ReleasePlanInput], max_records_per_shard: int
) -> list[ShardPlan]:
    """按发布序号升序、每发布内按记录顺序生成全局稠密片号的分片计划。

    本阶段只固定记录/字节区间与片号；载荷摘要在 :func:`finalize_shard_hashes`
    中按原件真实字节复算填充。
    """
    ordered = sorted(releases, key=lambda r: r.release_id)
    plans: list[ShardPlan] = []
    shard_no = 0
    for item in ordered:
        layout = item.layout
        ranges = shard_record_ranges(
            layout.data_record_count, max_records_per_shard
        )
        header_start, header_end = layout.header.content_byte_range
        for rec_lo, rec_hi in ranges:
            byte_start = layout.records[rec_lo].start
            # end 含行终止符：相邻片首尾相接，拼回时与原件字节完全一致。
            byte_end = layout.records[rec_hi - 1].end
            shard_no += 1
            plans.append(
                ShardPlan(
                    shard_no=shard_no,
                    release_id=item.release_id,
                    blob_sha256=item.blob_sha256,
                    record_lo=rec_lo,
                    record_hi=rec_hi,
                    byte_start=byte_start,
                    byte_end=byte_end,
                    header_start=header_start,
                    header_end=header_end,
                    record_count=rec_hi - rec_lo,
                    payload_sha256="",  # 由 finalize 时按真实字节填充
                    payload_size=0,
                )
            )
    return plans


def assemble_shard_payload(header_bytes: bytes, data_segment: bytes) -> bytes:
    """组装取回载荷：原件表头原样字节 + 换行衔接 + 该片数据段原样字节。

    表头与数据段都取自原件的固定字节区间，不做任何重新序列化，
    因此引号、CRLF、多字节字符、BOM 全部原样保留。
    """
    # 表头记录本身不含行终止符（解析时 end 停在 LF/CRLF 之前），
    # 用 LF 衔接表头与数据段；数据段内部的换行属于记录原文，保持不动。
    return header_bytes + b"\n" + data_segment


def finalize_shard_hashes(
    plans: list[ShardPlan], raw_by_digest: dict[str, bytes]
) -> list[ShardPlan]:
    """按原件真实字节复算每个片的载荷摘要并做整文件覆盖自检。

    制单时调用：把每个发布的全部片数据段按序拼接，必须与原件除去表头行
    之后的字节布局一致（记录边界由解析器保证连续），任何对不上都抛错，
    从而确保“每条数据恰好分入一片、无重叠无缺口”。
    """
    finalized: list[ShardPlan] = []
    # 按发布分组做覆盖自检。
    by_release: dict[int, list[ShardPlan]] = {}
    for plan in plans:
        by_release.setdefault(plan.release_id, []).append(plan)

    verified_plans: dict[int, ShardPlan] = {}
    for release_id, group in by_release.items():
        group = sorted(group, key=lambda p: p.shard_no)
        digest = group[0].blob_sha256
        raw = raw_by_digest[digest]
        # 1) 片区间必须连续覆盖 [首记录起点, 文件尾)，无重叠无缺口。
        cursor = group[0].byte_start
        for plan in group:
            if plan.byte_start != cursor:
                raise ValueError(
                    f"release {release_id} 分片字节区间不连续，存在缺口或重叠"
                )
            cursor = plan.byte_end
        if cursor != len(raw):
            raise ValueError(
                f"release {release_id} 分片未覆盖到原件尾 "
                f"(覆盖到 {cursor}，原件 {len(raw)})"
            )
        # 2) 逐片按真实字节组装并摘要。
        for plan in group:
            header_bytes = raw[plan.header_start:plan.header_end]
            segment = raw[plan.byte_start:plan.byte_end]
            payload = assemble_shard_payload(header_bytes, segment)
            digest_payload = hashlib.sha256(payload).hexdigest()
            verified_plans[plan.shard_no] = ShardPlan(
                shard_no=plan.shard_no,
                release_id=plan.release_id,
                blob_sha256=plan.blob_sha256,
                record_lo=plan.record_lo,
                record_hi=plan.record_hi,
                byte_start=plan.byte_start,
                byte_end=plan.byte_end,
                header_start=plan.header_start,
                header_end=plan.header_end,
                record_count=plan.record_count,
                payload_sha256=digest_payload,
                payload_size=len(payload),
            )

    for plan in plans:
        finalized.append(verified_plans[plan.shard_no])
    return finalized


def canonical_request_key(
    checkpoint_seq: int, release_ids: list[int], max_records_per_shard: int
) -> str:
    """请求指纹：规范化 JSON 后取 SHA-256。

    发布序号顺序不敏感（排序后入参），因此同一请求以不同顺序重发仍命中
    同一份清单；任一字段不同则指纹不同，得到新清单。
    """
    document = {
        "checkpoint_seq": checkpoint_seq,
        "release_ids": sorted(int(r) for r in release_ids),
        "max_records_per_shard": int(max_records_per_shard),
    }
    canonical = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()
