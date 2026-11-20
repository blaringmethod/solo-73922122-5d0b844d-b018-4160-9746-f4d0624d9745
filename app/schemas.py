"""HTTP 响应模型。"""
from __future__ import annotations

from pydantic import BaseModel, Field


class ReleaseOut(BaseModel):
    id: int = Field(..., description="叶子下标 = id - 1")
    station_id: str
    sample_date: str
    order_no: str
    blob_sha256: str
    size: int
    created_at: str


class PublishResult(BaseModel):
    reused: bool = Field(..., description="True 表示同单同输入重试，复用既有记录")
    release: ReleaseOut
    checkpoint: "CheckpointOut"


class CheckpointOut(BaseModel):
    seq: int = Field(..., description="截至该检查点的发布条数")
    root_hex: str
    created_at: str


class LeafRecordOut(BaseModel):
    v: int
    station_id: str
    sample_date: str
    order_no: str
    blob_sha256: str
    size: int


class InclusionProofOut(BaseModel):
    index: int
    checkpoint_seq: int
    checkpoint_root: str
    tree_depth: int
    leaf_record: LeafRecordOut
    leaf_hash: str
    path: list[dict]


class AppendProofOut(BaseModel):
    old_seq: int
    new_seq: int
    old_root: str
    new_root: str
    tree_depth: int
    entries: list[dict] = Field(
        ..., description="完美子树条目 (start, height, hash)，数量 O(log n)"
    )


class HealthOut(BaseModel):
    status: str
    release_count: int
    problems: list[str]


class ManifestCreateIn(BaseModel):
    checkpoint_seq: int = Field(
        ..., ge=1, description="检查点序号（该检查点对应的发布条数，>=1）"
    )
    release_ids: list[int] = Field(
        ...,
        min_length=1,
        max_length=8,
        description="该检查点内至多 8 个发布序号（1 起稠密），顺序不敏感",
    )
    max_records_per_shard: int = Field(
        ..., ge=1, description="每片记录数上限（正整数）"
    )


class ShardOut(BaseModel):
    shard_no: int = Field(..., description="片号，清单内 1 起稠密，制单后永不变更")
    release_id: int = Field(..., description="该片所属发布序号")
    record_range: tuple[int, int] = Field(
        ..., description="数据记录序号半开区间 [lo, hi)，不含表头"
    )
    record_count: int
    byte_range: tuple[int, int] = Field(
        ..., description="该片数据段在原件中的半开字节区间 [start, end)"
    )
    header_range: tuple[int, int] = Field(
        ..., description="原件表头半开字节区间，取回时与数据段拼接"
    )
    payload_sha256: str = Field(..., description="取回载荷（表头+数据段）摘要")
    payload_size: int


class ManifestOut(BaseModel):
    manifest_id: int
    request_key: str = Field(..., description="请求指纹，同请求重试返回同一清单")
    reused: bool = Field(..., description="True 表示同请求重试，复用既有清单")
    checkpoint_seq: int
    max_records_per_shard: int
    release_ids: list[int]
    shard_count: int
    shards: list[ShardOut]
    created_at: str


class ShardFetchOut(BaseModel):
    manifest_id: int
    shard_no: int
    release_id: int
    record_range: tuple[int, int]
    byte_range: tuple[int, int]
    payload_sha256: str
    payload_size: int


PublishResult.model_rebuild()
