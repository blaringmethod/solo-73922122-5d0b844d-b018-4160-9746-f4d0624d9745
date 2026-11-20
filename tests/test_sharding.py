"""分片清单与按片取回测试：CSV 字节级解析、跨行记录、幂等制单、取片失败隔离。"""
from __future__ import annotations

import hashlib
import io
import json

import pytest

from app import main as main_mod
from app import sharding
from app.csv_records import CsvFormatError, parse_csv, shard_record_ranges
from app.storage import BlobError


# --------------------------------------------------------------------------- #
# 纯函数：字节级 CSV 解析
# --------------------------------------------------------------------------- #
def test_parse_simple_csv_byte_ranges():
    data = b"a,b\n1,2\n3,4\n"
    layout = parse_csv(data)
    assert layout.header.fields == ("a", "b")
    assert layout.data_record_count == 2
    assert [r.fields for r in layout.records] == [("1", "2"), ("3", "4")]
    # 区间连续覆盖：表头 + 两条记录恰好覆盖整个原件。
    assert layout.header.start == 0
    assert layout.records[0].byte_range == (4, 8)
    assert layout.records[1].byte_range == (8, 12)
    assert layout.records[-1].end == len(data)


def test_quoted_newline_stays_in_one_record():
    # 引号内换行属于同一条记录：物理 5 行，逻辑只有表头 + 2 条数据。
    data = b'station,note\nS1,"line1\nline2"\nS2,ok\n'
    layout = parse_csv(data)
    assert layout.data_record_count == 2
    rec0 = layout.records[0]
    assert rec0.fields == ("S1", "line1\nline2")
    # 跨行记录的字节区间跨越物理换行，且与下一条记录首尾相接。
    assert data[rec0.start:rec0.end] == b'S1,"line1\nline2"\n'
    assert layout.records[1].start == rec0.end
    assert layout.records[-1].end == len(data)


def test_crlf_and_doubled_quote_and_no_trailing_newline():
    data = b'a,b\r\nx,"he said ""hi"""\r\ny,z'
    layout = parse_csv(data)
    assert layout.header.content_byte_range == (0, 3)  # 不含 CRLF
    assert layout.records[0].fields == ("x", 'he said "hi"')
    assert layout.records[1].fields == ("y", "z")
    # 最后一条无尾换行：content_end == end。
    last = layout.records[-1]
    assert last.content_end == last.end == len(data)
    # 片区间连续覆盖到文件尾。
    assert layout.records[0].end == layout.records[1].start


def test_utf8_multibyte_field_preserved():
    data = "站点,备注\n甲站,良好\n乙站,异常\n".encode("utf-8")
    layout = parse_csv(data)
    assert layout.header.fields == ("站点", "备注")
    assert layout.records[0].fields == ("甲站", "良好")
    # 字节区间切在 UTF-8 字符边界上，切片仍是合法 UTF-8。
    lo, hi = layout.records[0].byte_range
    data[lo:hi].decode("utf-8")


def test_bom_kept_in_bytes_but_stripped_from_header_field():
    data = b"\xef\xbb\xbfa,b\n1,2\n"
    layout = parse_csv(data)
    assert layout.header.fields == ("a", "b")
    # BOM 计入原字节，表头 content 区间从 0 起并包含 BOM。
    assert layout.header.content_byte_range[0] == 0
    assert data[layout.header.start:layout.header.content_end].startswith(
        b"\xef\xbb\xbf"
    )


@pytest.mark.parametrize(
    "bad",
    [
        b"",  # 空文件
        b"a,b\n",  # 只有表头
        b"a,b\n1,2\nunterminated,\"quote",  # 引号未闭合
        b"a,b\n1,2\nx\"bad,2\n",  # 引号未出现在字段开头
        b"a,b\r1,2\n",  # 裸 CR
        b"\xff\xfe,a\n1,2\n",  # 非法 UTF-8
    ],
)
def test_invalid_csv_rejected(bad):
    with pytest.raises(CsvFormatError):
        parse_csv(bad)


def test_shard_ranges_cover_every_record_once():
    ranges = shard_record_ranges(10, 3)
    assert ranges == [(0, 3), (3, 6), (6, 9), (9, 10)]
    # 稠密、无重叠、无缺口。
    flat = [i for lo, hi in ranges for i in range(lo, hi)]
    assert flat == list(range(10))


# --------------------------------------------------------------------------- #
# API：制单、幂等、片号稳定、取片
# --------------------------------------------------------------------------- #
def _publish(client, station, day, order, data):
    return client.post(
        "/releases",
        data={"station_id": station, "sample_date": day, "order_no": order},
        files={"file": ("day.csv", io.BytesIO(data), "text/csv")},
    )


def _publish_three(client):
    csv1 = b"station,ph\nS1,7.0\nS2,6.5\nS3,8.1\n"
    csv2 = b'station,note\nA,"x\ny"\nB,z\n'  # 跨行记录
    csv3 = b"station,ph\nC,9.9\n"
    id1 = _publish(client, "ST1", "2026-11-01", "O1", csv1).json()["release"]["id"]
    id2 = _publish(client, "ST2", "2026-11-01", "O2", csv2).json()["release"]["id"]
    id3 = _publish(client, "ST3", "2026-11-01", "O3", csv3).json()["release"]["id"]
    return (csv1, csv2, csv3), (id1, id2, id3)


def test_create_manifest_and_list_shards(app_client):
    client, _, _ = app_client
    _, (id1, id2, id3) = _publish_three(client)
    cp = client.get("/checkpoints/latest").json()["seq"]
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": [id1, id2],
            "max_records_per_shard": 2,
        },
    )
    assert r.status_code == 201, r.text
    m = r.json()
    assert m["reused"] is False
    assert m["checkpoint_seq"] == cp
    assert m["release_ids"] == [id1, id2]
    # release1 有 3 条记录 -> 2 片；release2 有 2 条记录（含 1 条跨行）-> 1 片。
    assert m["shard_count"] == 3
    nos = [s["shard_no"] for s in m["shards"]]
    assert nos == [1, 2, 3]
    # 片号按发布升序分配，记录范围连续。
    assert m["shards"][0]["release_id"] == id1
    assert m["shards"][0]["record_range"] == [0, 2]
    assert m["shards"][1]["record_range"] == [2, 3]
    assert m["shards"][2]["release_id"] == id2
    assert m["shards"][2]["record_range"] == [0, 2]


def test_multiline_record_fetched_as_complete_shard(app_client):
    client, _, _ = app_client
    (_, csv2, _), (_, id2, _) = _publish_three(client)
    cp = client.get("/checkpoints/latest").json()["seq"]
    r = client.post(
        "/manifests",
        json={"checkpoint_seq": cp, "release_ids": [id2], "max_records_per_shard": 10},
    )
    m = r.json()
    assert m["shard_count"] == 1
    shard = m["shards"][0]
    f = client.get(f"/manifests/{m['manifest_id']}/shards/1")
    assert f.status_code == 200
    # 载荷 = 原表头 + 换行 + 该发布全部数据段（跨行记录完整保留）。
    layout2 = parse_csv(csv2)
    expected = csv2[0:layout2.header.content_end] + b"\n" + csv2[
        shard["byte_range"][0]:shard["byte_range"][1]
    ]
    assert f.content == expected
    assert b'"x\ny"' in f.content  # 引号内换行未被切断
    assert f.headers["x-payload-sha256"] == shard["payload_sha256"]


def test_fetch_reconstructs_original_when_concatenated(app_client):
    client, _, _ = app_client
    (csv1, _, _), (id1, _, _) = _publish_three(client)
    cp = client.get("/checkpoints/latest").json()["seq"]
    r = client.post(
        "/manifests",
        json={"checkpoint_seq": cp, "release_ids": [id1], "max_records_per_shard": 1},
    )
    m = r.json()
    assert m["shard_count"] == 3  # 每条记录一片
    pieces = []
    for s in m["shards"]:
        f = client.get(f"/manifests/{m['manifest_id']}/shards/{s['shard_no']}")
        assert f.status_code == 200
        pieces.append(f.content)
    # 去掉每片重复的表头后，数据段拼回应与原件（表头+全部数据）一致。
    header_end = parse_csv(csv1).header.content_end
    header = csv1[:header_end]
    data_segments = b"".join(p.split(b"\n", 1)[1] for p in pieces)
    assert header + b"\n" + data_segments == csv1
    # 每条记录恰好分入一片：数据段覆盖区间连续无重叠。
    ranges = [tuple(s["byte_range"]) for s in m["shards"]]
    cursor = parse_csv(csv1).records[0].start
    for lo, hi in ranges:
        assert lo == cursor
        cursor = hi
    assert cursor == len(csv1)


def test_idempotent_retry_returns_same_manifest(app_client):
    client, _, _ = app_client
    _, (id1, _, id3) = _publish_three(client)
    cp = client.get("/checkpoints/latest").json()["seq"]
    body = {"checkpoint_seq": cp, "release_ids": [id3, id1], "max_records_per_shard": 2}
    r1 = client.post("/manifests", json=body)
    # 不同排列顺序、同参数 -> 同一请求指纹。
    r2 = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": [id1, id3],
            "max_records_per_shard": 2,
        },
    )
    assert r1.status_code == r2.status_code == 201
    assert r1.json()["manifest_id"] == r2.json()["manifest_id"]
    assert r1.json()["request_key"] == r2.json()["request_key"]
    assert r1.json()["shards"] == r2.json()["shards"]
    assert r1.json()["reused"] is False and r2.json()["reused"] is True


def test_shard_numbers_stable_after_new_publish(app_client):
    client, _, _ = app_client
    _, (id1, id2, _) = _publish_three(client)
    cp = client.get("/checkpoints/latest").json()["seq"]
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": [id1, id2],
            "max_records_per_shard": 5,
        },
    )
    before = r.json()
    # 后续发布新原件（检查点增长），既有清单片号不变。
    _publish(client, "ST9", "2026-11-02", "O9", b"station,ph\nZ,1.0\n")
    again = client.get(f"/manifests/{before['manifest_id']}").json()
    assert again["shards"] == before["shards"]
    assert again["shard_count"] == before["shard_count"]
    # 最新检查点已增长，但清单快照仍绑定旧检查点。
    assert client.get("/checkpoints/latest").json()["seq"] == cp + 1
    assert again["checkpoint_seq"] == cp


def test_validation_errors_and_atomic_no_effect(app_client):
    client, _, _ = app_client
    _, ids = _publish_three(client)
    cp = client.get("/checkpoints/latest").json()["seq"]
    before_count = len(client.get("/manifests").json())
    # 超过 8 个发布序号。
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": list(range(1, 10)),
            "max_records_per_shard": 2,
        },
    )
    assert r.status_code == 422
    # 重复发布序号。
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": [ids[0], ids[0]],
            "max_records_per_shard": 2,
        },
    )
    assert r.status_code == 422
    # 发布序号超出检查点覆盖范围（构造一个大于当前条数的序号）。
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": 1,
            "release_ids": [cp + 1],
            "max_records_per_shard": 2,
        },
    )
    assert r.status_code == 422
    # 不存在的检查点。
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp + 100,
            "release_ids": [ids[0]],
            "max_records_per_shard": 2,
        },
    )
    assert r.status_code == 422
    # max_records_per_shard 非正。
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": [ids[0]],
            "max_records_per_shard": 0,
        },
    )
    assert r.status_code == 422
    # 全部失败后没有新增任何清单（会话级客户端与其他用例共享状态，故比较增量）。
    assert len(client.get("/manifests").json()) == before_count


def test_invalid_csv_makes_whole_manifest_ineffective(app_client):
    client, _, _ = app_client
    # 一份合法 CSV、一份真正非法（引号未闭合）的 CSV。
    good_id = _publish(
        client, "GOOD", "2026-11-01", "G1", b"a,b\n1,2\n"
    ).json()["release"]["id"]
    bad_id = _publish(
        client, "BAD", "2026-11-01", "B1", b'a,b\n1,"unterminated\n'
    ).json()["release"]["id"]
    cp = client.get("/checkpoints/latest").json()["seq"]
    before_count = len(client.get("/manifests").json())
    # 同时纳入合法与非法发布：整单拒绝，合法发布也不单独生效。
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": [good_id, bad_id],
            "max_records_per_shard": 2,
        },
    )
    assert r.status_code == 422 and "CSV" in r.json()["detail"]
    assert len(client.get("/manifests").json()) == before_count
    # 仅纳入合法发布则制单成功，证明拒绝确实由非法原件触发。
    ok = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": [good_id],
            "max_records_per_shard": 2,
        },
    )
    assert ok.status_code == 201


def test_missing_or_tampered_blob_rejects_manifest(app_client):
    client, _, _ = app_client
    _, (id1, _, _) = _publish_three(client)
    cp = client.get("/checkpoints/latest").json()["seq"]
    rec = client.get("/releases/ST1/2026-11-01/O1").json()
    path = main_mod.blob_store.path_for(rec["blob_sha256"])
    backup = path.read_bytes()
    before_count = len(client.get("/manifests").json())
    # 篡改原件 -> 摘要不符，整单 409 拒绝。
    path.write_bytes(backup + b"tampered")
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": [id1],
            "max_records_per_shard": 2,
        },
    )
    assert r.status_code == 409
    assert len(client.get("/manifests").json()) == before_count
    # 缺件同样拒绝。
    path.unlink()
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": [id1],
            "max_records_per_shard": 2,
        },
    )
    assert r.status_code == 409
    # 恢复后制单成功。
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(backup)
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": [id1],
            "max_records_per_shard": 2,
        },
    )
    assert r.status_code == 201


def test_corrupted_shard_rejected_but_other_shards_ok(app_client):
    """某片数据段损坏：该片拒绝，其他完好片仍可取回（故障隔离）。"""
    client, _, _ = app_client
    _, (id1, _, _) = _publish_three(client)
    cp = client.get("/checkpoints/latest").json()["seq"]
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": [id1],
            "max_records_per_shard": 1,
        },
    )
    m = r.json()
    mid = m["manifest_id"]
    # 制单后篡改原件，使中间片的数据段字节与制单时不符。
    rec = client.get("/releases/ST1/2026-11-01/O1").json()
    path = main_mod.blob_store.path_for(rec["blob_sha256"])
    backup = path.read_bytes()
    target = m["shards"][1]
    lo, hi = target["byte_range"]
    corrupted = bytearray(backup)
    corrupted[lo] = ord("X")  # 改写中间片首字节，保持文件长度不变
    path.write_bytes(bytes(corrupted))
    try:
        ok1 = client.get(f"/manifests/{mid}/shards/1")
        bad2 = client.get(f"/manifests/{mid}/shards/2")
        ok3 = client.get(f"/manifests/{mid}/shards/3")
        # 损坏片被拒绝，其余片照常返回。
        assert bad2.status_code == 409
        assert ok1.status_code == 200 and ok3.status_code == 200
        assert ok1.headers["x-payload-sha256"] == m["shards"][0]["payload_sha256"]
    finally:
        path.write_bytes(backup)
    # 恢复后损坏片也能取回。
    recovered = client.get(f"/manifests/{mid}/shards/2")
    assert recovered.status_code == 200
    assert recovered.headers["x-payload-sha256"] == target["payload_sha256"]


def test_short_read_rejected(monkeypatch, app_client):
    """字节读不全（read_range 抛 BlobError）时取片被拒绝。"""
    client, _, _ = app_client
    _, (id1, _, _) = _publish_three(client)
    cp = client.get("/checkpoints/latest").json()["seq"]
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": [id1],
            "max_records_per_shard": 2,
        },
    )
    mid = r.json()["manifest_id"]

    def boom_range(self, digest, start, end):
        raise BlobError("原件字节读不全：期望 20 字节，实际 7 字节")

    monkeypatch.setattr(main_mod.BlobStore, "read_range", boom_range)
    resp = client.get(f"/manifests/{mid}/shards/1")
    assert resp.status_code == 409 and "读不全" in resp.json()["detail"]


def test_manifest_404_and_shard_404(app_client):
    client, _, _ = app_client
    _, (id1, _, _) = _publish_three(client)
    cp = client.get("/checkpoints/latest").json()["seq"]
    assert client.get("/manifests/999").status_code == 404
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": [id1],
            "max_records_per_shard": 2,
        },
    )
    mid = r.json()["manifest_id"]
    total = r.json()["shard_count"]
    assert client.get(f"/manifests/{mid}/shards/{total + 1}").status_code == 404


def test_manifest_persists_across_restart(app_client):
    """重启视角：清单与片号从共享卷上的 SQLite 复现，不依赖进程内存。"""
    client, _, settings = app_client
    _, ids = _publish_three(client)
    cp = client.get("/checkpoints/latest").json()["seq"]
    r = client.post(
        "/manifests",
        json={
            "checkpoint_seq": cp,
            "release_ids": ids,
            "max_records_per_shard": 1,
        },
    )
    before = r.json()
    # 用同一 DATA_DIR 新建 Database 实例，等价于另一实例/重启后挂载同卷。
    from app.db import Database

    fresh_db = Database(settings)
    reloaded = fresh_db.get_manifest(before["manifest_id"])
    assert reloaded is not None
    assert reloaded["checkpoint_seq"] == cp
    assert [s["shard_no"] for s in reloaded["shards"]] == list(
        range(1, before["shard_count"] + 1)
    )
    # 同指纹请求在新实例上仍命中同一清单。
    key = before["request_key"]
    assert fresh_db.get_manifest_by_request_key(key)["id"] == before["manifest_id"]
