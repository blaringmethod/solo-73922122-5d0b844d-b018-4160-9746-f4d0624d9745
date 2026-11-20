"""SQLite 持久层：目录与摘要（不存原件字节）。

并发模型
--------
* WAL 模式 + ``busy_timeout``：两个实例共享同一库文件，读者不阻塞写者；
* 发布事务使用 ``BEGIN IMMEDIATE``，跨实例写操作被数据库锁串行化，
  后到者阻塞等待后重检唯一键，因此不会分叉、不会互相覆盖。
* 无任何 UPDATE/DELETE 路径，发布后记录不可改、不可删。
"""
from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Sequence

from . import merkle
from .config import Settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS releases (
    id           INTEGER PRIMARY KEY,          -- 1 起稠密序号，叶子下标 = id - 1
    station_id   TEXT NOT NULL,
    sample_date  TEXT NOT NULL,
    order_no     TEXT NOT NULL,
    blob_sha256  TEXT NOT NULL,
    size         INTEGER NOT NULL,
    created_at   TEXT NOT NULL,
    UNIQUE(station_id, sample_date, order_no)
);

CREATE TABLE IF NOT EXISTS checkpoints (
    seq         INTEGER PRIMARY KEY,            -- 该检查点对应的条数
    root_hex    TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);

-- 分片清单：制单成功即冻结，只增不改。request_key 为请求指纹，保证幂等重试。
CREATE TABLE IF NOT EXISTS manifests (
    id                      INTEGER PRIMARY KEY,   -- 清单编号，1 起稠密
    request_key             TEXT NOT NULL UNIQUE,
    checkpoint_seq          INTEGER NOT NULL,
    max_records_per_shard   INTEGER NOT NULL,
    release_ids_json        TEXT NOT NULL,         -- 规范化排序后的发布序号
    shard_count             INTEGER NOT NULL,
    created_at              TEXT NOT NULL
);

-- 清单内各片：片号在清单内稠密，字节区间与载荷摘要在制单时固定。
CREATE TABLE IF NOT EXISTS manifest_shards (
    manifest_id      INTEGER NOT NULL,
    shard_no         INTEGER NOT NULL,
    release_id       INTEGER NOT NULL,
    blob_sha256      TEXT NOT NULL,
    record_lo        INTEGER NOT NULL,
    record_hi        INTEGER NOT NULL,
    byte_start       INTEGER NOT NULL,
    byte_end         INTEGER NOT NULL,
    header_start     INTEGER NOT NULL,
    header_end       INTEGER NOT NULL,
    record_count     INTEGER NOT NULL,
    payload_sha256   TEXT NOT NULL,
    payload_size     INTEGER NOT NULL,
    PRIMARY KEY (manifest_id, shard_no)
);
"""


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class ConflictError(Exception):
    """业务冲突：异输入重试或超出累计上限。"""


class Database:
    def __init__(self, settings: Settings):
        self._settings = settings
        settings.ensure_dirs()
        self._init_schema()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(
            self._settings.db_path,
            timeout=15.0,
            isolation_level=None,  # 手动事务，显式 BEGIN/COMMIT
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = 15000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executescript(SCHEMA)
                # 零号检查点（空树根），幂等创建。
                empty = merkle.empty_root(self._settings.tree_depth).hex()
                conn.execute(
                    "INSERT OR IGNORE INTO checkpoints(seq, root_hex, created_at) "
                    "VALUES (0, ?, ?)",
                    (empty, _utcnow()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    # ------------------------------------------------------------------ #
    # 发布
    # ------------------------------------------------------------------ #
    def publish(
        self,
        station_id: str,
        sample_date: str,
        order_no: str,
        blob_sha256: str,
        size: int,
    ) -> tuple[dict, bool]:
        """串行化发布。返回 (发布记录, 是否复用既有记录)。

        调用方必须已把原件原子落盘成功后再调用本方法，
        从而保证“库中有记录 => 卷上有完整原件”。
        """
        # 双实例并发时 BEGIN IMMEDIATE 可能撞上写锁；busy_timeout 之外再有限重试。
        for attempt in range(3):
            try:
                return self._publish_once(
                    station_id, sample_date, order_no, blob_sha256, size
                )
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() and attempt < 2:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                raise
        raise RuntimeError("unreachable")  # pragma: no cover

    def _publish_once(
        self,
        station_id: str,
        sample_date: str,
        order_no: str,
        blob_sha256: str,
        size: int,
    ) -> tuple[dict, bool]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM releases "
                    "WHERE station_id = ? AND sample_date = ? AND order_no = ?",
                    (station_id, sample_date, order_no),
                ).fetchone()
                if row is not None:
                    record = dict(row)
                    same = (
                        record["blob_sha256"] == blob_sha256
                        and record["size"] == size
                    )
                    conn.rollback()  # 只读复用，不产生写入
                    if not same:
                        raise ConflictError(
                            "同一站号/日期/发布单号已存在且原件内容不同，禁止改写"
                        )
                    return record, True

                count = conn.execute(
                    "SELECT COUNT(*) AS c FROM releases"
                ).fetchone()["c"]
                if count >= self._settings.max_releases:
                    conn.rollback()
                    raise ConflictError(
                        f"累计发布已达上限 {self._settings.max_releases} 件"
                    )

                cursor = conn.execute(
                    "INSERT INTO releases(station_id, sample_date, order_no, "
                    "blob_sha256, size, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        station_id,
                        sample_date,
                        order_no,
                        blob_sha256,
                        size,
                        _utcnow(),
                    ),
                )
                release_id = cursor.lastrowid
                # 无删除路径，rowid 必然稠密且等于新条数。
                assert release_id == count + 1

                leaves = self._leaf_hashes_locked(conn)
                root = merkle.root_from_leaves(leaves, self._settings.tree_depth)
                conn.execute(
                    "INSERT INTO checkpoints(seq, root_hex, created_at) "
                    "VALUES (?, ?, ?)",
                    (len(leaves), root.hex(), _utcnow()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        record = self.get_release_by_id(release_id)
        return record, False

    @staticmethod
    def _leaf_hashes_locked(conn: sqlite3.Connection) -> list[bytes]:
        rows = conn.execute(
            "SELECT station_id, sample_date, order_no, blob_sha256, size "
            "FROM releases ORDER BY id ASC"
        ).fetchall()
        return [
            merkle.leaf_hash(
                r["station_id"],
                r["sample_date"],
                r["order_no"],
                r["blob_sha256"],
                r["size"],
            )
            for r in rows
        ]

    def leaf_hashes(self) -> list[bytes]:
        with self.connect() as conn:
            return self._leaf_hashes_locked(conn)

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def get_release(self, station_id: str, sample_date: str, order_no: str):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE station_id = ? AND sample_date = ? "
                "AND order_no = ?",
                (station_id, sample_date, order_no),
            ).fetchone()
            return dict(row) if row else None

    def get_release_by_id(self, release_id: int):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE id = ?", (release_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_releases(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM releases ORDER BY id ASC").fetchall()
            return [dict(r) for r in rows]

    def latest_checkpoint(self) -> dict:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoints ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            return dict(row)

    def get_checkpoint(self, seq: int):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoints WHERE seq = ?", (seq,)
            ).fetchone()
            return dict(row) if row else None

    def list_checkpoints(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM checkpoints ORDER BY seq ASC"
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # 分片清单
    # ------------------------------------------------------------------ #
    def get_manifest_by_request_key(self, request_key: str):
        """幂等查重：同请求指纹返回既有清单（含全部片），否则 None。"""
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM manifests WHERE request_key = ?", (request_key,)
            ).fetchone()
            if row is None:
                return None
            manifest = dict(row)
            manifest["shards"] = self._shards_locked(conn, manifest["id"])
            return manifest

    def get_manifest(self, manifest_id: int):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM manifests WHERE id = ?", (manifest_id,)
            ).fetchone()
            if row is None:
                return None
            manifest = dict(row)
            manifest["shards"] = self._shards_locked(conn, manifest_id)
            return manifest

    def list_manifests(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id, request_key, checkpoint_seq, max_records_per_shard, "
                "release_ids_json, shard_count, created_at "
                "FROM manifests ORDER BY id ASC"
            ).fetchall()
            return [dict(r) for r in rows]

    @staticmethod
    def _shards_locked(conn: sqlite3.Connection, manifest_id: int) -> list[dict]:
        rows = conn.execute(
            "SELECT * FROM manifest_shards WHERE manifest_id = ? ORDER BY shard_no",
            (manifest_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_shard(self, manifest_id: int, shard_no: int):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM manifest_shards WHERE manifest_id = ? AND shard_no = ?",
                (manifest_id, shard_no),
            ).fetchone()
            return dict(row) if row else None

    def insert_manifest(
        self,
        request_key: str,
        checkpoint_seq: int,
        max_records_per_shard: int,
        release_ids: list[int],
        shard_plans,
    ) -> tuple[dict, bool]:
        """原子提交整份清单及其分片。

        调用方必须已完成全部原件存在性、摘要与 CSV 格式校验；
        本方法在单个 BEGIN IMMEDIATE 事务内写入，失败整体回滚，不留半成品。
        并发同指纹请求靠 request_key 唯一约束串行化，后到者复用先提交的清单。
        返回 (清单, 是否复用了并发中已先提交的同指纹清单)。
        """
        import json as _json

        for attempt in range(3):
            try:
                with self.connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    try:
                        existing = conn.execute(
                            "SELECT id FROM manifests WHERE request_key = ?",
                            (request_key,),
                        ).fetchone()
                        if existing is not None:
                            conn.rollback()
                            return self.get_manifest(existing["id"]), True
                        cursor = conn.execute(
                            "INSERT INTO manifests(request_key, checkpoint_seq, "
                            "max_records_per_shard, release_ids_json, shard_count, "
                            "created_at) VALUES (?, ?, ?, ?, ?, ?)",
                            (
                                request_key,
                                checkpoint_seq,
                                max_records_per_shard,
                                _json.dumps(release_ids, separators=(",", ":")),
                                len(shard_plans),
                                _utcnow(),
                            ),
                        )
                        manifest_id = cursor.lastrowid
                        conn.executemany(
                            "INSERT INTO manifest_shards(manifest_id, shard_no, "
                            "release_id, blob_sha256, record_lo, record_hi, "
                            "byte_start, byte_end, header_start, header_end, "
                            "record_count, payload_sha256, payload_size) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            [
                                (
                                    manifest_id,
                                    p.shard_no,
                                    p.release_id,
                                    p.blob_sha256,
                                    p.record_lo,
                                    p.record_hi,
                                    p.byte_start,
                                    p.byte_end,
                                    p.header_start,
                                    p.header_end,
                                    p.record_count,
                                    p.payload_sha256,
                                    p.payload_size,
                                )
                                for p in shard_plans
                            ],
                        )
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        raise
                return self.get_manifest(manifest_id), False
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() and attempt < 2:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                raise
        raise RuntimeError("unreachable")  # pragma: no cover

    # ------------------------------------------------------------------ #
    # 启动自检：不得带着缺件记录对外服务
    # ------------------------------------------------------------------ #
    def integrity_problems(self, blob_verifier) -> list[str]:
        problems: list[str] = []
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id, blob_sha256 FROM releases ORDER BY id"
            ).fetchall()
            expected_ids = list(range(1, len(rows) + 1))
            actual_ids = [r["id"] for r in rows]
            if actual_ids != expected_ids:
                problems.append("releases 主键不稠密，存在缺口或乱序")
            for row in rows:
                if not blob_verifier(row["blob_sha256"]):
                    problems.append(
                        f"release id={row['id']} 的原件缺失或摘要不符 "
                        f"(sha256={row['blob_sha256'][:12]}...)"
                    )
            leaves = self._leaf_hashes_locked(conn)
            root = merkle.root_from_leaves(leaves, self._settings.tree_depth)
            cp_rows = conn.execute(
                "SELECT seq, root_hex FROM checkpoints ORDER BY seq"
            ).fetchall()
            cp_seqs = [r["seq"] for r in cp_rows]
            if cp_seqs != list(range(len(cp_seqs))):
                problems.append("checkpoints 序号不连续")
            latest = cp_rows[-1]
            if latest["seq"] != len(leaves) or latest["root_hex"] != root.hex():
                problems.append("最新检查点与实际叶子集合不一致")
            # 分片清单自检：片号稠密、每片引用的发布与摘要存在且可核验。
            manifests = conn.execute(
                "SELECT id, shard_count FROM manifests ORDER BY id"
            ).fetchall()
            expected_mid = 1
            for mf in manifests:
                if mf["id"] != expected_mid:
                    problems.append(
                        f"manifests 编号不稠密：期望 {expected_mid}，实际 {mf['id']}"
                    )
                expected_mid += 1
                shard_rows = conn.execute(
                    "SELECT shard_no, release_id, blob_sha256, payload_sha256 "
                    "FROM manifest_shards WHERE manifest_id = ? ORDER BY shard_no",
                    (mf["id"],),
                ).fetchall()
                if [s["shard_no"] for s in shard_rows] != list(
                    range(1, mf["shard_count"] + 1)
                ):
                    problems.append(
                        f"manifest {mf['id']} 片号不稠密或片数不一致"
                    )
                for s in shard_rows:
                    if not blob_verifier(s["blob_sha256"]):
                        problems.append(
                            f"manifest {mf['id']} 片 {s['shard_no']} 的源原件"
                            "缺失或摘要不符"
                        )
        return problems
