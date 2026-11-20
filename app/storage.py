"""原件字节存储：只写不改、内容寻址、原子落盘。

* 文件按 SHA-256 摘要分片存放（``ab/cd/<digest>``），数据库只保存目录与摘要；
* 先写临时文件并 ``fsync``，再 ``os.replace`` 原子改名，随后 ``fsync`` 目录；
* 任何一步失败都不会留下被数据库引用的缺件，失败后可安全重试。
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

from .config import Settings


class BlobError(Exception):
    """写盘失败（空间、权限、IO 等）。"""


class BlobStore:
    def __init__(self, settings: Settings):
        self._settings = settings
        settings.ensure_dirs()

    @staticmethod
    def _target_path(blob_dir: Path, digest: str) -> Path:
        return blob_dir / digest[:2] / digest[2:4] / digest

    def path_for(self, digest: str) -> Path:
        return self._target_path(self._settings.blob_dir, digest)

    def exists(self, digest: str) -> bool:
        return self._target_path(self._settings.blob_dir, digest).is_file()

    def store(self, data: bytes) -> tuple[str, Path]:
        """原子写入原件，返回 (sha256, 最终路径)。

        同摘要重复写入直接复用，绝不覆盖既有内容。
        """
        digest = hashlib.sha256(data).hexdigest()
        target = self._target_path(self._settings.blob_dir, digest)
        if target.is_file():
            # 复用同内容原件（同单同输入重试，或不同单据的相同字节）。
            return digest, target
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._settings.temp_dir / f".upload-{digest}-{os.getpid()}"
        try:
            with open(tmp_path, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, target)
            self._fsync_dir(target.parent)
        except OSError as exc:
            tmp_path.unlink(missing_ok=True)
            raise BlobError(f"原件写盘失败: {exc}") from exc
        return digest, target

    def read(self, digest: str) -> bytes:
        return self._target_path(self._settings.blob_dir, digest).read_bytes()

    def read_range(self, digest: str, start: int, end: int) -> bytes:
        """读取原件半开字节区间 [start, end)。

        专门服务按片取回：调用方只读取表头与该片所需字节，不全量加载原件。
        读不到文件或实际读得字节数与请求长度不一致时抛出 :class:`BlobError`，
        由上层拒绝本次取片（绝不返回不完整数据）。
        """
        if start < 0 or end < start:
            raise BlobError(f"非法字节区间 [{start}, {end})")
        path = self._target_path(self._settings.blob_dir, digest)
        if not path.is_file():
            raise BlobError(f"原件缺失: {digest[:12]}...")
        wanted = end - start
        try:
            with open(path, "rb") as handle:
                handle.seek(start)
                chunk = handle.read(wanted)
        except OSError as exc:
            raise BlobError(f"原件字节读取失败: {exc}") from exc
        if len(chunk) != wanted:
            raise BlobError(
                f"原件字节读不全：期望 {wanted} 字节，实际 {len(chunk)} 字节"
            )
        return chunk

    def file_size(self, digest: str) -> int:
        path = self._target_path(self._settings.blob_dir, digest)
        if not path.is_file():
            raise BlobError(f"原件缺失: {digest[:12]}...")
        return path.stat().st_size

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def verify_present(self, digest: str) -> bool:
        """启动自检用：摘要与文件实际字节一致且存在。"""
        path = self._target_path(self._settings.blob_dir, digest)
        if not path.is_file():
            return False
        return hashlib.sha256(path.read_bytes()).hexdigest() == digest
