"""带表头 UTF-8 CSV 的字节级记录解析（纯函数，无服务端状态）。

划片必须按“完整记录”进行，且取片时要按字节范围读取原件，因此本模块直接在
**字节** 上按 RFC 4180 风格的引用规则切分记录，而不是先解码成文本：

* 记录分隔符为 ``\\n`` 或 ``\\r\\n``；引号字段内的换行属于同一条记录；
* 引号字段内 ``""`` 表示一个转义的字面引号；
* 原件必须是合法 UTF-8（允许文件开头的 BOM，BOM 计入原字节、原样返回）；
* 第一条记录为表头，其余为数据记录；
* 每条记录给出其在原件中的半开字节区间 ``[start, end)``，
  ``end`` 指向记录最后一个内容字节的下一个位置（不含行终止符）。

UTF-8 多字节字符不会被误切：其续字节均 >= 0x80，不可能与 ASCII 分隔符重合。
解析失败抛出 :class:`CsvFormatError`，调用方据此整单拒绝、清单不生效。
"""
from __future__ import annotations

from dataclasses import dataclass

# 记录分隔与引号字符（按字节比较）。
LF = 0x0A
CR = 0x0D
QUOTE = 0x22
COMMA = 0x2C
BOM = b"\xef\xbb\xbf"


class CsvFormatError(ValueError):
    """原件不是合法的带表头 UTF-8 CSV。"""


@dataclass(frozen=True)
class CsvRecord:
    """一条 CSV 记录在原件中的字节定位与解析后的字段。

    ``content_end`` 停在记录最后一个内容字节之后（不含行终止符）；
    ``end`` 则包含行终止符（LF 或 CRLF），文件尾无终止符时与 content_end 相同。
    划片使用 ``start``/``end``，因此相邻片的字节区间首尾相接、连续覆盖原件。
    """

    index: int  # 0 = 表头，>=1 为数据记录
    start: int  # 记录首字节偏移（含）
    content_end: int  # 记录内容末字节偏移（不含行终止符）
    end: int  # 记录整体末字节偏移（含行终止符，半开）
    fields: tuple[str, ...]

    @property
    def byte_range(self) -> tuple[int, int]:
        return self.start, self.end

    @property
    def content_byte_range(self) -> tuple[int, int]:
        return self.start, self.content_end


@dataclass(frozen=True)
class CsvLayout:
    """整份 CSV 的解析布局。"""

    header: CsvRecord
    records: tuple[CsvRecord, ...]  # 仅数据记录（不含表头），按出现顺序
    raw_size: int

    @property
    def data_record_count(self) -> int:
        return len(self.records)


def parse_csv(data: bytes) -> CsvLayout:
    """把带表头 UTF-8 CSV 原件解析为字节级记录布局。

    任何编码或引用格式错误都抛 :class:`CsvFormatError`；
    空文件、无表头、无数据记录同样视为无效。
    """
    if not isinstance(data, (bytes, bytearray)):
        raise CsvFormatError("原件必须是字节序列")
    if len(data) == 0:
        raise CsvFormatError("原件为空，不是有效的带表头 CSV")
    # 整体 UTF-8 校验：BOM 合法且保留在原字节中，字段解码时单独去 BOM。
    try:
        data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvFormatError(f"原件不是合法 UTF-8 编码: {exc}") from exc

    records: list[CsvRecord] = []
    n = len(data)
    pos = 0
    rec_start = 0
    # 字段以字节缓冲收集，整文件已通过 UTF-8 校验，切出的字段一定可独立解码。
    fields: list[str] = []
    field_buf = bytearray()
    in_quotes = False

    def finish_field(is_first_field: bool) -> None:
        raw = bytes(field_buf)
        if is_first_field and raw.startswith(BOM):
            raw = raw[len(BOM):]
        fields.append(raw.decode("utf-8"))
        field_buf.clear()

    def finish_record(content_end: int, end: int) -> None:
        finish_field(is_first_field=not fields)
        records.append(
            CsvRecord(
                index=len(records),
                start=rec_start,
                content_end=content_end,
                end=end,
                fields=tuple(fields),
            )
        )
        fields.clear()

    while pos < n:
        b = data[pos]
        if in_quotes:
            if b == QUOTE:
                if pos + 1 < n and data[pos + 1] == QUOTE:
                    # 转义引号 "" -> 字面引号，只保留一个引号字节。
                    field_buf.append(QUOTE)
                    pos += 2
                    continue
                # 引号闭合：之后只允许逗号、换行或文件尾。
                in_quotes = False
                pos += 1
                continue
            # 引号内的任意字节（含 CR/LF/逗号/多字节 UTF-8）都是字段内容。
            field_buf.append(b)
            pos += 1
            continue

        if b == QUOTE:
            if field_buf:
                # 字段已有非引号内容后再出现引号，属非法引用（RFC 4180 严格模式）。
                raise CsvFormatError(
                    f"字节偏移 {pos} 处引号未出现在字段起始位置，格式无效"
                )
            in_quotes = True
            pos += 1
            continue
        if b == COMMA:
            finish_field(is_first_field=not fields)
            pos += 1
            continue
        if b == CR:
            # 仅接受 CRLF 作为行尾；裸 CR 非法，避免歧义切分。
            if pos + 1 >= n or data[pos + 1] != LF:
                raise CsvFormatError(f"字节偏移 {pos} 处出现裸 CR，格式无效")
            finish_record(pos, pos + 2)  # 内容止于 CR，记录整体含 CRLF
            pos += 2  # 跳过 CRLF
            rec_start = pos
            continue
        if b == LF:
            finish_record(pos, pos + 1)  # 内容止于 LF 前，记录整体含 LF
            pos += 1
            rec_start = pos
            continue
        field_buf.append(b)
        pos += 1

    # 文件尾：引号未闭合非法；最后一条记录若有内容则收尾（允许无尾换行）。
    if in_quotes:
        raise CsvFormatError("引号字段在文件结束前未闭合，格式无效")
    if rec_start < n:
        finish_record(n, n)  # 无尾换行：内容与整体区间相同

    if not records:
        raise CsvFormatError("原件不含任何记录，缺少表头")
    if len(records) < 2:
        raise CsvFormatError("原件只有表头没有数据记录，格式无效")

    header = records[0]
    if len(header.fields) == 0 or all(f == "" for f in header.fields):
        raise CsvFormatError("表头为空，格式无效")

    return CsvLayout(
        header=header,
        records=tuple(records[1:]),
        raw_size=n,
    )


def shard_record_ranges(total: int, max_per_shard: int) -> list[tuple[int, int]]:
    """把 ``total`` 条数据记录按每片至多 ``max_per_shard`` 条切成稠密区间。

    返回若干 ``[lo, hi)``（记录序号，0 起，半开区间），
    保证每条记录恰好落入一片、区间连续无重叠、无缺口。
    """
    if total <= 0:
        raise CsvFormatError("没有数据记录可划片")
    if max_per_shard <= 0:
        raise ValueError("每片记录数上限必须为正整数")
    ranges: list[tuple[int, int]] = []
    lo = 0
    while lo < total:
        hi = min(lo + max_per_shard, total)
        ranges.append((lo, hi))
        lo = hi
    return ranges
