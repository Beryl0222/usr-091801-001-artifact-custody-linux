"""事件信封、时间与内容哈希工具。

账本中的每条事件都采用统一信封：顺序号与前一条事件的哈希构成哈希链，
事件自身的规范化 JSON 哈希覆盖全部业务字段，任何原地篡改都会在
:func:`store.Journal.verify` 重放时暴露。
"""

from __future__ import annotations

import hashlib
import json
import secrets
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

# 交接状态约定（见 README）
STATUS_DRAFT = "拟定"
STATUS_PENDING = "待双方签署"
STATUS_EFFECTED = "已生效"
STATUS_WITHDRAWN = "已撤回"
STATUS_CORRECTED = "已更正"

PENDING_STATUSES = (STATUS_DRAFT, STATUS_PENDING)

# 保管链阶段：实物流转的四个责任节点
STAGE_SEIZED = "查获"
STAGE_HANDOVER = "交接"
STAGE_ENTRY = "入境"
STAGE_ACCESSION = "入藏"
CHAIN_STAGES = (STAGE_SEIZED, STAGE_HANDOVER, STAGE_ENTRY, STAGE_ACCESSION)

NEXT_STAGE = {
    STAGE_SEIZED: STAGE_HANDOVER,
    STAGE_HANDOVER: STAGE_ENTRY,
    STAGE_ENTRY: STAGE_ACCESSION,
}

# 待处置队列的原因码
REASON_OVERDUE = "超期未签"
REASON_DISCREPANCY = "实物数量不符"
REASON_BROKEN_CHAIN = "保管链断点"

ENVELOPE_KEYS = (
    "event_id",
    "event_type",
    "occurred_at",
    "at_local",
    "tz",
    "actor",
    "payload",
    "idem_key",
    "idem_fingerprint",
    "seq",
    "prev_hash",
)


class DomainError(Exception):
    """业务校验失败，映射为 HTTP 400。"""


class ConflictError(DomainError):
    """状态冲突或重复请求负载不一致，映射为 HTTP 409。"""


class NotFoundError(DomainError):
    """引用对象不存在，映射为 HTTP 404。"""


def canonical_json(value: Any) -> bytes:
    """以稳定排序、无多余空白的 UTF-8 JSON 作为哈希输入。"""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_hex(value: Any) -> str:
    if isinstance(value, str):
        value = value.encode("utf-8")
    elif not isinstance(value, (bytes, bytearray)):
        value = canonical_json(value)
    return hashlib.sha256(value).hexdigest()


def new_id(prefix: str) -> str:
    seed = f"{datetime.now(timezone.utc).isoformat()}:{secrets.token_hex(8)}"
    return f"{prefix}_{sha256_hex(seed)[:16]}"


def resolve_time(
    ts_local: str | None,
    tz: str | None,
    occurred_at: str | None = None,
) -> tuple[str, str | None, str | None]:
    """把签署方提供的本地时间解析为 ``(UTC, 本地时间, 时区)``。

    境外机构时间以原始时区保存（``at_local`` + ``tz``），同时记录 UTC；
    仅提供 UTC 时本地字段留空。
    """

    if ts_local is not None:
        if not tz:
            raise DomainError("提供本地时间时必须同时给出 tz 时区名")
        try:
            zone = ZoneInfo(tz)
        except Exception as exc:  # noqa: BLE001
            raise DomainError(f"无法识别的时区: {tz}") from exc
        try:
            parsed = datetime.fromisoformat(ts_local)
        except ValueError as exc:
            raise DomainError(f"无法解析的本地时间: {ts_local}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=zone)
        utc = parsed.astimezone(timezone.utc)
        at_local = parsed.astimezone(zone).isoformat()
        return utc.isoformat(), at_local, tz
    if occurred_at is not None:
        try:
            parsed = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DomainError(f"无法解析的 UTC 时间: {occurred_at}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat(), None, None
    now = datetime.now(timezone.utc)
    return now.isoformat(), None, None


def seal_event(raw: dict, seq: int, prev_hash: str) -> dict:
    """补全顺序号与哈希链字段，返回不可变事件信封。"""

    event = dict(raw)
    event["seq"] = seq
    event["prev_hash"] = prev_hash
    body = {key: event.get(key) for key in ENVELOPE_KEYS}
    event["hash"] = sha256_hex(body)
    return event


def event_hash(event: dict) -> str:
    body = {key: event.get(key) for key in ENVELOPE_KEYS}
    return sha256_hex(body)
