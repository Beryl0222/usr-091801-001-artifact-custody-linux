"""返还文物交接账 —— 领域内核。

只包含与存储、传输无关的领域约定：状态枚举、身份标识、规范化 JSON、
内容哈希、哈希链、时区处理与领域错误。所有状态推进都由事件承载，
已确认记录不做原地更新。
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import string
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

SERVICE_ID = "artifact-custody"
SERVICE_NAME = "返还文物交接账"
SCHEMA_VERSION = 1
GENESIS_HASH = "0" * 64

# 交接/事件生命周期状态
STATUS_DRAFT = "拟定"
STATUS_PENDING_SIGN = "待双方签署"
STATUS_EFFECTIVE = "已生效"
STATUS_WITHDRAWN = "已撤回"
STATUS_CORRECTED = "已更正"

LIFECYCLE_STATUSES = (
    STATUS_DRAFT,
    STATUS_PENDING_SIGN,
    STATUS_EFFECTIVE,
    STATUS_WITHDRAWN,
    STATUS_CORRECTED,
)

# 交接环节（保管链节点类型）
STAGE_SEIZURE = "查获"
STAGE_HANDOVER = "交接"
STAGE_ENTRY = "入境"
STAGE_ACCESSION = "入藏"
STAGE_INVENTORY = "盘点"
STAGES = (STAGE_SEIZURE, STAGE_HANDOVER, STAGE_ENTRY, STAGE_ACCESSION, STAGE_INVENTORY)

# 交接方向
DIR_RETURN = "返还"
DIR_INTERNAL = "内部移交"

# 当事方角色
PARTY_FOREIGN = "境外移交方"
PARTY_DOMESTIC = "国内接收方"
PARTY_CUSTODIAN = "保管方"

# 数量修订类型
SPLIT = "拆分"
MERGE = "合并"
CORRECTION = "身份更正"
QUANTITY_OPS = (SPLIT, MERGE, CORRECTION)

# 数量单位：按套计数 / 单件追踪
UNIT_SET = "套"
UNIT_PIECE = "件"

# 登记粒度
KIND_SET = "按套"
KIND_SINGLE = "单件"

# 待处置问题类型
ISSUE_OVERDUE = "超期未签"
ISSUE_QTY_MISMATCH = "实物数量不符"
ISSUE_CHAIN_GAP = "保管链断点"
ISSUE_STATUSES = ("待处置", "处置中", "已关闭")

_ID_ALPHABET = string.ascii_lowercase + string.digits


def new_id(prefix: str) -> str:
    """生成形如 CASE-a1b2… 的领域标识。"""
    body = "".join(secrets.choice(_ID_ALPHABET) for _ in range(10))
    return f"{prefix}-{body}"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_tz(name: str) -> ZoneInfo:
    """解析 IANA 时区名，非法时区抛出 DomainError。"""
    if not isinstance(name, str) or not name:
        raise DomainError("时区不能为空")
    try:
        return ZoneInfo(name)
    except Exception as exc:  # ZoneInfoNotFoundError
        raise DomainError(f"未知时区: {name}") from exc


_ISO_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?"
    r"(Z|[+-]\d{2}:\d{2})?$"
)


def parse_moment(value: Any, tz_name: str | None) -> dict:
    """把外部传入的时间解析为 {utc, local, tz}。

    - value 为 ISO 字符串：可带偏移；若不带偏移则用 tz_name 定位；
      local/tz 取该时刻在当事方时区的表示。
    - value 为空：取当前 UTC 时刻，再投影到 tz_name。
    """
    tzinfo = parse_tz(tz_name) if tz_name else timezone.utc
    if value in (None, ""):
        dt = now_utc().astimezone(tzinfo)
    else:
        if not isinstance(value, str) or not _ISO_RE.match(value.strip()):
            raise DomainError(f"时间格式无法解析: {value!r}")
        text = value.strip().replace(" ", "T")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise DomainError(f"时间格式无法解析: {value!r}") from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tzinfo)
        dt = dt.astimezone(tzinfo)
    return {
        "utc": dt.astimezone(timezone.utc).isoformat(),
        "local": dt.isoformat(),
        "tz": str(tzinfo),
    }


def canonical_json(value: Any) -> str:
    """规范化 JSON：键排序、无空白、非 ASCII 不转义，供哈希使用。"""
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def content_hash(value: Any) -> str:
    """对任意可 JSON 化内容计算 SHA-256；字符串可按原文（清单/照片字节另存指纹）。"""
    if isinstance(value, (bytes, bytearray)):
        return sha256_hex(bytes(value))
    if isinstance(value, str):
        return sha256_hex(value.encode("utf-8"))
    return sha256_hex(canonical_json(value).encode("utf-8"))


class DomainError(Exception):
    """业务规则冲突（不可重试或需修正后重试）。"""

    def __init__(self, message: str, code: str = "domain_error", status: int = 422):
        super().__init__(message)
        self.message = message
        self.code = code
        self.status = status


class NotFound(DomainError):
    def __init__(self, message: str):
        super().__init__(message, code="not_found", status=404)


class Conflict(DomainError):
    """并发/状态冲突，客户端应刷新后重试。"""

    def __init__(self, message: str, code: str = "conflict"):
        super().__init__(message, code=code, status=409)


class DuplicateCallback(Conflict):
    def __init__(self, client_key: str, event_id: str):
        super().__init__(
            f"重复请求 {client_key}，对应事件 {event_id}",
            code="duplicate_callback",
        )
        self.event_id = event_id


class ChainBroken(DomainError):
    def __init__(self, message: str):
        super().__init__(message, code="chain_broken", status=500)


def require(value: Any, field: str) -> Any:
    if value is None or (isinstance(value, str) and not value.strip()):
        raise DomainError(f"字段 {field} 不能为空")
    return value


def require_choice(value: str, choices, field: str) -> str:
    require(value, field)
    if value not in choices:
        raise DomainError(f"字段 {field} 取值非法: {value}，允许 {list(choices)}")
    return value


def require_int(value: Any, field: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DomainError(f"字段 {field} 必须为整数")
    if value < minimum:
        raise DomainError(f"字段 {field} 不能小于 {minimum}")
    return value
