"""仅追加（append-only）事件账本持久化。

- 事件写入 ``journal.jsonl``，每行一条带哈希链的信封，文件永不重写历史；
- ``idem.json`` 记录幂等键 -> 事件 ID，重复回调直接返回首次结果；
- ``snapshot.json`` 为加速启动的投影缓存，重建后必须与重放结果一致；
- :meth:`EventStore.verify` 逐条重算哈希链，任何篡改、缺行、断链都会抛出
  :class:`JournalIntegrityError`，审计档案据此自证未被改写。
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from typing import Callable

from events import event_hash

GENESIS_HASH = "0" * 64


class JournalIntegrityError(RuntimeError):
    """审计哈希链校验失败。"""


class EventStore:
    def __init__(self, data_dir: str, clock: Callable[[], str] | None = None):
        self.data_dir = data_dir
        self._clock = clock
        os.makedirs(data_dir, exist_ok=True)
        self.journal_path = os.path.join(data_dir, "journal.jsonl")
        self.idem_path = os.path.join(data_dir, "idem.json")
        self.snapshot_path = os.path.join(data_dir, "snapshot.json")
        self._lock = threading.RLock()
        self._seq = 0
        self._tail_hash = GENESIS_HASH
        self._idem: dict[str, dict] = {}
        if os.path.exists(self.journal_path):
            self._load()
        else:
            self._load_idem()

    # ------------------------------------------------------------------ 读取

    def _load_idem(self) -> None:
        if os.path.exists(self.idem_path):
            with open(self.idem_path, encoding="utf-8") as handle:
                self._idem = json.load(handle)

    def _load(self) -> None:
        """启动重放：校验哈希链并恢复序号、链尾与幂等索引。"""

        self._seq = 0
        prev_hash = GENESIS_HASH
        self._tail_hash = GENESIS_HASH
        self._idem = {}
        with open(self.journal_path, encoding="utf-8") as handle:
            for raw_lineno, line in enumerate(handle, 1):
                line = line.strip()
                if not line:
                    continue
                self._seq += 1
                lineno = self._seq
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise JournalIntegrityError(
                        f"journal.jsonl 第 {raw_lineno} 行不是合法 JSON"
                    ) from exc
                self._check_event(event, lineno)
                if event["prev_hash"] != prev_hash:
                    raise JournalIntegrityError(f"第 {lineno} 条事件前链断裂")
                prev_hash = event["hash"]
                self._tail_hash = event["hash"]
                idem_key = event.get("idem_key")
                if idem_key:
                    self._idem[idem_key] = {
                        "event_id": event["event_id"],
                        "event_type": event["event_type"],
                        "fingerprint": event.get("idem_fingerprint"),
                    }
        self._atomic_write_text(self.idem_path, json.dumps(self._idem, ensure_ascii=False, indent=2))

    @staticmethod
    def _check_event(event: dict, lineno: int) -> None:
        for field in ("seq", "prev_hash", "hash", "event_id", "event_type"):
            if field not in event:
                raise JournalIntegrityError(f"第 {lineno} 行缺少字段 {field}")
        if event["seq"] != lineno:
            raise JournalIntegrityError(
                f"第 {lineno} 行顺序号为 {event['seq']}，哈希链断裂"
            )
        if lineno == 1 and event["prev_hash"] != GENESIS_HASH:
            raise JournalIntegrityError("首条事件 prev_hash 必须为创世哈希")
        actual = event_hash(event)
        if actual != event["hash"]:
            raise JournalIntegrityError(f"第 {lineno} 行内容哈希不一致，记录被改动")

    def verify(self) -> dict:
        """重算整条哈希链，返回审计摘要。以磁盘账本自身为核验依据。"""

        with self._lock:
            count = 0
            prev = GENESIS_HASH
            last_event_id = None
            tail = GENESIS_HASH
            with open(self.journal_path, encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    count += 1
                    event = json.loads(line)
                    if event["seq"] != count:
                        raise JournalIntegrityError(f"第 {count} 条事件顺序号错乱")
                    if event["prev_hash"] != prev:
                        raise JournalIntegrityError(f"第 {count} 条事件前链断裂")
                    if event_hash(event) != event["hash"]:
                        raise JournalIntegrityError(f"第 {count} 条事件哈希不匹配")
                    prev = event["hash"]
                    tail = event["hash"]
                    last_event_id = event["event_id"]
            return {
                "events": count,
                "tail_hash": tail,
                "last_event_id": last_event_id,
                "verified": True,
                "in_sync": count == self._seq and tail == self._tail_hash,
            }

    def read_events(self) -> list[dict]:
        with self._lock:
            if not os.path.exists(self.journal_path):
                return []
            with open(self.journal_path, encoding="utf-8") as handle:
                return [json.loads(line) for line in handle if line.strip()]

    def find_idem(self, idem_key: str) -> dict | None:
        with self._lock:
            record = self._idem.get(idem_key)
            return dict(record) if record else None

    # ------------------------------------------------------------------ 写入

    def append(self, raw_event: dict, fingerprint: str | None = None) -> dict:
        """追加事件。调用方必须先在自身锁内完成幂等与业务校验。"""

        with self._lock:
            self._seq += 1
            event = dict(raw_event)
            event["seq"] = self._seq
            event["prev_hash"] = self._tail_hash
            if fingerprint:
                event["idem_fingerprint"] = fingerprint
            event["hash"] = event_hash(event)
            with open(self.journal_path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, ensure_ascii=False) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            self._tail_hash = event["hash"]
            if event.get("idem_key"):
                self._idem[event["idem_key"]] = {
                    "event_id": event["event_id"],
                    "event_type": event["event_type"],
                    "fingerprint": fingerprint,
                }
                self._atomic_write_text(
                    self.idem_path, json.dumps(self._idem, ensure_ascii=False, indent=2)
                )
            return event

    def save_snapshot(self, state: dict) -> None:
        with self._lock:
            self._atomic_write_text(
                self.snapshot_path, json.dumps(state, ensure_ascii=False)
            )

    def load_snapshot(self) -> dict | None:
        with self._lock:
            if not os.path.exists(self.snapshot_path):
                return None
            with open(self.snapshot_path, encoding="utf-8") as handle:
                return json.load(handle)

    @staticmethod
    def _atomic_write_text(path: str, text: str) -> None:
        directory = os.path.dirname(path) or "."
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".json")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    @property
    def seq(self) -> int:
        return self._seq

    @property
    def tail_hash(self) -> str:
        return self._tail_hash
