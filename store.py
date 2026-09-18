"""仅追加事件存储与可核验审计档案。

- SQLite 保存事件信封（全局序号 seq、流内序号 stream_seq、prev_hash、hash）。
- 全部写入在同一把锁 + BEGIN IMMEDIATE 事务内完成，并发签署串行化，
  重复 client_key / 重复签署由数据库约束兜底。
- 每次提交同步追加一行 JSONL 审计档案，档案可离线独立重放核验。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any

from domain import (
    GENESIS_HASH,
    SCHEMA_VERSION,
    ChainBroken,
    Conflict,
    canonical_json,
    content_hash,
    now_utc,
)
from projection import Projection

_DDL = """
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    stream_id TEXT NOT NULL,
    stream_seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    hash TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    client_key TEXT,
    UNIQUE(stream_id, stream_seq)
);
CREATE TABLE IF NOT EXISTS signatures (
    handover_id TEXT NOT NULL,
    party_key TEXT NOT NULL,
    event_id TEXT NOT NULL,
    PRIMARY KEY (handover_id, party_key)
) WITHOUT ROWID;
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_client_key
    ON events(client_key) WHERE client_key IS NOT NULL;
"""


def envelope_hash(
    *,
    event_id: str,
    stream_id: str,
    seq: int,
    stream_seq: int,
    event_type: str,
    payload: Any,
    prev_hash: str,
    recorded_at: str,
) -> str:
    body = {
        "schema": SCHEMA_VERSION,
        "event_id": event_id,
        "stream_id": stream_id,
        "seq": seq,
        "stream_seq": stream_seq,
        "event_type": event_type,
        "payload": payload,
        "prev_hash": prev_hash,
        "recorded_at": recorded_at,
    }
    return content_hash(canonical_json(body))


class EventStore:
    def __init__(self, db_path: str, audit_path: str | None = None) -> None:
        self.db_path = db_path
        self.audit_path = audit_path or (
            os.path.join(os.path.dirname(db_path) or ".", "audit.jsonl")
        )
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(_DDL)
        self.projection = Projection()
        self._reload()
        self._audit_fh = open(self.audit_path, "a", encoding="utf-8")

    # -- 内部 ---------------------------------------------------------------

    def _reload(self) -> None:
        self.projection = Projection()
        rows = self.conn.execute(
            "SELECT * FROM events ORDER BY seq"
        ).fetchall()
        prev = GENESIS_HASH
        for row in rows:
            envelope = self._row_to_envelope(row)
            if envelope["prev_hash"] != prev:
                raise ChainBroken(
                    f"事件 {envelope['event_id']} 前序哈希不匹配，档案可能被改动"
                )
            if envelope["hash"] != self._hash_envelope(envelope):
                raise ChainBroken(
                    f"事件 {envelope['event_id']} 内容哈希不匹配，档案可能被改动"
                )
            self.projection.apply(envelope)
            prev = envelope["hash"]
        self._head = prev

    @staticmethod
    def _row_to_envelope(row: sqlite3.Row) -> dict:
        return {
            "seq": row["seq"],
            "event_id": row["event_id"],
            "stream_id": row["stream_id"],
            "stream_seq": row["stream_seq"],
            "event_type": row["event_type"],
            "payload": json.loads(row["payload"]),
            "prev_hash": row["prev_hash"],
            "hash": row["hash"],
            "recorded_at": row["recorded_at"],
            "client_key": row["client_key"],
            "schema": SCHEMA_VERSION,
        }

    @staticmethod
    def _hash_envelope(envelope: dict) -> str:
        return envelope_hash(
            event_id=envelope["event_id"],
            stream_id=envelope["stream_id"],
            seq=envelope["seq"],
            stream_seq=envelope["stream_seq"],
            event_type=envelope["event_type"],
            payload=envelope["payload"],
            prev_hash=envelope["prev_hash"],
            recorded_at=envelope["recorded_at"],
        )

    def _write_audit_line(self, envelope: dict) -> None:
        self._audit_fh.write(
            json.dumps(envelope, sort_keys=True, ensure_ascii=False) + "\n"
        )
        self._audit_fh.flush()
        os.fsync(self._audit_fh.fileno())

    # -- 公共 API -----------------------------------------------------------

    @property
    def head(self) -> str:
        return self._head

    def append_many(self, specs: list[dict]) -> tuple[list[dict], bool]:
        """在一个事务内追加多个事件，全部成功或全部不落库。

        spec 字段：event_type, stream_id, payload, event_id,
        client_key(可选), signature(可选 (handover_id, party_key))。
        返回 (信封列表, 是否幂等重放)。带 client_key 的命令必须是单事件。
        """
        with self.lock:
            if len(specs) == 1 and specs[0].get("client_key"):
                existing = self.find_client_key(specs[0]["client_key"])
                if existing is not None:
                    return [existing], True

            recorded_at = now_utc().isoformat()
            envelopes: list[dict] = []
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                prev_hash = self._head
                for spec in specs:
                    seq_row = self.conn.execute(
                        "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM events"
                    ).fetchone()
                    seq = seq_row["next_seq"]
                    stream_seq_row = self.conn.execute(
                        "SELECT COALESCE(MAX(stream_seq), 0) + 1 AS next "
                        "FROM events WHERE stream_id = ?",
                        (spec["stream_id"],),
                    ).fetchone()
                    stream_seq = stream_seq_row["next"]
                    digest = envelope_hash(
                        event_id=spec["event_id"],
                        stream_id=spec["stream_id"],
                        seq=seq,
                        stream_seq=stream_seq,
                        event_type=spec["event_type"],
                        payload=spec["payload"],
                        prev_hash=prev_hash,
                        recorded_at=recorded_at,
                    )
                    self.conn.execute(
                        "INSERT INTO events (seq, event_id, stream_id, stream_seq, "
                        "event_type, payload, prev_hash, hash, recorded_at, client_key) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (
                            seq,
                            spec["event_id"],
                            spec["stream_id"],
                            stream_seq,
                            spec["event_type"],
                            json.dumps(spec["payload"], ensure_ascii=False),
                            prev_hash,
                            digest,
                            recorded_at,
                            spec.get("client_key"),
                        ),
                    )
                    if spec.get("signature"):
                        handover_id, party_key = spec["signature"]
                        self.conn.execute(
                            "INSERT INTO signatures (handover_id, party_key, event_id) "
                            "VALUES (?,?,?)",
                            (handover_id, party_key, spec["event_id"]),
                        )
                    envelope = {
                        "seq": seq,
                        "event_id": spec["event_id"],
                        "stream_id": spec["stream_id"],
                        "stream_seq": stream_seq,
                        "event_type": spec["event_type"],
                        "payload": spec["payload"],
                        "prev_hash": prev_hash,
                        "hash": digest,
                        "recorded_at": recorded_at,
                        "client_key": spec.get("client_key"),
                        "schema": SCHEMA_VERSION,
                    }
                    envelopes.append(envelope)
                    prev_hash = digest
                self.conn.execute("COMMIT")
            except sqlite3.IntegrityError as exc:
                self.conn.execute("ROLLBACK")
                message = str(exc)
                if "signatures" in message:
                    raise Conflict("该方已完成签署，请勿重复签署", code="already_signed")
                # 并发下 client_key 可能在事务外检查后才落入
                if len(specs) == 1 and specs[0].get("client_key"):
                    existing = self.find_client_key(specs[0]["client_key"])
                    if existing is not None:
                        return [existing], True
                raise Conflict(f"并发写入冲突: {message}")

            for envelope in envelopes:
                self.projection.apply(envelope)
                self._write_audit_line(envelope)
            self._head = prev_hash
            return envelopes, False

    def append(
        self,
        event_type: str,
        stream_id: str,
        payload: dict,
        *,
        event_id: str,
        client_key: str | None = None,
        signature: tuple[str, str] | None = None,
    ) -> tuple[dict, bool]:
        """追加单个事件，返回 (信封, 是否幂等重放)。"""
        envelopes, replayed = self.append_many(
            [
                {
                    "event_type": event_type,
                    "stream_id": stream_id,
                    "payload": payload,
                    "event_id": event_id,
                    "client_key": client_key,
                    "signature": signature,
                }
            ]
        )
        return envelopes[0], replayed

    def events(self, stream_id: str | None = None) -> list[dict]:
        if stream_id is None:
            rows = self.conn.execute("SELECT * FROM events ORDER BY seq").fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM events WHERE stream_id = ? ORDER BY seq", (stream_id,)
            ).fetchall()
        return [self._row_to_envelope(row) for row in rows]

    def get_event(self, event_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        return self._row_to_envelope(row) if row else None

    def find_client_key(self, client_key: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM events WHERE client_key = ?", (client_key,)
        ).fetchone()
        return self._row_to_envelope(row) if row else None

    def verify(self) -> dict:
        """全量重算哈希链并独立重放投影，输出可核验结论。"""
        rows = self.conn.execute(
            "SELECT event_id, stream_id, stream_seq, hash FROM events ORDER BY seq"
        ).fetchall()
        fresh = Projection()
        prev = GENESIS_HASH
        stream_counts: dict[str, int] = {}
        checked = 0
        for row in self.conn.execute("SELECT * FROM events ORDER BY seq"):
            envelope = self._row_to_envelope(row)
            if envelope["prev_hash"] != prev:
                raise ChainBroken(f"事件 {envelope['event_id']} 断链")
            if envelope["hash"] != self._hash_envelope(envelope):
                raise ChainBroken(f"事件 {envelope['event_id']} 哈希被改动")
            stream_counts[envelope["stream_id"]] = (
                stream_counts.get(envelope["stream_id"], 0) + 1
            )
            if envelope["stream_seq"] != stream_counts[envelope["stream_id"]]:
                raise ChainBroken(
                    f"流 {envelope['stream_id']} 序号不连续 "
                    f"({envelope['stream_seq']} != {stream_counts[envelope['stream_id']]})"
                )
            fresh.apply(envelope)
            prev = envelope["hash"]
            checked += 1
        return {
            "ok": True,
            "events": checked,
            "head": prev,
            "streams": len(stream_counts),
            "note": "全部事件哈希、前序链接与流内序号校验通过",
        }

    def close(self) -> None:
        with self.lock:
            self._audit_fh.close()
            self.conn.close()
