"""返还文物交接账领域服务。

所有写操作都以事件为唯一事实来源：命令先做业务校验，随后向仅追加账本
追加一条带签名时间（原始时区 + UTC）的事件，再折叠进内存投影。已生效
记录从不原地修改——拆分、成套合并、身份更正、交接更正都会产生新对象/
新事件并引用原对象，旧记录保留“已拆分/已并入/已更正”状态。

套与单件：合并成套时旧套标记“已并入”，单件保留独立身份但通过
``member_of`` 归属到唯一套（一件单件只能处于一条有效保管链）；顶层
在册数量 = 在册套数量 + 无归属的独立单件，不会重复计数。

并发：所有命令在同一把锁内完成“校验 + 幂等 + 追加”，并发签署串行化，
重复回调凭 ``idem_key`` 直接返回首次结果。
"""

from __future__ import annotations

import secrets
import threading
from datetime import datetime, timezone
from typing import Callable

from events import (
    CHAIN_STAGES,
    NEXT_STAGE,
    REASON_BROKEN_CHAIN,
    REASON_DISCREPANCY,
    REASON_OVERDUE,
    STAGE_ACCESSION,
    STAGE_ENTRY,
    STAGE_HANDOVER,
    STAGE_SEIZED,
    STATUS_CORRECTED,
    STATUS_DRAFT,
    STATUS_EFFECTED,
    STATUS_PENDING,
    STATUS_WITHDRAWN,
    ConflictError,
    DomainError,
    NotFoundError,
    new_id,
    resolve_time,
    sha256_hex,
)
from store import EventStore

LIFE_ACTIVE = "在册"
LIFE_SPLIT = "已拆分"
LIFE_MERGED = "已并入"
LIFE_CORRECTED = "已更正"

QUEUE_OPEN = "待处置"
QUEUE_RESOLVED = "已处置"

KINDS = ("套", "单件")
PARTIES = ("from", "to")
SUBJECT_TABLES = ("case", "batch", "artifact", "handover")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_utc(value: str) -> str:
    """把截止时间统一为 UTC（朴素时间视为 UTC），便于字典序比较。"""

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DomainError(f"无法解析的时间: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


class CustodyService:
    def __init__(self, store: EventStore, clock: Callable[[], str] | None = None):
        self.store = store
        self._clock = clock or _utc_now
        self._lock = threading.RLock()
        self._state = self._empty_state()
        self._replay()

    # ============================================================== 投影框架

    @staticmethod
    def _empty_state() -> dict:
        return {
            "cases": {},
            "batches": {},
            "locations": {},
            "artifacts": {},
            "handovers": {},
            "documents": {},
            "queue": {},
            "counters": {},
        }

    def _replay(self) -> None:
        """从账本首条事件重放投影，重建编号计数并与快照指纹比对。"""

        state = self._empty_state()
        for event in self.store.read_events():
            self._apply(state, event)
        self._rebuild_counters(state)
        self._state = state
        snapshot = self.store.load_snapshot()
        self.snapshot_match = (
            snapshot is not None and snapshot.get("state_hash") == self._state_hash(state)
        )

    @staticmethod
    def _rebuild_counters(state: dict) -> None:
        maxima: dict[str, int] = {}
        sequences = [
            ("case", (c["case_no"] for c in state["cases"].values())),
            ("batch", (b["batch_no"] for b in state["batches"].values())),
            ("handover", (h["handover_no"] for h in state["handovers"].values())),
            ("artifact", (a["artifact_no"] for a in state["artifacts"].values())),
        ]
        for kind, nos in sequences:
            for no in nos:
                try:
                    value = int(no.rsplit("-", 1)[1])
                except (IndexError, ValueError):
                    continue
                maxima[kind] = max(maxima.get(kind, 0), value)
        state["counters"] = maxima

    def _state_hash(self, state: dict) -> str:
        def art_view(a: dict) -> dict:
            return {
                k: a.get(k)
                for k in (
                    "artifact_no",
                    "batch_id",
                    "case_id",
                    "kind",
                    "category",
                    "name",
                    "quantity",
                    "components",
                    "lifecycle",
                    "current_stage",
                    "current_location_id",
                    "split_from",
                    "merged_from",
                    "merged_into",
                    "corrected_from",
                    "member_of",
                )
            }

        view = {
            "cases": {k: c["case_no"] for k, c in state["cases"].items()},
            "batches": {
                k: (b["batch_no"], b["case_id"], b.get("location_id"))
                for k, b in state["batches"].items()
            },
            "locations": sorted(state["locations"]),
            "artifacts": {k: art_view(a) for k, a in state["artifacts"].items()},
            "handovers": {
                k: (h["handover_no"], h["status"], h["signatures"], h.get("corrects"))
                for k, h in state["handovers"].items()
            },
            "documents": {
                k: [v["sha256"] for v in d["versions"]]
                for k, d in state["documents"].items()
            },
            "queue": {
                k: (q["reason"], q["status"]) for k, q in state["queue"].items()
            },
        }
        return sha256_hex(view)

    # ------------------------------------------------------------------ 追加

    @staticmethod
    def _fingerprint(data: dict) -> str:
        return sha256_hex(data)

    def _commit(
        self,
        event_type: str,
        payload_or_factory,
        *,
        actor: str | None = None,
        idem_key: str | None = None,
        ts_local: str | None = None,
        tz: str | None = None,
        fingerprint: dict | str | None = None,
    ) -> dict:
        """幂等检查 + 追加 + 折叠，必须在持锁状态下调用。

        ``payload_or_factory`` 可为 dict 或零参数工厂；工厂命令（需要生成
        新编号/ID）在首次提交时才构造负载，避免重复回调时因服务端 ID 不同
        而误判冲突。``fingerprint`` 是客户端原始输入的指纹：同键不同输入
        视为冲突，相同输入直接返回首次事件。
        """

        fp = fingerprint
        if isinstance(fp, dict):
            fp = self._fingerprint(fp)
        if idem_key:
            existing = self.store.find_idem(idem_key)
            if existing is not None:
                if existing["event_type"] != event_type:
                    raise ConflictError(
                        f"幂等键 {idem_key} 已用于 {existing['event_type']}"
                    )
                if fp is None:
                    # 无显式指纹的命令：以规范化事件负载兜底判重
                    payload = (
                        payload_or_factory()
                        if callable(payload_or_factory)
                        else payload_or_factory
                    )
                    fp = self._fingerprint(payload)
                if existing.get("fingerprint") != fp:
                    raise ConflictError("重复请求的内容与首次提交不一致")
                event = self._get_event(existing["event_id"])
                return {"replayed": True, "event": event}
        payload = (
            payload_or_factory()
            if callable(payload_or_factory)
            else payload_or_factory
        )
        if fp is None:
            fp = self._fingerprint(payload)
        utc, at_local, tz_name = resolve_time(ts_local, tz, self._clock())
        raw = {
            "event_id": new_id("evt"),
            "event_type": event_type,
            "occurred_at": utc,
            "at_local": at_local,
            "tz": tz_name,
            "actor": actor,
            "payload": payload,
            "idem_key": idem_key,
        }
        event = self.store.append(raw, fp)
        self._apply(self._state, event)
        self.store.save_snapshot(
            {"state_hash": self._state_hash(self._state), "taken_at": self._clock()}
        )
        return {"replayed": False, "event": event}

    def _get_event(self, event_id: str) -> dict:
        for event in self.store.read_events():
            if event["event_id"] == event_id:
                return event
        raise NotFoundError(f"事件 {event_id} 不存在")

    # ============================================================== 事件折叠

    def _apply(self, state: dict, event: dict) -> None:
        etype = event["event_type"]
        handler = getattr(self, f"_apply_{etype}", None)
        if handler is None:
            raise DomainError(f"未知事件类型 {etype}，无法重放")
        handler(state, event, event["payload"])

    def _apply_case_opened(self, state, event, p):
        state["cases"][p["case_id"]] = {
            "case_id": p["case_id"],
            "case_no": p["case_no"],
            "title": p["title"],
            "source_country": p.get("source_country"),
            "note": p.get("note"),
            "opened_at": event["occurred_at"],
        }

    def _apply_location_registered(self, state, event, p):
        state["locations"][p["location_id"]] = {
            "location_id": p["location_id"],
            "name": p["name"],
            "kind": p.get("kind"),
            "address": p.get("address"),
        }

    def _apply_batch_created(self, state, event, p):
        state["batches"][p["batch_id"]] = {
            "batch_id": p["batch_id"],
            "batch_no": p["batch_no"],
            "case_id": p["case_id"],
            "foreign_agency": p["foreign_agency"],
            "agency_timezone": p.get("agency_timezone"),
            "expected_on": p.get("expected_on"),
            "location_id": p.get("location_id"),
            "note": p.get("note"),
            "created_at": event["occurred_at"],
        }

    @staticmethod
    def _artifact_seed(p: dict, event, **extra) -> dict:
        seed = {
            "artifact_id": p["artifact_id"],
            "artifact_no": p["artifact_no"],
            "batch_id": p["batch_id"],
            "case_id": p["case_id"],
            "kind": p["kind"],
            "category": p["category"],
            "name": p["name"],
            "quantity": p["quantity"],
            "components": [dict(c) for c in p.get("components", [])],
            "lifecycle": LIFE_ACTIVE,
            "current_stage": None,
            "current_location_id": None,
            "split_from": None,
            "merged_from": [],
            "merged_into": None,
            "corrected_from": None,
            "member_of": None,
        }
        seed.update(extra)
        return seed

    def _apply_artifact_registered(self, state, event, p):
        state["artifacts"][p["artifact_id"]] = self._artifact_seed(p, event)

    def _apply_chain_recorded(self, state, event, p):
        for art_id in p["artifact_ids"]:
            art = state["artifacts"][art_id]
            art["current_stage"] = p["stage"]
            art["current_location_id"] = p.get("location_id")

    def _apply_handover_prepared(self, state, event, p):
        state["handovers"][p["handover_id"]] = {
            "handover_id": p["handover_id"],
            "handover_no": p["handover_no"],
            "case_id": p["case_id"],
            "batch_id": p["batch_id"],
            "stage": p["stage"],
            "from_party": p["from_party"],
            "to_party": p["to_party"],
            "expected_quantity": p["expected_quantity"],
            "deadline": p.get("deadline"),
            "status": STATUS_PENDING if p.get("submit") else STATUS_DRAFT,
            "signatures": {},
            "corrects": p.get("corrects"),
            "superseded_by": None,
            "effected_at": None,
            "history": [
                {"status": STATUS_DRAFT, "at": event["occurred_at"], "seq": event["seq"]}
            ],
        }

    def _apply_handover_submitted(self, state, event, p):
        ho = state["handovers"][p["handover_id"]]
        ho["status"] = STATUS_PENDING
        if p.get("deadline"):
            ho["deadline"] = p["deadline"]
        if p.get("expected_quantity") is not None:
            ho["expected_quantity"] = p["expected_quantity"]
        ho["history"].append(
            {"status": STATUS_PENDING, "at": event["occurred_at"], "seq": event["seq"]}
        )

    def _apply_handover_signed(self, state, event, p):
        ho = state["handovers"][p["handover_id"]]
        ho["signatures"][p["party"]] = {
            "actor": p["actor"],
            "occurred_at": event["occurred_at"],
            "at_local": event["at_local"],
            "tz": event["tz"],
            "event_id": event["event_id"],
            "observed_quantity": p.get("observed_quantity"),
        }
        ho["history"].append(
            {"status": f"签署:{p['party']}", "at": event["occurred_at"], "seq": event["seq"]}
        )
        if p.get("effected"):
            ho["status"] = STATUS_EFFECTED
            ho["effected_at"] = event["occurred_at"]
            ho["history"].append(
                {"status": STATUS_EFFECTED, "at": event["occurred_at"], "seq": event["seq"]}
            )

    def _apply_handover_withdrawn(self, state, event, p):
        ho = state["handovers"][p["handover_id"]]
        ho["status"] = STATUS_WITHDRAWN
        ho["withdraw_reason"] = p.get("reason")
        ho["history"].append(
            {"status": STATUS_WITHDRAWN, "at": event["occurred_at"], "seq": event["seq"]}
        )

    def _apply_handover_resubmitted(self, state, event, p):
        ho = state["handovers"][p["handover_id"]]
        ho["status"] = STATUS_PENDING
        ho["signatures"] = {}
        if p.get("deadline"):
            ho["deadline"] = p["deadline"]
        if p.get("expected_quantity") is not None:
            ho["expected_quantity"] = p["expected_quantity"]
        ho["history"].append(
            {
                "status": STATUS_PENDING,
                "at": event["occurred_at"],
                "seq": event["seq"],
                "note": "撤回重提",
            }
        )

    def _apply_handover_corrected(self, state, event, p):
        old = state["handovers"][p["original_id"]]
        old["status"] = STATUS_CORRECTED
        old["superseded_by"] = p["new_id"]
        old["history"].append(
            {"status": STATUS_CORRECTED, "at": event["occurred_at"], "seq": event["seq"]}
        )
        state["handovers"][p["new_id"]] = {
            "handover_id": p["new_id"],
            "handover_no": p["handover_no"],
            "case_id": old["case_id"],
            "batch_id": old["batch_id"],
            "stage": p.get("stage") or old["stage"],
            "from_party": p.get("from_party") or old["from_party"],
            "to_party": p.get("to_party") or old["to_party"],
            "expected_quantity": p.get("expected_quantity")
            if p.get("expected_quantity") is not None
            else old["expected_quantity"],
            "deadline": p.get("deadline"),
            "status": STATUS_PENDING,
            "signatures": {},
            "corrects": p["original_id"],
            "superseded_by": None,
            "effected_at": None,
            "history": [
                {
                    "status": STATUS_CORRECTED,
                    "at": event["occurred_at"],
                    "seq": event["seq"],
                    "note": f"更正自 {p['original_id']}",
                }
            ],
        }

    def _apply_artifact_split(self, state, event, p):
        source = state["artifacts"][p["source_id"]]
        source["lifecycle"] = LIFE_SPLIT
        source_singletons = [
            c["singleton_id"] for c in source["components"] if c.get("singleton_id")
        ]
        for sid in source_singletons:
            state["artifacts"][sid]["member_of"] = None
        for out in p["outputs"]:
            if out.get("reused_singleton"):
                art = state["artifacts"][out["artifact_id"]]
                art["member_of"] = None
                art["current_stage"] = source["current_stage"]
                art["current_location_id"] = source["current_location_id"]
                art["split_from"] = p["source_id"]
                continue
            art = self._artifact_seed(
                {**out, "batch_id": p["batch_id"], "case_id": p["case_id"]},
                event,
                current_stage=source["current_stage"],
                current_location_id=source["current_location_id"],
                split_from=p["source_id"],
            )
            state["artifacts"][out["artifact_id"]] = art
            for comp in art["components"]:
                sid = comp.get("singleton_id")
                if sid:
                    state["artifacts"][sid]["member_of"] = art["artifact_id"]

    def _apply_artifacts_merged(self, state, event, p):
        target_stage = None
        for sid in p["source_ids"]:
            source = state["artifacts"][sid]
            target_stage = source["current_stage"]
            if source["kind"] == "套":
                source["lifecycle"] = LIFE_MERGED
                source["merged_into"] = p["artifact_id"]
            else:
                # 单件保留在册身份，但归属唯一套
                source["member_of"] = p["artifact_id"]
            for comp in source.get("components", []):
                cid = comp.get("singleton_id")
                if cid and source["kind"] == "套":
                    state["artifacts"][cid]["member_of"] = p["artifact_id"]
        state["artifacts"][p["artifact_id"]] = self._artifact_seed(
            p,
            event,
            current_stage=target_stage,
            merged_from=list(p["source_ids"]),
        )

    def _apply_identity_corrected(self, state, event, p):
        old = state["artifacts"][p["original_id"]]
        old["lifecycle"] = LIFE_CORRECTED
        member_of = old["member_of"]
        seed = self._artifact_seed(
            {
                "artifact_id": p["artifact_id"],
                "artifact_no": p["artifact_no"],
                "batch_id": old["batch_id"],
                "case_id": old["case_id"],
                "kind": p["after"].get("kind", old["kind"]),
                "category": p["after"].get("category", old["category"]),
                "name": p["after"].get("name", old["name"]),
                "quantity": old["quantity"],
                "components": old.get("components", []),
            },
            event,
            current_stage=old["current_stage"],
            current_location_id=old["current_location_id"],
            corrected_from=p["original_id"],
            member_of=member_of,
        )
        state["artifacts"][p["artifact_id"]] = seed
        if member_of:
            # 套组件清单中的标识同步指向新身份
            host = state["artifacts"][member_of]
            for comp in host["components"]:
                if comp.get("singleton_id") == p["original_id"]:
                    comp["singleton_id"] = p["artifact_id"]

    def _apply_document_recorded(self, state, event, p):
        state["documents"][p["doc_id"]] = {
            "doc_id": p["doc_id"],
            "subject_type": p["subject_type"],
            "subject_id": p["subject_id"],
            "kind": p["kind"],
            "filename": p["filename"],
            "current_version": 1,
            "versions": [
                {
                    "version": 1,
                    "sha256": p["sha256"],
                    "size": p.get("size"),
                    "media_type": p.get("media_type"),
                    "note": p.get("note"),
                    "occurred_at": event["occurred_at"],
                    "event_id": event["event_id"],
                }
            ],
        }

    def _apply_document_versioned(self, state, event, p):
        doc = state["documents"][p["doc_id"]]
        doc["current_version"] = p["version"]
        doc["versions"].append(
            {
                "version": p["version"],
                "sha256": p["sha256"],
                "size": p.get("size"),
                "media_type": p.get("media_type"),
                "note": p.get("note"),
                "occurred_at": event["occurred_at"],
                "event_id": event["event_id"],
            }
        )

    def _apply_queue_opened(self, state, event, p):
        state["queue"][p["queue_id"]] = {
            "queue_id": p["queue_id"],
            "reason": p["reason"],
            "subject_type": p["subject_type"],
            "subject_id": p["subject_id"],
            "detail": p.get("detail"),
            "status": QUEUE_OPEN,
            "opened_at": event["occurred_at"],
            "resolved_at": None,
            "resolution_note": None,
        }

    def _apply_queue_resolved(self, state, event, p):
        item = state["queue"][p["queue_id"]]
        item["status"] = QUEUE_RESOLVED
        item["resolved_at"] = event["occurred_at"]
        item["resolution_note"] = p.get("note")

    # ============================================================== 公共查询

    def get_case(self, case_id: str) -> dict:
        return self._state["cases"].get(case_id) or self._missing("案件", case_id)

    def get_batch(self, batch_id: str) -> dict:
        return self._state["batches"].get(batch_id) or self._missing("批次", batch_id)

    def get_artifact(self, artifact_id: str) -> dict:
        return self._state["artifacts"].get(artifact_id) or self._missing(
            "器物", artifact_id
        )

    def get_handover(self, handover_id: str) -> dict:
        return self._state["handovers"].get(handover_id) or self._missing(
            "交接", handover_id
        )

    @staticmethod
    def _missing(label: str, key: str):
        raise NotFoundError(f"{label} {key} 不存在")

    def list_cases(self) -> list[dict]:
        return list(self._state["cases"].values())

    def list_batches(self, case_id: str | None = None) -> list[dict]:
        batches = self._state["batches"].values()
        if case_id:
            batches = [b for b in batches if b["case_id"] == case_id]
        return list(batches)

    def list_artifacts(self, batch_id: str | None = None) -> list[dict]:
        arts = self._state["artifacts"].values()
        if batch_id:
            arts = [a for a in arts if a["batch_id"] == batch_id]
        return list(arts)

    def list_handovers(self, batch_id: str | None = None) -> list[dict]:
        hos = self._state["handovers"].values()
        if batch_id:
            hos = [h for h in hos if h["batch_id"] == batch_id]
        return list(hos)

    def list_locations(self) -> list[dict]:
        return list(self._state["locations"].values())

    def list_documents(self, subject_type: str | None = None, subject_id: str | None = None):
        docs = self._state["documents"].values()
        if subject_type:
            docs = [d for d in docs if d["subject_type"] == subject_type]
        if subject_id:
            docs = [d for d in docs if d["subject_id"] == subject_id]
        return list(docs)

    def list_queue(self, include_resolved: bool = False) -> list[dict]:
        items = self._state["queue"].values()
        if not include_resolved:
            items = [i for i in items if i["status"] == QUEUE_OPEN]
        return sorted(items, key=lambda i: i["opened_at"])

    def list_events(self) -> list[dict]:
        return self.store.read_events()

    # ------------------------------------------------------------------ 编号

    def _next_no(self, kind: str, pattern: str) -> str:
        n = self._state["counters"].get(kind, 0) + 1
        self._state["counters"][kind] = n
        return pattern.format(n=n)

    # ============================================================== 命令：主数据

    def open_case(
        self,
        title: str,
        *,
        source_country: str | None = None,
        note: str | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        with self._lock:
            def make_payload():
                return {
                    "case_id": new_id("case"),
                    "case_no": self._next_no("case", "CASE-{n:04d}"),
                    "title": title,
                    "source_country": source_country,
                    "note": note,
                }

            return self._commit(
                "case_opened",
                make_payload,
                actor=actor,
                idem_key=idem_key,
                fingerprint={
                    "title": title,
                    "source_country": source_country,
                    "note": note,
                },
            )

    def register_location(
        self,
        name: str,
        *,
        kind: str | None = None,
        address: str | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        with self._lock:
            return self._commit(
                "location_registered",
                lambda: {
                    "location_id": new_id("loc"),
                    "name": name,
                    "kind": kind,
                    "address": address,
                },
                actor=actor,
                idem_key=idem_key,
                fingerprint={"name": name, "kind": kind, "address": address},
            )

    def create_batch(
        self,
        case_id: str,
        foreign_agency: str,
        *,
        agency_timezone: str | None = None,
        expected_on: str | None = None,
        location_id: str | None = None,
        note: str | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        with self._lock:
            self.get_case(case_id)
            if location_id:
                self._require_location(location_id)

            def make_payload():
                return {
                    "batch_id": new_id("batch"),
                    "batch_no": self._next_no("batch", "BATCH-{n:04d}"),
                    "case_id": case_id,
                    "foreign_agency": foreign_agency,
                    "agency_timezone": agency_timezone,
                    "expected_on": expected_on,
                    "location_id": location_id,
                    "note": note,
                }

            return self._commit(
                "batch_created",
                make_payload,
                actor=actor,
                idem_key=idem_key,
                fingerprint={
                    "case_id": case_id,
                    "foreign_agency": foreign_agency,
                    "agency_timezone": agency_timezone,
                    "expected_on": expected_on,
                    "location_id": location_id,
                    "note": note,
                },
            )

    def _require_location(self, location_id: str) -> dict:
        return self._state["locations"].get(location_id) or self._missing(
            "保管地点", location_id
        )

    # ---------------------------------------------------------- 命令：器物登记

    def register_artifact(
        self,
        batch_id: str,
        kind: str,
        category: str,
        name: str,
        *,
        quantity: int | None = None,
        components: list[dict] | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        with self._lock:
            batch = self.get_batch(batch_id)
            if kind not in KINDS:
                raise DomainError(f"器物类型必须是 {KINDS} 之一")
            if kind == "单件" and quantity is not None and quantity != 1:
                raise DomainError("单件器物数量必须为 1")
            components = self._normalize_components(kind, quantity, components, allow_singleton=False)
            total_quantity = sum(c["quantity"] for c in components)

            def make_payload():
                return {
                    "artifact_id": new_id("art"),
                    "artifact_no": self._next_no("artifact", "ART-{n:05d}"),
                    "batch_id": batch_id,
                    "case_id": batch["case_id"],
                    "kind": kind,
                    "category": category,
                    "name": name,
                    "quantity": total_quantity,
                    "components": [dict(c) for c in components],
                }

            return self._commit(
                "artifact_registered",
                make_payload,
                actor=actor,
                idem_key=idem_key,
                fingerprint={
                    "batch_id": batch_id,
                    "kind": kind,
                    "category": category,
                    "name": name,
                    "quantity": quantity,
                    "components": components
                    and [
                        {
                            "name": c.get("name"),
                            "quantity": c.get("quantity", 1),
                            "singleton_id": c.get("singleton_id"),
                        }
                        for c in components
                    ],
                },
            )

    def _normalize_components(
        self,
        kind: str,
        quantity: int | None,
        components: list[dict] | None,
        *,
        allow_singleton: bool,
        known_singletons: set[str] | None = None,
    ) -> list[dict]:
        if kind == "单件":
            if components:
                raise DomainError("单件器物不能声明组件")
            return [{"component_id": new_id("cmp"), "name": "本体", "quantity": 1}]
        if not components:
            if quantity is None or quantity <= 0:
                raise DomainError("成套器物必须给出正整数数量或组件清单")
            return [
                {"component_id": new_id("cmp"), "name": f"组件{i + 1}", "quantity": 1}
                for i in range(quantity)
            ]
        normalized: list[dict] = []
        total = 0
        for index, comp in enumerate(components, 1):
            qty = int(comp.get("quantity", 1))
            if qty <= 0:
                raise DomainError("组件数量必须为正整数")
            total += qty
            entry = {
                "component_id": comp.get("component_id") or new_id("cmp"),
                "name": comp.get("name") or f"组件{index}",
                "quantity": qty,
            }
            singleton_id = comp.get("singleton_id")
            if singleton_id:
                if not allow_singleton:
                    raise DomainError("单件归属成套只能通过合套操作建立")
                if known_singletons is not None and singleton_id not in known_singletons:
                    raise DomainError(
                        f"单件 {singleton_id} 不在待拆分套的组件中，不能重新分配"
                    )
                entry["singleton_id"] = singleton_id
            normalized.append(entry)
        if quantity is not None and quantity != total:
            raise DomainError(f"组件数量合计 {total} 与申报数量 {quantity} 不一致")
        return normalized

    def _require_active_artifact(self, artifact_id: str) -> dict:
        art = self.get_artifact(artifact_id)
        if art["lifecycle"] != LIFE_ACTIVE:
            raise DomainError(f"器物 {artifact_id} 已{art['lifecycle']}，不可再操作")
        return art

    def _physical_count(self, batch_id: str) -> int:
        """顶层在册数量：在册套按套计数，独立单件逐件计数，归属件不重复。"""

        total = 0
        for art in self._state["artifacts"].values():
            if art["batch_id"] != batch_id or art["lifecycle"] != LIFE_ACTIVE:
                continue
            if art["kind"] == "套":
                total += art["quantity"]
            elif art["member_of"] is None:
                total += 1
        return total

    # ---------------------------------------------------------- 命令：责任链

    def record_chain(
        self,
        artifact_ids: list[str],
        stage: str,
        *,
        location_id: str | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
        at_local: str | None = None,
        tz: str | None = None,
    ) -> dict:
        """登记保管链节点。阶段跳跃或缺少生效交接凭证时进入待处置队列。"""

        with self._lock:
            if not artifact_ids:
                raise DomainError("至少指定一件器物")
            if len(set(artifact_ids)) != len(artifact_ids):
                raise DomainError("器物清单重复")
            if stage not in CHAIN_STAGES:
                raise DomainError(f"阶段必须是 {CHAIN_STAGES} 之一")
            if location_id:
                self._require_location(location_id)
            arts = [self._require_active_artifact(aid) for aid in artifact_ids]
            for art in arts:
                if art["member_of"]:
                    raise DomainError(
                        f"单件 {art['artifact_id']} 随套 {art['member_of']} 保管，"
                        "请对套登记责任链节点"
                    )
            broken: list[str] = []
            for art in arts:
                expected = NEXT_STAGE.get(art["current_stage"], STAGE_SEIZED)
                if art["current_stage"] is None and stage != STAGE_SEIZED:
                    broken.append(art["artifact_id"])
                elif art["current_stage"] is not None and stage != expected:
                    broken.append(art["artifact_id"])
            if not broken and stage != STAGE_SEIZED:
                batches = {art["batch_id"] for art in arts}
                for batch_id in batches:
                    effected = any(
                        h["batch_id"] == batch_id
                        and h["stage"] == stage
                        and h["status"] == STATUS_EFFECTED
                        for h in self._state["handovers"].values()
                    )
                    if not effected:
                        broken.extend(
                            art["artifact_id"]
                            for art in arts
                            if art["batch_id"] == batch_id
                        )
            if broken:
                queue_id = self._open_queue(
                    REASON_BROKEN_CHAIN,
                    "artifact",
                    ",".join(sorted(set(broken))),
                    {
                        "attempted_stage": stage,
                        "artifact_ids": sorted(set(broken)),
                        "detail": "责任链阶段跳跃或缺少已生效交接凭证",
                    },
                    actor=actor,
                )
                return {
                    "queued": True,
                    "queue_id": queue_id,
                    "reason": REASON_BROKEN_CHAIN,
                    "artifact_ids": sorted(set(broken)),
                }
            result = self._commit(
                "chain_recorded",
                {
                    "artifact_ids": [a["artifact_id"] for a in arts],
                    "stage": stage,
                    "location_id": location_id,
                },
                actor=actor,
                idem_key=idem_key,
                ts_local=at_local,
                tz=tz,
            )
            result["queued"] = False
            return result

    # ---------------------------------------------------------- 命令：交接

    def prepare_handover(
        self,
        batch_id: str,
        stage: str,
        from_party: str,
        to_party: str,
        *,
        expected_quantity: int | None = None,
        deadline: str | None = None,
        submit: bool = True,
        corrects: str | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        with self._lock:
            batch = self.get_batch(batch_id)
            if stage not in (STAGE_HANDOVER, STAGE_ENTRY, STAGE_ACCESSION):
                raise DomainError("交接阶段必须是 交接/入境/入藏 之一")
            if corrects:
                original = self.get_handover(corrects)
                if original["batch_id"] != batch_id:
                    raise DomainError("更正交接必须属于同一批次")
            count = expected_quantity if expected_quantity is not None else self._physical_count(batch_id)
            if count <= 0:
                raise DomainError("批次尚无在册器物，不能建立交接")
            deadline_utc = _as_utc(deadline) if deadline else None

            def make_payload():
                return {
                    "handover_id": new_id("ho"),
                    "handover_no": self._next_no("handover", "HO-{n:05d}"),
                    "case_id": batch["case_id"],
                    "batch_id": batch_id,
                    "stage": stage,
                    "from_party": from_party,
                    "to_party": to_party,
                    "expected_quantity": count,
                    "deadline": deadline_utc,
                    "submit": submit,
                    "corrects": corrects,
                }

            return self._commit(
                "handover_prepared",
                make_payload,
                actor=actor,
                idem_key=idem_key,
                fingerprint={
                    "batch_id": batch_id,
                    "stage": stage,
                    "from_party": from_party,
                    "to_party": to_party,
                    "expected_quantity": count,
                    "deadline": deadline_utc,
                    "submit": submit,
                    "corrects": corrects,
                },
            )

    def submit_handover(
        self,
        handover_id: str,
        *,
        deadline: str | None = None,
        expected_quantity: int | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        with self._lock:
            ho = self.get_handover(handover_id)
            if ho["status"] != STATUS_DRAFT:
                raise ConflictError(f"交接当前为 {ho['status']}，不能提交签署")
            return self._commit(
                "handover_submitted",
                {
                    "handover_id": handover_id,
                    "deadline": _as_utc(deadline) if deadline else ho.get("deadline"),
                    "expected_quantity": expected_quantity,
                },
                actor=actor,
                idem_key=idem_key,
            )

    def sign_handover(
        self,
        handover_id: str,
        party: str,
        actor: str,
        *,
        at_local: str,
        tz: str,
        observed_quantity: int | None = None,
        idem_key: str | None = None,
    ) -> dict:
        """一方签署；第二方签署后交接生效。重复回调不会产生第二次交接。"""

        with self._lock:
            ho = self.get_handover(handover_id)
            if party not in PARTIES:
                raise DomainError("签署方必须是 from 或 to")
            fp_data = {
                "handover_id": handover_id,
                "party": party,
                "actor": actor,
                "at_local": at_local,
                "tz": tz,
                "observed_quantity": observed_quantity,
            }
            fp = self._fingerprint(fp_data)
            if idem_key:
                existing = self.store.find_idem(idem_key)
                if existing is not None:
                    if existing["event_type"] != "handover_signed":
                        raise ConflictError(
                            f"幂等键 {idem_key} 已用于 {existing['event_type']}"
                        )
                    if existing.get("fingerprint") != fp:
                        raise ConflictError("重复请求的内容与首次提交不一致")
                    return {
                        "replayed": True,
                        "event": self._get_event(existing["event_id"]),
                    }
            if party in ho["signatures"]:
                # 无幂等键的重试：返回既有签署事件，绝不重复记账
                sig = ho["signatures"][party]
                return {
                    "replayed": True,
                    "event": self._get_event(sig["event_id"]),
                    "already_signed": True,
                }
            if ho["status"] != STATUS_PENDING:
                raise ConflictError(f"交接当前为 {ho['status']}，不能签署")
            other = "to" if party == "from" else "from"
            effected = other in ho["signatures"]
            result = self._commit(
                "handover_signed",
                {
                    "handover_id": handover_id,
                    "party": party,
                    "actor": actor,
                    "observed_quantity": observed_quantity,
                    "effected": effected,
                },
                actor=actor,
                idem_key=idem_key,
                ts_local=at_local,
                tz=tz,
                fingerprint=fp_data,
            )
            if effected:
                self._resolve_subject_queue(
                    REASON_OVERDUE, "handover", handover_id, "双方签署完成，交接生效"
                )
                mismatch = self._detect_discrepancy(ho, party, observed_quantity)
                if mismatch:
                    queue_id = self._open_queue(
                        REASON_DISCREPANCY,
                        "handover",
                        handover_id,
                        {
                            "expected_quantity": ho["expected_quantity"],
                            "observed": {
                                party: observed_quantity,
                                other: ho["signatures"][other].get("observed_quantity"),
                            },
                            "physical_quantity": self._physical_count(ho["batch_id"]),
                            "detail": mismatch,
                        },
                        actor=actor,
                    )
                    result["queue_id"] = queue_id
                    result["reason"] = REASON_DISCREPANCY
                    result["effected"] = True
            return result

    def _detect_discrepancy(
        self, ho: dict, party: str, observed_quantity: int | None
    ) -> str | None:
        other = "to" if party == "from" else "from"
        other_observed = ho["signatures"][other].get("observed_quantity")
        if (
            observed_quantity is not None
            and other_observed is not None
            and observed_quantity != other_observed
        ):
            return "双方现场清点数量不一致"
        for observed in (observed_quantity, other_observed):
            if observed is not None and observed != ho["expected_quantity"]:
                return "现场清点数量与交接清单不符"
        physical = self._physical_count(ho["batch_id"])
        if physical != ho["expected_quantity"]:
            return f"实物在册数量 {physical} 与交接清单 {ho['expected_quantity']} 不符"
        return None

    def withdraw_handover(
        self,
        handover_id: str,
        *,
        reason: str | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        with self._lock:
            ho = self.get_handover(handover_id)
            if ho["status"] not in (STATUS_DRAFT, STATUS_PENDING):
                raise ConflictError(f"交接当前为 {ho['status']}，不能撤回")
            result = self._commit(
                "handover_withdrawn",
                {"handover_id": handover_id, "reason": reason},
                actor=actor,
                idem_key=idem_key,
            )
            self._resolve_subject_queue(
                REASON_OVERDUE, "handover", handover_id, f"交接已撤回：{reason or ''}"
            )
            return result

    def resubmit_handover(
        self,
        handover_id: str,
        *,
        deadline: str | None = None,
        expected_quantity: int | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        with self._lock:
            ho = self.get_handover(handover_id)
            if ho["status"] != STATUS_WITHDRAWN:
                raise ConflictError(f"交接当前为 {ho['status']}，只有已撤回可重提")
            return self._commit(
                "handover_resubmitted",
                {
                    "handover_id": handover_id,
                    "deadline": _as_utc(deadline) if deadline else ho.get("deadline"),
                    "expected_quantity": expected_quantity,
                },
                actor=actor,
                idem_key=idem_key,
            )

    def correct_handover(
        self,
        handover_id: str,
        *,
        expected_quantity: int | None = None,
        deadline: str | None = None,
        from_party: str | None = None,
        to_party: str | None = None,
        stage: str | None = None,
        reason: str | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        """已生效交接只能被更正：原单标记“已更正”，新单引用原单重新签署。"""

        with self._lock:
            ho = self.get_handover(handover_id)
            if ho["status"] != STATUS_EFFECTED:
                raise ConflictError(f"交接当前为 {ho['status']}，只有已生效交接可更正")
            if stage is not None and stage not in (
                STAGE_HANDOVER,
                STAGE_ENTRY,
                STAGE_ACCESSION,
            ):
                raise DomainError("交接阶段必须是 交接/入境/入藏 之一")
            deadline_utc = _as_utc(deadline) if deadline else None
            fp_data = {
                "original_id": handover_id,
                "expected_quantity": expected_quantity,
                "deadline": deadline_utc,
                "from_party": from_party,
                "to_party": to_party,
                "stage": stage,
                "reason": reason,
            }
            return self._commit(
                "handover_corrected",
                lambda: {
                    "original_id": handover_id,
                    "new_id": new_id("ho"),
                    "handover_no": self._next_no("handover", "HO-{n:05d}"),
                    "expected_quantity": expected_quantity,
                    "deadline": deadline_utc,
                    "from_party": from_party,
                    "to_party": to_party,
                    "stage": stage,
                    "reason": reason,
                },
                actor=actor,
                idem_key=idem_key,
                fingerprint=fp_data,
            )

    # ---------------------------------------------------------- 命令：拆分/合并/更正

    def split_artifact(
        self,
        artifact_id: str,
        outputs: list[dict],
        *,
        reason: str | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        with self._lock:
            source = self._require_active_artifact(artifact_id)
            if source["kind"] != "套":
                raise DomainError("只有成套器物可以拆分")
            if not outputs:
                raise DomainError("必须给出拆分结果")
            source_singletons = {
                c["singleton_id"]
                for c in source["components"]
                if c.get("singleton_id")
            }
            reused: set[str] = set()
            specs = []
            total = 0
            for out in outputs:
                kind = out.get("kind", "单件")
                if kind not in KINDS:
                    raise DomainError(f"器物类型必须是 {KINDS} 之一")
                reuse = out.get("reuse_singleton")
                if reuse:
                    if kind != "单件":
                        raise DomainError("reuse_singleton 只能用于单件输出")
                    if reuse not in source_singletons:
                        raise DomainError(f"单件 {reuse} 不在原套组件中")
                    if reuse in reused:
                        raise DomainError(f"单件 {reuse} 被重复分配")
                    reused.add(reuse)
                    singleton = self.get_artifact(reuse)
                    total += 1
                    specs.append(
                        {
                            "reuse": reuse,
                            "name": out.get("name") or singleton["name"],
                            "category": singleton["category"],
                        }
                    )
                    continue
                comps = self._normalize_components(
                    kind,
                    out.get("quantity"),
                    out.get("components"),
                    allow_singleton=True,
                    known_singletons=source_singletons - reused,
                )
                for comp in comps:
                    sid = comp.get("singleton_id")
                    if sid:
                        reused.add(sid)
                qty = sum(c["quantity"] for c in comps)
                total += qty
                specs.append(
                    {
                        "reuse": None,
                        "kind": kind,
                        "category": out.get("category", source["category"]),
                        "name": out["name"],
                        "quantity": qty,
                        "components": comps,
                    }
                )
            unassigned = source_singletons - reused
            if unassigned:
                raise DomainError(f"原套单件未全部安置: {sorted(unassigned)}")
            if total != source["quantity"]:
                raise DomainError(
                    f"拆分后数量合计 {total} 与原数量 {source['quantity']} 不符"
                )

            def make_payload():
                normalized_outputs = []
                for spec in specs:
                    if spec["reuse"]:
                        singleton = self.get_artifact(spec["reuse"])
                        normalized_outputs.append(
                            {
                                "artifact_id": spec["reuse"],
                                "artifact_no": singleton["artifact_no"],
                                "reused_singleton": True,
                                "kind": "单件",
                                "category": spec["category"],
                                "name": spec["name"],
                                "quantity": 1,
                                "components": [
                                    {
                                        "component_id": new_id("cmp"),
                                        "name": "本体",
                                        "quantity": 1,
                                    }
                                ],
                            }
                        )
                    else:
                        normalized_outputs.append(
                            {
                                "artifact_id": new_id("art"),
                                "artifact_no": self._next_no(
                                    "artifact", "ART-{n:05d}"
                                ),
                                "reused_singleton": False,
                                "kind": spec["kind"],
                                "category": spec["category"],
                                "name": spec["name"],
                                "quantity": spec["quantity"],
                                "components": [dict(c) for c in spec["components"]],
                            }
                        )
                return {
                    "source_id": artifact_id,
                    "batch_id": source["batch_id"],
                    "case_id": source["case_id"],
                    "outputs": normalized_outputs,
                    "reason": reason,
                }

            return self._commit(
                "artifact_split",
                make_payload,
                actor=actor,
                idem_key=idem_key,
                fingerprint={
                    "source_id": artifact_id,
                    "reason": reason,
                    "outputs": [
                        {
                            "reuse_singleton": o.get("reuse_singleton"),
                            "kind": o.get("kind", "单件"),
                            "category": o.get("category"),
                            "name": o.get("name"),
                            "quantity": o.get("quantity"),
                            "components": o.get("components"),
                        }
                        for o in outputs
                    ],
                },
            )

    def merge_artifacts(
        self,
        artifact_ids: list[str],
        name: str,
        *,
        category: str | None = None,
        reason: str | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        with self._lock:
            ids = list(dict.fromkeys(artifact_ids))
            if len(ids) < 2:
                raise DomainError("合套至少需要两件不同器物")
            sources = [self._require_active_artifact(aid) for aid in ids]
            for art in sources:
                if art["member_of"]:
                    raise DomainError(f"单件 {art['artifact_id']} 已属于套 {art['member_of']}")
            batches = {a["batch_id"] for a in sources}
            if len(batches) != 1:
                raise DomainError("只能合并同一批次内的器物")
            stages = {a["current_stage"] for a in sources}
            if len(stages) != 1:
                raise DomainError("各器物保管链阶段不一致，合套会造成链断点")
            categories = {a["category"] for a in sources}
            category = category or (
                next(iter(categories)) if len(categories) == 1 else "组合套"
            )
            component_specs: list[dict] = []
            total = 0
            for art in sources:
                if art["kind"] == "单件":
                    component_specs.append(
                        {"name": art["name"], "quantity": 1, "singleton_id": art["artifact_id"]}
                    )
                    total += 1
                else:
                    for comp in art["components"]:
                        component_specs.append(
                            {
                                "name": f"{art['name']}·{comp['name']}",
                                "quantity": comp["quantity"],
                                "singleton_id": comp.get("singleton_id"),
                            }
                        )
                        total += comp["quantity"]
            batch_id = next(iter(batches))
            case_id = sources[0]["case_id"]

            def make_payload():
                components = []
                for spec in component_specs:
                    entry = {
                        "component_id": new_id("cmp"),
                        "name": spec["name"],
                        "quantity": spec["quantity"],
                    }
                    if spec.get("singleton_id"):
                        entry["singleton_id"] = spec["singleton_id"]
                    components.append(entry)
                return {
                    "artifact_id": new_id("art"),
                    "artifact_no": self._next_no("artifact", "ART-{n:05d}"),
                    "batch_id": batch_id,
                    "case_id": case_id,
                    "source_ids": ids,
                    "kind": "套",
                    "name": name,
                    "category": category,
                    "quantity": total,
                    "components": components,
                    "reason": reason,
                }

            return self._commit(
                "artifacts_merged",
                make_payload,
                actor=actor,
                idem_key=idem_key,
                fingerprint={
                    "source_ids": ids,
                    "name": name,
                    "category": category,
                    "reason": reason,
                },
            )

    def correct_identity(
        self,
        artifact_id: str,
        after: dict,
        *,
        reason: str | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        with self._lock:
            old = self._require_active_artifact(artifact_id)
            changes = {k: v for k, v in after.items() if k in ("name", "category")}
            if not changes:
                raise DomainError("身份更正至少包含 name 或 category")
            before = {k: old[k] for k in ("name", "category", "kind")}

            def make_payload():
                return {
                    "original_id": artifact_id,
                    "artifact_id": new_id("art"),
                    "artifact_no": self._next_no("artifact", "ART-{n:05d}"),
                    "before": before,
                    "after": changes,
                    "reason": reason,
                }

            return self._commit(
                "identity_corrected",
                make_payload,
                actor=actor,
                idem_key=idem_key,
                fingerprint={
                    "original_id": artifact_id,
                    "after": changes,
                    "reason": reason,
                },
            )

    # ---------------------------------------------------------- 命令：文件哈希

    def record_document(
        self,
        subject_type: str,
        subject_id: str,
        kind: str,
        filename: str,
        sha256: str,
        *,
        size: int | None = None,
        media_type: str | None = None,
        note: str | None = None,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        with self._lock:
            self._require_subject(subject_type, subject_id)
            self._validate_hash(sha256)
            return self._commit(
                "document_recorded",
                lambda: {
                    "doc_id": new_id("doc"),
                    "subject_type": subject_type,
                    "subject_id": subject_id,
                    "kind": kind,
                    "filename": filename,
                    "sha256": sha256.lower(),
                    "size": size,
                    "media_type": media_type,
                    "note": note,
                },
                actor=actor,
                idem_key=idem_key,
                fingerprint={
                    "subject_type": subject_type,
                    "subject_id": subject_id,
                    "kind": kind,
                    "filename": filename,
                    "sha256": sha256.lower(),
                    "size": size,
                    "media_type": media_type,
                    "note": note,
                },
            )

    def add_document_version(self, doc_id: str, sha256: str, **kwargs) -> dict:
        with self._lock:
            doc = self._state["documents"].get(doc_id) or self._missing("文件", doc_id)
            self._validate_hash(sha256)
            if sha256.lower() in {v["sha256"] for v in doc["versions"]}:
                raise ConflictError("该内容哈希已存在，不能作为新版本重复登记")
            version = doc["current_version"] + 1
            return self._commit(
                "document_versioned",
                {
                    "doc_id": doc_id,
                    "version": version,
                    "sha256": sha256.lower(),
                    "size": kwargs.get("size"),
                    "media_type": kwargs.get("media_type"),
                    "note": kwargs.get("note"),
                },
                actor=kwargs.get("actor"),
                idem_key=kwargs.get("idem_key"),
            )

    def get_document(self, doc_id: str) -> dict:
        return self._state["documents"].get(doc_id) or self._missing("文件", doc_id)

    def _require_subject(self, subject_type: str, subject_id: str) -> None:
        if subject_type == "case":
            self.get_case(subject_id)
        elif subject_type == "batch":
            self.get_batch(subject_id)
        elif subject_type == "artifact":
            self.get_artifact(subject_id)
        elif subject_type == "handover":
            self.get_handover(subject_id)
        else:
            raise DomainError(
                f"文件归属类型必须是 {SUBJECT_TABLES} 之一"
            )

    @staticmethod
    def _validate_hash(sha256: str) -> None:
        if not (isinstance(sha256, str) and len(sha256) == 64):
            raise DomainError("sha256 必须为 64 位十六进制内容哈希")
        int(sha256, 16)

    # ---------------------------------------------------------- 命令：待处置队列

    def _open_queue(
        self,
        reason: str,
        subject_type: str,
        subject_id: str,
        detail: dict,
        *,
        actor: str | None = None,
    ) -> str:
        for item in self._state["queue"].values():
            if (
                item["status"] == QUEUE_OPEN
                and item["reason"] == reason
                and item["subject_type"] == subject_type
                and item["subject_id"] == subject_id
            ):
                return item["queue_id"]
        queue_id = new_id("que")
        self._commit(
            "queue_opened",
            {
                "queue_id": queue_id,
                "reason": reason,
                "subject_type": subject_type,
                "subject_id": subject_id,
                "detail": detail,
            },
            actor=actor,
        )
        return queue_id

    def _resolve_subject_queue(
        self, reason: str, subject_type: str, subject_id: str, note: str
    ) -> None:
        for item in self._state["queue"].values():
            if (
                item["status"] == QUEUE_OPEN
                and item["reason"] == reason
                and item["subject_type"] == subject_type
                and item["subject_id"] == subject_id
            ):
                self._commit(
                    "queue_resolved",
                    {"queue_id": item["queue_id"], "note": note},
                )

    def resolve_queue(
        self,
        queue_id: str,
        note: str,
        *,
        actor: str | None = None,
        idem_key: str | None = None,
    ) -> dict:
        with self._lock:
            item = self._state["queue"].get(queue_id) or self._missing(
                "待处置项", queue_id
            )
            if item["status"] == QUEUE_RESOLVED:
                return {
                    "replayed": True,
                    "event": self._get_event(self._resolved_event_id(queue_id)),
                }
            return self._commit(
                "queue_resolved",
                {"queue_id": queue_id, "note": note},
                actor=actor,
                idem_key=idem_key,
            )

    def _resolved_event_id(self, queue_id: str) -> str:
        for event in reversed(self.store.read_events()):
            if (
                event["event_type"] == "queue_resolved"
                and event["payload"]["queue_id"] == queue_id
            ):
                return event["event_id"]
        raise NotFoundError(f"待处置项 {queue_id} 无处置事件")

    def sweep_overdue(self) -> list[dict]:
        """检查待签署交接是否超期，超期项进入待处置队列。"""

        with self._lock:
            now = self._clock()
            opened = []
            for ho in list(self._state["handovers"].values()):
                if (
                    ho["status"] == STATUS_PENDING
                    and ho.get("deadline")
                    and ho["deadline"] < now
                    and not self._has_open_queue(
                        REASON_OVERDUE, "handover", ho["handover_id"]
                    )
                ):
                    queue_id = self._open_queue(
                        REASON_OVERDUE,
                        "handover",
                        ho["handover_id"],
                        {
                            "handover_no": ho["handover_no"],
                            "deadline": ho["deadline"],
                            "checked_at": now,
                            "detail": "超过签署截止时间仍未双方签署",
                        },
                    )
                    opened.append(self._state["queue"][queue_id])
            return opened

    def _has_open_queue(self, reason: str, subject_type: str, subject_id: str) -> bool:
        return any(
            item["status"] == QUEUE_OPEN
            and item["reason"] == reason
            and item["subject_type"] == subject_type
            and item["subject_id"] == subject_id
            for item in self._state["queue"].values()
        )

    # ============================================================== 溯源与审计

    def _family(self, artifact_id: str) -> set[str]:
        """沿拆分/合并/更正/套属关系无向收集同一谱系的全部器物。"""

        if artifact_id not in self._state["artifacts"]:
            self._missing("器物", artifact_id)
        family: set[str] = set()
        stack = [artifact_id]
        while stack:
            current = stack.pop()
            if current in family:
                continue
            family.add(current)
            art = self._state["artifacts"][current]
            parents = list(art.get("merged_from", []))
            if art.get("split_from"):
                parents.append(art["split_from"])
            if art.get("corrected_from"):
                parents.append(art["corrected_from"])
            if art.get("member_of"):
                parents.append(art["member_of"])
            for parent in parents:
                if parent not in family:
                    stack.append(parent)
            for other in self._state["artifacts"].values():
                if other["artifact_id"] in family:
                    continue
                linked = False
                if other.get("split_from") == current:
                    linked = True
                elif current in other.get("merged_from", []):
                    linked = True
                elif other.get("corrected_from") == current:
                    linked = True
                elif other.get("member_of") == current and other["kind"] == "单件":
                    linked = True
                elif (
                    art.get("member_of") == other["artifact_id"]
                ):
                    linked = True
                if linked:
                    stack.append(other["artifact_id"])
        return family

    def lineage(self, artifact_id: str) -> dict:
        """前后谱系：旧标识 -> 当前在册标识，含拆分/合并/更正/成套归属。"""

        family = self._family(artifact_id)
        nodes = []
        for aid in sorted(family):
            a = self._state["artifacts"][aid]
            nodes.append(
                {
                    "artifact_id": aid,
                    "artifact_no": a["artifact_no"],
                    "name": a["name"],
                    "kind": a["kind"],
                    "category": a["category"],
                    "quantity": a["quantity"],
                    "lifecycle": a["lifecycle"],
                    "current_stage": a["current_stage"],
                    "member_of": a["member_of"],
                    "split_from": a["split_from"],
                    "merged_from": a["merged_from"],
                    "merged_into": a["merged_into"],
                    "corrected_from": a["corrected_from"],
                }
            )
        current_ids = self._current_ids(artifact_id, family)
        return {
            "requested": artifact_id,
            "current_artifact_ids": sorted(current_ids),
            "nodes": nodes,
        }

    def _current_ids(self, artifact_id: str, family: set[str]) -> list[str]:
        """从请求标识沿正向关系找到当前在册顶层器物（拆分可能有多个后继）。"""

        found: set[str] = set()
        stack = [artifact_id]
        seen = set()
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            art = self._state["artifacts"][current]
            moved = False
            for other in family:
                o = self._state["artifacts"][other]
                forward = (
                    o.get("split_from") == current
                    or current in o.get("merged_from", [])
                    or o.get("corrected_from") == current
                )
                if forward:
                    moved = True
                    stack.append(other)
            if art.get("member_of") and art["member_of"] in family:
                moved = True
                stack.append(art["member_of"])
            if not moved and art["lifecycle"] == LIFE_ACTIVE:
                found.add(current)
        return sorted(found)

    def trace(self, artifact_id: str) -> dict:
        """从任一器物（含已失效历史标识）反查完整来源、责任链与文件。"""

        family = self._family(artifact_id)
        current_ids = self._current_ids(artifact_id, family)
        batch_ids = {
            self._state["artifacts"][aid]["batch_id"]
            for aid in family
            if aid in self._state["artifacts"]
        }
        case_ids = {self._state["batches"][bid]["case_id"] for bid in batch_ids}
        handover_ids = {
            hid
            for hid, h in self._state["handovers"].items()
            if h["batch_id"] in batch_ids
        }
        timeline = []
        for event in self.store.read_events():
            p = event["payload"]
            related = False
            etype = event["event_type"]
            if etype == "artifact_registered":
                related = p["artifact_id"] in family
            elif etype == "chain_recorded":
                related = bool(family & set(p["artifact_ids"]))
            elif etype == "artifact_split":
                related = p["source_id"] in family or bool(
                    family & {o["artifact_id"] for o in p["outputs"]}
                )
            elif etype == "artifacts_merged":
                related = p["artifact_id"] in family or bool(family & set(p["source_ids"]))
            elif etype == "identity_corrected":
                related = p["original_id"] in family or p["artifact_id"] in family
            elif etype in (
                "handover_prepared",
                "handover_submitted",
                "handover_signed",
                "handover_withdrawn",
                "handover_resubmitted",
            ):
                related = p.get("handover_id") in handover_ids
            elif etype == "handover_corrected":
                related = p["original_id"] in handover_ids or p["new_id"] in handover_ids
            elif etype in ("queue_opened", "queue_resolved"):
                subject = p.get("subject_id", "")
                related = subject in family or subject in handover_ids
            if related:
                timeline.append(self._public_event(event))
        documents = []
        for doc in self._state["documents"].values():
            if doc["subject_type"] == "artifact" and doc["subject_id"] in family:
                documents.append(doc)
            elif doc["subject_type"] == "batch" and doc["subject_id"] in batch_ids:
                documents.append(doc)
            elif doc["subject_type"] == "case" and doc["subject_id"] in case_ids:
                documents.append(doc)
        return {
            "requested": artifact_id,
            "current_artifact_ids": current_ids,
            "current_artifacts": [
                self._public_artifact(self._state["artifacts"][aid]) for aid in current_ids
            ],
            "lineage": self.lineage(artifact_id),
            "custody_chain": self._chain_for_family(family),
            "documents": documents,
            "timeline": timeline,
        }

    def _chain_for_family(self, family: set[str]) -> list[dict]:
        records = []
        for event in self.store.read_events():
            if event["event_type"] != "chain_recorded":
                continue
            ids = set(event["payload"]["artifact_ids"]) & family
            if ids:
                records.append(
                    {
                        "stage": event["payload"]["stage"],
                        "location_id": event["payload"].get("location_id"),
                        "artifact_ids": sorted(ids),
                        "occurred_at": event["occurred_at"],
                        "at_local": event["at_local"],
                        "tz": event["tz"],
                        "event_id": event["event_id"],
                    }
                )
        return records

    @staticmethod
    def _public_event(event: dict) -> dict:
        return {
            "seq": event["seq"],
            "event_id": event["event_id"],
            "event_type": event["event_type"],
            "occurred_at": event["occurred_at"],
            "at_local": event["at_local"],
            "tz": event["tz"],
            "actor": event["actor"],
            "payload": event["payload"],
            "hash": event["hash"],
            "prev_hash": event["prev_hash"],
        }

    def _public_artifact(self, art: dict) -> dict:
        return {
            k: v
            for k, v in art.items()
            if k
            in (
                "artifact_id",
                "artifact_no",
                "batch_id",
                "case_id",
                "kind",
                "category",
                "name",
                "quantity",
                "components",
                "lifecycle",
                "current_stage",
                "current_location_id",
                "split_from",
                "merged_from",
                "merged_into",
                "corrected_from",
                "member_of",
            )
        }

    def audit_report(self) -> dict:
        """可核验审计档案：哈希链校验、独立重放投影与快照指纹对照。"""

        with self._lock:
            verification = self.store.verify()
            replayed = self._empty_state()
            for event in self.store.read_events():
                self._apply(replayed, event)
            self._rebuild_counters(replayed)
            replay_hash = self._state_hash(replayed)
            live_hash = self._state_hash(self._state)
            snapshot = self.store.load_snapshot()
            return {
                "journal": verification,
                "projection_match": replay_hash == live_hash,
                "replay_state_hash": replay_hash,
                "snapshot_state_hash": snapshot.get("state_hash") if snapshot else None,
                "snapshot_match": snapshot is not None
                and snapshot.get("state_hash") == replay_hash,
                "counts": {
                    "cases": len(self._state["cases"]),
                    "batches": len(self._state["batches"]),
                    "artifacts": len(self._state["artifacts"]),
                    "active_artifacts": sum(
                        1
                        for a in self._state["artifacts"].values()
                        if a["lifecycle"] == LIFE_ACTIVE
                    ),
                    "handovers": len(self._state["handovers"]),
                    "effected_handovers": sum(
                        1
                        for h in self._state["handovers"].values()
                        if h["status"] == STATUS_EFFECTED
                    ),
                    "documents": len(self._state["documents"]),
                    "queue_open": sum(
                        1
                        for q in self._state["queue"].values()
                        if q["status"] == QUEUE_OPEN
                    ),
                },
            }
