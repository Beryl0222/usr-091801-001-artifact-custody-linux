"""应用层：登记、签署、修订命令与核验查询。

所有写操作都翻译成仅追加事件；命令在存储级单锁内“读投影—校验—落事件”，
复合状态推进在同一事务完成，保证并发签署、重复回调下不会产生第二次交接。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from domain import (
    DIR_RETURN,
    ISSUE_CHAIN_GAP,
    ISSUE_OVERDUE,
    ISSUE_QTY_MISMATCH,
    KIND_SET,
    KIND_SINGLE,
    MERGE,
    SPLIT,
    STAGE_ACCESSION,
    STAGE_ENTRY,
    STAGE_HANDOVER,
    STAGE_SEIZURE,
    STAGES,
    STATUS_EFFECTIVE,
    STATUS_PENDING_SIGN,
    STATUS_WITHDRAWN,
    UNIT_PIECE,
    UNIT_SET,
    Conflict,
    DomainError,
    NotFound,
    content_hash,
    new_id,
    now_utc,
    parse_moment,
    require,
    require_choice,
    require_int,
)
from projection import (
    ARTIFACT_REGISTERED,
    BATCH_REGISTERED,
    CASE_OPENED,
    FILE_REGISTERED,
    FILE_VERSION_ADDED,
    HANDOVER_CORRECTION_LINKED,
    HANDOVER_DRAFTED,
    HANDOVER_RECEIVED,
    HANDOVER_SIGNED,
    HANDOVER_SUBMITTED,
    HANDOVER_WITHDRAWN,
    IDENTITY_CORRECTED,
    ISSUE_ACKNOWLEDGED,
    ISSUE_RAISED,
    ISSUE_RESOLVED,
    LOCATION_REGISTERED,
    PIECE_RECORDED,
    QUANTITY_SPLIT,
    SET_MERGED,
)
from store import EventStore

REQUIRED_CHAIN = [STAGE_SEIZURE, STAGE_HANDOVER, STAGE_ENTRY, STAGE_ACCESSION]

_ARTIFACT_CORRECTABLE = {"name", "category", "description", "catalog_no"}
_PIECE_CORRECTABLE = {"label", "description"}
_FILE_KINDS = ("照片", "清单", "鉴定文件")


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


class Service:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    @property
    def p(self):
        return self.store.projection

    # -- 通用辅助 -----------------------------------------------------------

    def _get(self, collection: dict, entity_id: str, label: str) -> dict:
        entity = collection.get(entity_id)
        if entity is None:
            raise NotFound(f"{label}不存在: {entity_id}")
        return entity

    def _party(self, data: dict, field: str) -> dict:
        require(data, field)
        key = require(data[field].get("key"), f"{field}.key")
        name = require(data[field].get("name"), f"{field}.name")
        tz = require(data[field].get("tz"), f"{field}.tz")
        parse_moment(None, tz)  # 校验时区
        return {"key": key, "name": name, "tz": tz}

    def _issue_specs(self) -> list[tuple]:
        """扫描投影，返回需要新建的待处置事项元组（按 kind+对象去重）。"""
        specs: list[tuple] = []
        moment = parse_moment(None, "UTC")
        now_ts = datetime.fromisoformat(moment["utc"])

        # 1) 超期未签
        for h in self.p.handovers.values():
            if h["status"] != STATUS_PENDING_SIGN or not h.get("sign_deadline"):
                continue
            deadline = h["sign_deadline"]
            deadline_ts = datetime.fromisoformat(
                deadline["utc"] if isinstance(deadline, dict) else deadline
            )
            if now_ts > deadline_ts:
                detail = f"签署截止 {deadline_ts.isoformat()} 仍未完成双方签署"
                dedup = (ISSUE_OVERDUE, "handover", h["id"], detail)
                if dedup not in self.p.open_issue_index:
                    specs.append(
                        (
                            ISSUE_OVERDUE,
                            "handover",
                            h["id"],
                            detail,
                            moment,
                        )
                    )

        # 2) 实物数量不符（接收清点结果与交接清单不一致）
        for h in self.p.handovers.values():
            if h["status"] != STATUS_EFFECTIVE or not h.get("actual_qty"):
                continue
            if h["actual_qty"] != h["expected_qty"]:
                detail = f"期望 {h['expected_qty']}，实收 {h['actual_qty']}"
                dedup = (ISSUE_QTY_MISMATCH, "handover", h["id"], detail)
                if dedup not in self.p.open_issue_index:
                    specs.append(
                        (
                            ISSUE_QTY_MISMATCH,
                            "handover",
                            h["id"],
                            detail,
                            moment,
                        )
                    )

        # 3) 保管链断点
        specs.extend(self._chain_gap_specs(moment))
        return specs

    def _chain_gap_specs(self, moment: dict) -> list[tuple]:
        # unit_key -> {"ref_type","ref_id","artifact_id","events":[(h, rank)]}
        coverage: dict[tuple, dict] = {}

        def register(unit_key, ref_type, ref_id, aid, handover):
            unit = coverage.setdefault(
                unit_key,
                {"ref_type": ref_type, "ref_id": ref_id,
                 "artifact_id": aid, "events": []},
            )
            unit["events"].append(handover)

        for h in self.p.handovers.values():
            if h["status"] != STATUS_EFFECTIVE:
                continue
            for aid in h.get("artifact_ids", ()):
                artifact = self.p.artifacts.get(aid)
                if artifact is None:
                    continue
                if artifact["kind"] == KIND_SET:
                    register(("artifact", aid), "artifact", aid, aid, h)
                else:
                    for pid in artifact["pieces"]:
                        register(("piece", pid), "piece", pid, aid, h)
            for pid in h.get("piece_ids", ()):
                piece = self.p.pieces.get(pid)
                if piece is None:
                    continue
                register(("piece", pid), "piece", pid, piece["artifact_id"], h)

        specs: list[tuple] = []

        def raise_gap(unit, detail):
            dedup_key = (ISSUE_CHAIN_GAP, unit["ref_type"], unit["ref_id"], detail)
            if dedup_key not in self.p.open_issue_index:
                specs.append(
                    (ISSUE_CHAIN_GAP, unit["ref_type"], unit["ref_id"], detail, moment)
                )

        for unit in coverage.values():
            events = sorted(
                unit["events"],
                key=lambda x: (x["effective_at"] or x["drafted_at"]["utc"], x["id"]),
            )
            present = {h["stage"] for h in events}
            # 规则一：必备环节缺失——最早已出现环节之前的每个环节都立项
            present_required = [
                idx for idx, stage in enumerate(REQUIRED_CHAIN) if stage in present
            ]
            if present_required:
                earliest = min(present_required)
                for idx in range(earliest):
                    raise_gap(
                        unit,
                        f"已存在“{REQUIRED_CHAIN[earliest]}”环节，"
                        f"但缺少前序“{REQUIRED_CHAIN[idx]}”",
                    )
            # 规则二：相邻生效交接之间责任主体必须衔接
            for prev_h, cur_h in zip(events, events[1:]):
                prev_to = (prev_h["to_party"] or {}).get("key")
                cur_from = (cur_h["from_party"] or {}).get("key")
                if prev_to != cur_from:
                    raise_gap(
                        unit,
                        f"“{prev_h['stage']}”（{prev_h['id']}）→"
                        f"“{cur_h['stage']}”（{cur_h['id']}）责任主体不衔接："
                        f"交付方为 {prev_to}，下一环节交出方却是 {cur_from}",
                    )
        return specs

    @staticmethod
    def _build_issue_spec(tup: tuple) -> dict:
        kind, ref_type, ref_id, detail, moment = tup
        issue_id = new_id("ISS")
        return {
            "event_type": ISSUE_RAISED,
            "stream_id": f"issue-{issue_id}",
            "event_id": new_id("EVT"),
            "payload": {
                "issue_id": issue_id,
                "kind": kind,
                "ref_type": ref_type,
                "ref_id": ref_id,
                "detail": detail,
                "at": moment,
            },
        }

    def scan_issues(self) -> dict:
        with self.store.lock:
            specs = [self._build_issue_spec(t) for t in self._issue_specs()]
            if not specs:
                return {"raised": [], "count": 0}
            envelopes, _ = self.store.append_many(specs)
            return {
                "raised": [e["payload"]["issue_id"] for e in envelopes],
                "count": len(envelopes),
            }

    def _raise_issues_txn(self) -> list[str]:
        """签署生效/清点后立即扫描；调用方已持锁。返回新建 issue id。"""
        specs = [self._build_issue_spec(t) for t in self._issue_specs()]
        if not specs:
            return []
        envelopes, _ = self.store.append_many(specs)
        return [e["payload"]["issue_id"] for e in envelopes]

    # -- 案件 / 地点 / 批次 --------------------------------------------------

    def open_case(self, data: dict) -> dict:
        case_id = new_id("CASE")
        payload = {
            "case_id": case_id,
            "case_no": require(data.get("case_no"), "case_no"),
            "name": require(data.get("name"), "name"),
            "source_country": data.get("source_country"),
            "note": data.get("note"),
            "at": parse_moment(data.get("at"), "Asia/Shanghai"),
        }
        envelope, replayed = self.store.append(
            CASE_OPENED, f"case-{case_id}", payload,
            event_id=new_id("EVT"), client_key=data.get("client_key"),
        )
        if replayed:
            case_id = envelope["payload"]["case_id"]
        return {"case": self.case_view(case_id), "event_id": envelope["event_id"],
                "replayed": replayed}

    def case_view(self, case_id: str) -> dict:
        return dict(self._get(self.p.cases, case_id, "案件"))

    def list_cases(self) -> list[dict]:
        return [dict(c) for c in self.p.cases.values()]

    def register_location(self, data: dict) -> dict:
        loc_id = new_id("LOC")
        payload = {
            "location_id": loc_id,
            "code": require(data.get("code"), "code"),
            "name": require(data.get("name"), "name"),
            "kind": require_choice(data.get("kind"), ("查验场地", "口岸", "库房", "展厅"), "kind"),
            "address": data.get("address"),
        }
        envelope, replayed = self.store.append(
            LOCATION_REGISTERED, f"location-{loc_id}", payload,
            event_id=new_id("EVT"), client_key=data.get("client_key"),
        )
        if replayed:
            loc_id = envelope["payload"]["location_id"]
        return {"location": dict(self.p.locations[loc_id]),
                "event_id": envelope["event_id"], "replayed": replayed}

    def list_locations(self) -> list[dict]:
        return [dict(x) for x in self.p.locations.values()]

    def register_batch(self, data: dict) -> dict:
        case_id = require(data.get("case_id"), "case_id")
        with self.store.lock:
            self._get(self.p.cases, case_id, "案件")
            agency = self._party(data, "foreign_agency")
            batch_id = new_id("BAT")
            payload = {
                "batch_id": batch_id,
                "case_id": case_id,
                "batch_no": require(data.get("batch_no"), "batch_no"),
                "foreign_agency": agency,
                "agency_party_key": data.get("agency_party_key") or agency["key"],
                "handover_city": require(data.get("handover_city"), "handover_city"),
                "handover_date": require(data.get("handover_date"), "handover_date"),
                "tz": agency["tz"],
                "note": data.get("note"),
            }
            envelope, replayed = self.store.append(
                BATCH_REGISTERED, f"batch-{batch_id}", payload,
                event_id=new_id("EVT"), client_key=data.get("client_key"),
            )
            if replayed:
                batch_id = envelope["payload"]["batch_id"]
        return {"batch": self.batch_view(batch_id),
                "event_id": envelope["event_id"], "replayed": replayed}

    def batch_view(self, batch_id: str) -> dict:
        batch = dict(self._get(self.p.batches, batch_id, "批次"))
        batch["artifacts"] = [a["id"] for a in self.p.batch_artifacts(batch_id)]
        return batch

    def list_batches(self, case_id: str | None = None) -> list[dict]:
        batches = self.p.batches.values()
        if case_id:
            batches = [b for b in batches if b["case_id"] == case_id]
        return [self.batch_view(b["id"]) for b in batches]

    # -- 器物登记 ------------------------------------------------------------

    def register_artifact(self, data: dict) -> dict:
        batch_id = require(data.get("batch_id"), "batch_id")
        kind = require_choice(data.get("kind"), (KIND_SET, KIND_SINGLE), "kind")
        unit = UNIT_SET if kind == KIND_SET else UNIT_PIECE
        if data.get("unit") and data["unit"] != unit:
            raise DomainError(f"{kind}器物的计量单位必须为“{unit}”")
        qty = require_int(data.get("qty"), "qty", minimum=1)
        specs: list[dict] = []
        with self.store.lock:
            self._get(self.p.batches, batch_id, "批次")
            # 多事件命令的幂等：client_key 命中已落库的登记事件时原样回放
            if data.get("client_key"):
                existing = self.store.find_client_key(data["client_key"])
                if existing is not None:
                    replay_id = existing["payload"]["artifact_id"]
                    same_txn = [
                        e for e in self.store.events(f"artifact-{replay_id}")
                        if e["recorded_at"] == existing["recorded_at"]
                    ]
                    return {
                        "artifact": self.artifact_view(replay_id),
                        "event_ids": [e["event_id"] for e in same_txn],
                        "replayed": True,
                    }
            artifact_id = new_id("ART")
            specs.append(
                {
                    "event_type": ARTIFACT_REGISTERED,
                    "stream_id": f"artifact-{artifact_id}",
                    "event_id": new_id("EVT"),
                    "payload": {
                        "artifact_id": artifact_id,
                        "batch_id": batch_id,
                        "catalog_no": require(data.get("catalog_no"), "catalog_no"),
                        "name": require(data.get("name"), "name"),
                        "category": require(data.get("category"), "category"),
                        "kind": kind,
                        "unit": unit,
                        "qty": qty,
                        "description": data.get("description"),
                    },
                }
            )
            pieces_data = data.get("pieces")
            if kind == KIND_SINGLE:
                if pieces_data is not None:
                    if len(pieces_data) != qty:
                        raise DomainError("单件清单项数必须与 qty 一致")
                for i in range(qty):
                    piece_id = new_id("PIE")
                    item = pieces_data[i] if pieces_data else {}
                    specs.append(
                        {
                            "event_type": PIECE_RECORDED,
                            "stream_id": f"artifact-{artifact_id}",
                            "event_id": new_id("EVT"),
                            "payload": {
                                "piece_id": piece_id,
                                "artifact_id": artifact_id,
                                "seq_no": i + 1,
                                "label": item.get("label") or f"{data['catalog_no']}-{i + 1:03d}",
                                "description": item.get("description"),
                            },
                        }
                    )
            elif pieces_data:
                raise DomainError("按套计数器物不接受单件清单")
            specs[0]["client_key"] = data.get("client_key")
            envelopes, replayed = self.store.append_many(specs)
            if replayed:
                artifact_id = envelopes[0]["payload"]["artifact_id"]
        return {"artifact": self.artifact_view(artifact_id),
                "event_ids": [e["event_id"] for e in envelopes], "replayed": replayed}

    def add_piece(self, artifact_id: str, data: dict) -> dict:
        with self.store.lock:
            artifact = self._get(self.p.artifacts, artifact_id, "器物")
            if artifact["kind"] != KIND_SINGLE:
                raise DomainError("只有单件追踪器物可以追加单件")
            piece_id = new_id("PIE")
            seq_no = len(artifact["pieces"]) + 1
            payload = {
                "piece_id": piece_id,
                "artifact_id": artifact_id,
                "seq_no": data.get("seq_no") or seq_no,
                "label": require(data.get("label"), "label"),
                "description": data.get("description"),
            }
            envelope, replayed = self.store.append(
                PIECE_RECORDED, f"artifact-{artifact_id}", payload,
                event_id=new_id("EVT"), client_key=data.get("client_key"),
            )
        return {"piece": dict(self.p.pieces[piece_id]),
                "event_id": envelope["event_id"], "replayed": replayed}

    def artifact_view(self, artifact_id: str) -> dict:
        a = dict(self._get(self.p.artifacts, artifact_id, "器物"))
        a["pieces"] = [dict(self.p.pieces[pid]) for pid in a["pieces"]]
        a["revision_ids"] = [
            rid for rid, r in self.p.revisions.items()
            if (r["target_type"] == "artifact" and r["target_id"] == artifact_id)
            or artifact_id in r["inputs"]
            or any(o["artifact_id"] == artifact_id for o in r["outputs"])
        ]
        return a

    def list_artifacts(self, batch_id: str | None = None) -> list[dict]:
        items = self.p.artifacts.values()
        if batch_id:
            items = [a for a in items if a["batch_id"] == batch_id]
        return [self.artifact_view(a["id"]) for a in items]

    # -- 数量拆分 / 成套合并 / 身份更正 -------------------------------------

    def split_artifact(self, data: dict) -> dict:
        artifact_id = require(data.get("artifact_id"), "artifact_id")
        with self.store.lock:
            source = self._get(self.p.artifacts, artifact_id, "器物")
            if source["closed"]:
                raise DomainError("器物已经终结（合并/全量拆分），不能继续拆分")
            outputs = require(data.get("outputs"), "outputs")
            if not outputs:
                raise DomainError("至少给出一个拆分项")
            revision_id = new_id("REV")
            before_qty = source["current_qty"]
            out_payloads: list[dict] = []
            if source["kind"] == KIND_SET:
                allocated = 0
                for item in outputs:
                    q = require_int(item.get("qty"), "outputs[].qty", minimum=1)
                    allocated += q
                    out_payloads.append(
                        {
                            "artifact_id": new_id("ART"),
                            "qty": q,
                            "kind": KIND_SET,
                            "unit": UNIT_SET,
                            "catalog_no": require(item.get("catalog_no"), "outputs[].catalog_no"),
                            "name": item.get("name") or source["name"],
                            "description": item.get("description"),
                            "piece_ids": [],
                        }
                    )
                if allocated > before_qty:
                    raise DomainError(
                        f"拆分数量 {allocated} 超过现存 {before_qty}（数量必须守恒）"
                    )
            else:
                allocated_pieces: list[str] = []
                for item in outputs:
                    pids = require(item.get("piece_ids"), "outputs[].piece_ids")
                    for pid in pids:
                        piece = self.p.pieces.get(pid)
                        if piece is None or piece["artifact_id"] != artifact_id:
                            raise DomainError(f"单件不属于本器物: {pid}")
                        if pid in allocated_pieces:
                            raise DomainError(f"单件被重复分配: {pid}")
                        allocated_pieces.append(pid)
                    out_payloads.append(
                        {
                            "artifact_id": new_id("ART"),
                            "qty": len(pids),
                            "kind": KIND_SINGLE,
                            "unit": UNIT_PIECE,
                            "catalog_no": require(item.get("catalog_no"), "outputs[].catalog_no"),
                            "name": item.get("name") or source["name"],
                            "description": item.get("description"),
                            "piece_ids": pids,
                        }
                    )
                allocated = len(allocated_pieces)
                if allocated > before_qty:
                    raise DomainError("拆分单件数量超过现存数量")
            close_source = bool(data.get("close_source"))
            after_qty = before_qty - allocated
            if close_source and after_qty != 0:
                raise DomainError("终结原器物必须把全部数量分配出去")
            if not close_source and after_qty == 0:
                raise DomainError("全量拆分必须显式 close_source=true")
            payload = {
                "revision_id": revision_id,
                "artifact_id": artifact_id,
                "before_qty": before_qty,
                "after_qty": after_qty,
                "outputs": out_payloads,
                "close_source": close_source,
                "reason": data.get("reason"),
                "at": parse_moment(data.get("at"), "Asia/Shanghai"),
            }
            envelope, replayed = self.store.append(
                QUANTITY_SPLIT, f"artifact-{artifact_id}", payload,
                event_id=new_id("EVT"), client_key=data.get("client_key"),
            )
            if replayed:
                payload = envelope["payload"]
                revision_id = payload["revision_id"]
                out_payloads = payload["outputs"]
        return {
            "revision": self.revision_view(revision_id),
            "outputs": [self.artifact_view(o["artifact_id"]) for o in out_payloads],
            "source": self.artifact_view(artifact_id),
            "event_id": envelope["event_id"],
            "replayed": replayed,
        }

    def merge_artifacts(self, data: dict) -> dict:
        ids = require(data.get("input_artifact_ids"), "input_artifact_ids")
        if len(ids) < 2:
            raise DomainError("合并至少需要两个器物")
        with self.store.lock:
            inputs = [self._get(self.p.artifacts, i, "器物") for i in ids]
            batch_ids = {a["batch_id"] for a in inputs}
            if len(batch_ids) != 1:
                raise DomainError("只能合并同一批次内的器物")
            if len({a["kind"] for a in inputs}) != 1:
                raise DomainError("按套器物与单件追踪器物不能混合合并")
            if any(a["closed"] for a in inputs):
                raise DomainError("已终结器物不能再次参与合并")
            kind = inputs[0]["kind"]
            unit = inputs[0]["unit"]
            if kind == KIND_SET:
                qty = sum(a["current_qty"] for a in inputs)
            else:
                qty = sum(len(a["pieces"]) for a in inputs)
            output_id = new_id("ART")
            payload = {
                "revision_id": new_id("REV"),
                "input_artifact_ids": ids,
                "output_artifact_id": output_id,
                "kind": kind,
                "unit": unit,
                "qty": qty,
                "catalog_no": require(data.get("catalog_no"), "catalog_no"),
                "name": require(data.get("name"), "name"),
                "category": data.get("category") or inputs[0]["category"],
                "description": data.get("description"),
                "reason": data.get("reason"),
                "at": parse_moment(data.get("at"), "Asia/Shanghai"),
            }
            revision_id = payload["revision_id"]
            envelope, replayed = self.store.append(
                SET_MERGED, f"artifact-{output_id}", payload,
                event_id=new_id("EVT"), client_key=data.get("client_key"),
            )
            if replayed:
                output_id = envelope["payload"]["output_artifact_id"]
                revision_id = envelope["payload"]["revision_id"]
        return {
            "revision": self.revision_view(revision_id),
            "output": self.artifact_view(output_id),
            "event_id": envelope["event_id"],
            "replayed": replayed,
        }

    def correct_identity(self, data: dict) -> dict:
        target_type = require_choice(data.get("target_type"), ("artifact", "piece"), "target_type")
        target_id = require(data.get("target_id"), "target_id")
        after_in = require(data.get("after"), "after")
        with self.store.lock:
            if target_type == "artifact":
                target = self._get(self.p.artifacts, target_id, "器物")
                allowed = _ARTIFACT_CORRECTABLE
                stream = f"artifact-{target_id}"
            else:
                target = self._get(self.p.pieces, target_id, "单件")
                allowed = _PIECE_CORRECTABLE
                stream = f"artifact-{target['artifact_id']}"
            unknown = set(after_in) - allowed
            if unknown:
                raise DomainError(f"不允许更正的字段: {sorted(unknown)}")
            if not after_in:
                raise DomainError("after 至少包含一个字段")
            before = {k: target.get(k) for k in after_in}
            after = {k: after_in[k] for k in after_in}
            if before == after:
                raise DomainError("更正内容与现状完全相同，无需修订")
            revision_id = new_id("REV")
            payload = {
                "revision_id": revision_id,
                "target_type": target_type,
                "target_id": target_id,
                "before": before,
                "after": after,
                "reason": require(data.get("reason"), "reason"),
                "at": parse_moment(data.get("at"), "Asia/Shanghai"),
            }
            envelope, replayed = self.store.append(
                IDENTITY_CORRECTED, stream, payload,
                event_id=new_id("EVT"), client_key=data.get("client_key"),
            )
            if replayed:
                revision_id = envelope["payload"]["revision_id"]
        return {"revision": self.revision_view(revision_id),
                "event_id": envelope["event_id"], "replayed": replayed}

    def revision_view(self, revision_id: str) -> dict:
        return dict(self._get(self.p.revisions, revision_id, "修订记录"))

    def list_revisions(self) -> list[dict]:
        return [dict(r) for r in self.p.revisions.values()]

    # -- 文件：只存哈希与版本关系 -------------------------------------------

    def register_file(self, data: dict) -> dict:
        ref_type = require_choice(
            data.get("ref_type"), ("case", "batch", "artifact", "handover"), "ref_type"
        )
        ref_id = require(data.get("ref_id"), "ref_id")
        collection = {
            "case": self.p.cases,
            "batch": self.p.batches,
            "artifact": self.p.artifacts,
            "handover": self.p.handovers,
        }[ref_type]
        with self.store.lock:
            self._get(collection, ref_id, "关联对象")
            kind = require_choice(data.get("kind"), _FILE_KINDS, "kind")
            sha = require(data.get("sha256"), "sha256").lower()
            if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
                raise DomainError("sha256 必须为 64 位十六进制")
            file_id = new_id("FILE")
            payload = {
                "file_id": file_id,
                "ref_type": ref_type,
                "ref_id": ref_id,
                "kind": kind,
                "filename": require(data.get("filename"), "filename"),
                "media_type": data.get("media_type"),
                "sha256": sha,
                "size": data.get("size"),
                "note": data.get("note"),
                "at": parse_moment(data.get("at"), "UTC"),
            }
            envelope, replayed = self.store.append(
                FILE_REGISTERED, f"file-{file_id}", payload,
                event_id=new_id("EVT"), client_key=data.get("client_key"),
            )
            if replayed:
                file_id = envelope["payload"]["file_id"]
        return {"file": self.file_view(file_id),
                "event_id": envelope["event_id"], "replayed": replayed}

    def add_file_version(self, file_id: str, data: dict) -> dict:
        with self.store.lock:
            record = self._get(self.p.files, file_id, "文件")
            sha = require(data.get("sha256"), "sha256").lower()
            if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
                raise DomainError("sha256 必须为 64 位十六进制")
            if any(v["sha256"] == sha for v in record["versions"]):
                raise DomainError("该内容哈希已存在，不能作为新版本重复登记")
            version_no = record["current_version"] + 1
            payload = {
                "file_id": file_id,
                "version_no": version_no,
                "sha256": sha,
                "size": data.get("size"),
                "filename": data.get("filename") or record["filename"],
                "supersedes_version": data.get("supersedes_version")
                or record["current_version"],
                "note": data.get("note"),
                "at": parse_moment(data.get("at"), "UTC"),
            }
            envelope, replayed = self.store.append(
                FILE_VERSION_ADDED, f"file-{file_id}", payload,
                event_id=new_id("EVT"), client_key=data.get("client_key"),
            )
        return {"file": self.file_view(file_id),
                "event_id": envelope["event_id"], "replayed": replayed}

    def file_view(self, file_id: str) -> dict:
        return dict(self._get(self.p.files, file_id, "文件"))

    def list_files(self, ref_type: str | None = None, ref_id: str | None = None) -> list[dict]:
        result = self.p.files.values()
        if ref_type:
            result = [f for f in result if f["ref_type"] == ref_type]
        if ref_id:
            result = [f for f in result if f["ref_id"] == ref_id]
        return [dict(f) for f in result]

    # -- 交接：起草 / 提交 / 双时区签署 / 撤回 / 更正 ------------------------

    def _resolve_targets(self, artifact_ids, piece_ids):
        counts: dict[str, int] = {}
        artifact_ids = artifact_ids or []
        piece_ids = piece_ids or []
        for aid in artifact_ids:
            a = self._get(self.p.artifacts, aid, "器物")
            if a["closed"]:
                raise DomainError(f"器物已终结：{aid}，请使用修订后的现行器物")
            counts[aid] = len(a["pieces"]) if a["kind"] == KIND_SINGLE else a["current_qty"]
            if counts[aid] == 0:
                raise DomainError(f"器物现存数量为零：{aid}")
        for pid in piece_ids:
            piece = self._get(self.p.pieces, pid, "单件")
            aid = piece["artifact_id"]
            if aid in counts and self.p.artifacts[aid]["kind"] == KIND_SINGLE:
                raise DomainError(
                    f"单件 {pid} 已随器物 {aid} 整体交接，不能再重复列入"
                )
            counts[aid] = counts.get(aid, 0) + 1
        if not counts:
            raise DomainError("交接清单不能为空")
        return counts

    def draft_handover(self, data: dict) -> dict:
        stage = require_choice(data.get("stage"), STAGES, "stage")
        with self.store.lock:
            artifact_ids = data.get("artifact_ids") or []
            piece_ids = data.get("piece_ids") or []
            expected_qty = self._resolve_targets(artifact_ids, piece_ids)
            to_party = self._party(data, "to_party")
            if stage == STAGE_SEIZURE:
                from_party = None
                if data.get("from_party"):
                    from_party = self._party(data, "from_party")
                required = [to_party["key"]]
            else:
                from_party = self._party(data, "from_party")
                if from_party["key"] == to_party["key"]:
                    raise DomainError("交接双方不能为同一当事方")
                required = [from_party["key"], to_party["key"]]
            location_id = data.get("location_id")
            if location_id:
                self._get(self.p.locations, location_id, "保管地点")
            batch_id = data.get("batch_id")
            if batch_id:
                self._get(self.p.batches, batch_id, "批次")
            deadline = None
            if data.get("sign_deadline"):
                deadline = parse_moment(data["sign_deadline"], to_party["tz"])
            handover_id = new_id("HO")
            payload = {
                "handover_id": handover_id,
                "batch_id": batch_id,
                "stage": stage,
                "direction": data.get("direction") or DIR_RETURN,
                "from_party": from_party,
                "to_party": to_party,
                "required_signer_keys": required,
                "artifact_ids": artifact_ids,
                "piece_ids": piece_ids,
                "expected_qty": expected_qty,
                "location_id": location_id,
                "note": data.get("note"),
                "sign_deadline": deadline,
                "at": parse_moment(data.get("at"), to_party["tz"]),
                "corrects_handover_id": data.get("corrects_handover_id"),
                "reopens_handover_id": data.get("reopens_handover_id"),
            }
            corrects = data.get("corrects_handover_id")
            if corrects:
                original = self._get(self.p.handovers, corrects, "被更正交接")
                if original["status"] != STATUS_EFFECTIVE:
                    raise DomainError("只有已生效的交接才能被更正")
                if original["stage"] != stage:
                    raise DomainError("更正事件的环节必须与原事件一致")
            envelope, replayed = self.store.append(
                HANDOVER_DRAFTED, f"handover-{handover_id}", payload,
                event_id=new_id("EVT"), client_key=data.get("client_key"),
            )
            if replayed:
                handover_id = envelope["payload"]["handover_id"]
        return {"handover": self.handover_view(handover_id),
                "event_id": envelope["event_id"], "replayed": replayed}

    def _prior_for_key(self, client_key: str | None) -> dict | None:
        if client_key:
            return self.store.find_client_key(client_key)
        return None

    def submit_handover(self, handover_id: str, data: dict | None = None) -> dict:
        data = data or {}
        with self.store.lock:
            prior = self._prior_for_key(data.get("client_key"))
            if prior is not None:
                return {"handover": self.handover_view(handover_id),
                        "event_id": prior["event_id"], "replayed": True}
            h = self._get(self.p.handovers, handover_id, "交接事件")
            if h["status"] not in ("拟定", STATUS_WITHDRAWN):
                raise Conflict(f"当前状态 {h['status']} 不能提交签署")
            deadline = h.get("sign_deadline")
            hours = data.get("sign_deadline_hours")
            if hours is not None:
                dt = now_utc() + timedelta(hours=require_int(hours, "sign_deadline_hours", 1))
                deadline = parse_moment(_iso(dt), "UTC")
            elif data.get("sign_deadline"):
                deadline = parse_moment(data["sign_deadline"], h["to_party"]["tz"])
            payload = {
                "handover_id": handover_id,
                "at": parse_moment(data.get("at"), "UTC"),
                "sign_deadline": deadline,
            }
            envelope, replayed = self.store.append(
                HANDOVER_SUBMITTED, f"handover-{handover_id}", payload,
                event_id=new_id("EVT"), client_key=data.get("client_key"),
            )
        return {"handover": self.handover_view(handover_id),
                "event_id": envelope["event_id"], "replayed": replayed}

    def sign_handover(self, handover_id: str, data: dict) -> dict:
        party_key = require(data.get("party_key"), "party_key")
        signer = require(data.get("signer"), "signer")
        client_key = data.get("client_key")
        with self.store.lock:
            h0 = self.p.handovers.get(handover_id)
            if h0 is not None and client_key:
                prior = self.store.find_client_key(client_key)
                if prior is not None:
                    view = self.handover_view(handover_id)
                    return {"handover": view,
                            "completed": view["status"] == STATUS_EFFECTIVE,
                            "event_id": prior["event_id"], "replayed": True,
                            "issues_raised": []}
            h = self._get(self.p.handovers, handover_id, "交接事件")
            if h["status"] != STATUS_PENDING_SIGN:
                raise Conflict(f"当前状态 {h['status']} 不能签署")
            if party_key not in h["required_signer_keys"]:
                raise DomainError(f"当事方 {party_key} 不在签署方名单内")
            if party_key in h["signatures"]:
                raise Conflict("该方已完成签署，请勿重复签署", code="already_signed")
            party = None
            for side in (h["from_party"], h["to_party"]):
                if side and side["key"] == party_key:
                    party = side
            signed_at = parse_moment(data.get("signed_at"), party["tz"])
            signature_ref = data.get("signature_ref")
            if signature_ref is None and data.get("signature_payload") is not None:
                signature_ref = content_hash(data["signature_payload"])
            payload = {
                "handover_id": handover_id,
                "party_key": party_key,
                "party_name": party["name"],
                "signer": signer,
                "signature_ref": signature_ref,
                "signed_at": signed_at,
            }
            completes = set(h["signatures"]) | {party_key} >= set(
                h["required_signer_keys"]
            )
            specs = [
                {
                    "event_type": HANDOVER_SIGNED,
                    "stream_id": f"handover-{handover_id}",
                    "event_id": new_id("EVT"),
                    "payload": payload,
                    "client_key": client_key,
                    "signature": (handover_id, party_key),
                }
            ]
            if completes and h.get("corrects_handover_id"):
                specs.append(
                    {
                        "event_type": HANDOVER_CORRECTION_LINKED,
                        "stream_id": f"handover-{handover_id}",
                        "event_id": new_id("EVT"),
                        "payload": {
                            "original_handover_id": h["corrects_handover_id"],
                            "new_handover_id": handover_id,
                            "at": signed_at,
                        },
                    }
                )
            envelopes, replayed = self.store.append_many(specs)
            raised = []
            if completes and not replayed:
                raised = self._raise_issues_txn()
        view = self.handover_view(handover_id)
        return {"handover": view, "completed": view["status"] == STATUS_EFFECTIVE,
                "event_id": envelopes[0]["event_id"], "replayed": replayed,
                "issues_raised": raised}

    def receive_handover(self, handover_id: str, data: dict) -> dict:
        actual_in = require(data.get("actual_qty"), "actual_qty")
        with self.store.lock:
            prior = self._prior_for_key(data.get("client_key"))
            if prior is not None:
                return {"handover": self.handover_view(handover_id),
                        "event_id": prior["event_id"], "replayed": True,
                        "issues_raised": []}
            h = self._get(self.p.handovers, handover_id, "交接事件")
            if h["status"] != STATUS_EFFECTIVE:
                raise Conflict(f"当前状态 {h['status']}，尚未生效不能清点接收")
            actual: dict[str, int] = {}
            for aid, q in actual_in.items():
                self._get(self.p.artifacts, aid, "器物")
                actual[aid] = require_int(q, f"actual_qty.{aid}", 0)
            payload = {
                "handover_id": handover_id,
                "actual_qty": actual,
                "by": data.get("by"),
                "at": parse_moment(data.get("at"), "Asia/Shanghai"),
            }
            envelope, replayed = self.store.append(
                HANDOVER_RECEIVED, f"handover-{handover_id}", payload,
                event_id=new_id("EVT"), client_key=data.get("client_key"),
            )
            raised = []
            if not replayed:
                raised = self._raise_issues_txn()
        return {"handover": self.handover_view(handover_id),
                "event_id": envelope["event_id"], "replayed": replayed,
                "issues_raised": raised}

    def withdraw_handover(self, handover_id: str, data: dict | None = None) -> dict:
        data = data or {}
        with self.store.lock:
            prior = self._prior_for_key(data.get("client_key"))
            if prior is not None:
                return {"handover": self.handover_view(handover_id),
                        "event_id": prior["event_id"], "replayed": True}
            h = self._get(self.p.handovers, handover_id, "交接事件")
            if h["status"] not in ("拟定", STATUS_PENDING_SIGN):
                raise Conflict(f"当前状态 {h['status']} 不能撤回")
            if h["signatures"]:
                raise Conflict("已有当事方签署，不能撤回；如需变更请走更正流程")
            payload = {
                "handover_id": handover_id,
                "by": data.get("by"),
                "reason": require(data.get("reason"), "reason"),
                "at": parse_moment(data.get("at"), "UTC"),
            }
            envelope, replayed = self.store.append(
                HANDOVER_WITHDRAWN, f"handover-{handover_id}", payload,
                event_id=new_id("EVT"), client_key=data.get("client_key"),
            )
        return {"handover": self.handover_view(handover_id),
                "event_id": envelope["event_id"], "replayed": replayed}

    def handover_view(self, handover_id: str) -> dict:
        h = dict(self._get(self.p.handovers, handover_id, "交接事件"))
        h["signatures"] = [dict(s) for s in h["signatures"].values()]
        h["files"] = [
            f["id"] for f in self.p.files.values() if f["ref_id"] == handover_id
        ]
        return h

    def list_handovers(self, stage: str | None = None) -> list[dict]:
        items = self.p.handovers.values()
        if stage:
            items = [h for h in items if h["stage"] == stage]
        return [self.handover_view(h["id"]) for h in items]

    # -- 待处置队列 ----------------------------------------------------------

    def issue_view(self, issue_id: str) -> dict:
        return dict(self._get(self.p.issues, issue_id, "待处置事项"))

    def list_issues(self, status: str | None = None) -> list[dict]:
        items = self.p.issues.values()
        if status:
            items = [i for i in items if i["status"] == status]
        return [dict(i) for i in items]

    def acknowledge_issue(self, issue_id: str, data: dict | None = None) -> dict:
        data = data or {}
        with self.store.lock:
            issue = self._get(self.p.issues, issue_id, "待处置事项")
            if issue["status"] != "待处置":
                raise Conflict(f"事项状态为 {issue['status']}，无需受理")
            payload = {
                "issue_id": issue_id,
                "by": data.get("by"),
                "note": data.get("note"),
                "at": parse_moment(data.get("at"), "Asia/Shanghai"),
            }
            envelope, _ = self.store.append(
                ISSUE_ACKNOWLEDGED, f"issue-{issue_id}", payload,
                event_id=new_id("EVT"), client_key=data.get("client_key"),
            )
        return {"issue": self.issue_view(issue_id), "event_id": envelope["event_id"]}

    def resolve_issue(self, issue_id: str, data: dict) -> dict:
        with self.store.lock:
            issue = self._get(self.p.issues, issue_id, "待处置事项")
            if issue["status"] == "已关闭":
                raise Conflict("事项已关闭")
            payload = {
                "issue_id": issue_id,
                "by": data.get("by"),
                "resolution": require(data.get("resolution"), "resolution"),
                "at": parse_moment(data.get("at"), "Asia/Shanghai"),
            }
            envelope, _ = self.store.append(
                ISSUE_RESOLVED, f"issue-{issue_id}", payload,
                event_id=new_id("EVT"), client_key=data.get("client_key"),
            )
        return {"issue": self.issue_view(issue_id), "event_id": envelope["event_id"]}

    # -- 来源反查与审计核验 --------------------------------------------------

    def provenance(self, artifact_id: str) -> dict:
        """从任一现行器物反查：批次/案件、修订谱系、各环节交接与签署、文件、链状态。"""
        with self.store.lock:
            artifact = self._get(self.p.artifacts, artifact_id, "器物")
            batch = self.p.batches[artifact["batch_id"]]
            case = self.p.cases[batch["case_id"]]

            # 递归收集同一条来源线上的器物（拆分/合并前后）
            family: dict[str, dict] = {}
            stack = [artifact_id]
            while stack:
                aid = stack.pop()
                if aid in family or aid not in self.p.artifacts:
                    continue
                member = self.p.artifacts[aid]
                family[aid] = member
                stack.extend(member["parents"])
                stack.extend(member["children"])

            family_handovers = []
            for h in self.p.handovers.values():
                touched = bool(
                    set(h.get("artifact_ids", ())) & set(family)
                    or any(
                        self.p.pieces.get(pid, {}).get("artifact_id") in family
                        for pid in h.get("piece_ids", ())
                    )
                )
                if touched:
                    family_handovers.append(h["id"])

            family_pieces = {
                pid for member in family.values() for pid in member["pieces"]
            }
            revisions = [
                self.revision_view(rid)
                for rid, r in self.p.revisions.items()
                if (
                    (r["target_type"] == "artifact" and r["target_id"] in family)
                    or (r["target_type"] == "piece" and r["target_id"] in family_pieces)
                    or set(r["inputs"]) & set(family)
                    or any(o["artifact_id"] in family for o in r["outputs"])
                )
            ]
            files = [
                self.file_view(fid)
                for fid, f in self.p.files.items()
                if f["ref_type"] == "artifact" and f["ref_id"] in family
            ]
            handovers = [self.handover_view(hid) for hid in family_handovers]

            chain = self._chain_status(family)

            # 事件锚点：用于和审计档案逐条核对
            anchors = []
            streams = {f"artifact-{aid}" for aid in family}
            streams.update(f"handover-{hid}" for hid in family_handovers)
            streams.add(f"batch-{batch['id']}")
            for envelope in self.store.events():
                if envelope["stream_id"] in streams:
                    anchors.append(
                        {
                            "seq": envelope["seq"],
                            "stream_id": envelope["stream_id"],
                            "stream_seq": envelope["stream_seq"],
                            "event_type": envelope["event_type"],
                            "event_id": envelope["event_id"],
                            "hash": envelope["hash"],
                            "prev_hash": envelope["prev_hash"],
                            "recorded_at": envelope["recorded_at"],
                        }
                    )
            anchors.sort(key=lambda x: x["seq"])

            return {
                "artifact": self.artifact_view(artifact_id),
                "batch": dict(batch),
                "case": dict(case),
                "family": [
                    {
                        "artifact_id": aid,
                        "catalog_no": member["catalog_no"],
                        "name": member["name"],
                        "current_qty": member["current_qty"],
                        "status": (
                            "已终结" if member["closed"] else "现行"
                        ),
                        "parents": list(member["parents"]),
                        "children": list(member["children"]),
                    }
                    for aid, member in family.items()
                ],
                "revisions": sorted(revisions, key=lambda r: r["at"]["utc"]),
                "handovers": sorted(
                    handovers,
                    key=lambda h: (h["effective_at"] or h["drafted_at"]["utc"], h["id"]),
                ),
                "files": files,
                "custody_chain": chain,
                "event_anchors": anchors,
                "audit_head": self.store.head,
            }

    def _chain_status(self, family: dict[str, dict]) -> list[dict]:
        rows = []
        for aid, member in family.items():
            if member["kind"] == KIND_SET:
                units = [("artifact", aid, aid)]
            else:
                units = [("piece", pid, aid) for pid in member["pieces"]]
            for ref_type, unit_id, art_id in units:
                covering = []
                for h in self.p.handovers.values():
                    if h["status"] != STATUS_EFFECTIVE:
                        continue
                    covered = art_id in h.get("artifact_ids", ()) or (
                        ref_type == "piece" and unit_id in h.get("piece_ids", ())
                    )
                    if covered:
                        covering.append(h)
                covering.sort(
                    key=lambda x: (x["effective_at"] or x["drafted_at"]["utc"], x["id"])
                )
                present = {h["stage"] for h in covering}
                missing = [s for s in REQUIRED_CHAIN if s not in present]
                rows.append(
                    {
                        "unit_type": ref_type,
                        "unit_id": unit_id,
                        "artifact_id": art_id,
                        "events": [
                            {
                                "stage": h["stage"],
                                "handover_id": h["id"],
                                "from": (h["from_party"] or {}).get("key"),
                                "custodian": h["to_party"]["key"],
                                "location_id": h.get("location_id"),
                                "effective_at": h["effective_at"],
                            }
                            for h in covering
                        ],
                        "missing_stages": missing,
                        "complete": not missing,
                    }
                )
        return rows

    def audit_events(self, stream_id: str | None = None) -> list[dict]:
        return self.store.events(stream_id)

    def audit_verify(self) -> dict:
        result = self.store.verify()
        result["head"] = self.store.head
        return result
