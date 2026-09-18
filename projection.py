"""事件投影：从仅追加事件流重放出现行状态。

本模块不做任何 I/O，apply 结果完全由事件序列决定，
因此审计重放与线上状态共享同一套规则。
"""

from __future__ import annotations

from copy import deepcopy

from domain import (
    CORRECTION,
    KIND_SINGLE,
    MERGE,
    SPLIT,
    STATUS_CORRECTED,
    STATUS_DRAFT,
    STATUS_EFFECTIVE,
    STATUS_PENDING_SIGN,
    STATUS_WITHDRAWN,
)

# ---- 事件类型 -------------------------------------------------------------

CASE_OPENED = "CaseOpened"
LOCATION_REGISTERED = "LocationRegistered"
BATCH_REGISTERED = "BatchRegistered"
ARTIFACT_REGISTERED = "ArtifactRegistered"
PIECE_RECORDED = "PieceRecorded"
FILE_REGISTERED = "FileRegistered"
FILE_VERSION_ADDED = "FileVersionAdded"
QUANTITY_SPLIT = "QuantitySplit"
SET_MERGED = "SetMerged"
IDENTITY_CORRECTED = "IdentityCorrected"
HANDOVER_DRAFTED = "HandoverDrafted"
HANDOVER_SUBMITTED = "HandoverSubmitted"
HANDOVER_SIGNED = "HandoverSigned"
HANDOVER_RECEIVED = "HandoverReceived"
HANDOVER_WITHDRAWN = "HandoverWithdrawn"
HANDOVER_CORRECTION_LINKED = "HandoverCorrectionLinked"
ISSUE_RAISED = "IssueRaised"
ISSUE_ACKNOWLEDGED = "IssueAcknowledged"
ISSUE_RESOLVED = "IssueResolved"


class Projection:
    """内存投影。所有字典均为可变现行状态，历史只存在于事件流。"""

    def __init__(self) -> None:
        self.cases: dict[str, dict] = {}
        self.locations: dict[str, dict] = {}
        self.batches: dict[str, dict] = {}
        self.artifacts: dict[str, dict] = {}
        self.pieces: dict[str, dict] = {}
        self.files: dict[str, dict] = {}
        self.handovers: dict[str, dict] = {}
        self.revisions: dict[str, dict] = {}
        self.issues: dict[str, dict] = {}
        self.streams: dict[str, list[int]] = {}
        # open issue 去重索引：(kind, ref_type, ref_id, detail) -> issue_id
        self.open_issue_index: dict[tuple, str] = {}

    # -- 公共 ---------------------------------------------------------------

    def apply(self, event: dict) -> None:
        seq = event["seq"]
        stream = event["stream_id"]
        self.streams.setdefault(stream, []).append(seq)
        payload = event["payload"]
        handler = _HANDLERS.get(event["event_type"])
        if handler is None:
            raise ValueError(f"未知事件类型: {event['event_type']}")
        handler(self, payload)

    def history(self, stream_id: str) -> list[int]:
        return list(self.streams.get(stream_id, []))

    # -- 查询辅助 -----------------------------------------------------------

    def batch_artifacts(self, batch_id: str) -> list[dict]:
        return [a for a in self.artifacts.values() if a["batch_id"] == batch_id]

    def snapshot(self) -> dict:
        return {
            "cases": deepcopy(self.cases),
            "locations": deepcopy(self.locations),
            "batches": deepcopy(self.batches),
            "artifacts": deepcopy(self.artifacts),
            "pieces": deepcopy(self.pieces),
            "files": deepcopy(self.files),
            "handovers": deepcopy(self.handovers),
            "revisions": deepcopy(self.revisions),
            "issues": deepcopy(self.issues),
        }


# ---- 单个事件 apply 规则 ---------------------------------------------------


def _case_opened(p: Projection, d: dict) -> None:
    p.cases[d["case_id"]] = {
        "id": d["case_id"],
        "case_no": d["case_no"],
        "name": d["name"],
        "source_country": d.get("source_country"),
        "note": d.get("note"),
        "opened_at": d["at"],
    }


def _location_registered(p: Projection, d: dict) -> None:
    p.locations[d["location_id"]] = {
        "id": d["location_id"],
        "code": d["code"],
        "name": d["name"],
        "kind": d["kind"],
        "address": d.get("address"),
    }


def _batch_registered(p: Projection, d: dict) -> None:
    p.batches[d["batch_id"]] = {
        "id": d["batch_id"],
        "case_id": d["case_id"],
        "batch_no": d["batch_no"],
        "foreign_agency": d["foreign_agency"],
        "agency_party_key": d.get("agency_party_key"),
        "handover_city": d["handover_city"],
        "handover_date": d["handover_date"],
        "tz": d["tz"],
        "note": d.get("note"),
    }


def _upsert_artifact(p: Projection, d: dict, born_from: str | None = None) -> None:
    artifact = {
        "id": d["artifact_id"],
        "batch_id": d["batch_id"],
        "catalog_no": d["catalog_no"],
        "name": d["name"],
        "category": d["category"],
        "kind": d["kind"],
        "unit": d["unit"],
        "registered_qty": d["qty"],
        "current_qty": d["qty"],
        "description": d.get("description"),
        "pieces": [],
        "parents": [],
        "children": [],
        "born_from_revision": born_from,
        "closed": False,
        "closed_by_revision": None,
        "merged_into": None,
        "correction_seq": 0,
    }
    p.artifacts[d["artifact_id"]] = artifact


def _artifact_registered(p: Projection, d: dict) -> None:
    _upsert_artifact(p, d)


def _piece_recorded(p: Projection, d: dict) -> None:
    artifact = p.artifacts[d["artifact_id"]]
    pid = d["piece_id"]
    piece = {
        "id": pid,
        "artifact_id": d["artifact_id"],
        "seq_no": d["seq_no"],
        "label": d["label"],
        "description": d.get("description"),
        "current_location_id": None,
        "current_custodian": None,
        "current_stage": None,
    }
    p.pieces[pid] = piece
    artifact["pieces"].append(pid)


def _file_registered(p: Projection, d: dict) -> None:
    p.files[d["file_id"]] = {
        "id": d["file_id"],
        "ref_type": d["ref_type"],
        "ref_id": d["ref_id"],
        "kind": d["kind"],
        "filename": d["filename"],
        "media_type": d.get("media_type"),
        "current_version": 1,
        "versions": [
            {
                "version_no": 1,
                "sha256": d["sha256"],
                "size": d.get("size"),
                "filename": d["filename"],
                "note": d.get("note"),
                "recorded_at": d["at"],
            }
        ],
    }


def _file_version_added(p: Projection, d: dict) -> None:
    record = p.files[d["file_id"]]
    record["versions"].append(
        {
            "version_no": d["version_no"],
            "sha256": d["sha256"],
            "size": d.get("size"),
            "filename": d.get("filename"),
            "supersedes_version": d.get("supersedes_version"),
            "note": d.get("note"),
            "recorded_at": d["at"],
        }
    )
    record["current_version"] = d["version_no"]


def _quantity_split(p: Projection, d: dict) -> None:
    source = p.artifacts[d["artifact_id"]]
    revision = {
        "id": d["revision_id"],
        "kind": SPLIT,
        "target_type": "artifact",
        "target_id": d["artifact_id"],
        "inputs": [d["artifact_id"]],
        "outputs": [],
        "before": {"qty": d["before_qty"], "pieces": list(source["pieces"])},
        "after": None,
        "reason": d.get("reason"),
        "at": d["at"],
    }
    for out in d["outputs"]:
        if out["artifact_id"] not in p.artifacts:
            _upsert_artifact(
                p,
                {
                    "artifact_id": out["artifact_id"],
                    "batch_id": source["batch_id"],
                    "catalog_no": out.get("catalog_no"),
                    "name": out.get("name", source["name"]),
                    "category": source["category"],
                    "kind": out["kind"],
                    "unit": out["unit"],
                    "qty": out["qty"],
                    "description": out.get("description"),
                },
                born_from=d["revision_id"],
            )
        target = p.artifacts[out["artifact_id"]]
        target["parents"].append(d["artifact_id"])
        source["children"].append(out["artifact_id"])
        revision["outputs"].append(
            {"artifact_id": out["artifact_id"], "qty": out["qty"]}
        )
        for pid in out.get("piece_ids", []):
            piece = p.pieces[pid]
            p.artifacts[piece["artifact_id"]]["pieces"].remove(pid)
            piece["artifact_id"] = out["artifact_id"]
            target["pieces"].append(pid)
    source["current_qty"] = d["after_qty"]
    if d.get("close_source"):
        source["closed"] = True
        source["closed_by_revision"] = d["revision_id"]
    revision["after"] = {
        "qty": d["after_qty"],
        "pieces": list(source["pieces"]),
        "closed": source["closed"],
    }
    p.revisions[d["revision_id"]] = revision


def _set_merged(p: Projection, d: dict) -> None:
    inputs = [p.artifacts[i] for i in d["input_artifact_ids"]]
    batch_id = inputs[0]["batch_id"]
    _upsert_artifact(
        p,
        {
            "artifact_id": d["output_artifact_id"],
            "batch_id": batch_id,
            "catalog_no": d.get("catalog_no"),
            "name": d.get("name"),
            "category": d.get("category", inputs[0]["category"]),
            "kind": d["kind"],
            "unit": d["unit"],
            "qty": d["qty"],
            "description": d.get("description"),
        },
        born_from=d["revision_id"],
    )
    output = p.artifacts[d["output_artifact_id"]]
    moved_pieces: list[str] = []
    for source in inputs:
        source["closed"] = True
        source["closed_by_revision"] = d["revision_id"]
        source["merged_into"] = d["output_artifact_id"]
        output["parents"].append(source["id"])
        source["children"].append(d["output_artifact_id"])
        for pid in list(source["pieces"]):
            piece = p.pieces[pid]
            source["pieces"].remove(pid)
            piece["artifact_id"] = d["output_artifact_id"]
            output["pieces"].append(pid)
            moved_pieces.append(pid)
    p.revisions[d["revision_id"]] = {
        "id": d["revision_id"],
        "kind": MERGE,
        "target_type": "artifact",
        "target_id": d["output_artifact_id"],
        "inputs": list(d["input_artifact_ids"]),
        "outputs": [{"artifact_id": d["output_artifact_id"], "qty": d["qty"]}],
        "before": {
            "inputs": {
                s["id"]: {"qty": s["current_qty"], "pieces": list(s["pieces"])}
                for s in inputs
            }
        },
        "after": {"qty": d["qty"], "pieces": moved_pieces},
        "reason": d.get("reason"),
        "at": d["at"],
    }


def _identity_corrected(p: Projection, d: dict) -> None:
    if d["target_type"] == "artifact":
        target = p.artifacts[d["target_id"]]
    else:
        target = p.pieces[d["target_id"]]
    for key, value in d["after"].items():
        target[key] = value
    if d["target_type"] == "artifact":
        target["correction_seq"] += 1
    p.revisions[d["revision_id"]] = {
        "id": d["revision_id"],
        "kind": CORRECTION,
        "target_type": d["target_type"],
        "target_id": d["target_id"],
        "inputs": [],
        "outputs": [],
        "before": d["before"],
        "after": d["after"],
        "reason": d.get("reason"),
        "at": d["at"],
    }


def _handover_drafted(p: Projection, d: dict) -> None:
    p.handovers[d["handover_id"]] = {
        "id": d["handover_id"],
        "batch_id": d.get("batch_id"),
        "stage": d["stage"],
        "direction": d.get("direction"),
        "status": STATUS_DRAFT,
        "from_party": d["from_party"],
        "to_party": d["to_party"],
        "required_signer_keys": d["required_signer_keys"],
        "artifact_ids": list(d.get("artifact_ids", [])),
        "piece_ids": list(d.get("piece_ids", [])),
        "expected_qty": dict(d.get("expected_qty", {})),
        "actual_qty": None,
        "location_id": d.get("location_id"),
        "note": d.get("note"),
        "sign_deadline": d.get("sign_deadline"),
        "drafted_at": d["at"],
        "submitted_at": None,
        "signatures": {},
        "effective_at": None,
        "received_records": [],
        "corrects_handover_id": d.get("corrects_handover_id"),
        "reopens_handover_id": d.get("reopens_handover_id"),
        "corrected_by": None,
    }


def _handover_submitted(p: Projection, d: dict) -> None:
    h = p.handovers[d["handover_id"]]
    h["status"] = STATUS_PENDING_SIGN
    h["submitted_at"] = d["at"]
    if d.get("sign_deadline"):
        h["sign_deadline"] = d["sign_deadline"]


def _advance_custody(p: Projection, h: dict) -> None:
    """生效瞬间把责任与位置推进到被移交的单件。"""
    custodian = h["to_party"]["key"]
    custodian_name = h["to_party"]["name"]
    piece_ids = set(h.get("piece_ids", ()))
    for aid in h.get("artifact_ids", ()):
        artifact = p.artifacts.get(aid)
        if artifact:
            piece_ids.update(artifact["pieces"])
    for pid in piece_ids:
        piece = p.pieces.get(pid)
        if piece is None:
            continue
        piece["current_custodian"] = {
            "key": custodian,
            "name": custodian_name,
            "handover_id": h["id"],
            "stage": h["stage"],
        }
        piece["current_location_id"] = h.get("location_id")
        piece["current_stage"] = h["stage"]


def _handover_signed(p: Projection, d: dict) -> None:
    h = p.handovers[d["handover_id"]]
    h["signatures"][d["party_key"]] = {
        "party_key": d["party_key"],
        "party_name": d.get("party_name"),
        "signer": d["signer"],
        "signature_ref": d.get("signature_ref"),
        "signed_at": d["signed_at"],
    }
    if set(h["signatures"]) >= set(h["required_signer_keys"]):
        h["status"] = STATUS_EFFECTIVE
        h["effective_at"] = d["signed_at"]["utc"]
        _advance_custody(p, h)


def _handover_received(p: Projection, d: dict) -> None:
    h = p.handovers[d["handover_id"]]
    h["actual_qty"] = dict(d["actual_qty"])
    h["received_records"].append(
        {"actual_qty": dict(d["actual_qty"]), "at": d["at"], "by": d.get("by")}
    )


def _handover_withdrawn(p: Projection, d: dict) -> None:
    h = p.handovers[d["handover_id"]]
    h["status"] = STATUS_WITHDRAWN
    h["withdrawn"] = {"by": d.get("by"), "reason": d.get("reason"), "at": d["at"]}


def _handover_correction_linked(p: Projection, d: dict) -> None:
    original = p.handovers[d["original_handover_id"]]
    original["status"] = STATUS_CORRECTED
    original["corrected_by"] = d["new_handover_id"]
    if d["new_handover_id"] in p.handovers:
        p.handovers[d["new_handover_id"]]["corrects_handover_id"] = d[
            "original_handover_id"
        ]


def _index_open_issue(p: Projection, issue: dict) -> None:
    p.open_issue_index[
        (issue["kind"], issue["ref_type"], issue["ref_id"], issue["detail"])
    ] = issue["id"]


def _drop_open_issue_index(p: Projection, issue: dict) -> None:
    p.open_issue_index.pop(
        (issue["kind"], issue["ref_type"], issue["ref_id"], issue["detail"]), None
    )


def _issue_raised(p: Projection, d: dict) -> None:
    issue = {
        "id": d["issue_id"],
        "kind": d["kind"],
        "ref_type": d["ref_type"],
        "ref_id": d["ref_id"],
        "detail": d.get("detail"),
        "status": "待处置",
        "raised_at": d["at"],
        "history": [{"status": "待处置", "at": d["at"], "note": d.get("detail")}],
    }
    p.issues[d["issue_id"]] = issue
    _index_open_issue(p, issue)


def _issue_acknowledged(p: Projection, d: dict) -> None:
    issue = p.issues[d["issue_id"]]
    _drop_open_issue_index(p, issue)
    issue["status"] = "处置中"
    issue["history"].append(
        {"status": "处置中", "at": d["at"], "by": d.get("by"), "note": d.get("note")}
    )


def _issue_resolved(p: Projection, d: dict) -> None:
    issue = p.issues[d["issue_id"]]
    _drop_open_issue_index(p, issue)
    issue["status"] = "已关闭"
    issue["resolution"] = d.get("resolution")
    issue["history"].append(
        {"status": "已关闭", "at": d["at"], "by": d.get("by"), "note": d.get("resolution")}
    )


_HANDLERS = {
    CASE_OPENED: _case_opened,
    LOCATION_REGISTERED: _location_registered,
    BATCH_REGISTERED: _batch_registered,
    ARTIFACT_REGISTERED: _artifact_registered,
    PIECE_RECORDED: _piece_recorded,
    FILE_REGISTERED: _file_registered,
    FILE_VERSION_ADDED: _file_version_added,
    QUANTITY_SPLIT: _quantity_split,
    SET_MERGED: _set_merged,
    IDENTITY_CORRECTED: _identity_corrected,
    HANDOVER_DRAFTED: _handover_drafted,
    HANDOVER_SUBMITTED: _handover_submitted,
    HANDOVER_SIGNED: _handover_signed,
    HANDOVER_RECEIVED: _handover_received,
    HANDOVER_WITHDRAWN: _handover_withdrawn,
    HANDOVER_CORRECTION_LINKED: _handover_correction_linked,
    ISSUE_RAISED: _issue_raised,
    ISSUE_ACKNOWLEDGED: _issue_acknowledged,
    ISSUE_RESOLVED: _issue_resolved,
}
