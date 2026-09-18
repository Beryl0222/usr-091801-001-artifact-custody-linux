"""返还文物交接账 HTTP 接口。

路由以 ``/api/`` 为前缀，全部使用 JSON；写接口支持请求头
``Idempotency-Key``，同一键重复回调返回首次事件，不会产生第二次交接。

错误映射：:class:`DomainError` -> 400，:class:`ConflictError` -> 409，
:class:`NotFoundError` -> 404。
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

from app import CustodyService
from events import (
    ConflictError,
    DomainError,
    NotFoundError,
)


def build_handler():
    class WebHandler(BaseHTTPRequestHandler):
        server_version = "ArtifactCustody/1.0"

        @property
        def service(self) -> CustodyService:
            svc = getattr(self.server, "service", None)
            if svc is None:
                raise DomainError("领域服务未挂载")
            return svc

        # -------------------------------------------------------------- 基础

        def _send_json(self, status: int, body: dict | list) -> None:
            data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _read_body(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            try:
                body = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise DomainError(f"请求体不是合法 JSON: {exc}") from exc
            if not isinstance(body, dict):
                raise DomainError("请求体必须是 JSON 对象")
            return body

        def _idem(self, body: dict) -> str | None:
            return self.headers.get("Idempotency-Key") or body.pop("idem_key", None)

        def _handle_errors(self, fn):
            try:
                fn()
            except NotFoundError as exc:
                self._send_json(404, {"error": "not_found", "message": str(exc)})
            except ConflictError as exc:
                self._send_json(409, {"error": "conflict", "message": str(exc)})
            except (DomainError, ValueError) as exc:
                self._send_json(400, {"error": "bad_request", "message": str(exc)})
            except KeyError as exc:
                self._send_json(
                    400,
                    {"error": "bad_request", "message": f"缺少必填字段: {exc.args[0]}"},
                )

        def log_message(self, *_args):
            return

        # -------------------------------------------------------------- 分发

        def do_GET(self):
            self._handle_errors(lambda: self._route("GET"))

        def do_POST(self):
            self._handle_errors(lambda: self._route("POST"))

        def _route(self, method: str) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            segments = [s for s in path.split("/") if s]

            if method == "GET" and path == "/health":
                from service import health_payload

                self._send_json(200, health_payload())
                return
            if segments and segments[0] != "api":
                self._send_json(404, {"error": "not_found", "message": "未知路由"})
                return
            parts = segments[1:]
            handler = self._match(method, parts, query)
            if handler is None:
                self._send_json(404, {"error": "not_found", "message": "未知路由"})
                return
            handler()

        def _match(self, method, parts, query):
            p = parts
            # ---- 主数据
            if method == "POST" and p == ["cases"]:
                return self.create_case
            if method == "GET" and p == ["cases"]:
                return lambda: self.ok(self.service.list_cases())
            if method == "GET" and len(p) == 2 and p[0] == "cases":
                return lambda: self.ok(self.service.get_case(p[1]))
            if method == "POST" and p == ["locations"]:
                return self.create_location
            if method == "GET" and p == ["locations"]:
                return lambda: self.ok(self.service.list_locations())
            if method == "POST" and p == ["batches"]:
                return self.create_batch
            if method == "GET" and p == ["batches"]:
                return lambda: self.ok(
                    self.service.list_batches(case_id=query.get("case_id"))
                )
            if method == "GET" and len(p) == 2 and p[0] == "batches":
                return lambda: self.batch_detail(p[1])
            # ---- 器物
            if method == "POST" and p == ["artifacts"]:
                return self.create_artifact
            if method == "GET" and p == ["artifacts"]:
                return lambda: self.ok(
                    self.service.list_artifacts(batch_id=query.get("batch_id"))
                )
            if method == "POST" and p == ["artifacts", "merge"]:
                return self.merge_artifacts
            if method == "GET" and len(p) == 3 and p[0] == "artifacts" and p[2] == "trace":
                return lambda: self.ok(self.service.trace(p[1]))
            if method == "GET" and len(p) == 3 and p[0] == "artifacts" and p[2] == "lineage":
                return lambda: self.ok(self.service.lineage(p[1]))
            if method == "POST" and len(p) == 3 and p[0] == "artifacts" and p[2] == "split":
                return lambda: self.split_artifact(p[1])
            if method == "POST" and len(p) == 4 and p[0] == "artifacts" and p[2] == "identity":
                return lambda: self.correct_identity(p[1])
            if method == "GET" and len(p) == 2 and p[0] == "artifacts":
                return lambda: self.ok(self.service.get_artifact(p[1]))
            # ---- 责任链
            if method == "POST" and p == ["chain"]:
                return self.record_chain
            # ---- 交接
            if method == "POST" and p == ["handovers"]:
                return self.prepare_handover
            if method == "GET" and p == ["handovers"]:
                return lambda: self.ok(
                    self.service.list_handovers(batch_id=query.get("batch_id"))
                )
            if method == "GET" and len(p) == 2 and p[0] == "handovers":
                return lambda: self.ok(self.service.get_handover(p[1]))
            if method == "POST" and len(p) == 3 and p[0] == "handovers" and p[2] == "submit":
                return lambda: self.submit_handover(p[1])
            if method == "POST" and len(p) == 3 and p[0] == "handovers" and p[2] == "sign":
                return lambda: self.sign_handover(p[1])
            if method == "POST" and len(p) == 3 and p[0] == "handovers" and p[2] == "withdraw":
                return lambda: self.withdraw_handover(p[1])
            if method == "POST" and len(p) == 3 and p[0] == "handovers" and p[2] == "resubmit":
                return lambda: self.resubmit_handover(p[1])
            if method == "POST" and len(p) == 3 and p[0] == "handovers" and p[2] == "correct":
                return lambda: self.correct_handover(p[1])
            # ---- 文件哈希
            if method == "POST" and p == ["documents"]:
                return self.create_document
            if method == "GET" and p == ["documents"]:
                return lambda: self.ok(
                    self.service.list_documents(
                        subject_type=query.get("subject_type"),
                        subject_id=query.get("subject_id"),
                    )
                )
            if method == "GET" and len(p) == 2 and p[0] == "documents":
                return lambda: self.ok(self.service.get_document(p[1]))
            if method == "POST" and len(p) == 3 and p[0] == "documents" and p[2] == "versions":
                return lambda: self.add_document_version(p[1])
            # ---- 待处置队列
            if method == "GET" and p == ["queue"]:
                return lambda: self.ok(
                    self.service.list_queue(
                        include_resolved=query.get("include_resolved") in ("1", "true")
                    )
                )
            if method == "POST" and len(p) == 3 and p[0] == "queue" and p[2] == "resolve":
                return lambda: self.resolve_queue(p[1])
            if method == "POST" and p == ["sweep-overdue"]:
                return lambda: self.ok({"opened": self.service.sweep_overdue()})
            # ---- 审计与事件
            if method == "GET" and p == ["events"]:
                return lambda: self.ok(
                    [self.service._public_event(e) for e in self.service.list_events()]
                )
            if method == "GET" and p == ["audit"]:
                return lambda: self.ok(self.service.audit_report())
            return None

        def ok(self, body, status: int = 200):
            self._send_json(status, body)

        def created(self, result: dict):
            status = 200 if result.get("replayed") else 201
            self._send_json(status, result)

        # -------------------------------------------------------------- 写接口

        def create_case(self):
            b = self._read_body()
            self.created(
                self.service.open_case(
                    b["title"],
                    source_country=b.get("source_country"),
                    note=b.get("note"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def create_location(self):
            b = self._read_body()
            self.created(
                self.service.register_location(
                    b["name"],
                    kind=b.get("kind"),
                    address=b.get("address"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def create_batch(self):
            b = self._read_body()
            self.created(
                self.service.create_batch(
                    b["case_id"],
                    b["foreign_agency"],
                    agency_timezone=b.get("agency_timezone"),
                    expected_on=b.get("expected_on"),
                    location_id=b.get("location_id"),
                    note=b.get("note"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def batch_detail(self, batch_id):
            batch = self.service.get_batch(batch_id)
            body = {
                "batch": batch,
                "artifacts": self.service.list_artifacts(batch_id),
                "handovers": self.service.list_handovers(batch_id),
            }
            self.ok(body)

        def create_artifact(self):
            b = self._read_body()
            self.created(
                self.service.register_artifact(
                    b["batch_id"],
                    b["kind"],
                    b["category"],
                    b["name"],
                    quantity=b.get("quantity"),
                    components=b.get("components"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def record_chain(self):
            b = self._read_body()
            result = self.service.record_chain(
                b["artifact_ids"],
                b["stage"],
                location_id=b.get("location_id"),
                actor=b.get("actor"),
                idem_key=self._idem(b),
                at_local=b.get("at_local"),
                tz=b.get("tz"),
            )
            self._send_json(200 if result.get("queued") else 201, result)

        def prepare_handover(self):
            b = self._read_body()
            self.created(
                self.service.prepare_handover(
                    b["batch_id"],
                    b["stage"],
                    b["from_party"],
                    b["to_party"],
                    expected_quantity=b.get("expected_quantity"),
                    deadline=b.get("deadline"),
                    submit=b.get("submit", True),
                    corrects=b.get("corrects"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def submit_handover(self, handover_id):
            b = self._read_body()
            self.created(
                self.service.submit_handover(
                    handover_id,
                    deadline=b.get("deadline"),
                    expected_quantity=b.get("expected_quantity"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def sign_handover(self, handover_id):
            b = self._read_body()
            result = self.service.sign_handover(
                handover_id,
                b["party"],
                b.get("actor") or self.headers.get("X-Actor") or b["party"],
                at_local=b["at_local"],
                tz=b["tz"],
                observed_quantity=b.get("observed_quantity"),
                idem_key=self._idem(b),
            )
            self._send_json(200 if result.get("replayed") else 201, result)

        def withdraw_handover(self, handover_id):
            b = self._read_body()
            self.created(
                self.service.withdraw_handover(
                    handover_id,
                    reason=b.get("reason"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def resubmit_handover(self, handover_id):
            b = self._read_body()
            self.created(
                self.service.resubmit_handover(
                    handover_id,
                    deadline=b.get("deadline"),
                    expected_quantity=b.get("expected_quantity"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def correct_handover(self, handover_id):
            b = self._read_body()
            self.created(
                self.service.correct_handover(
                    handover_id,
                    expected_quantity=b.get("expected_quantity"),
                    deadline=b.get("deadline"),
                    from_party=b.get("from_party"),
                    to_party=b.get("to_party"),
                    stage=b.get("stage"),
                    reason=b.get("reason"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def split_artifact(self, artifact_id):
            b = self._read_body()
            self.created(
                self.service.split_artifact(
                    artifact_id,
                    b["outputs"],
                    reason=b.get("reason"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def merge_artifacts(self):
            b = self._read_body()
            self.created(
                self.service.merge_artifacts(
                    b["artifact_ids"],
                    b["name"],
                    category=b.get("category"),
                    reason=b.get("reason"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def correct_identity(self, artifact_id):
            b = self._read_body()
            self.created(
                self.service.correct_identity(
                    artifact_id,
                    b.get("after", {}),
                    reason=b.get("reason"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def create_document(self):
            b = self._read_body()
            self.created(
                self.service.record_document(
                    b["subject_type"],
                    b["subject_id"],
                    b["kind"],
                    b["filename"],
                    b["sha256"],
                    size=b.get("size"),
                    media_type=b.get("media_type"),
                    note=b.get("note"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def add_document_version(self, doc_id):
            b = self._read_body()
            self.created(
                self.service.add_document_version(
                    doc_id,
                    b["sha256"],
                    size=b.get("size"),
                    media_type=b.get("media_type"),
                    note=b.get("note"),
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

        def resolve_queue(self, queue_id):
            b = self._read_body()
            self.created(
                self.service.resolve_queue(
                    queue_id,
                    b["note"],
                    actor=b.get("actor"),
                    idem_key=self._idem(b),
                )
            )

    return WebHandler
