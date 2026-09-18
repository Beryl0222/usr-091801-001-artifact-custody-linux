"""HTTP 入口：返还文物交接账。

保留基线契约（SERVICE_ID / SERVICE_NAME / health_payload / Handler / --check），
其余路径路由到应用层。Handler 可通过 make_handler(store) 绑定独立存储，
便于测试隔离；默认使用 data/ledger.db。
"""

from __future__ import annotations

import argparse
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from app import Service
from domain import DomainError, SERVICE_ID, SERVICE_NAME
from store import EventStore

DEFAULT_DB = os.environ.get(
    "ARTIFACT_DB_PATH", os.path.join("data", "ledger.db")
)

_TOP_LEVEL_RESOURCES = {
    "cases", "locations", "batches", "artifacts", "revisions",
    "files", "handovers", "issues", "audit",
}


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


_STORE_LOCK = threading.Lock()
_DEFAULT_STORE: EventStore | None = None


def get_default_store() -> EventStore:
    global _DEFAULT_STORE
    with _STORE_LOCK:
        if _DEFAULT_STORE is None:
            _DEFAULT_STORE = EventStore(DEFAULT_DB)
        return _DEFAULT_STORE


def make_handler(store: EventStore) -> type[BaseHTTPRequestHandler]:
    """生成绑定指定存储的 Handler 类（测试隔离用）。"""

    class BoundHandler(Handler):
        pass

    BoundHandler.store = store
    BoundHandler.service = Service(store)
    return BoundHandler


class Handler(BaseHTTPRequestHandler):
    store: EventStore | None = None
    service: Service | None = None

    def svc(self) -> Service:
        if self.service is None:
            type(self).store = get_default_store()
            type(self).service = Service(type(self).store)
        return self.service  # type: ignore[return-value]

    # -- 基础框架 -----------------------------------------------------------

    def _send_json(self, payload, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise DomainError(f"请求体不是合法 JSON: {exc}", code="bad_json", status=400)
        if not isinstance(data, dict):
            raise DomainError("请求体必须为 JSON 对象", code="bad_json", status=400)
        return data

    def _handle(self, method: str):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        try:
            if method == "GET" and path == "/health":
                self._send_json(health_payload(), 200)
                return
            handler = self._route(method, path)
            if handler is None:
                self._send_json(
                    {"error": "not_found", "message": f"路径不存在: {path}"}, 404
                )
                return
            body = self._read_json() if method in ("POST", "PUT", "PATCH") else {}
            result, status = handler(body, query)
            self._send_json(result, status)
        except DomainError as exc:
            self._send_json(
                {"error": exc.code, "message": exc.message}, exc.status
            )
        except Exception as exc:  # noqa: BLE001
            self._send_json(
                {"error": "internal_error", "message": str(exc)}, 500
            )

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    def log_message(self, *_args):
        return

    # -- 路由 ---------------------------------------------------------------

    def _route(self, method: str, path: str):
        segments = [s for s in path.split("/") if s]
        # 未知顶层资源直接 404，不触发存储初始化
        if not segments or segments[0] not in _TOP_LEVEL_RESOURCES:
            return None
        svc = self.svc()
        routes = self._routes(svc)
        for methods, prefix, func in routes:
            if method in methods and len(segments) == len(prefix):
                args = {}
                ok = True
                for seg, token in zip(segments, prefix):
                    if token.startswith("{") and token.endswith("}"):
                        args[token[1:-1]] = seg
                    elif seg != token:
                        ok = False
                        break
                if ok:
                    return lambda body, query, f=func, a=args: f(body, query, **a)
        return None

    def _routes(self, svc: Service):
        def c(fn):
            return lambda body, query, **a: (fn(body, **a), 200)

        def q(fn):
            return lambda body, query, **a: (fn(body, query, **a), 200)

        return [
            # 案件
            ({"POST"}, ["cases"], c(lambda b: svc.open_case(b))),
            ({"GET"}, ["cases"], c(lambda b: {"cases": svc.list_cases()})),
            ({"GET"}, ["cases", "{case_id}"], c(lambda b, case_id: {"case": svc.case_view(case_id)})),
            # 地点
            ({"POST"}, ["locations"], c(lambda b: svc.register_location(b))),
            ({"GET"}, ["locations"], c(lambda b: {"locations": svc.list_locations()})),
            # 批次
            ({"POST"}, ["batches"], c(lambda b: svc.register_batch(b))),
            ({"GET"}, ["batches"], q(
                lambda b, query: {"batches": svc.list_batches(query.get("case_id"))})),
            ({"GET"}, ["batches", "{batch_id}"], c(
                lambda b, batch_id: {"batch": svc.batch_view(batch_id)})),
            # 器物
            ({"POST"}, ["artifacts"], c(lambda b: svc.register_artifact(b))),
            ({"GET"}, ["artifacts"], q(
                lambda b, query: {"artifacts": svc.list_artifacts(query.get("batch_id"))})),
            ({"GET"}, ["artifacts", "{artifact_id}"], c(
                lambda b, artifact_id: {"artifact": svc.artifact_view(artifact_id)})),
            ({"POST"}, ["artifacts", "{artifact_id}", "pieces"], c(
                lambda b, artifact_id: svc.add_piece(artifact_id, b))),
            ({"GET"}, ["artifacts", "{artifact_id}", "provenance"], c(
                lambda b, artifact_id: svc.provenance(artifact_id))),
            # 修订
            ({"POST"}, ["revisions", "split"], c(lambda b: svc.split_artifact(b))),
            ({"POST"}, ["revisions", "merge"], c(lambda b: svc.merge_artifacts(b))),
            ({"POST"}, ["revisions", "correct"], c(lambda b: svc.correct_identity(b))),
            ({"GET"}, ["revisions"], c(lambda b: {"revisions": svc.list_revisions()})),
            ({"GET"}, ["revisions", "{revision_id}"], c(
                lambda b, revision_id: {"revision": svc.revision_view(revision_id)})),
            # 文件（哈希与版本）
            ({"POST"}, ["files"], c(lambda b: svc.register_file(b))),
            ({"GET"}, ["files"], q(
                lambda b, query: {"files": svc.list_files(
                    query.get("ref_type"), query.get("ref_id"))})),
            ({"GET"}, ["files", "{file_id}"], c(
                lambda b, file_id: {"file": svc.file_view(file_id)})),
            ({"POST"}, ["files", "{file_id}", "versions"], c(
                lambda b, file_id: svc.add_file_version(file_id, b))),
            # 交接
            ({"POST"}, ["handovers"], c(lambda b: svc.draft_handover(b))),
            ({"GET"}, ["handovers"], q(
                lambda b, query: {"handovers": svc.list_handovers(query.get("stage"))})),
            ({"GET"}, ["handovers", "{handover_id}"], c(
                lambda b, handover_id: {"handover": svc.handover_view(handover_id)})),
            ({"POST"}, ["handovers", "{handover_id}", "submit"], c(
                lambda b, handover_id: svc.submit_handover(handover_id, b))),
            ({"POST"}, ["handovers", "{handover_id}", "sign"], c(
                lambda b, handover_id: svc.sign_handover(handover_id, b))),
            ({"POST"}, ["handovers", "{handover_id}", "receive"], c(
                lambda b, handover_id: svc.receive_handover(handover_id, b))),
            ({"POST"}, ["handovers", "{handover_id}", "withdraw"], c(
                lambda b, handover_id: svc.withdraw_handover(handover_id, b))),
            # 待处置
            ({"GET"}, ["issues"], q(
                lambda b, query: {"issues": svc.list_issues(query.get("status"))})),
            ({"POST"}, ["issues", "scan"], c(lambda b: svc.scan_issues())),
            ({"POST"}, ["issues", "{issue_id}", "acknowledge"], c(
                lambda b, issue_id: svc.acknowledge_issue(issue_id, b))),
            ({"POST"}, ["issues", "{issue_id}", "resolve"], c(
                lambda b, issue_id: svc.resolve_issue(issue_id, b))),
            # 审计
            ({"GET"}, ["audit", "events"], q(
                lambda b, query: {"events": svc.audit_events(query.get("stream_id")),
                                  "head": svc.store.head})),
            ({"POST"}, ["audit", "verify"], c(lambda b: svc.audit_verify())),
        ]


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        store = EventStore(args.db)
        result = store.verify()
        store.close()
        print(f"基础检查通过；审计链校验 {result['events']} 个事件，链头 {result['head'][:12]}…")
        return
    store = EventStore(args.db)
    handler = make_handler(store)
    ThreadingHTTPServer(("0.0.0.0", args.port), handler).serve_forever()


if __name__ == "__main__":
    main()
