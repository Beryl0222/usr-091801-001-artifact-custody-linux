"""返还文物交接账的运行入口。

默认在 ``./data`` 下维护仅追加事件账本，启动时重放恢复全部状态。
``--seed`` 写入两批返还样例，``--sweep`` 扫描超期未签交接，
``--check`` 执行基础配置与账本完整性检查。

未挂载领域服务时（如契约测试直接实例化 :class:`Handler`）仅 /health 可用。
"""

import argparse
import json
import os
from http.server import ThreadingHTTPServer

from app import CustodyService
from events import DomainError
from store import EventStore, JournalIntegrityError
from web import build_handler

SERVICE_ID = "artifact-custody"
SERVICE_NAME = "返还文物交接账"


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


Handler = build_handler()


def build_server(port: int, data_dir: str) -> tuple[ThreadingHTTPServer, CustodyService]:
    store = EventStore(data_dir)
    service = CustodyService(store)
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.service = service
    return server, service


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--data-dir", default=os.environ.get("CUSTODY_DATA_DIR", "data"))
    parser.add_argument("--check", action="store_true", help="基础配置与账本完整性检查")
    parser.add_argument("--seed", action="store_true", help="写入两批返还样例")
    parser.add_argument("--sweep", action="store_true", help="扫描超期未签交接并入待处置队列")
    args = parser.parse_args()

    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        store = EventStore(args.data_dir)
        verification = store.verify()
        print("基础检查通过")
        print(json.dumps(verification, ensure_ascii=False, indent=2))
        return

    store = EventStore(args.data_dir)
    service = CustodyService(store)

    if args.seed:
        from seed import seed_demo

        print(json.dumps(seed_demo(service), ensure_ascii=False, indent=2))

    if args.sweep:
        opened = service.sweep_overdue()
        print(
            json.dumps(
                {"swept": len(opened), "items": opened}, ensure_ascii=False, indent=2
            )
        )

    if args.seed or args.sweep:
        return

    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    server.service = service
    print(f"{SERVICE_NAME} 监听 0.0.0.0:{args.port}，数据目录 {args.data_dir}")
    server.serve_forever()


if __name__ == "__main__":
    main()
