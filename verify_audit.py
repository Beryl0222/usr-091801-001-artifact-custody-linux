"""离线审计档案校验器。

只凭 JSONL 审计档案即可：
1. 逐行重算信封哈希并核对 prev_hash 链接（发现任何篡改/断链即失败）；
2. 用与线上相同的投影规则独立重放，得出现行状态；
3. 与 SQLite 数据库（若提供）交叉比对事件数与链头。

用法：
  python3 verify_audit.py data/sample.audit.jsonl
  python3 verify_audit.py data/sample.audit.jsonl --db data/sample.db
"""

from __future__ import annotations

import argparse
import json
import sys

from domain import GENESIS_HASH
from projection import Projection
from store import envelope_hash


def verify_audit_file(path: str) -> dict:
    projection = Projection()
    prev = GENESIS_HASH
    stream_seqs: dict[str, int] = {}
    count = 0
    with open(path, encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            envelope = json.loads(line)
            if envelope["prev_hash"] != prev:
                raise SystemExit(
                    f"第 {line_no} 行断链：prev_hash 与上一事件哈希不一致"
                )
            digest = envelope_hash(
                event_id=envelope["event_id"],
                stream_id=envelope["stream_id"],
                seq=envelope["seq"],
                stream_seq=envelope["stream_seq"],
                event_type=envelope["event_type"],
                payload=envelope["payload"],
                prev_hash=envelope["prev_hash"],
                recorded_at=envelope["recorded_at"],
            )
            if digest != envelope["hash"]:
                raise SystemExit(f"第 {line_no} 行内容哈希不一致，档案已被改动")
            stream_seqs[envelope["stream_id"]] = (
                stream_seqs.get(envelope["stream_id"], 0) + 1
            )
            if envelope["stream_seq"] != stream_seqs[envelope["stream_id"]]:
                raise SystemExit(
                    f"第 {line_no} 行流内序号不连续（{envelope['stream_id']}）"
                )
            projection.apply(envelope)
            prev = envelope["hash"]
            count += 1
    return {
        "ok": True,
        "events": count,
        "streams": len(stream_seqs),
        "head": prev,
        "cases": len(projection.cases),
        "batches": len(projection.batches),
        "artifacts": len(projection.artifacts),
        "pieces": len(projection.pieces),
        "handovers": len(projection.handovers),
        "revisions": len(projection.revisions),
        "issues": len(projection.issues),
        "files": len(projection.files),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="审计档案离线校验")
    parser.add_argument("audit_path")
    parser.add_argument("--db", help="可选：同时与 SQLite 库交叉比对链头")
    args = parser.parse_args()

    result = verify_audit_file(args.audit_path)
    print("审计档案校验通过：")
    for key, value in result.items():
        print(f"  {key}: {value}")

    if args.db:
        import sqlite3

        conn = sqlite3.connect(args.db)
        row = conn.execute(
            "SELECT hash FROM events ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        db_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        conn.close()
        if row is None:
            print("警告：数据库为空，无法比对", file=sys.stderr)
        elif row[0] != result["head"]:
            raise SystemExit("数据库链头与审计档案链头不一致")
        elif db_count != result["events"]:
            raise SystemExit(
                f"事件数不一致：数据库 {db_count}，档案 {result['events']}"
            )
        else:
            print(f"交叉比对通过：数据库与档案链头一致，共 {db_count} 个事件")


if __name__ == "__main__":
    main()
