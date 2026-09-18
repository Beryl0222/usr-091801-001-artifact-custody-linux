"""两批跨境返还样例数据（可重复执行，落到独立数据库）。

案件：中美合作追索返还案
- 第一日 · 华盛顿批次：美国移民与海关执法局（ICE）在华盛顿移交
  · 石造像 1 套（按套计数）
  · 彩绘陶俑 2 套（按套计数）
  · 恐龙骨架 1 件（单件追踪）
- 第二日 · 纽约批次：纽约县地区检察官办公室（DANY）在纽约移交
  · 恐龙蛋化石 6 枚（单件追踪，逐枚编号）
  · 陶俑 1 套（按套计数）

环节链：查获 → 交接 → 入境（海关接收/放行两段）→ 入藏，
境外签署保留美东时间，国内签署保留北京时间，并同时留存 UTC。

运行：python3 seed_sample.py --db data/sample.db
"""

from __future__ import annotations

import argparse
import os

from app import Service
from domain import STAGE_ACCESSION, STAGE_ENTRY, STAGE_HANDOVER, STAGE_SEIZURE, content_hash
from store import EventStore

ICE = {"key": "US-ICE", "name": "美国移民与海关执法局", "tz": "America/New_York"}
DANY = {"key": "US-DANY", "name": "纽约县地区检察官办公室", "tz": "America/New_York"}
NCHA = {"key": "CN-NCHA", "name": "国家文物局", "tz": "Asia/Shanghai"}
CUSTOMS = {"key": "CN-CUSTOMS", "name": "北京海关", "tz": "Asia/Shanghai"}
MUSEUM = {"key": "CN-MUSEUM", "name": "国家博物馆库房", "tz": "Asia/Shanghai"}


def seed(db_path: str) -> dict:
    if os.path.exists(db_path):
        os.remove(db_path)
    audit_path = db_path[:-3] + ".audit.jsonl" if db_path.endswith(".db") else db_path + ".audit.jsonl"
    if os.path.exists(audit_path):
        os.remove(audit_path)
    store = EventStore(db_path, audit_path)
    svc = Service(store)

    case = svc.open_case({
        "case_no": "NCHA-RET-2024-007",
        "name": "中美合作追索古生物化石与文物返还案",
        "source_country": "美国",
        "note": "两执法机构分两日移交",
        "at": "2024-02-20T09:00:00+08:00",
    })["case"]

    loc_dc = svc.register_location({
        "code": "US-DC-VAULT", "name": "华盛顿 ICE 证物库", "kind": "查验场地",
        "address": "Washington, D.C.",
    })["location"]
    loc_jfk = svc.register_location({
        "code": "US-JFK-CBP", "name": "肯尼迪机场海关监管区", "kind": "查验场地",
        "address": "New York, NY",
    })["location"]
    loc_port = svc.register_location({
        "code": "CN-PEK-CARGO", "name": "北京首都机场口岸查验区", "kind": "口岸",
    })["location"]
    loc_vault = svc.register_location({
        "code": "CN-NM-VAULT-3", "name": "国家博物馆三号库房", "kind": "库房",
    })["location"]

    # ---------------- 第一日：华盛顿批次（2024-02-28 / 北京 02-29）-----------
    batch_dc = svc.register_batch({
        "case_id": case["id"],
        "batch_no": "RET-2024-DC-01",
        "foreign_agency": ICE,
        "handover_city": "华盛顿",
        "handover_date": "2024-02-28",
    })["batch"]

    statues = svc.register_artifact({
        "batch_id": batch_dc["id"],
        "catalog_no": "DC-STATUE-SET-01",
        "name": "石造像（一组）", "category": "造像",
        "kind": "按套", "qty": 1,
        "description": "含佛立像、菩萨像等，按整套接收",
    })["artifact"]
    figurines_dc = svc.register_artifact({
        "batch_id": batch_dc["id"],
        "catalog_no": "DC-FIGURINE-SET-01",
        "name": "彩绘陶俑（一组）", "category": "陶俑",
        "kind": "按套", "qty": 2,
        "description": "两组陶俑套盒",
    })["artifact"]
    dinosaur = svc.register_artifact({
        "batch_id": batch_dc["id"],
        "catalog_no": "DC-DINO-001",
        "name": "恐龙骨架化石", "category": "恐龙骨架",
        "kind": "单件", "qty": 1,
        "pieces": [{"label": "DC-DINO-001-A", "description": "装架完整骨架"}],
    })["artifact"]

    # ---------------- 第二日：纽约批次（2024-03-01 / 北京 03-02）-------------
    batch_ny = svc.register_batch({
        "case_id": case["id"],
        "batch_no": "RET-2024-NY-02",
        "foreign_agency": DANY,
        "handover_city": "纽约",
        "handover_date": "2024-03-01",
    })["batch"]

    eggs = svc.register_artifact({
        "batch_id": batch_ny["id"],
        "catalog_no": "NY-EGG-001",
        "name": "恐龙蛋化石", "category": "蛋化石",
        "kind": "单件", "qty": 6,
        "pieces": [
            {"label": f"NY-EGG-001-{i:02d}", "description": f"第 {i} 枚蛋化石"}
            for i in range(1, 7)
        ],
    })["artifact"]
    figurines_ny = svc.register_artifact({
        "batch_id": batch_ny["id"],
        "catalog_no": "NY-FIGURINE-SET-01",
        "name": "陶俑（一组）", "category": "陶俑",
        "kind": "按套", "qty": 1,
    })["artifact"]

    def full_chain(batch_id, artifact_ids, seizure_loc, foreign, signer,
                   day_us, day_cn_sign, day_cn_chain):
        """查获→交接→入境（海关接收/放行）→入藏，全部双方签署后生效。

        day_us：境外环节美东日期；day_cn_sign：交接仪式对应的北京日期
        （与美东同一时刻）；day_cn_chain：次日入境/入藏的北京日期。
        """
        def draft(stage, frm, to, loc, note, at, deadline=None):
            payload = {
                "batch_id": batch_id, "stage": stage,
                "to_party": to, "artifact_ids": artifact_ids,
                "location_id": loc["id"], "note": note, "at": at,
            }
            if frm:
                payload["from_party"] = frm
            if deadline:
                payload["sign_deadline"] = deadline
            return svc.draft_handover(payload)["handover"]["id"]

        def sign_both(hid, a_party, a_signer, a_at, b_party, b_signer, b_at):
            svc.submit_handover(hid, {})
            svc.sign_handover(hid, {
                "party_key": a_party["key"], "signer": a_signer, "signed_at": a_at,
            })
            svc.sign_handover(hid, {
                "party_key": b_party["key"], "signer": b_signer, "signed_at": b_at,
            })

        h_seizure = draft(
            STAGE_SEIZURE, None, foreign, seizure_loc, "执法查获暂扣",
            f"{day_us}T09:00:00-05:00",
        )
        # 查获为境外执法方单方动作：提交后仅该方签署即生效
        svc.submit_handover(h_seizure, {})
        svc.sign_handover(h_seizure, {
            "party_key": foreign["key"], "signer": signer,
            "signed_at": f"{day_us}T09:15:00-05:00",
        })

        h_handover = draft(
            STAGE_HANDOVER, foreign, NCHA, seizure_loc, "跨境返还交接",
            f"{day_us}T10:30:00-05:00",
        )
        sign_both(
            h_handover,
            foreign, signer, f"{day_us}T10:52:00-05:00",
            NCHA, "国家文物局接收专员 李某", f"{day_cn_sign}T23:52:00+08:00",
        )
        # 实物清点：数量全部相符
        svc.receive_handover(h_handover, {
            "actual_qty": {
                aid: (
                    svc.artifact_view(aid)["current_qty"]
                    if svc.artifact_view(aid)["kind"] == "按套"
                    else len(svc.artifact_view(aid)["pieces"])
                )
                for aid in artifact_ids
            },
            "by": "李某", "at": f"{day_cn_chain}T08:00:00+08:00",
        })

        h_entry = draft(
            STAGE_ENTRY, NCHA, CUSTOMS, loc_port, "入境申报、海关接收监管",
            f"{day_cn_chain}T08:30:00+08:00",
        )
        sign_both(
            h_entry,
            NCHA, "李某", f"{day_cn_chain}T08:30:00+08:00",
            CUSTOMS, "北京海关 王某", f"{day_cn_chain}T09:10:00+08:00",
        )
        h_release = draft(
            STAGE_ENTRY, CUSTOMS, NCHA, loc_port, "查验放行",
            f"{day_cn_chain}T11:00:00+08:00",
        )
        sign_both(
            h_release,
            CUSTOMS, "王某", f"{day_cn_chain}T11:00:00+08:00",
            NCHA, "李某", f"{day_cn_chain}T11:20:00+08:00",
        )
        h_accession = draft(
            STAGE_ACCESSION, NCHA, MUSEUM, loc_vault, "入藏国家博物馆",
            f"{day_cn_chain}T15:00:00+08:00",
        )
        sign_both(
            h_accession,
            NCHA, "李某", f"{day_cn_chain}T15:00:00+08:00",
            MUSEUM, "国博保管部 赵某", f"{day_cn_chain}T15:30:00+08:00",
        )
        return {
            "seizure": h_seizure, "handover": h_handover,
            "entry": h_entry, "release": h_release, "accession": h_accession,
        }

    dc_artifacts = [statues["id"], figurines_dc["id"], dinosaur["id"]]
    ny_artifacts = [eggs["id"], figurines_ny["id"]]
    ids_dc = full_chain(
        batch_dc["id"], dc_artifacts, loc_dc, ICE,
        "ICE 探员 R. Garcia",
        day_us="2024-02-28", day_cn_sign="2024-02-28", day_cn_chain="2024-02-29",
    )
    ids_ny = full_chain(
        batch_ny["id"], ny_artifacts, loc_jfk, DANY,
        "助理检察官 S. Chen",
        day_us="2024-03-01", day_cn_sign="2024-03-01", day_cn_chain="2024-03-02",
    )

    # 资料文件：只登记内容哈希与版本关系，不保存原文件
    f_manifest_dc = svc.register_file({
        "ref_type": "batch", "ref_id": batch_dc["id"],
        "kind": "清单", "filename": "washington-manifest-v1.pdf",
        "sha256": content_hash("WASHINGTON MANIFEST v1"), "size": 2048,
    })["file"]["id"]
    svc.add_file_version(f_manifest_dc, {
        "sha256": content_hash("WASHINGTON MANIFEST v2 corrected"),
        "filename": "washington-manifest-v2.pdf",
        "note": "修正一件品名拼写；v1 保留可查",
    })
    svc.register_file({
        "ref_type": "artifact", "ref_id": dinosaur["id"],
        "kind": "照片", "filename": "dino-skeleton-handed-over.jpg",
        "sha256": content_hash(b"dino photo bytes"),
    })
    svc.register_file({
        "ref_type": "artifact", "ref_id": dinosaur["id"],
        "kind": "鉴定文件", "filename": "paleo-appraisal-dino.pdf",
        "sha256": content_hash("appraisal report dino"),
    })
    svc.register_file({
        "ref_type": "artifact", "ref_id": eggs["id"],
        "kind": "鉴定文件", "filename": "egg-appraisal.pdf",
        "sha256": content_hash("appraisal report eggs"),
    })

    verify = svc.audit_verify()
    store.close()
    return {
        "db": db_path, "audit": audit_path, "case_id": case["id"],
        "batch_dc": batch_dc["id"], "batch_ny": batch_ny["id"],
        "statues": statues["id"], "figurines_dc": figurines_dc["id"],
        "dinosaur": dinosaur["id"], "eggs": eggs["id"],
        "figurines_ny": figurines_ny["id"],
        "handovers_dc": ids_dc, "handovers_ny": ids_ny,
        "verify": verify,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.path.join("data", "sample.db"))
    args = parser.parse_args()
    os.makedirs(os.path.dirname(args.db) or ".", exist_ok=True)
    summary = seed(args.db)
    print("样例数据已建立：")
    for key, value in summary.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
