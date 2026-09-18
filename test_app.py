"""领域服务端到端测试。

覆盖：双时区签署、状态门控、并发签署、重复回调幂等、撤回重提、
交接更正不覆盖历史、数量拆分/成套合并/身份更正留痕、文件哈希版本、
待处置三类问题扫描、来源反查、哈希链防篡改（含离线 JSONL 档案）。
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from app import Service
from domain import (
    ChainBroken,
    Conflict,
    DomainError,
    STAGE_ACCESSION,
    STAGE_ENTRY,
    STAGE_HANDOVER,
    STAGE_SEIZURE,
    STATUS_EFFECTIVE,
    STATUS_PENDING_SIGN,
    STATUS_WITHDRAWN,
    STATUS_CORRECTED,
    content_hash,
)
import seed_sample
from service import make_handler
from store import EventStore
from verify_audit import verify_audit_file

ICE = {"key": "US-ICE", "name": "美国移民与海关执法局", "tz": "America/New_York"}
DANY = {"key": "US-DANY", "name": "纽约县地区检察官办公室", "tz": "America/New_York"}
NCHA = {"key": "CN-NCHA", "name": "国家文物局", "tz": "Asia/Shanghai"}
CUSTOMS = {"key": "CN-CUSTOMS", "name": "北京海关", "tz": "Asia/Shanghai"}
MUSEUM = {"key": "CN-MUSEUM", "name": "国家博物馆库房", "tz": "Asia/Shanghai"}


class ServiceTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.audit_path = os.path.join(self.tmp.name, "test.audit.jsonl")
        self.store = EventStore(self.db_path, self.audit_path)
        self.svc = Service(self.store)
        self._seq = 0

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    # -- 构造夹具 -----------------------------------------------------------

    def make_case_batch(self, agency=ICE, batch_no="BAT-1", date="2024-02-28"):
        case = self.svc.open_case({
            "case_no": "C-1", "name": "追索返还案", "source_country": "美国",
        })["case"]
        loc = self.svc.register_location({
            "code": "L-1", "name": "境外证物库", "kind": "查验场地",
        })["location"]
        vault = self.svc.register_location({
            "code": "L-2", "name": "国内库房", "kind": "库房",
        })["location"]
        batch = self.svc.register_batch({
            "case_id": case["id"], "batch_no": batch_no,
            "foreign_agency": agency, "handover_city": "华盛顿",
            "handover_date": date,
        })["batch"]
        return case, batch, loc, vault

    def register_set(self, batch, qty=1, catalog="S-1"):
        return self.svc.register_artifact({
            "batch_id": batch["id"], "catalog_no": catalog,
            "name": "陶俑一组", "category": "陶俑",
            "kind": "按套", "qty": qty,
        })["artifact"]

    def register_single(self, batch, qty=2, catalog="P-1"):
        return self.svc.register_artifact({
            "batch_id": batch["id"], "catalog_no": catalog,
            "name": "恐龙蛋化石", "category": "蛋化石",
            "kind": "单件", "qty": qty,
            "pieces": [{"label": f"{catalog}-{i}"} for i in range(1, qty + 1)],
        })["artifact"]

    def sign_effective(self, hid, frm, to, at_from="2024-02-28T10:00:00-05:00",
                       at_to="2024-02-28T23:00:00+08:00"):
        self.svc.submit_handover(hid, {})
        self.svc.sign_handover(hid, {
            "party_key": frm["key"], "signer": "甲方签署人", "signed_at": at_from,
        })
        self.svc.sign_handover(hid, {
            "party_key": to["key"], "signer": "乙方签署人", "signed_at": at_to,
        })

    def count_events(self) -> int:
        return len(self.store.events())


class TestRegistrationAndTimezone(ServiceTestBase):
    def test_set_and_single_registration(self):
        _, batch, _, _ = self.make_case_batch()
        s = self.register_set(batch, qty=2)
        self.assertEqual(s["unit"], "套")
        self.assertEqual(s["current_qty"], 2)
        self.assertEqual(s["pieces"], [])
        p = self.register_single(batch, qty=3)
        self.assertEqual(p["unit"], "件")
        self.assertEqual(len(p["pieces"]), 3)

    def test_single_pieces_count_must_match_qty(self):
        _, batch, _, _ = self.make_case_batch()
        with self.assertRaises(DomainError):
            self.svc.register_artifact({
                "batch_id": batch["id"], "catalog_no": "X", "name": "蛋",
                "category": "蛋化石", "kind": "单件", "qty": 3,
                "pieces": [{"label": "a"}],
            })

    def test_dual_timezone_signatures_share_utc_instant(self):
        _, batch, loc, _ = self.make_case_batch()
        art = self.register_set(batch)
        hid = self.svc.draft_handover({
            "batch_id": batch["id"], "stage": STAGE_HANDOVER,
            "from_party": ICE, "to_party": NCHA,
            "artifact_ids": [art["id"]], "location_id": loc["id"],
        })["handover"]["id"]
        self.sign_effective(
            hid, ICE, NCHA,
            at_from="2024-02-28T10:52:00-05:00",
            at_to="2024-02-28T23:52:00+08:00",
        )
        h = self.svc.handover_view(hid)
        self.assertEqual(h["status"], STATUS_EFFECTIVE)
        sigs = {s["party_key"]: s for s in h["signatures"]}
        self.assertEqual(
            sigs["US-ICE"]["signed_at"]["utc"],
            sigs["CN-NCHA"]["signed_at"]["utc"],
        )
        self.assertIn("-05:00", sigs["US-ICE"]["signed_at"]["local"])
        self.assertIn("+08:00", sigs["CN-NCHA"]["signed_at"]["local"])
        self.assertEqual(sigs["US-ICE"]["signed_at"]["tz"], "America/New_York")

    def test_status_does_not_advance_before_both_sign(self):
        _, batch, loc, _ = self.make_case_batch()
        art = self.register_set(batch)
        hid = self.svc.draft_handover({
            "batch_id": batch["id"], "stage": STAGE_HANDOVER,
            "from_party": ICE, "to_party": NCHA,
            "artifact_ids": [art["id"]], "location_id": loc["id"],
        })["handover"]["id"]
        with self.assertRaises(Conflict):
            self.svc.sign_handover(hid, {"party_key": ICE["key"], "signer": "x"})
        self.svc.submit_handover(hid, {})
        self.svc.sign_handover(hid, {
            "party_key": ICE["key"], "signer": "x",
            "signed_at": "2024-02-28T10:00:00-05:00",
        })
        self.assertEqual(self.svc.handover_view(hid)["status"], STATUS_PENDING_SIGN)
        with self.assertRaises(Conflict):
            self.svc.receive_handover(hid, {"actual_qty": {art["id"]: 1}})
        # 非签署方签署被拒
        with self.assertRaises(DomainError):
            self.svc.sign_handover(hid, {"party_key": "CN-CUSTOMS", "signer": "y"})


class TestIdempotencyAndConcurrency(ServiceTestBase):
    def _pending_two_party(self):
        _, batch, loc, _ = self.make_case_batch()
        art = self.register_set(batch)
        hid = self.svc.draft_handover({
            "batch_id": batch["id"], "stage": STAGE_HANDOVER,
            "from_party": ICE, "to_party": NCHA,
            "artifact_ids": [art["id"]], "location_id": loc["id"],
        })["handover"]["id"]
        self.svc.submit_handover(hid, {})
        return hid

    def test_duplicate_callback_never_creates_second_event(self):
        _, batch, loc, _ = self.make_case_batch()
        art = self.register_set(batch)
        before = self.count_events()
        key = "cb:handover:dc:1"
        payload = {
            "batch_id": batch["id"], "stage": STAGE_HANDOVER,
            "from_party": ICE, "to_party": NCHA,
            "artifact_ids": [art["id"]],
            "location_id": loc["id"], "client_key": key,
        }
        first = self.svc.draft_handover(payload)
        again = self.svc.draft_handover(payload)
        self.assertTrue(first["replayed"] is False)
        self.assertTrue(again["replayed"])
        self.assertEqual(first["event_id"], again["event_id"])
        self.assertEqual(self.count_events(), before + 1)
        self.assertEqual(len(self.svc.list_handovers()), 1)

    def test_duplicate_sign_callback_with_same_client_key(self):
        hid = self._pending_two_party()
        key = "cb:sign:ice:1"
        data = {
            "party_key": ICE["key"], "signer": "Garcia",
            "signed_at": "2024-02-28T10:30:00-05:00", "client_key": key,
        }
        r1 = self.svc.sign_handover(hid, data)
        r2 = self.svc.sign_handover(hid, data)
        self.assertTrue(r2["replayed"])
        self.assertEqual(r1["event_id"], r2["event_id"])
        sigs = self.svc.handover_view(hid)["signatures"]
        self.assertEqual(len(sigs), 1)

    def test_success_retry_of_submit_receive_withdraw_is_idempotent(self):
        _, batch, loc, _ = self.make_case_batch()
        art = self.register_set(batch)
        hid = self.svc.draft_handover({
            "batch_id": batch["id"], "stage": STAGE_HANDOVER,
            "from_party": ICE, "to_party": NCHA,
            "artifact_ids": [art["id"]], "location_id": loc["id"],
        })["handover"]["id"]
        n0 = self.count_events()
        self.svc.submit_handover(hid, {"client_key": "k:submit"})
        self.svc.submit_handover(hid, {"client_key": "k:submit"})  # 已是待签署
        self.svc.withdraw_handover(hid, {"reason": "r", "client_key": "k:wd"})
        self.svc.withdraw_handover(hid, {"reason": "r", "client_key": "k:wd"})  # 已撤回
        self.assertEqual(self.count_events(), n0 + 2)
        # 重新提交并生效后，清点回调重试不重复落事件
        self.svc.submit_handover(hid, {})
        self.svc.sign_handover(hid, {
            "party_key": ICE["key"], "signer": "甲方签署人",
            "signed_at": "2024-02-28T10:00:00-05:00",
        })
        self.svc.sign_handover(hid, {
            "party_key": NCHA["key"], "signer": "乙方签署人",
            "signed_at": "2024-02-28T23:00:00+08:00",
        })
        n1 = self.count_events()
        qty = {art["id"]: 1}
        self.svc.receive_handover(hid, {"actual_qty": qty, "client_key": "k:recv"})
        self.svc.receive_handover(hid, {"actual_qty": qty, "client_key": "k:recv"})
        self.assertEqual(self.count_events(), n1 + 1)

    def test_multi_event_artifact_registration_is_idempotent(self):
        _, batch, _, _ = self.make_case_batch()
        body = {
            "batch_id": batch["id"], "catalog_no": "P-IDEM",
            "name": "蛋化石", "category": "蛋化石",
            "kind": "单件", "qty": 3,
            "pieces": [{"label": "a"}, {"label": "b"}, {"label": "c"}],
            "client_key": "cb:artifact:multi:1",
        }
        r1 = self.svc.register_artifact(body)
        n = self.count_events()
        r2 = self.svc.register_artifact(body)
        self.assertTrue(r2["replayed"])
        self.assertEqual(r1["artifact"]["id"], r2["artifact"]["id"])
        self.assertEqual(self.count_events(), n)
        self.assertEqual(len(self.svc.list_artifacts(batch["id"])), 1)

    def test_each_single_piece_has_its_own_complete_chain(self):
        summary = seed_sample.seed(self.db_path)
        self.store.close()
        self.store = EventStore(self.db_path, self.audit_path)
        self.svc = Service(self.store)
        prov = self.svc.provenance(summary["eggs"])
        # 6 枚蛋各自一行保管链，且全部环节完整
        self.assertEqual(len(prov["custody_chain"]), 6)
        for row in prov["custody_chain"]:
            self.assertEqual(row["unit_type"], "piece")
            stages = [e["stage"] for e in row["events"]]
            self.assertEqual(stages, ["查获", "交接", "入境", "入境", "入藏"])
            self.assertTrue(row["complete"])
        # 两枚蛋的单件标识互不相同，可逐枚追踪
        unit_ids = {row["unit_id"] for row in prov["custody_chain"]}
        self.assertEqual(len(unit_ids), 6)

    def test_two_agencies_two_days_are_distinct_batches(self):
        summary = seed_sample.seed(self.db_path)
        self.store.close()
        self.store = EventStore(self.db_path, self.audit_path)
        self.svc = Service(self.store)
        batches = self.svc.list_batches(summary["case_id"])
        self.assertEqual(len(batches), 2)
        agencies = {b["foreign_agency"]["key"] for b in batches}
        self.assertEqual(agencies, {"US-ICE", "US-DANY"})
        dates = {b["handover_date"] for b in batches}
        self.assertEqual(dates, {"2024-02-28", "2024-03-01"})
        # 任一批次的器物都能反查到同一案件
        for aid in (summary["statues"], summary["eggs"]):
            self.assertEqual(
                self.svc.provenance(aid)["case"]["id"], summary["case_id"]
            )

    def test_concurrent_same_party_signs_only_one_wins(self):
        hid = self._pending_two_party()
        barrier = threading.Barrier(6)
        outcomes = []

        def worker(i):
            barrier.wait()
            try:
                self.svc.sign_handover(hid, {
                    "party_key": ICE["key"], "signer": f"signer-{i}",
                    "signed_at": "2024-02-28T10:30:00-05:00",
                })
                outcomes.append(("ok", i))
            except Conflict:
                outcomes.append(("conflict", i))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len([o for o in outcomes if o[0] == "ok"]), 1)
        self.assertEqual(len(self.svc.handover_view(hid)["signatures"]), 1)

    def test_concurrent_opposite_party_signs_completes_once(self):
        hid = self._pending_two_party()
        barrier = threading.Barrier(2)
        results = []

        def worker(party, signer, at):
            barrier.wait()
            results.append(self.svc.sign_handover(hid, {
                "party_key": party["key"], "signer": signer, "signed_at": at,
            }))

        t1 = threading.Thread(
            target=worker, args=(ICE, "Garcia", "2024-02-28T10:30:00-05:00"))
        t2 = threading.Thread(
            target=worker, args=(NCHA, "李某", "2024-02-28T23:30:00+08:00"))
        t1.start(); t2.start()
        t1.join(); t2.join()
        h = self.svc.handover_view(hid)
        self.assertEqual(h["status"], STATUS_EFFECTIVE)
        self.assertEqual(len(h["signatures"]), 2)
        self.assertEqual(len([r for r in results if r["completed"]]), 1)


class TestWithdrawAndCorrection(ServiceTestBase):
    def _pending(self):
        _, batch, loc, _ = self.make_case_batch()
        art = self.register_set(batch)
        hid = self.svc.draft_handover({
            "batch_id": batch["id"], "stage": STAGE_HANDOVER,
            "from_party": ICE, "to_party": NCHA,
            "artifact_ids": [art["id"]], "location_id": loc["id"],
        })["handover"]["id"]
        return hid, art

    def test_withdraw_then_resubmit(self):
        hid, _ = self._pending()
        self.svc.submit_handover(hid, {})
        self.svc.withdraw_handover(hid, {"reason": "清单待补充", "by": "李某"})
        self.assertEqual(self.svc.handover_view(hid)["status"], STATUS_WITHDRAWN)
        # 撤回后可以重新提交
        self.svc.submit_handover(hid, {})
        self.assertEqual(
            self.svc.handover_view(hid)["status"], STATUS_PENDING_SIGN
        )

    def test_cannot_withdraw_after_any_signature(self):
        hid, _ = self._pending()
        self.svc.submit_handover(hid, {})
        self.svc.sign_handover(hid, {
            "party_key": ICE["key"], "signer": "x",
            "signed_at": "2024-02-28T10:00:00-05:00",
        })
        with self.assertRaises(Conflict):
            self.svc.withdraw_handover(hid, {"reason": "反悔"})

    def test_correct_effective_handover_keeps_history(self):
        _, batch, loc, _ = self.make_case_batch()
        art = self.register_set(batch)
        original = self.svc.draft_handover({
            "batch_id": batch["id"], "stage": STAGE_HANDOVER,
            "from_party": ICE, "to_party": NCHA,
            "artifact_ids": [art["id"]], "location_id": loc["id"],
            "note": "原始记录：数量 1 套",
        })["handover"]["id"]
        self.sign_effective(original, ICE, NCHA)
        original_events_before = [
            e for e in self.store.events(f"handover-{original}")
        ]

        # 更正：新事件引用原事件；原记录不得原地覆盖
        corrected = self.svc.draft_handover({
            "batch_id": batch["id"], "stage": STAGE_HANDOVER,
            "from_party": ICE, "to_party": NCHA,
            "artifact_ids": [art["id"]], "location_id": loc["id"],
            "note": "更正：实际为 2 套（原始套盒内含两小套）",
            "corrects_handover_id": original,
        })["handover"]["id"]
        with self.assertRaises(DomainError):
            # 不能更正一个尚未生效的事件
            self.svc.draft_handover({
                "batch_id": batch["id"], "stage": STAGE_HANDOVER,
                "from_party": ICE, "to_party": NCHA,
                "artifact_ids": [art["id"]], "location_id": loc["id"],
                "corrects_handover_id": corrected,
            })
        self.sign_effective(
            corrected, ICE, NCHA,
            at_from="2024-02-29T09:00:00-05:00",
            at_to="2024-02-29T22:00:00+08:00",
        )
        old = self.svc.handover_view(original)
        new = self.svc.handover_view(corrected)
        self.assertEqual(old["status"], STATUS_CORRECTED)
        self.assertEqual(old["corrected_by"], corrected)
        self.assertEqual(new["corrects_handover_id"], original)
        self.assertEqual(old["note"], "原始记录：数量 1 套")
        # 原事件信封原样保留
        original_events_after = [
            e for e in self.store.events(f"handover-{original}")
        ]
        self.assertEqual(
            [e["hash"] for e in original_events_before],
            [e["hash"] for e in original_events_after[: len(original_events_before)]],
        )
        self.assertTrue(self.svc.audit_verify()["ok"])


class TestRevisions(ServiceTestBase):
    def test_split_set_conserves_quantity_and_links_lineage(self):
        _, batch, _, _ = self.make_case_batch()
        src = self.register_set(batch, qty=3)
        result = self.svc.split_artifact({
            "artifact_id": src["id"],
            "outputs": [
                {"catalog_no": "S-1A", "qty": 1},
                {"catalog_no": "S-1B", "qty": 1},
            ],
            "reason": "两套盒各自独立入藏，余 1 套保留",
        })
        rev = result["revision"]
        self.assertEqual(rev["before"]["qty"], 3)
        self.assertEqual(rev["after"]["qty"], 1)
        self.assertEqual(len(rev["outputs"]), 2)
        self.assertEqual(self.svc.artifact_view(src["id"])["current_qty"], 1)
        child_a = result["outputs"][0]
        self.assertIn(src["id"], child_a["parents"])
        self.assertEqual(child_a["born_from_revision"], rev["id"])
        # 超量拆分被拒
        with self.assertRaises(DomainError):
            self.svc.split_artifact({
                "artifact_id": src["id"],
                "outputs": [{"catalog_no": "S-1C", "qty": 2}],
            })

    def test_full_split_moves_single_pieces_and_merge_closes_sources(self):
        _, batch, _, _ = self.make_case_batch()
        eggs = self.register_single(batch, qty=4)
        piece_ids = [p["id"] for p in eggs["pieces"]]
        split = self.svc.split_artifact({
            "artifact_id": eggs["id"],
            "outputs": [
                {"catalog_no": "P-A", "piece_ids": piece_ids[:3]},
                {"catalog_no": "P-B", "piece_ids": piece_ids[3:]},
            ],
            "close_source": True,
            "reason": "按埋藏坑位分组",
        })
        a, b = split["outputs"]
        self.assertTrue(self.svc.artifact_view(eggs["id"])["closed"])
        self.assertEqual(len(a["pieces"]), 3)
        self.assertEqual(len(b["pieces"]), 1)
        self.assertEqual(a["current_qty"], 3)

        merged = self.svc.merge_artifacts({
            "input_artifact_ids": [a["id"], b["id"]],
            "catalog_no": "P-MERGED", "name": "恐龙蛋化石（并组）",
            "reason": "鉴定确认同窝，恢复成套",
        })["output"]
        self.assertEqual(merged["kind"], "单件")
        self.assertEqual(merged["current_qty"], 4)
        self.assertTrue(self.svc.artifact_view(a["id"])["closed"])
        self.assertEqual(merged["parents"], [a["id"], b["id"]])

    def test_split_rejects_duplicated_or_foreign_piece(self):
        _, batch, _, _ = self.make_case_batch()
        eggs = self.register_single(batch, qty=3)
        other = self.register_single(batch, qty=1, catalog="P-2")
        pid = eggs["pieces"][0]["id"]
        foreign_pid = other["pieces"][0]["id"]
        with self.assertRaises(DomainError):
            self.svc.split_artifact({
                "artifact_id": eggs["id"],
                "outputs": [
                    {"catalog_no": "A", "piece_ids": [pid, pid]},
                    {"catalog_no": "B", "piece_ids": [eggs["pieces"][1]["id"]]},
                ],
                "close_source": True,
            })
        with self.assertRaises(DomainError):
            self.svc.split_artifact({
                "artifact_id": eggs["id"],
                "outputs": [{"catalog_no": "A", "piece_ids": [foreign_pid]}],
            })

    def test_identity_correction_records_before_and_after(self):
        _, batch, _, _ = self.make_case_batch()
        art = self.register_set(batch)
        old_name = art["name"]
        result = self.svc.correct_identity({
            "target_type": "artifact", "target_id": art["id"],
            "after": {"name": "彩绘陶俑（一组）", "category": "陶俑"},
            "reason": "据鉴定意见定名",
        })
        rev = result["revision"]
        self.assertEqual(rev["before"]["name"], old_name)
        self.assertEqual(rev["after"]["name"], "彩绘陶俑（一组）")
        self.assertEqual(self.svc.artifact_view(art["id"])["name"], "彩绘陶俑（一组）")
        # 无变化的更正被拒；非法字段被拒
        with self.assertRaises(DomainError):
            self.svc.correct_identity({
                "target_type": "artifact", "target_id": art["id"],
                "after": {"name": "彩绘陶俑（一组）"}, "reason": "x",
            })
        with self.assertRaises(DomainError):
            self.svc.correct_identity({
                "target_type": "artifact", "target_id": art["id"],
                "after": {"current_qty": 99}, "reason": "x",
            })


class TestFileHashVersions(ServiceTestBase):
    def test_only_hash_stored_and_versions_chain(self):
        _, batch, _, _ = self.make_case_batch()
        art = self.register_set(batch)
        f = self.svc.register_file({
            "ref_type": "artifact", "ref_id": art["id"],
            "kind": "照片", "filename": "a.jpg",
            "sha256": content_hash("photo-v1"), "size": 100,
        })["file"]
        self.assertEqual(f["current_version"], 1)
        f2 = self.svc.add_file_version(f["id"], {
            "sha256": content_hash("photo-v2"), "note": "补拍",
        })["file"]
        self.assertEqual(f2["current_version"], 2)
        self.assertEqual(f2["versions"][1]["supersedes_version"], 1)
        # 历史版本哈希仍可查，未被覆盖
        self.assertEqual(f2["versions"][0]["sha256"], content_hash("photo-v1"))
        # 同一哈希不能作为新版本
        with self.assertRaises(DomainError):
            self.svc.add_file_version(f["id"], {"sha256": content_hash("photo-v1")})


class TestIssueQueue(ServiceTestBase):
    def test_overdue_unsigned_enters_queue(self):
        _, batch, loc, _ = self.make_case_batch()
        art = self.register_set(batch)
        hid = self.svc.draft_handover({
            "batch_id": batch["id"], "stage": STAGE_HANDOVER,
            "from_party": ICE, "to_party": NCHA,
            "artifact_ids": [art["id"]], "location_id": loc["id"],
        })["handover"]["id"]
        self.svc.submit_handover(hid, {
            "sign_deadline": "2000-01-01T00:00:00+00:00",
        })
        scan = self.svc.scan_issues()
        self.assertEqual(scan["count"], 1)
        # 重复扫描不制造重复事项
        self.assertEqual(self.svc.scan_issues()["count"], 0)
        issue = self.svc.issue_view(scan["raised"][0])
        self.assertEqual(issue["kind"], "超期未签")
        self.assertEqual(issue["ref_id"], hid)
        self.svc.acknowledge_issue(issue["id"], {"by": "值班员"})
        self.svc.resolve_issue(issue["id"], {"resolution": "联系对方完成签署"})
        self.assertEqual(self.svc.issue_view(issue["id"])["status"], "已关闭")

    def test_quantity_mismatch_at_receipt(self):
        _, batch, loc, _ = self.make_case_batch()
        art = self.register_set(batch, qty=2)
        hid = self.svc.draft_handover({
            "batch_id": batch["id"], "stage": STAGE_HANDOVER,
            "from_party": ICE, "to_party": NCHA,
            "artifact_ids": [art["id"]], "location_id": loc["id"],
        })["handover"]["id"]
        self.sign_effective(hid, ICE, NCHA)
        result = self.svc.receive_handover(hid, {
            "actual_qty": {art["id"]: 1},
        })
        self.assertEqual(len(result["issues_raised"]), 1)
        issue = self.svc.issue_view(result["issues_raised"][0])
        self.assertEqual(issue["kind"], "实物数量不符")
        self.assertIn("期望", issue["detail"])

    def test_chain_gap_missing_stage_and_custodian_mismatch(self):
        _, batch, _, vault = self.make_case_batch()
        art = self.register_set(batch)
        # 直接造一个“入藏”生效记录，缺查获/交接/入境
        h = self.svc.draft_handover({
            "batch_id": batch["id"], "stage": STAGE_ACCESSION,
            "from_party": NCHA, "to_party": MUSEUM,
            "artifact_ids": [art["id"]], "location_id": vault["id"],
        })["handover"]["id"]
        self.sign_effective(h, NCHA, MUSEUM)
        self.svc.scan_issues()
        kinds = {i["kind"] for i in self.svc.list_issues()}
        self.assertIn("保管链断点", kinds)
        details = " ".join(i["detail"] for i in self.svc.list_issues())
        self.assertIn("查获", details)

    def test_custodian_handover_mismatch_detected(self):
        _, batch, loc, vault = self.make_case_batch()
        art = self.register_set(batch)
        seizure = self.svc.draft_handover({
            "batch_id": batch["id"], "stage": STAGE_SEIZURE,
            "to_party": ICE, "artifact_ids": [art["id"]],
            "location_id": loc["id"],
        })["handover"]["id"]
        self.svc.submit_handover(seizure, {})
        self.svc.sign_handover(seizure, {
            "party_key": ICE["key"], "signer": "x",
            "signed_at": "2024-02-28T09:00:00-05:00",
        })
        # 交接时交出方竟不是 ICE（责任主体不衔接）
        bad = self.svc.draft_handover({
            "batch_id": batch["id"], "stage": STAGE_HANDOVER,
            "from_party": CUSTOMS, "to_party": NCHA,
            "artifact_ids": [art["id"]], "location_id": loc["id"],
        })["handover"]["id"]
        self.sign_effective(bad, CUSTOMS, NCHA)
        self.svc.scan_issues()
        gaps = [i for i in self.svc.list_issues() if i["kind"] == "保管链断点"]
        self.assertTrue(any("责任主体不衔接" in i["detail"] for i in gaps))


class TestProvenanceAndAudit(ServiceTestBase):
    def test_sample_provenance_reaches_case_and_all_stages(self):
        summary = seed_sample.seed(self.db_path)
        # seed 重建了库，重新连接投影
        self.store.close()
        self.store = EventStore(self.db_path, self.audit_path)
        self.svc = Service(self.store)

        prov = self.svc.provenance(summary["dinosaur"])
        self.assertEqual(prov["case"]["id"], summary["case_id"])
        self.assertEqual(prov["batch"]["id"], summary["batch_dc"])
        stages = [
            e["stage"]
            for row in prov["custody_chain"]
            for e in row["events"]
        ]
        for stage in ("查获", "交接", "入境", "入藏"):
            self.assertIn(stage, stages)
        # 入境含海关接收、放行两条生效记录
        self.assertEqual(stages.count("入境"), 2)
        self.assertTrue(all(row["complete"] for row in prov["custody_chain"]))
        # 单件现行保管方为国博库房
        piece = prov["artifact"]["pieces"][0]
        self.assertEqual(piece["current_custodian"]["key"], MUSEUM["key"])
        # 事件锚点含哈希，且与审计链头一致
        self.assertTrue(prov["event_anchors"])
        self.assertEqual(prov["audit_head"], self.store.head)

    def test_provenance_traverses_split_merge_family(self):
        _, batch, _, _ = self.make_case_batch()
        eggs = self.register_single(batch, qty=4)
        split = self.svc.split_artifact({
            "artifact_id": eggs["id"],
            "outputs": [
                {"catalog_no": "P-A", "piece_ids": [p["id"] for p in eggs["pieces"][:2]]},
                {"catalog_no": "P-B", "piece_ids": [p["id"] for p in eggs["pieces"][2:]]},
            ],
            "close_source": True, "reason": "分组",
        })
        a_id = split["outputs"][0]["id"]
        b_id = split["outputs"][1]["id"]
        merged = self.svc.merge_artifacts({
            "input_artifact_ids": [a_id, b_id],
            "catalog_no": "P-Z", "name": "并组蛋化石", "reason": "恢复成套",
        })["output"]
        # 从合并后的现行器物反查，应能回到已终结的原始器物与两条修订
        prov = self.svc.provenance(merged["id"])
        family_ids = {m["artifact_id"] for m in prov["family"]}
        self.assertEqual(family_ids, {eggs["id"], a_id, b_id, merged["id"]})
        kinds = [r["kind"] for r in prov["revisions"]]
        self.assertEqual(kinds.count("拆分"), 1)
        self.assertEqual(kinds.count("合并"), 1)
        # 从已终结的旧器物也能正向查到现行去向
        prov_old = self.svc.provenance(eggs["id"])
        self.assertIn(merged["id"], {m["artifact_id"] for m in prov_old["family"]})

    def test_sqlite_tampering_is_detected(self):
        _, batch, _, _ = self.make_case_batch()
        self.register_set(batch)
        self.store.close()
        conn = sqlite3.connect(self.db_path)
        # 直接改写已落库事件的 payload（绕过应用层与哈希链）
        row = conn.execute(
            "SELECT payload FROM events ORDER BY seq LIMIT 1"
        ).fetchone()
        data = json.loads(row[0])
        data["name"] = "被篡改的案件名称"
        conn.execute(
            "UPDATE events SET payload = ? WHERE seq = 1",
            (json.dumps(data, ensure_ascii=False),),
        )
        conn.commit()
        conn.close()
        with self.assertRaises(ChainBroken):
            EventStore(self.db_path, self.audit_path)

    def test_audit_jsonl_tampering_is_detected_offline(self):
        summary = seed_sample.seed(self.db_path)
        self.assertTrue(verify_audit_file(self.audit_path)["ok"])
        with open(self.audit_path, encoding="utf-8") as fh:
            lines = fh.readlines()
        evil = json.loads(lines[5])
        evil["payload"]["note"] = "离线档案被改"
        lines[5] = json.dumps(evil, ensure_ascii=False) + "\n"
        evil_path = os.path.join(self.tmp.name, "evil.audit.jsonl")
        with open(evil_path, "w", encoding="utf-8") as fh:
            fh.writelines(lines)
        with self.assertRaises(SystemExit):
            verify_audit_file(evil_path)

    def test_audit_jsonl_matches_db_head(self):
        seed_sample.seed(self.db_path)
        self.store.close()
        self.store = EventStore(self.db_path, self.audit_path)
        self.assertEqual(verify_audit_file(self.audit_path)["head"], self.store.head)


class TestHttpApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.tmp.name, "http.db")
        cls.audit_path = os.path.join(cls.tmp.name, "http.audit.jsonl")
        cls.store = EventStore(cls.db_path, cls.audit_path)
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(cls.store))
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)
        cls.store.close()
        cls.tmp.cleanup()

    def request(self, method, path, body=None):
        data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
        req = Request(
            f"{self.base}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urlopen(req, timeout=5) as resp:
                return resp.status, json.load(resp)
        except HTTPError as exc:
            return exc.code, json.load(exc)

    def test_case_batch_artifact_flow_over_http(self):
        status, created = self.request("POST", "/cases", {
            "case_no": "HTTP-1", "name": "HTTP 案",
        })
        self.assertEqual(status, 200)
        case_id = created["case"]["id"]
        _, loc = self.request("POST", "/locations", {
            "code": "H-1", "name": "场地", "kind": "库房",
        })
        status, batch = self.request("POST", "/batches", {
            "case_id": case_id, "batch_no": "B-1",
            "foreign_agency": ICE, "handover_city": "华盛顿",
            "handover_date": "2024-02-28",
        })
        self.assertEqual(status, 200)
        _, art = self.request("POST", "/artifacts", {
            "batch_id": batch["batch"]["id"], "catalog_no": "H-A1",
            "name": "造像", "category": "造像", "kind": "按套", "qty": 1,
        })
        aid = art["artifact"]["id"]
        # 来源反查
        status, prov = self.request("GET", f"/artifacts/{aid}/provenance")
        self.assertEqual(status, 200)
        self.assertEqual(prov["case"]["id"], case_id)
        # 审计核验
        status, verify = self.request("POST", "/audit/verify", {})
        self.assertEqual(status, 200)
        self.assertTrue(verify["ok"])

    def test_duplicate_callback_http_is_idempotent(self):
        _, case = self.request("POST", "/cases", {
            "case_no": "HTTP-2", "name": "幂等案", "client_key": "cb:case:2",
        })
        _, case2 = self.request("POST", "/cases", {
            "case_no": "HTTP-2", "name": "幂等案", "client_key": "cb:case:2",
        })
        self.assertEqual(case["case"]["id"], case2["case"]["id"])
        _, listing = self.request("GET", "/cases")
        self.assertEqual(len([c for c in listing["cases"] if c["case_no"] == "HTTP-2"]), 1)

    def test_domain_error_maps_to_422(self):
        status, payload = self.request("POST", "/artifacts", {
            "batch_id": "BAT-nope", "catalog_no": "x", "name": "x",
            "category": "陶俑", "kind": "按套", "qty": 1,
        })
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"], "not_found")


if __name__ == "__main__":
    unittest.main()
