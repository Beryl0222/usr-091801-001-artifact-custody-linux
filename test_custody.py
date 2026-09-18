"""领域服务、HTTP 接口、并发签署与审计档案的综合测试。"""

import json
import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from app import LIFE_ACTIVE, CustodyService
from events import (
    REASON_BROKEN_CHAIN,
    REASON_DISCREPANCY,
    REASON_OVERDUE,
    STATUS_CORRECTED,
    STATUS_EFFECTED,
    STATUS_PENDING,
    STATUS_WITHDRAWN,
    ConflictError,
    DomainError,
)
from seed import seed_demo
from service import Handler, health_payload
from store import EventStore, JournalIntegrityError


def make_service():
    data_dir = tempfile.mkdtemp(prefix="custody-test-")
    return CustodyService(EventStore(data_dir)), data_dir


def build_case(service, *, agency="测试执法机构"):
    case_id = service.open_case("测试返还案", source_country="美国")["event"]["payload"]["case_id"]
    loc = service.register_location("测试库房")["event"]["payload"]["location_id"]
    batch_id = service.create_batch(
        case_id, agency, agency_timezone="America/New_York", location_id=loc
    )["event"]["payload"]["batch_id"]
    return case_id, loc, batch_id


def event_id(result):
    return result["event"]["event_id"]


class RegistrationTest(unittest.TestCase):
    def setUp(self):
        self.service, _ = make_service()

    def test_set_counts_by_components_singleton_by_piece(self):
        _, _, batch_id = build_case(self.service)
        s = self.service.register_artifact(batch_id, "套", "造像", "石造像组", quantity=4)
        singleton = self.service.register_artifact(batch_id, "单件", "恐龙骨架", "骨架")
        self.assertEqual(s["event"]["payload"]["quantity"], 4)
        self.assertEqual(len(singleton["event"]["payload"]["components"]), 1)
        self.assertEqual(self.service._physical_count(batch_id), 5)

    def test_set_requires_positive_quantity(self):
        _, _, batch_id = build_case(self.service)
        with self.assertRaises(DomainError):
            self.service.register_artifact(batch_id, "套", "陶俑", "陶俑组")
        with self.assertRaises(DomainError):
            self.service.register_artifact(batch_id, "单件", "蛋", "蛋", quantity=2)

    def test_unknown_batch_rejected(self):
        with self.assertRaises(DomainError):
            self.service.register_artifact("batch_nope", "单件", "蛋", "蛋")


class HandoverLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.service, _ = make_service()
        _, self.loc, self.batch = build_case(self.service)
        self.set_a = self.service.register_artifact(self.batch, "套", "造像", "造像组", quantity=2)
        self.bone = self.service.register_artifact(self.batch, "单件", "恐龙骨架", "骨架")
        self.set_id = self.set_a["event"]["payload"]["artifact_id"]
        self.bone_id = self.bone["event"]["payload"]["artifact_id"]
        self.service.record_chain([self.set_id, self.bone_id], "查获")

    def _handover(self, deadline="2026-12-31T23:59:59Z"):
        return self.service.prepare_handover(
            self.batch, "交接", "境外机构", "文物局", deadline=deadline
        )["event"]["payload"]["handover_id"]

    def test_dual_signatures_in_own_timezones_then_effect(self):
        hid = self._handover()
        self.service.sign_handover(
            hid, "from", "Foreign Officer",
            at_local="2026-09-10T16:30", tz="America/New_York", observed_quantity=3,
            idem_key="from-1",
        )
        ho = self.service.get_handover(hid)
        self.assertEqual(ho["status"], STATUS_PENDING)
        self.service.sign_handover(
            hid, "to", "接收人 李",
            at_local="2026-09-11T10:15", tz="Asia/Shanghai", observed_quantity=3,
            idem_key="to-1",
        )
        ho = self.service.get_handover(hid)
        self.assertEqual(ho["status"], STATUS_EFFECTED)
        sig_from = ho["signatures"]["from"]
        sig_to = ho["signatures"]["to"]
        # 原始时区时间保留，同时存在 UTC
        self.assertTrue(sig_from["at_local"].endswith("-04:00"))
        self.assertEqual(sig_from["tz"], "America/New_York")
        self.assertTrue(sig_to["at_local"].endswith("+08:00"))
        self.assertIn("+00:00", sig_from["occurred_at"])
        self.assertIn("+00:00", sig_to["occurred_at"])
        # 跨两日：华盛顿 9/10 与北京 9/11 实际为同一 UTC 日的不同时刻
        self.assertLess(sig_from["occurred_at"], sig_to["occurred_at"])

    def test_duplicate_callback_returns_first_event(self):
        hid = self._handover()
        first = self.service.sign_handover(
            hid, "from", "Officer", at_local="2026-09-10T10:00",
            tz="America/New_York", idem_key="cb-1",
        )
        duplicate = self.service.sign_handover(
            hid, "from", "Officer", at_local="2026-09-10T10:00",
            tz="America/New_York", idem_key="cb-1",
        )
        self.assertTrue(duplicate["replayed"])
        self.assertEqual(event_id(first), event_id(duplicate))
        self.assertEqual(len(self.service.get_handover(hid)["signatures"]), 1)

    def test_same_idem_key_with_different_payload_conflicts(self):
        hid = self._handover()
        self.service.sign_handover(
            hid, "from", "Officer", at_local="2026-09-10T10:00",
            tz="America/New_York", observed_quantity=3, idem_key="cb-2",
        )
        with self.assertRaises(ConflictError):
            self.service.sign_handover(
                hid, "from", "Officer", at_local="2026-09-10T10:00",
                tz="America/New_York", observed_quantity=9, idem_key="cb-2",
            )

    def test_duplicate_sign_without_idem_key_does_not_double_book(self):
        hid = self._handover()
        self.service.sign_handover(
            hid, "from", "Officer", at_local="2026-09-10T10:00", tz="America/New_York"
        )
        retry = self.service.sign_handover(
            hid, "from", "Officer", at_local="2026-09-10T10:00", tz="America/New_York"
        )
        self.assertTrue(retry.get("already_signed"))
        signed_events = [
            e for e in self.service.list_events()
            if e["event_type"] == "handover_signed"
        ]
        self.assertEqual(len(signed_events), 1)

    def test_withdraw_and_resubmit_clears_signatures(self):
        hid = self._handover()
        self.service.sign_handover(
            hid, "from", "Officer", at_local="2026-09-10T10:00", tz="America/New_York"
        )
        self.service.withdraw_handover(hid, reason="清单复核")
        self.assertEqual(self.service.get_handover(hid)["status"], STATUS_WITHDRAWN)
        with self.assertRaises(ConflictError):
            self.service.withdraw_handover(hid)
        self.service.resubmit_handover(hid)
        ho = self.service.get_handover(hid)
        self.assertEqual(ho["status"], STATUS_PENDING)
        self.assertEqual(ho["signatures"], {})
        statuses = [h["status"] for h in ho["history"]]
        self.assertIn("已撤回", statuses)
        self.assertIn("待双方签署", statuses)

    def test_cannot_sign_after_withdrawal(self):
        hid = self._handover()
        self.service.withdraw_handover(hid)
        with self.assertRaises(ConflictError):
            self.service.sign_handover(
                hid, "from", "Officer", at_local="2026-09-10T10:00",
                tz="America/New_York",
            )

    def test_effected_handover_can_only_be_corrected(self):
        hid = self._handover()
        self.service.sign_handover(hid, "from", "A", at_local="2026-09-10T10:00", tz="America/New_York")
        self.service.sign_handover(hid, "to", "B", at_local="2026-09-11T10:00", tz="Asia/Shanghai")
        with self.assertRaises(ConflictError):
            self.service.withdraw_handover(hid)
        result = self.service.correct_handover(hid, expected_quantity=4, reason="复核")
        new_id = result["event"]["payload"]["new_id"]
        self.assertEqual(self.service.get_handover(hid)["status"], STATUS_CORRECTED)
        self.assertEqual(self.service.get_handover(hid)["superseded_by"], new_id)
        self.assertEqual(self.service.get_handover(new_id)["corrects"], hid)
        self.assertEqual(self.service.get_handover(new_id)["status"], STATUS_PENDING)


class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.service, _ = make_service()
        _, self.loc, self.batch = build_case(self.service)
        ids = []
        for i in range(3):
            ids.append(
                self.service.register_artifact(self.batch, "单件", "蛋化石", f"蛋{i}")
                ["event"]["payload"]["artifact_id"]
            )
        self.eggs = ids
        self.service.record_chain(ids, "查获")

    def test_concurrent_same_party_callbacks_book_once(self):
        hid = self.service.prepare_handover(
            self.batch, "交接", "X", "Y"
        )["event"]["payload"]["handover_id"]

        def sign(_):
            return self.service.sign_handover(
                hid, "from", "Officer",
                at_local="2026-09-10T10:00", tz="America/New_York",
                idem_key="concurrent-cb",
            )

        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(sign, range(32)))
        event_ids = {event_id(r) for r in results}
        self.assertEqual(len(event_ids), 1)
        signed = [
            e for e in self.service.list_events() if e["event_type"] == "handover_signed"
        ]
        self.assertEqual(len(signed), 1)

    def test_concurrent_parties_lead_to_exactly_one_effect(self):
        for _ in range(8):
            hid = self.service.prepare_handover(
                self.batch, "交接", "X", "Y", deadline="2026-12-31T23:59:59Z"
            )["event"]["payload"]["handover_id"]
            barrier = threading.Barrier(2)

            def sign(party, local, tz):
                barrier.wait()
                return self.service.sign_handover(
                    hid, party, party, at_local=local, tz=tz
                )

            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(
                    sign, ["from", "to"],
                    ["2026-09-10T10:00", "2026-09-11T10:00"],
                    ["America/New_York", "Asia/Shanghai"],
                ))
            self.assertEqual(self.service.get_handover(hid)["status"], STATUS_EFFECTED)
            self.assertEqual(len(self.service.get_handover(hid)["signatures"]), 2)

    def test_concurrent_splits_only_one_succeeds(self):
        # 先把三件蛋合成一个套，再并发尝试拆分（必须显式安置原单件）
        merged = self.service.merge_artifacts(self.eggs, "蛋化石组")
        set_id = merged["event"]["payload"]["artifact_id"]
        outputs = [
            {"reuse_singleton": egg_id, "name": f"分出{i}"}
            for i, egg_id in enumerate(self.eggs)
        ]

        def split():
            try:
                self.service.split_artifact(set_id, outputs)
                return "ok"
            except DomainError:
                return "rejected"

        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(lambda _: split(), range(8)))
        self.assertEqual(outcomes.count("ok"), 1)
        self.assertEqual(self.service.get_artifact(set_id)["lifecycle"], "已拆分")
        # 三个单件均脱离套、保持在册
        for egg_id in self.eggs:
            egg = self.service.get_artifact(egg_id)
            self.assertIsNone(egg["member_of"])
            self.assertEqual(egg["lifecycle"], LIFE_ACTIVE)


class QueueTest(unittest.TestCase):
    def setUp(self):
        self.service, _ = make_service()
        _, self.loc, self.batch = build_case(self.service)
        self.egg = self.service.register_artifact(self.batch, "单件", "蛋化石", "蛋")[
            "event"]["payload"]["artifact_id"]
        self.service.record_chain([self.egg], "查获")

    def test_overdue_pending_handover_queued_and_resolved_on_effect(self):
        hid = self.service.prepare_handover(
            self.batch, "交接", "X", "Y", deadline="2020-01-01T00:00:00Z"
        )["event"]["payload"]["handover_id"]
        opened = self.service.sweep_overdue()
        self.assertEqual([q["reason"] for q in opened], [REASON_OVERDUE])
        # 再次扫描不重复开单
        self.assertEqual(self.service.sweep_overdue(), [])
        self.service.withdraw_handover(hid, reason="改期")
        open_items = self.service.list_queue()
        self.assertEqual(open_items, [])

    def test_quantity_discrepancy_queued(self):
        hid = self.service.prepare_handover(
            self.batch, "交接", "X", "Y", expected_quantity=1
        )["event"]["payload"]["handover_id"]
        self.service.sign_handover(
            hid, "from", "A", at_local="2026-09-10T10:00",
            tz="America/New_York", observed_quantity=1,
        )
        result = self.service.sign_handover(
            hid, "to", "B", at_local="2026-09-11T10:00",
            tz="Asia/Shanghai", observed_quantity=2,
        )
        self.assertEqual(result["reason"], REASON_DISCREPANCY)
        self.assertEqual(self.service.get_handover(hid)["status"], STATUS_EFFECTED)
        reasons = {q["reason"] for q in self.service.list_queue()}
        self.assertIn(REASON_DISCREPANCY, reasons)

    def test_chain_skip_stage_queued(self):
        # 未建立生效交接直接登记交接节点
        result = self.service.record_chain([self.egg], "交接", location_id=self.loc)
        self.assertTrue(result["queued"])
        self.assertEqual(result["reason"], REASON_BROKEN_CHAIN)

    def test_chain_must_be_sequential(self):
        hid = self.service.prepare_handover(self.batch, "交接", "X", "Y")
        hid = hid["event"]["payload"]["handover_id"]
        self.service.sign_handover(hid, "from", "A", at_local="2026-09-10T10:00", tz="America/New_York")
        self.service.sign_handover(hid, "to", "B", at_local="2026-09-11T10:00", tz="Asia/Shanghai")
        self.service.record_chain([self.egg], "交接", location_id=self.loc)
        # 跳过入境直接入藏
        result = self.service.record_chain([self.egg], "入藏", location_id=self.loc)
        self.assertEqual(result["reason"], REASON_BROKEN_CHAIN)

    def test_resolve_queue_is_idempotent(self):
        self.service.prepare_handover(
            self.batch, "交接", "X", "Y", deadline="2020-01-01T00:00:00Z"
        )
        self.service.sweep_overdue()
        qid = self.service.list_queue()[0]["queue_id"]
        r1 = self.service.resolve_queue(qid, "补签完成", idem_key="resolve-1")
        r2 = self.service.resolve_queue(qid, "补签完成", idem_key="resolve-1")
        self.assertEqual(event_id(r1), event_id(r2))
        self.assertTrue(r2["replayed"])


class RevisionTest(unittest.TestCase):
    def setUp(self):
        self.service, _ = make_service()
        _, self.loc, self.batch = build_case(self.service)

    def test_split_preserves_quantity_and_links_history(self):
        created = self.service.register_artifact(
            self.batch, "套", "造像", "三尊组", quantity=3
        )
        source = created["event"]["payload"]["artifact_id"]
        result = self.service.split_artifact(source, [
            {"name": "主尊", "kind": "套", "quantity": 1},
            {"name": "胁侍一", "kind": "套", "quantity": 1},
            {"name": "胁侍二", "kind": "套", "quantity": 1},
        ], reason="入库分藏")
        outputs = result["event"]["payload"]["outputs"]
        self.assertEqual(sum(o["quantity"] for o in outputs), 3)
        self.assertEqual(self.service.get_artifact(source)["lifecycle"], "已拆分")
        for o in outputs:
            self.assertEqual(o["split_from"] if "split_from" in o else
                             self.service.get_artifact(o["artifact_id"])["split_from"], source)
        with self.assertRaises(DomainError):
            self.service.split_artifact(source, [{"name": "x", "kind": "单件"}])

    def test_split_quantity_mismatch_rejected(self):
        source = self.service.register_artifact(
            self.batch, "套", "造像", "三尊组", quantity=3
        )["event"]["payload"]["artifact_id"]
        with self.assertRaises(DomainError):
            self.service.split_artifact(source, [
                {"name": "a", "kind": "套", "quantity": 1},
                {"name": "b", "kind": "套", "quantity": 1},
            ])

    def test_merge_and_identity_correction_keep_full_lineage(self):
        a = self.service.register_artifact(self.batch, "单件", "恐龙骨架", "旧名骨架")[
            "event"]["payload"]["artifact_id"]
        b = self.service.register_artifact(self.batch, "单件", "恐龙骨架", "尾椎")[
            "event"]["payload"]["artifact_id"]
        self.service.record_chain([a, b], "查获")
        merged = self.service.merge_artifacts([a, b], "骨架合璧套")["event"]["payload"]
        merged_id = merged["artifact_id"]
        # 单件保留在册身份但归属唯一套
        self.assertEqual(self.service.get_artifact(a)["member_of"], merged_id)
        self.assertEqual(self.service.get_artifact(merged_id)["quantity"], 2)
        # 顶层物理数量不重复计数
        self.assertEqual(self.service._physical_count(self.batch), 2)

        corrected = self.service.correct_identity(
            merged_id, {"name": "特暴龙骨架合璧套", "category": "特暴龙化石"},
            reason="馆方鉴定",
        )["event"]["payload"]
        new_id = corrected["artifact_id"]
        self.assertEqual(self.service.get_artifact(merged_id)["lifecycle"], "已更正")

        # 从任一历史标识都能反查到当前器物
        for historical in (a, b, merged_id):
            trace = self.service.trace(historical)
            self.assertIn(new_id, trace["current_artifact_ids"])
            self.assertTrue(trace["timeline"])
        # 新标识也能回溯全部来源
        trace = self.service.trace(new_id)
        family = {n["artifact_id"] for n in trace["lineage"]["nodes"]}
        self.assertEqual(family, {a, b, merged_id, new_id})
        # 修订没有抹去历史：旧事件原样可查且哈希仍在链上
        types = [e["event_type"] for e in trace["timeline"]]
        self.assertIn("artifacts_merged", types)
        self.assertIn("identity_corrected", types)
        self.service.store.verify()

    def test_merge_requires_same_stage(self):
        a = self.service.register_artifact(self.batch, "单件", "蛋", "蛋1")[
            "event"]["payload"]["artifact_id"]
        b = self.service.register_artifact(self.batch, "单件", "蛋", "蛋2")[
            "event"]["payload"]["artifact_id"]
        self.service.record_chain([a, b], "查获")
        self.service.record_chain([a], "交接")  # 无生效交接将进队列，但 a 阶段仍不变
        # a、b 阶段一致（都停留在查获），先构造真实阶段差
        hid = self.service.prepare_handover(self.batch, "交接", "X", "Y")[
            "event"]["payload"]["handover_id"]
        self.service.sign_handover(hid, "from", "A", at_local="2026-09-10T10:00", tz="America/New_York")
        self.service.sign_handover(hid, "to", "B", at_local="2026-09-11T10:00", tz="Asia/Shanghai")
        self.service.record_chain([a, b], "交接", location_id=self.loc)
        c = self.service.register_artifact(self.batch, "单件", "蛋", "蛋3")[
            "event"]["payload"]["artifact_id"]
        with self.assertRaises(DomainError):
            self.service.merge_artifacts([a, c], "混搭套")


class DocumentTest(unittest.TestCase):
    def setUp(self):
        self.service, _ = make_service()
        _, _, self.batch = build_case(self.service)
        self.art = self.service.register_artifact(
            self.batch, "单件", "恐龙骨架", "骨架"
        )["event"]["payload"]["artifact_id"]

    def test_versions_chain_and_duplicate_hash_rejected(self):
        doc_id = self.service.record_document(
            "artifact", self.art, "鉴定书", "v1.pdf", "a" * 64
        )["event"]["payload"]["doc_id"]
        self.service.add_document_version(doc_id, "b" * 64, note="修订版")
        with self.assertRaises(ConflictError):
            self.service.add_document_version(doc_id, "b" * 64)
        doc = self.service.get_document(doc_id)
        self.assertEqual(doc["current_version"], 2)
        self.assertEqual([v["sha256"] for v in doc["versions"]], ["a" * 64, "b" * 64])

    def test_bad_hash_rejected(self):
        with self.assertRaises(DomainError):
            self.service.record_document("artifact", self.art, "照片", "x.jpg", "nope")


class PersistenceAndAuditTest(unittest.TestCase):
    def test_replay_restores_state_and_numbers(self):
        service, data_dir = make_service()
        case_id, loc, batch = build_case(service)
        art = service.register_artifact(batch, "套", "造像", "组", quantity=2)[
            "event"]["payload"]["artifact_id"]
        service.record_chain([art], "查获")
        hid = service.prepare_handover(batch, "交接", "X", "Y")[
            "event"]["payload"]["handover_id"]
        service.sign_handover(hid, "from", "A", at_local="2026-09-10T10:00", tz="America/New_York")
        service.sign_handover(hid, "to", "B", at_local="2026-09-11T10:00", tz="Asia/Shanghai")
        service.record_chain([art], "交接", location_id=loc)

        rebuilt = CustodyService(EventStore(data_dir))
        self.assertEqual(rebuilt.get_artifact(art)["current_stage"], "交接")
        self.assertEqual(rebuilt.get_handover(hid)["status"], STATUS_EFFECTED)
        report = rebuilt.audit_report()
        self.assertTrue(report["journal"]["verified"])
        self.assertTrue(report["projection_match"])
        # 编号序列在重放后继续增长而不撞号
        next_batch = rebuilt.create_batch(case_id, "另一机构")["event"]["payload"]["batch_no"]
        self.assertEqual(next_batch, "BATCH-0002")

    def _tamper(self, data_dir, mutate):
        path = os.path.join(data_dir, "journal.jsonl")
        with open(path, encoding="utf-8") as handle:
            lines = handle.readlines()
        mutate(lines)
        with open(path, "w", encoding="utf-8") as handle:
            handle.writelines(lines)

    def test_modified_event_breaks_hash_chain(self):
        service, data_dir = make_service()
        build_case(service)

        def mutate(lines):
            event = json.loads(lines[0])
            event["payload"]["title"] = "被篡改的名称"
            lines[0] = json.dumps(event, ensure_ascii=False) + "\n"

        self._tamper(data_dir, mutate)
        with self.assertRaises(JournalIntegrityError):
            EventStore(data_dir)

    def test_deleted_event_breaks_hash_chain(self):
        service, data_dir = make_service()
        build_case(service)
        self._tamper(data_dir, lambda lines: lines.pop(1))
        with self.assertRaises(JournalIntegrityError):
            EventStore(data_dir)

    def test_audit_report_after_revisions_shows_chain_and_history(self):
        service, _ = make_service()
        _, _, batch = build_case(service)
        art = service.register_artifact(batch, "单件", "蛋", "蛋")[
            "event"]["payload"]["artifact_id"]
        service.correct_identity(art, {"name": "恐龙蛋化石"})
        report = service.audit_report()
        self.assertTrue(report["journal"]["verified"])
        self.assertTrue(report["projection_match"])
        self.assertTrue(report["snapshot_match"])
        self.assertGreaterEqual(report["journal"]["events"], 5)


class SeedDemoTest(unittest.TestCase):
    def test_seed_two_batches_cross_day_signatures(self):
        service, _ = make_service()
        result = seed_demo(service)
        self.assertTrue(result["seeded"])
        # 再次播种不重复
        again = seed_demo(service)
        self.assertFalse(again["seeded"])

        ho = service.get_handover(result["washington"]["handover_id"])
        self.assertEqual(ho["status"], STATUS_EFFECTED)
        self.assertEqual(ho["expected_quantity"], 11)
        self.assertEqual(ho["signatures"]["from"]["tz"], "America/New_York")
        self.assertEqual(ho["signatures"]["to"]["tz"], "Asia/Shanghai")

        trex = result["washington"]["artifact_ids"]["trex_singleton"]
        trace = service.trace(trex)
        self.assertEqual(trace["current_artifact_ids"], [trex])
        stages = [c["stage"] for c in trace["custody_chain"]]
        self.assertEqual(stages, ["查获", "交接"])
        # 鉴定书两个版本哈希都保留
        report_id = result["washington"]["documents"]["report"]
        versions = service.get_document(report_id)["versions"]
        self.assertEqual(len(versions), 2)
        self.assertNotEqual(versions[0]["sha256"], versions[1]["sha256"])

        ny_ho = service.get_handover(result["new_york"]["handover_id"])
        self.assertEqual(ny_ho["status"], STATUS_PENDING)
        opened = service.sweep_overdue()
        self.assertEqual([q["reason"] for q in opened], [REASON_OVERDUE])

        report = service.audit_report()
        self.assertTrue(report["journal"]["verified"])
        self.assertTrue(report["projection_match"])
        self.assertEqual(report["counts"]["batches"], 2)
        self.assertEqual(report["counts"]["effected_handovers"], 1)


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service, _ = make_service()
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.server.service = cls.service
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def _request(self, method, path, body=None, headers=None, expect_status=None):
        data = None
        hdrs = headers or {}
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            hdrs = {"Content-Type": "application/json", **hdrs}
        req = Request(f"{self.base}{path}", data=data, headers=hdrs, method=method)
        try:
            with urlopen(req, timeout=5) as response:
                payload = json.load(response)
                status = response.status
        except HTTPError as exc:
            payload = json.load(exc)
            status = exc.code
            exc.close()
        if expect_status is not None:
            self.assertEqual(status, expect_status, payload)
        return status, payload

    def test_health_still_served(self):
        status, payload = self._request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(payload, health_payload())

    def test_full_flow_over_http_with_idempotency_header(self):
        _, case = self._request("POST", "/api/cases", {"title": "HTTP返还案"},
                                headers={"Idempotency-Key": "http-case"}, expect_status=201)
        _, case_dupe = self._request("POST", "/api/cases", {"title": "HTTP返还案"},
                                     headers={"Idempotency-Key": "http-case"}, expect_status=200)
        self.assertEqual(case["event"]["event_id"], case_dupe["event"]["event_id"])
        case_id = case["event"]["payload"]["case_id"]

        _, loc = self._request("POST", "/api/locations", {"name": "口岸库房"}, expect_status=201)
        loc_id = loc["event"]["payload"]["location_id"]
        _, batch = self._request("POST", "/api/batches", {
            "case_id": case_id, "foreign_agency": "ICE",
            "agency_timezone": "America/New_York", "location_id": loc_id,
        }, expect_status=201)
        batch_id = batch["event"]["payload"]["batch_id"]

        _, art = self._request("POST", "/api/artifacts", {
            "batch_id": batch_id, "kind": "单件", "category": "蛋化石", "name": "蛋",
        }, expect_status=201)
        art_id = art["event"]["payload"]["artifact_id"]

        self._request("POST", "/api/chain", {
            "artifact_ids": [art_id], "stage": "查获",
        }, expect_status=201)
        _, ho = self._request("POST", "/api/handovers", {
            "batch_id": batch_id, "stage": "交接",
            "from_party": "ICE", "to_party": "NCHA",
        }, expect_status=201)
        ho_id = ho["event"]["payload"]["handover_id"]

        self._request("POST", f"/api/handovers/{ho_id}/sign", {
            "party": "from", "actor": "A",
            "at_local": "2026-09-10T10:00", "tz": "America/New_York",
        }, headers={"Idempotency-Key": "http-sign-from"}, expect_status=201)
        self._request("POST", f"/api/handovers/{ho_id}/sign", {
            "party": "to", "actor": "B",
            "at_local": "2026-09-11T10:00", "tz": "Asia/Shanghai",
        }, expect_status=201)
        _, detail = self._request("GET", f"/api/handovers/{ho_id}")
        self.assertEqual(detail["status"], STATUS_EFFECTED)

        self._request("POST", "/api/chain", {
            "artifact_ids": [art_id], "stage": "交接", "location_id": loc_id,
        }, expect_status=201)
        _, trace = self._request("GET", f"/api/artifacts/{art_id}/trace")
        self.assertEqual(len(trace["custody_chain"]), 2)

        _, audit = self._request("GET", "/api/audit")
        self.assertTrue(audit["journal"]["verified"])

    def test_error_mapping(self):
        status, body = self._request("GET", "/api/cases/case_missing")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"], "not_found")
        status, body = self._request("POST", "/api/artifacts", {
            "batch_id": "batch_nope", "kind": "单件", "category": "x", "name": "y",
        })
        self.assertEqual(status, 404)
        status, body = self._request("POST", "/api/cases", {})
        self.assertEqual(status, 400)
        status, _ = self._request("GET", "/totally-unknown")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
