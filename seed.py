"""项目资料样例：两批跨境返还的演示数据。

- 华盛顿批次（美国移民海关执法局 ICE 华盛顿办公室）：石刻造像一套 4 件、
  陶俑一套 6 件按套计数；霸王龙骨架化石单件追踪。双方分两日、各按所在
  时区签署后交接生效。
- 纽约批次（纽约县地区检察官办公室）：狼鳍鱼化石 1 件、恐龙蛋化石 2 枚
  全部单件追踪；交接处于待签署且已过截止时间，``--sweep`` 后进入待处置队列。

幂等：账本已有案件时跳过播种，不重复写入。
"""

from __future__ import annotations

from events import sha256_hex

WASHINGTON_TZ = "America/New_York"
BEIJING_TZ = "Asia/Shanghai"


def seed_demo(service) -> dict:
    if service.list_cases():
        return {"seeded": False, "reason": "账本非空，跳过样例播种"}

    case = service.open_case(
        "中美文化财产返还接收案（2026年9月）",
        source_country="美国",
        note="国家文物管理部门登记两批境外执法机构返还物",
        actor="系统管理员",
        idem_key="seed:case",
    )["event"]["payload"]
    case_id = case["case_id"]

    vault = service.register_location(
        "国家文物局首都入库暂存库房",
        kind="海关监管库房",
        address="北京市东城区",
        actor="系统管理员",
        idem_key="seed:loc-vault",
    )["event"]["payload"]["location_id"]
    museum = service.register_location(
        "中国古动物馆修复库房",
        kind="馆藏库房",
        address="北京市西城区",
        actor="系统管理员",
        idem_key="seed:loc-museum",
    )["event"]["payload"]["location_id"]

    # ============================================================ 华盛顿批次

    b1 = service.create_batch(
        case_id,
        "美国移民海关执法局（ICE）华盛顿办公室",
        agency_timezone=WASHINGTON_TZ,
        expected_on="2026-09-10",
        location_id=vault,
        note="9月10日华盛顿交接、9月11日北京时间接收签署",
        actor="系统管理员",
        idem_key="seed:batch-washington",
    )["event"]["payload"]["batch_id"]

    statues = service.register_artifact(
        b1,
        "套",
        "石刻造像",
        "北周石刻造像一组",
        components=[
            {"name": "释迦坐像", "quantity": 1},
            {"name": "胁侍菩萨立像", "quantity": 2},
            {"name": "佛座残件", "quantity": 1},
        ],
        actor="登记员",
        idem_key="seed:art-statues",
    )["event"]["payload"]["artifact_id"]
    figures = service.register_artifact(
        b1,
        "套",
        "陶俑",
        "汉代彩绘陶俑一组",
        quantity=6,
        actor="登记员",
        idem_key="seed:art-figures",
    )["event"]["payload"]["artifact_id"]
    trex = service.register_artifact(
        b1,
        "单件",
        "恐龙骨架",
        "霸王龙骨架化石（约65%完整度）",
        actor="登记员",
        idem_key="seed:art-trex",
    )["event"]["payload"]["artifact_id"]

    service.record_chain(
        [statues, figures, trex],
        "查获",
        at_local="2026-08-20T09:00",
        tz=WASHINGTON_TZ,
        actor="ICE探员M.Diaz",
        idem_key="seed:b1-seized",
    )

    h1 = service.prepare_handover(
        b1,
        "交接",
        "ICE华盛顿办公室",
        "国家文物局接收工作组",
        expected_quantity=11,
        deadline="2026-09-12T23:59:59Z",
        actor="系统管理员",
        idem_key="seed:b1-handover",
    )["event"]["payload"]["handover_id"]

    # 照片、清单、鉴定文件只登记内容哈希；鉴定书后续追加版本
    service.record_document(
        "handover",
        h1,
        "交接清单",
        "washington-manifest-v1.pdf",
        sha256_hex("washington-manifest-v1"),
        media_type="application/pdf",
        note="随附11件/套清点清单",
        actor="ICE探员M.Diaz",
        idem_key="seed:b1-doc-manifest",
    )
    photo = service.record_document(
        "artifact",
        trex,
        "现场照片",
        "trex-handover-01.jpg",
        sha256_hex("trex-handover-01.jpg"),
        media_type="image/jpeg",
        actor="接收工作组",
        idem_key="seed:b1-doc-photo",
    )["event"]["payload"]["doc_id"]
    report = service.record_document(
        "artifact",
        trex,
        "鉴定文件",
        "trex-identification.pdf",
        sha256_hex("trex-identification-v1"),
        media_type="application/pdf",
        note="初步鉴定：霸王龙成年个体",
        actor="古生物鉴定组",
        idem_key="seed:b1-doc-report",
    )["event"]["payload"]["doc_id"]

    # 分两日签署：移交方 9月10日华盛顿时间，接收方 9月11日北京时间
    service.sign_handover(
        h1,
        "from",
        "ICE授权官员 J.Carter",
        at_local="2026-09-10T16:30",
        tz=WASHINGTON_TZ,
        observed_quantity=11,
        idem_key="seed:b1-sign-from",
    )
    service.sign_handover(
        h1,
        "to",
        "国家文物局接收人 李·文保",
        at_local="2026-09-11T10:15",
        tz=BEIJING_TZ,
        observed_quantity=11,
        idem_key="seed:b1-sign-to",
    )

    service.record_chain(
        [statues, figures, trex],
        "交接",
        location_id=vault,
        at_local="2026-09-11T10:30",
        tz=BEIJING_TZ,
        actor="李·文保",
        idem_key="seed:b1-chain-handover",
    )

    # 鉴定书新版本：复核后修订，旧版本哈希保留
    service.add_document_version(
        report,
        sha256_hex("trex-identification-v2"),
        media_type="application/pdf",
        note="复核修订：完整度由约60%核定为65%，旧版留存",
        actor="古生物鉴定组",
        idem_key="seed:b1-doc-report-v2",
    )
    service.add_document_version(
        photo,
        sha256_hex("trex-handover-02.jpg"),
        media_type="image/jpeg",
        note="入库后补拍侧面照",
        actor="接收工作组",
        idem_key="seed:b1-doc-photo-v2",
    )

    # ============================================================== 纽约批次

    b2 = service.create_batch(
        case_id,
        "纽约县地区检察官办公室（Manhattan DA）",
        agency_timezone=WASHINGTON_TZ,
        expected_on="2026-09-12",
        location_id=vault,
        note="9月12日纽约交接，待双方签署",
        actor="系统管理员",
        idem_key="seed:batch-newyork",
    )["event"]["payload"]["batch_id"]

    fish = service.register_artifact(
        b2,
        "单件",
        "鱼类化石",
        "狼鳍鱼化石标本",
        actor="登记员",
        idem_key="seed:art-fish",
    )["event"]["payload"]["artifact_id"]
    egg_a = service.register_artifact(
        b2,
        "单件",
        "恐龙蛋化石",
        "窃蛋龙蛋化石（甲枚）",
        actor="登记员",
        idem_key="seed:art-egg-a",
    )["event"]["payload"]["artifact_id"]
    egg_b = service.register_artifact(
        b2,
        "单件",
        "恐龙蛋化石",
        "窃蛋龙蛋化石（乙枚）",
        actor="登记员",
        idem_key="seed:art-egg-b",
    )["event"]["payload"]["artifact_id"]

    service.record_chain(
        [fish, egg_a, egg_b],
        "查获",
        at_local="2026-08-25T10:00",
        tz=WASHINGTON_TZ,
        actor="DA调查员R.Cohen",
        idem_key="seed:b2-seized",
    )
    h2 = service.prepare_handover(
        b2,
        "交接",
        "纽约县地区检察官办公室",
        "国家文物局接收工作组",
        expected_quantity=3,
        deadline="2026-09-15T23:59:59Z",
        actor="系统管理员",
        idem_key="seed:b2-handover",
    )["event"]["payload"]["handover_id"]
    service.record_document(
        "handover",
        h2,
        "交接清单",
        "newyork-manifest.pdf",
        sha256_hex("newyork-manifest"),
        media_type="application/pdf",
        actor="DA调查员R.Cohen",
        idem_key="seed:b2-doc-manifest",
    )

    return {
        "seeded": True,
        "case_id": case_id,
        "locations": {"vault": vault, "museum": museum},
        "washington": {
            "batch_id": b1,
            "handover_id": h1,
            "artifact_ids": {
                "statues_set": statues,
                "figures_set": figures,
                "trex_singleton": trex,
            },
            "documents": {"report": report, "photo": photo},
        },
        "new_york": {
            "batch_id": b2,
            "handover_id": h2,
            "artifact_ids": {
                "fish_singleton": fish,
                "egg_singletons": [egg_a, egg_b],
            },
        },
    }
