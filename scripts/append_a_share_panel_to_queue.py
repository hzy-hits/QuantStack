"""Append the user-supplied A-share 6-agent BFS panel to the source-review queue.

Inputs are 6 agent sub-pools plus three hit-count cohorts (3-hit / 2-hit /
1-hit). 62 raw candidates total, 1 already in queue (002902.SZ), 61 new.

Tier / pool derivation (lightweight rule of thumb — operator can override):
- evidence_state=原文已证明 + BFS D1   → P0_first_batch
- evidence_state=原文已证明 + BFS D2   → P1_d1_d3_followup
- evidence_state=原文已证明 + BFS D3+  → P2_radar_if_blocks_d2
- evidence_state=合理推论              → P2_radar_if_blocks_d2
- evidence_state=待原文核验            → P3_deep_radar
- hit_count_bucket=3-hit lifts one tier; 1-hit demotes one tier.

current_pool / score_bucket follow priority_tier; total_score is a sketch
that the readiness scorer will refine once the evidence-state lattice is
filled in.

All new rows enter at verification_status=pending_original_source_verification.
The script never touches global_universe_v2.jsonl.
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
QUEUE = STACK_ROOT / "ai_infra" / "reports" / "source_verification_queue_v1.csv"

FIELDS = (
    "rank", "priority_tier", "ticker", "company", "market_country", "asset_pool",
    "bfs_depth", "module", "current_pool", "total_score", "score_bucket",
    "verification_status", "source_priority", "primary_sources_to_find",
    "metrics_to_verify", "upgrade_conditions", "downgrade_conditions",
    "evidence_state", "counterevidence", "dependency_path", "dependency_edge",
    "etf_clue", "smart_money_clue",
)

TPL_SOURCE_PRIORITY = "Find latest annual report, latest quarterly results, earnings call transcript, investor presentation, and official product/capacity pages first."
TPL_DEFAULT_VERIFICATION = "pending_original_source_verification"
TPL_DEFAULT_SMART_MONEY = "公募/北向/陆股通"


# Mapping helper: (tier, pool, score, bucket) from evidence_state + bfs_depth + hit bucket.
def _tier_for(evidence_state: str, bfs_depth: str, hit_bucket: str) -> tuple[str, str, str, str]:
    state = (evidence_state or "").strip()
    depth = (bfs_depth or "").upper()
    bump = {"3-hit": 1, "agent": 0, "2-hit": 0, "1-hit": -1}.get(hit_bucket, 0)

    base_tier_index = 3  # P3 default
    if "原文已证明" in state:
        if depth.startswith("D1") or depth == "D1":
            base_tier_index = 0
        elif depth.startswith("D2"):
            base_tier_index = 1
        else:
            base_tier_index = 2
    elif "合理推论" in state:
        base_tier_index = 2
    elif "待原文核验" in state:
        base_tier_index = 3

    tier_idx = max(0, min(3, base_tier_index - bump))
    tier_table = [
        ("P0_first_batch", "核心候选", "92", "core_review"),
        ("P1_d1_d3_followup", "候选池", "82", "core_review"),
        ("P2_radar_if_blocks_d2", "雷达池", "72", "radar_review"),
        ("P3_deep_radar", "雷达池", "60", "radar_review"),
    ]
    return tier_table[tier_idx]


# Sector hint → etf_clue keyword map (rough; A-share retail ETFs).
def _etf_for(module: str) -> str:
    text = (module or "").lower()
    if "pcb" in text or "ccl" in text or "覆铜板" in text or "覆铜" in text:
        return "通信/电子/PCB ETF (515260/512480)"
    if "光" in text or "optical" in text:
        return "通信/光通信 ETF (515160/515880)"
    if "液冷" in text or "冷却" in text or "冷源" in text or "热管理" in text or "cdu" in text or "换热" in text:
        return "数据中心/液冷主题 ETF (516000)"
    if "电源" in text or "ups" in text or "hvdc" in text or "配电" in text or "断路器" in text or "变压器" in text or "开关柜" in text or "供电" in text:
        return "电力设备 ETF (516950/515030)"
    if "封测" in text or "封装" in text or "osat" in text or "靶材" in text or "电子特气" in text or "光刻" in text or "湿电子" in text or "石英" in text or "电子化学" in text:
        return "半导体材料/设备 ETF (512760/516920)"
    if "ai" in text and ("server" in text or "服务器" in text or "算力" in text or "rack" in text):
        return "云计算/算力 ETF (516630/AIQ proxy)"
    if "idc" in text or "数据中心" in text or "智算" in text:
        return "云计算/IDC ETF (516630)"
    if "运营商" in text or "联通" in text or "移动" in text:
        return "通信运营商 ETF (515050)"
    return "云计算/AI主题 ETF"


def _dependency_path(hit_seed: list[str], module: str) -> str:
    seeds = ", ".join(hit_seed[:3]) if hit_seed else "AI infra demand"
    return f"AI infra demand (seeds: {seeds}) → {module}"


# ── Raw panel from user (62 rows; 1 already in queue) ────────────────────
# Schema: (ticker, company, bfs_depth, module, edge_type, [seeds],
#          reason/upgrade, primary_sources, evidence_state, counterevidence, hit_bucket)
PANEL: list[dict[str, str]] = [
    # ── Agent 1: AI server / 算力整机 ─────────────────────────────────
    {"ticker": "002261.SZ", "company": "拓维信息", "bfs_depth": "D1",
     "module": "国产智能计算/AI服务器/昇腾服务器/AI一体机",
     "edge_type": "技术边", "hit_seed": ["浪潮信息", "紫光股份", "中兴通讯"],
     "reason": "公司基于鲲鹏+昇腾AI算力底座构建智能计算产品体系；高性能服务器、推理服务器、DeepSeek一体机、AI工作站等产品线",
     "primary_sources": ["2025年年度报告", "交易所公告", "官网产品页", "华为伙伴认证资料"],
     "evidence_state": "原文已证明",
     "counterevidence": "需拆分智能计算业务收入/毛利率/订单客户与华为生态依赖；软件/考试/鸿蒙混杂不能直接等同AI server利润池",
     "hit_bucket": "agent"},
    {"ticker": "603220.SH", "company": "中贝通信", "bfs_depth": "D1",
     "module": "算力服务/AI算力服务器/智算中心交付",
     "edge_type": "客户边", "hit_seed": ["中国联通", "中国移动", "中国电信"],
     "reason": "签署并公告多项算力服务和AI算力服务器合同，包括中国联通青海、济南超级计算中心",
     "primary_sources": ["2025年年度报告", "算力服务合同公告", "客户验收公告"],
     "evidence_state": "原文已证明",
     "counterevidence": "算力业务资产投入、财务费用、减值和客户集中风险；合同履约≠高利用率高毛利",
     "hit_bucket": "agent"},
    {"ticker": "603496.SH", "company": "恒为科技", "bfs_depth": "D2",
     "module": "算力基础设施/AI底座/AI一体机/智算服务",
     "edge_type": "技术边", "hit_seed": ["浪潮信息", "紫光股份", "运营商"],
     "reason": "公司聚焦算力基础设施与AI底座，完成运营商、金融领域算力交付和调优项目；探索AI一体机",
     "primary_sources": ["2025年年度报告", "算力交付项目公告", "官网产品页", "客户案例"],
     "evidence_state": "原文已证明",
     "counterevidence": "项目型交付难规模化；需核验AI一体机收入、客户复购、毛利率、存货风险",
     "hit_bucket": "agent"},
    {"ticker": "000158.SZ", "company": "常山北明", "bfs_depth": "D3",
     "module": "云服务器/云存储/算力基础设施/AI服务",
     "edge_type": "技术边", "hit_seed": ["紫光股份", "中国移动", "中国电信"],
     "reason": "产品涵盖云服务器、云存储、云网络及安全服务；提出围绕算力基础设施建设、AI服务升级",
     "primary_sources": ["2025年年度报告", "云与算力业务收入拆分", "官网产品页", "重大合同公告"],
     "evidence_state": "待原文核验",
     "counterevidence": "战略表述多于硬订单证据；2025年收入下降、扣非仍亏损",
     "hit_bucket": "agent"},
    {"ticker": "002771.SZ", "company": "真视通", "bfs_depth": "D3",
     "module": "AIDC建设/算力建设运营/液冷数据中心/绿色算力",
     "edge_type": "技术边", "hit_seed": ["英维克", "浪潮信息", "佳力图"],
     "reason": "聚焦AI算力建设与运营、AI应用落地和传统业务智能化升级；液冷技术作为算力建设业务核心壁垒",
     "primary_sources": ["2025年年度报告", "液冷技术公告", "算力项目合同", "客户验收资料"],
     "evidence_state": "待原文核验",
     "counterevidence": "2025年亏损、传统业务下滑；AI算力与液冷可能仍处转型早期，防止扭亏故事误判",
     "hit_bucket": "agent"},

    # ── Agent 2: PCB / CCL / 连接器 / 铜互连 ──────────────────────────
    {"ticker": "002938.SZ", "company": "鹏鼎控股", "bfs_depth": "D1",
     "module": "AI服务器PCB/光模块PCB/AI数据中心PCB/高端PCB",
     "edge_type": "BOM边", "hit_seed": ["沪电股份", "深南电路", "景旺电子"],
     "reason": "AI服务器、光模块等AI数据中心市场开拓加速；AI服务器类产品收入较2024年增长超1倍",
     "primary_sources": ["2025年年度报告", "客户结构披露", "产品收入拆分", "资本开支计划"],
     "evidence_state": "原文已证明",
     "counterevidence": "公司体量较大；消费电子/汽车业务占比仍需拆分；AI server PCB收入增长≠毛利率提升",
     "hit_bucket": "agent"},
    {"ticker": "603328.SH", "company": "依顿电子", "bfs_depth": "D2",
     "module": "高端PCB/AI服务器PCB/HDI/高频高速板",
     "edge_type": "BOM边", "hit_seed": ["沪电股份", "深南电路", "景旺电子"],
     "reason": "聚焦10层及以上、HDI、高频高速、埋铜等高附加值产品；大力开发AI服务器等高端PCB产品",
     "primary_sources": ["2025年年度报告", "2026经营计划", "产品认证记录", "客户导入情况"],
     "evidence_state": "原文已证明",
     "counterevidence": "当前仍以汽车电子为主，AI服务器更像新品拓展计划；需核验是否已有收入/订单/客户认证",
     "hit_bucket": "agent"},
    {"ticker": "002815.SZ", "company": "崇达技术", "bfs_depth": "D2",
     "module": "服务器PCB/GPU加速卡PCB/交换机PCB/通信服务器",
     "edge_type": "BOM边", "hit_seed": ["沪电股份", "深南电路", "中兴通讯"],
     "reason": "服务器领域与中兴、新华三、云尖、浪潮等建立合作；供应超级计算机主板、服务器存储设备、GPU加速卡PCB",
     "primary_sources": ["2025年年度报告", "2025年半年度报告", "客户认证资料", "服务器PCB收入拆分"],
     "evidence_state": "原文已证明",
     "counterevidence": "需确认AI服务器与普通服务器、通信设备PCB收入占比；多品种小批量模式限制规模弹性",
     "hit_bucket": "agent"},
    {"ticker": "603920.SH", "company": "世运电路", "bfs_depth": "D2",
     "module": "高多层PCB/AI服务器PCB/数据中心PCB/超低损耗PCB",
     "edge_type": "BOM边", "hit_seed": ["沪电股份", "景旺电子", "东山精密"],
     "reason": "产品应用于人工智能、计算机等领域；云端数据中心、AI大算力模组、高多层超低损PCB已量产含28层AI服务器板",
     "primary_sources": ["2025年年度报告", "AI服务器PCB产品说明", "客户认证资料", "订单与收入拆分"],
     "evidence_state": "合理推论",
     "counterevidence": "汽车电子仍主要下游；AI server量产≠收入占比高；需核验28层产品是否进核心客户BOM",
     "hit_bucket": "agent"},
    {"ticker": "002130.SZ", "company": "沃尔核材", "bfs_depth": "D1",
     "module": "高速通信线/DAC-AEC/800G铜互连/PCIe5.0",
     "edge_type": "BOM边", "hit_seed": ["立讯精密", "东山精密", "工业富联"],
     "reason": "2025年高速通信线收入快速增长；单通道224G/s、多通道800G/s、PCIe5.0已应用于国际前沿数据中心、服务器",
     "primary_sources": ["2025年年度报告", "投资者关系记录", "高速线产品目录", "客户认证资料"],
     "evidence_state": "待原文核验",
     "counterevidence": "证据偏互动平台/二手解读；需核验高速线收入、客户、毛利、单一大客户风险",
     "hit_bucket": "agent"},

    # ── Agent 3: 光网络 / 光通信 ─────────────────────────────────────
    {"ticker": "601869.SH", "company": "长飞光纤", "bfs_depth": "D1",
     "module": "光纤光缆/算力网络/光互连/空芯光纤",
     "edge_type": "技术边", "hit_seed": ["光迅科技", "华工科技", "运营商"],
     "reason": "生成式AI和算力数据中心建设对算力网络光通信产品产生结构性影响；推进空芯光纤商用",
     "primary_sources": ["2025年年度报告", "空芯光纤项目资料", "运营商/数据中心客户公告", "产品规格页"],
     "evidence_state": "原文已证明",
     "counterevidence": "光纤光缆需求受运营商周期影响；需拆分AI数据中心相关收入和传统通信收入",
     "hit_bucket": "agent"},
    {"ticker": "600105.SH", "company": "永鼎股份", "bfs_depth": "D2",
     "module": "光纤光缆/MPO连接器/数据电缆/AI数据通信",
     "edge_type": "技术边", "hit_seed": ["光迅科技", "中兴通讯", "运营商"],
     "reason": "光通信产业链向光芯片、光器件、MPO连接器延伸；满足AI数据通信增长需求",
     "primary_sources": ["2025年年度报告", "光通信产品页", "数据中心客户公告", "光器件/MPO收入拆分"],
     "evidence_state": "原文已证明",
     "counterevidence": "需区分运营商光缆周期、铜导体/汽车线束和AI数据中心光互连；MPO/光器件占比待核验",
     "hit_bucket": "agent"},
    # 002902.SZ 铭普光磁 — already in queue; skipped automatically
    {"ticker": "000070.SZ", "company": "特发信息", "bfs_depth": "D3",
     "module": "光通信/光纤光缆/智能终端/线缆服务",
     "edge_type": "技术边", "hit_seed": ["中兴通讯", "运营商", "光迅科技"],
     "reason": "立足光通信领域，深耕光纤光缆、智能终端业务",
     "primary_sources": ["2025年年度报告", "光纤光缆收入拆分", "运营商中标公告", "数据中心客户资料"],
     "evidence_state": "待原文核验",
     "counterevidence": "AI数据中心直接收入证据较弱，可能只是传统光通信/运营商周期映射",
     "hit_bucket": "agent"},
    {"ticker": "603042.SH", "company": "华脉科技", "bfs_depth": "D3",
     "module": "数据中心解决方案/ODN/光无源器件/机柜机房",
     "edge_type": "技术边", "hit_seed": ["运营商", "中兴通讯", "光迅科技"],
     "reason": "产品覆盖ODN、光无源器件、光缆、机箱机柜、数据中心解决方案",
     "primary_sources": ["2025年年度报告", "数据中心解决方案产品页", "客户项目公告", "收入结构拆分"],
     "evidence_state": "原文已证明",
     "counterevidence": "偏通信配套/机房解决方案，离AI训练推理集群较远；需核验是否AIDC还是普通机房建设",
     "hit_bucket": "agent"},

    # ── Agent 4: 液冷 / 电源 / UPS / HVDC / 电网 ──────────────────────
    {"ticker": "002706.SZ", "company": "良信股份", "bfs_depth": "D1",
     "module": "数据中心低压配电/UPS/HVDC/智能配电柜",
     "edge_type": "BOM边", "hit_seed": ["科华数据", "科士达", "英维克"],
     "reason": "基于AI算力需求开发可用于北美数据中心UPS及配电柜主开关产品；低压电器应用于数据中心低压配电柜/电力模组/UPS/HVDC",
     "primary_sources": ["2025年年度报告", "互动易/投资者关系记录", "数据中心产品页", "北美客户认证资料"],
     "evidence_state": "合理推论",
     "counterevidence": "需核验数据中心业务收入、海外客户认证和UL开关实际出货；低压电器竞争压缩议价权",
     "hit_bucket": "agent"},
    {"ticker": "603861.SH", "company": "白云电器", "bfs_depth": "D1",
     "module": "智算中心供电/变压器/高低压开关/母线槽/直流供电",
     "edge_type": "产能边", "hit_seed": ["国电南瑞", "思源电气", "中国西电"],
     "reason": "长期服务数据中心行业；智能算力领域电力能源解决方案；数据中心产品集群覆盖变压器、电源、高低压开关等",
     "primary_sources": ["2025年年度报告", "投资者关系活动记录", "数据中心解决方案产品页", "项目中标公告"],
     "evidence_state": "原文已证明",
     "counterevidence": "数据中心只是众多应用之一；需核验AIDC项目占比、毛利率、订单持续性、回款周期",
     "hit_bucket": "agent"},
    {"ticker": "000400.SZ", "company": "许继电气", "bfs_depth": "D2",
     "module": "变配电设备/一体化管控平台/HVDC电源系统/算力中心供电",
     "edge_type": "技术边", "hit_seed": ["国电南瑞", "思源电气", "中国西电"],
     "reason": "可为算力中心提供变配电设备、一体化管控平台、HVDC电源系统；客户含IDC服务商、电网企业、互联网企业",
     "primary_sources": ["2025年年度报告", "互动平台原文", "HVDC产品资料", "数据中心项目验收公告"],
     "evidence_state": "待原文核验",
     "counterevidence": "公司提示相关产品市场开拓初期、订单占比很小；雷达池而非核心池",
     "hit_bucket": "agent"},
    {"ticker": "002272.SZ", "company": "川润股份", "bfs_depth": "D1",
     "module": "算力液冷/数据中心液冷系统/绿色能源/储能液冷",
     "edge_type": "技术边", "hit_seed": ["英维克", "佳力图", "科华数据"],
     "reason": "聚焦战略级液冷业务；提出为客户提供算力液冷+绿色能源整体解决方案；研发数据/算力中心液冷系统关键技术",
     "primary_sources": ["2025年年度报告", "液冷系统产品页", "数据中心客户订单", "行业标准参与文件"],
     "evidence_state": "合理推论",
     "counterevidence": "2025年仍亏损；液冷收入增速需配合绝对规模、毛利率、客户验收、现金流核验",
     "hit_bucket": "agent"},
    {"ticker": "002126.SZ", "company": "银轮股份", "bfs_depth": "D1",
     "module": "数据中心液冷模组/换热器/热管理零部件/北美台系客户",
     "edge_type": "BOM边", "hit_seed": ["英维克", "佳力图", "三花智控"],
     "reason": "数据中心领域定位液冷模组零部件供应商；聚焦北美及台系客户；以不锈钢板式换热器突破",
     "primary_sources": ["2025年年度报告", "数据中心液冷产品页", "客户认证与订单", "数字能源业务收入拆分"],
     "evidence_state": "原文已证明",
     "counterevidence": "车用热管理仍主业；数据中心业务可能尚处爬坡；需核验北美/台系是否形成稳定量产",
     "hit_bucket": "agent"},
    {"ticker": "002927.SZ", "company": "泰永长征", "bfs_depth": "D3",
     "module": "数据中心配电/固态断路器/直流框架隔离开关/低压电器",
     "edge_type": "BOM边", "hit_seed": ["科华数据", "科士达", "中恒电气"],
     "reason": "数据中心配电领域技术突破；为新一代算力基础设施奠定基础；有专为数据中心开发的固态断路器",
     "primary_sources": ["2025年年度报告", "投资者关系记录", "固态断路器产品页", "数据中心客户订单"],
     "evidence_state": "待原文核验",
     "counterevidence": "2025年营收下降且归母亏损，现金流明显承压；先作为雷达池",
     "hit_bucket": "agent"},

    # ── Agent 5: 封测 / 半导体设备 / 材料 ────────────────────────────
    {"ticker": "600641.SH", "company": "先导基电(凯世通)", "bfs_depth": "D2",
     "module": "离子注入机/半导体设备/WFE/集成电路核心装备",
     "edge_type": "技术边", "hit_seed": ["北方华创", "至纯科技", "雅克科技"],
     "reason": "AI服务器与HBM需求推动WFE投资高位；旗下凯世通聚焦集成电路离子注入设备",
     "primary_sources": ["2025年年度报告", "离子注入机交付/验收公告", "客户认证资料", "设备订单与收入拆分"],
     "evidence_state": "原文已证明",
     "counterevidence": "WFE国产替代≠AI直接拉动；需拆分AI/HBM相关扩产与成熟制程、功率半导体设备需求",
     "hit_bucket": "agent"},
    {"ticker": "603688.SH", "company": "石英股份", "bfs_depth": "D2",
     "module": "半导体石英材料/晶圆制造材料/高纯石英",
     "edge_type": "BOM边", "hit_seed": ["北方华创", "至纯科技", "雅克科技"],
     "reason": "半导体用石英产品认证周期长；半导体晶圆制造环节推出更高纯度、耐高温新品；受益AI带动存储芯片需求",
     "primary_sources": ["2025年年度报告", "半导体石英认证进展", "客户认证资料", "半导体收入占比"],
     "evidence_state": "原文已证明",
     "counterevidence": "光伏石英周期和半导体石英需严格拆分；AI需求主要是二阶推导，量产收入更关键",
     "hit_bucket": "agent"},
    {"ticker": "002119.SZ", "company": "康强电子", "bfs_depth": "D2",
     "module": "封装材料/引线框架/键合丝/集成电路封测基础材料",
     "edge_type": "BOM边", "hit_seed": ["长电科技", "通富微电", "华天科技"],
     "reason": "主营半导体封装材料引线框架、键合丝",
     "primary_sources": ["2025年年度报告", "产品收入结构", "先进封装客户资料", "封测客户公告"],
     "evidence_state": "原文已证明",
     "counterevidence": "引线框架/键合丝偏传统封装材料，和AI先进封装/HBM链距离较远；需核验是否进高端封测",
     "hit_bucket": "agent"},
    {"ticker": "003043.SZ", "company": "华亚智能", "bfs_depth": "D2",
     "module": "半导体设备结构件/设备集成装配/精密金属结构件",
     "edge_type": "产能边", "hit_seed": ["北方华创", "至纯科技", "半导体设备链"],
     "reason": "半导体设备领域结构件业务是精密金属结构件核心业务",
     "primary_sources": ["2025年年度报告", "半导体设备客户清单", "结构件收入拆分", "设备维修业务客户"],
     "evidence_state": "原文已证明",
     "counterevidence": "更接近半导体设备配套件，AI链条为三阶映射；需核验客户是否对应先进制程/HBM",
     "hit_bucket": "agent"},
    {"ticker": "002549.SZ", "company": "凯美特气", "bfs_depth": "D2",
     "module": "电子特气/准分子激光气体/光刻气/半导体材料",
     "edge_type": "BOM边", "hit_seed": ["雅克科技", "至纯科技", "昊华科技"],
     "reason": "高品质电子特种气体获多家客户认可；准分子激光气体获Coherent认证；光刻气产品获相关认证",
     "primary_sources": ["2025年年度报告", "电子特气认证资料", "客户认证公告", "电子特气收入拆分"],
     "evidence_state": "原文已证明",
     "counterevidence": "主业仍含二氧化碳、氢气、燃料类产品；电子特气收入占比和半导体客户放量需核验",
     "hit_bucket": "agent"},
    {"ticker": "600160.SH", "company": "巨化股份", "bfs_depth": "D3",
     "module": "含氟电子化学品/氟化工/电子化学品/半导体材料",
     "edge_type": "BOM边", "hit_seed": ["雅克科技", "昊华科技", "江化微"],
     "reason": "氟化工拥有含氟电子化学品和无机氟化物产品体系；含氟电子化学品上游雷达",
     "primary_sources": ["2025年年度报告", "含氟电子化学品收入拆分", "半导体客户资料", "电子级产品认证"],
     "evidence_state": "待原文核验",
     "counterevidence": "2025年业绩主要可能来自制冷剂配额和氟化工景气；防止化工周期误映射为AI Infra材料",
     "hit_bucket": "agent"},

    # ── Agent 6: IDC / 运营商 / 云买方 ───────────────────────────────
    {"ticker": "603887.SH", "company": "城地香江", "bfs_depth": "D1",
     "module": "IDC投资运营/IDC综合解决方案/智算中心建设/运营商数据中心",
     "edge_type": "客户边", "hit_seed": ["中国移动", "中国电信", "运营商IDC"],
     "reason": "智算中心建设；交付中国电信江北、中国移动长三角数据中心楼栋；新签中国移动呼和浩特IDC等重大项目",
     "primary_sources": ["2025年年度报告", "重大合同公告", "项目交付公告", "IDC收入与毛利拆分"],
     "evidence_state": "原文已证明",
     "counterevidence": "公司仍有亏损和减值；IDC项目进入稳定运营期前，收入/毛利/现金流兑现需逐季跟踪",
     "hit_bucket": "agent"},
    {"ticker": "600126.SH", "company": "杭钢股份", "bfs_depth": "D2",
     "module": "云计算数据中心/数据中心机柜/算力服务/钢铁转型IDC",
     "edge_type": "产能边", "hit_seed": ["中国移动", "中国电信", "IDC"],
     "reason": "浙江云计算数据中心项目北区5栋已完成建设和交付，累计6,908个机柜运营",
     "primary_sources": ["2025年年度报告", "云计算数据中心项目公告", "机柜出租率", "客户合同"],
     "evidence_state": "原文已证明",
     "counterevidence": "钢铁主业周期与IDC转型混杂；需核验机柜利用率、电力成本、客户期限、IDC利润贡献",
     "hit_bucket": "agent"},
    {"ticker": "600797.SH", "company": "浙大网新", "bfs_depth": "D2",
     "module": "IDC托管/智算云服务/算网融合/云计算",
     "edge_type": "客户边", "hit_seed": ["中国移动", "中国电信", "云服务"],
     "reason": "第三方独立IDC运营商，通过杭州、成都等六大数据中心提供算网融合、IDC托管、云计算服务",
     "primary_sources": ["2025年年度报告", "智算云服务收入拆分", "数据中心利用率", "客户合同"],
     "evidence_state": "原文已证明",
     "counterevidence": "2025年仍亏损；政务云/政府IT支出和项目结算周期拖累；需确认智算云能否独立改善盈利",
     "hit_bucket": "agent"},
    {"ticker": "603881.SH", "company": "数据港", "bfs_depth": "D1",
     "module": "IDC/IDC解决方案/云服务销售/智算业务",
     "edge_type": "客户边", "hit_seed": ["中国移动", "中国电信", "IDC"],
     "reason": "主要业务包括IDC业务、IDC解决方案、云服务销售、智算业务四大类",
     "primary_sources": ["2025年年度报告", "客户结构披露", "智算业务收入拆分", "机柜利用率"],
     "evidence_state": "原文已证明",
     "counterevidence": "重点核验客户集中度和阿里等大客户投放节奏；IDC稳定收入不一定转化为AI算力弹性",
     "hit_bucket": "agent"},
    {"ticker": "002929.SZ", "company": "润建股份", "bfs_depth": "D2",
     "module": "智能算力中心/AI训练算力/推理算力/算力服务",
     "edge_type": "客户边", "hit_seed": ["中国移动", "中国电信", "运营商"],
     "reason": "智能算力中心定位为面向社会多主体的新型公共基础设施，建成后主要提供AI大模型训练/推理算力",
     "primary_sources": ["2025年年度报告", "智能算力中心项目公告", "设备采购与融资资料", "客户合同和利用率"],
     "evidence_state": "待原文核验",
     "counterevidence": "需核验项目建设进度、GPU/国产卡配置、客户锁定、融资成本、折旧压力；募集资金变更需跟踪",
     "hit_bucket": "agent"},

    # ── 3-hit priority verification ──────────────────────────────────
    {"ticker": "603019.SH", "company": "中科曙光", "bfs_depth": "D1",
     "module": "AI server/国产算力/算力服务/超节点scaleX640",
     "edge_type": "技术边", "hit_seed": ["浪潮信息", "工业富联", "紫光股份"],
     "reason": "聚焦算力基础设施全产业链；产品含高性能计算机、通用服务器、存储；披露单机柜级超节点 scaleX640",
     "primary_sources": ["2025年年度报告", "scaleX640产品资料", "客户中标公告"],
     "evidence_state": "原文已证明",
     "counterevidence": "国产化政策红利与AI实际拉动需拆分；客户集中于政企；需核验毛利率持续性",
     "hit_bucket": "3-hit"},
    {"ticker": "002475.SZ", "company": "立讯精密", "bfs_depth": "D1",
     "module": "AI rack/高速互连/铜缆/1.6T光高速互联/热管理/Power Shelf",
     "edge_type": "BOM边", "hit_seed": ["工业富联", "东山精密", "中科曙光"],
     "reason": "数据中心业务覆盖铜缆高速互连、1.6T光高速互联、热管理、AI机柜电源 Power Shelf",
     "primary_sources": ["2025年年度报告", "数据中心业务收入拆分", "客户认证资料", "产品规格页"],
     "evidence_state": "原文已证明",
     "counterevidence": "消费电子（果链）仍主业；数据中心业务毛利与收入占比需核验",
     "hit_bucket": "3-hit"},
    {"ticker": "603186.SH", "company": "华正新材", "bfs_depth": "D2",
     "module": "高速CCL/AI server/交换机/光模块用CCL",
     "edge_type": "BOM边", "hit_seed": ["沪电股份", "深南电路", "生益科技"],
     "reason": "高速覆铜板聚焦AI服务器、交换机、光模块；研发低介电损耗、低热膨胀材料",
     "primary_sources": ["2025年年度报告", "高速CCL客户认证", "收入拆分", "新品规格页"],
     "evidence_state": "原文已证明",
     "counterevidence": "CCL行业有 EMC/Panasonic/生益 强竞争；需核验高速产品爬坡和客户认证进度",
     "hit_bucket": "3-hit"},
    {"ticker": "600050.SH", "company": "中国联通", "bfs_depth": "D1",
     "module": "运营商/AI云/IDC/网络采购/AI云演进",
     "edge_type": "客户边", "hit_seed": ["中国移动", "中国电信", "数据港"],
     "reason": "加快向AI云演进；构建应用+模型+资源的算力经营模式；作为运营商CapEx买方",
     "primary_sources": ["2025年年度报告", "投资人简报", "CapEx计划", "AI云产品页"],
     "evidence_state": "原文已证明",
     "counterevidence": "运营商资本开支节奏受政策影响；AI云对集团利润贡献有限",
     "hit_bucket": "3-hit"},
    {"ticker": "002851.SZ", "company": "麦格米特", "bfs_depth": "D1",
     "module": "AI rack电源/数据中心电源/超算机柜HVDC",
     "edge_type": "BOM边", "hit_seed": ["科华数据", "科士达", "中恒电气"],
     "reason": "为AI服务器超算机柜提供高效率高压直流电源；兼具断电短时备电和超级电容功能",
     "primary_sources": ["2025年年度报告", "Power Shelf产品页", "客户认证资料", "数据中心收入拆分"],
     "evidence_state": "原文已证明",
     "counterevidence": "工控/家电/汽车业务混杂；数据中心电源占比和大客户认证节奏需核验",
     "hit_bucket": "3-hit"},
    {"ticker": "002364.SZ", "company": "中恒电气", "bfs_depth": "D1",
     "module": "HVDC/数据中心供电/IDC电源改造/10kV预制化直流系统",
     "edge_type": "BOM边", "hit_seed": ["科华数据", "麦格米特", "良信股份"],
     "reason": "数据中心用预制化10kV直转直流电源系统；半年度报告指向数据中心HVDC直流供配电",
     "primary_sources": ["2025年年度报告", "10kV直流系统产品页", "数据中心客户公告"],
     "evidence_state": "原文已证明",
     "counterevidence": "HVDC渗透率仍低；客户集中政企/运营商；需核验订单连续性",
     "hit_bucket": "3-hit"},
    {"ticker": "000811.SZ", "company": "冰轮环境", "bfs_depth": "D1",
     "module": "数据中心冷源/热交换器/液冷",
     "edge_type": "BOM边", "hit_seed": ["英维克", "佳力图", "三花智控"],
     "reason": "子公司服务数据中心冷却系统；研发液冷系统热交换器",
     "primary_sources": ["2025年年度报告", "液冷热交换器产品页", "客户认证"],
     "evidence_state": "原文已证明",
     "counterevidence": "工业制冷主业占比仍高；数据中心液冷收入占比待核验",
     "hit_bucket": "3-hit"},
    {"ticker": "600487.SH", "company": "亨通光电", "bfs_depth": "D1",
     "module": "光通信/数据中心互联/光纤光缆/AI服务器间高速数据传输",
     "edge_type": "技术边", "hit_seed": ["光迅科技", "长飞光纤", "运营商"],
     "reason": "光收发模块是数据中心互连关键组件；AI服务器间高速数据传输需求",
     "primary_sources": ["2025年年度报告", "光收发模块产品页", "运营商和数据中心客户"],
     "evidence_state": "原文已证明",
     "counterevidence": "海缆+光缆传统业务为主；AI数据中心直接收入占比待拆分",
     "hit_bucket": "3-hit"},

    # ── 2-hit watch (16 names) ───────────────────────────────────────
    {"ticker": "000066.SZ", "company": "中国长城", "bfs_depth": "D2",
     "module": "国产服务器/AI训推一体机",
     "edge_type": "技术边", "hit_seed": ["浪潮信息", "紫光股份", "中科曙光"],
     "reason": "交付多款AI训推一体机；官网展示AI服务器等算力基础设施产品",
     "primary_sources": ["2025年年度报告", "AI一体机产品页", "客户合同"],
     "evidence_state": "原文已证明",
     "counterevidence": "政企客户回款慢；国产化补贴依赖；毛利率不稳",
     "hit_bucket": "2-hit"},
    {"ticker": "000034.SZ", "company": "神州数码", "bfs_depth": "D2",
     "module": "AI server分销/系统集成/云与算力服务",
     "edge_type": "客户边", "hit_seed": ["浪潮信息", "中科曙光", "运营商"],
     "reason": "服务器分销与集成；披露AI相关业务收入线索",
     "primary_sources": ["2025年年度报告", "AI收入披露", "代理品牌结构"],
     "evidence_state": "原文已证明",
     "counterevidence": "高收入低毛利搬砖风险；分销业务对汇率/库存敏感",
     "hit_bucket": "2-hit"},
    {"ticker": "600498.SH", "company": "烽火通信", "bfs_depth": "D2",
     "module": "光通信系统/光纤光缆/光电子器件",
     "edge_type": "技术边", "hit_seed": ["中兴通讯", "光迅科技", "运营商"],
     "reason": "覆盖光通信系统、光纤光缆、光电子器件三大战略技术",
     "primary_sources": ["2025年年度报告", "光电子器件产品页"],
     "evidence_state": "原文已证明",
     "counterevidence": "运营商招标周期主导；AI数据中心直接拉动需拆分",
     "hit_bucket": "2-hit"},
    {"ticker": "600522.SH", "company": "中天科技", "bfs_depth": "D2",
     "module": "光通信/算力网络互联/电力线缆",
     "edge_type": "技术边", "hit_seed": ["亨通光电", "长飞光纤", "运营商"],
     "reason": "数据中心建设与算力网络互联；通信支撑数据中心建设",
     "primary_sources": ["2025年年度报告", "数据中心产品页"],
     "evidence_state": "原文已证明",
     "counterevidence": "海缆/电力线缆周期；AI数据中心直接收入占比待拆分",
     "hit_bucket": "2-hit"},
    {"ticker": "603936.SH", "company": "博敏电子", "bfs_depth": "D2",
     "module": "AI server PCB/高层板/超算PCB",
     "edge_type": "BOM边", "hit_seed": ["沪电股份", "崇达技术", "深南电路"],
     "reason": "AI服务器与超算高端PCB技术突破；攻克24/26层超高层高精密PCB及AI服务器主板核心技术",
     "primary_sources": ["2025年年度报告", "高层板产品页", "客户认证"],
     "evidence_state": "原文已证明",
     "counterevidence": "高层板良率与产能爬坡风险；客户进入主流AI server BOM需要验证",
     "hit_bucket": "2-hit"},
    {"ticker": "600601.SH", "company": "方正科技", "bfs_depth": "D2",
     "module": "PCB/交换机/光模块/AI服务器",
     "edge_type": "BOM边", "hit_seed": ["沪电股份", "深南电路", "崇达技术"],
     "reason": "PCB产品应用于通讯设备、交换机、光模块、AI服务器",
     "primary_sources": ["2025年年度报告", "客户结构", "产品类别收入"],
     "evidence_state": "原文已证明",
     "counterevidence": "公司经营存在历史问题；需核验AI server PCB实际占比",
     "hit_bucket": "2-hit"},
    {"ticker": "002050.SZ", "company": "三花智控", "bfs_depth": "D2",
     "module": "液冷CDU部件/热管理/PUE",
     "edge_type": "BOM边", "hit_seed": ["英维克", "佳力图", "银轮股份"],
     "reason": "商用液冷CDU核心部件升级；数据中心PUE场景",
     "primary_sources": ["2025年年度报告", "CDU产品页", "客户认证"],
     "evidence_state": "原文已证明",
     "counterevidence": "新能源车热管理仍主业；数据中心收入占比尚低",
     "hit_bucket": "2-hit"},
    {"ticker": "002011.SZ", "company": "盾安环境", "bfs_depth": "D2",
     "module": "液冷系统阀件/换热器/微通道",
     "edge_type": "BOM边", "hit_seed": ["英维克", "佳力图", "三花智控"],
     "reason": "推进数据中心液冷技术及项目；制冷阀件、微通道换热器用于液冷散热",
     "primary_sources": ["2025年年度报告", "液冷阀件产品页", "客户合同"],
     "evidence_state": "原文已证明",
     "counterevidence": "家电制冷主业占比高；数据中心新业务利润贡献尚小",
     "hit_bucket": "2-hit"},
    {"ticker": "600481.SH", "company": "双良节能", "bfs_depth": "D2",
     "module": "数据中心循环水冷却/空冷系统",
     "edge_type": "BOM边", "hit_seed": ["英维克", "佳力图", "冰轮环境"],
     "reason": "数据中心基础建设需要大量循环水冷却设备；空冷系统技术与客户积累",
     "primary_sources": ["2025年年度报告", "空冷系统产品页"],
     "evidence_state": "原文已证明",
     "counterevidence": "光伏硅片业务拖累；数据中心收入占比待核验",
     "hit_bucket": "2-hit"},
    {"ticker": "603290.SH", "company": "斯达半导", "bfs_depth": "D2",
     "module": "服务器电源/数据中心功率器件/IGBT/SiC/GaN",
     "edge_type": "BOM边", "hit_seed": ["中恒电气", "麦格米特", "良信股份"],
     "reason": "产品已批量用于服务器电源及数据中心设备；开发下一代AI服务器电源IGBT/SiC/GaN",
     "primary_sources": ["2025年半年度报告", "客户认证资料", "产品规格页"],
     "evidence_state": "原文已证明",
     "counterevidence": "EV车规IGBT价格战；产能扩张折旧压力",
     "hit_bucket": "2-hit"},
    {"ticker": "601208.SH", "company": "东材科技", "bfs_depth": "D2",
     "module": "高速电子树脂/CCL上游材料",
     "edge_type": "BOM边", "hit_seed": ["生益科技", "华正新材", "鹏鼎控股"],
     "reason": "高速电子树脂等材料受人工智能、算力升级带动",
     "primary_sources": ["2025年半年度报告", "高速树脂产品页", "客户认证"],
     "evidence_state": "原文已证明",
     "counterevidence": "电子树脂工艺壁垒下沉；定价压力来自下游CCL",
     "hit_bucket": "2-hit"},
    {"ticker": "600206.SH", "company": "有研新材", "bfs_depth": "D2",
     "module": "半导体靶材/先进封装材料/12英寸晶圆配套",
     "edge_type": "BOM边", "hit_seed": ["雅克科技", "至纯科技", "北方华创"],
     "reason": "高纯金属靶材、先进封装材料、12英寸晶圆生产线应用",
     "primary_sources": ["2025年年度报告", "靶材认证资料", "客户中标公告"],
     "evidence_state": "原文已证明",
     "counterevidence": "金属价格波动；先进封装客户认证慢",
     "hit_bucket": "2-hit"},
    {"ticker": "600378.SH", "company": "昊华科技", "bfs_depth": "D2",
     "module": "电子特气/含氟电子化学品",
     "edge_type": "BOM边", "hit_seed": ["雅克科技", "凯美特气", "巨化股份"],
     "reason": "电子化学品业务以含氟电子特气为核心",
     "primary_sources": ["2025年年度报告", "电子特气客户清单", "产品规格"],
     "evidence_state": "原文已证明",
     "counterevidence": "化工景气波动；电子特气在公司总营收占比待核验",
     "hit_bucket": "2-hit"},
    {"ticker": "603078.SH", "company": "江化微", "bfs_depth": "D2",
     "module": "湿电子化学品/半导体工艺材料",
     "edge_type": "BOM边", "hit_seed": ["至纯科技", "雅克科技", "凯美特气"],
     "reason": "湿电子化学品配套平板显示、半导体、LED、太阳能",
     "primary_sources": ["2025年年度报告", "客户认证", "半导体收入占比"],
     "evidence_state": "原文已证明",
     "counterevidence": "半导体级湿化学品认证慢；同行竞争(德邦/兴福)激烈",
     "hit_bucket": "2-hit"},
    {"ticker": "603650.SH", "company": "彤程新材", "bfs_depth": "D2",
     "module": "半导体光刻胶/显示光刻胶/电子材料",
     "edge_type": "BOM边", "hit_seed": ["雅克科技", "江化微", "至纯科技"],
     "reason": "半导体光刻胶为芯片制造关键光刻材料；同时显示光刻胶布局",
     "primary_sources": ["2025年年度报告", "光刻胶认证进展", "客户验证"],
     "evidence_state": "原文已证明",
     "counterevidence": "光刻胶国产化认证窗口期；导入大客户进度未知",
     "hit_bucket": "2-hit"},
    {"ticker": "600667.SH", "company": "太极实业", "bfs_depth": "D2",
     "module": "半导体封测/海太半导体/太极半导体",
     "edge_type": "客户边", "hit_seed": ["长电科技", "通富微电", "华天科技"],
     "reason": "半导体封测业务依托海太半导体和太极半导体开展",
     "primary_sources": ["2025年年度报告", "封测客户公告", "产能扩建公告"],
     "evidence_state": "原文已证明",
     "counterevidence": "封测客户集中度高；AI先进封装订单占比需核验",
     "hit_bucket": "2-hit"},

    # ── 1-hit radar (6 names) ────────────────────────────────────────
    {"ticker": "002897.SZ", "company": "意华股份", "bfs_depth": "D3",
     "module": "800G高速连接器/QSFP-DD112/OSFP112",
     "edge_type": "BOM边", "hit_seed": ["立讯精密", "鹏鼎控股", "沃尔核材"],
     "reason": "QSFP-DD 112、OSFP112系列800G高速连接器产品开发并通过下游关键客户验证测试",
     "primary_sources": ["2025年年度报告", "高速连接器产品页", "客户验证公告"],
     "evidence_state": "原文已证明",
     "counterevidence": "光伏支架业务拖累；高速连接器实际收入占比未知",
     "hit_bucket": "1-hit"},
    {"ticker": "000823.SZ", "company": "超声电子", "bfs_depth": "D3",
     "module": "PCB/覆铜板/超薄覆铜板/特种覆铜板",
     "edge_type": "BOM边", "hit_seed": ["生益科技", "华正新材", "鹏鼎控股"],
     "reason": "印制线路板、超薄及特种覆铜板",
     "primary_sources": ["2025年年度报告", "高速覆铜板产品页"],
     "evidence_state": "待原文核验",
     "counterevidence": "需核验AI server高速材料占比；公司经营改善节奏不明",
     "hit_bucket": "1-hit"},
    {"ticker": "600171.SH", "company": "上海贝岭", "bfs_depth": "D3",
     "module": "电源管理IC/信号链/能效监测/功率半导体",
     "edge_type": "BOM边", "hit_seed": ["麦格米特", "斯达半导", "中恒电气"],
     "reason": "电源管理、信号链、能效监测、功率半导体；与AI DC电源链可能有弱映射",
     "primary_sources": ["2025年年度报告", "电源IC产品页", "客户认证"],
     "evidence_state": "待原文核验",
     "counterevidence": "电源IC行业小批量低毛利；数据中心客户认证较慢",
     "hit_bucket": "1-hit"},
    {"ticker": "605111.SH", "company": "新洁能", "bfs_depth": "D3",
     "module": "MOSFET/GaN功率器件/服务器电源",
     "edge_type": "BOM边", "hit_seed": ["斯达半导", "麦格米特", "中恒电气"],
     "reason": "功率MOSFET、GaN功率半导体；服务器/数据中心电源二阶映射",
     "primary_sources": ["2025年年度报告", "GaN产品页", "客户认证资料"],
     "evidence_state": "待原文核验",
     "counterevidence": "EV车规价格压力；GaN在数据中心的实际渗透率未确认",
     "hit_bucket": "1-hit"},
    {"ticker": "600845.SH", "company": "宝信软件", "bfs_depth": "D3",
     "module": "IDC/工业软件/数据中心服务",
     "edge_type": "客户边", "hit_seed": ["数据港", "城地香江", "运营商IDC"],
     "reason": "服务外包含IDC业务保持稳健；但软件开发及工程服务受钢铁行业周期拖累",
     "primary_sources": ["2025年年度报告", "IDC业务收入拆分", "客户合同"],
     "evidence_state": "待原文核验",
     "counterevidence": "钢铁IT周期混杂；需严格拆分IDC稳态收入",
     "hit_bucket": "1-hit"},
    {"ticker": "600602.SH", "company": "云赛智联", "bfs_depth": "D3",
     "module": "云计算/大数据/城市算力/上海本地算力",
     "edge_type": "客户边", "hit_seed": ["浙大网新", "数据港", "中国电信"],
     "reason": "可放入上海本地算力和政企云链路雷达池",
     "primary_sources": ["2025年年度报告", "城市算力项目公告", "客户合同"],
     "evidence_state": "待原文核验",
     "counterevidence": "政企云项目结算长且利润薄；GPU/IDC收入和利润质量需核验",
     "hit_bucket": "1-hit"},
]


def _row_from_panel_entry(entry: dict[str, str], rank: int) -> dict[str, str]:
    tier, pool, score, bucket = _tier_for(entry["evidence_state"], entry["bfs_depth"], entry["hit_bucket"])
    module = entry["module"]
    row = {key: "" for key in FIELDS}
    row.update({
        "rank": str(rank),
        "priority_tier": tier,
        "ticker": entry["ticker"],
        "company": entry["company"],
        "market_country": "A股主板",
        "asset_pool": "中国资产池",
        "bfs_depth": entry["bfs_depth"],
        "module": module,
        "current_pool": pool,
        "total_score": score,
        "score_bucket": bucket,
        "verification_status": TPL_DEFAULT_VERIFICATION,
        "source_priority": TPL_SOURCE_PRIORITY,
        "primary_sources_to_find": " / ".join(entry["primary_sources"]),
        "metrics_to_verify": "AI/数据中心相关收入拆分、毛利率、客户结构、订单与交付节奏",
        "upgrade_conditions": entry["reason"],
        "downgrade_conditions": "AI收入披露薄弱、客户/订单不可持续、毛利率压缩、现金流恶化",
        "evidence_state": entry["evidence_state"],
        "counterevidence": entry["counterevidence"],
        "dependency_path": _dependency_path(entry["hit_seed"], module),
        "dependency_edge": entry["edge_type"],
        "etf_clue": _etf_for(module),
        "smart_money_clue": TPL_DEFAULT_SMART_MONEY,
    })
    return row


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", type=Path, default=QUEUE)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()

    if not args.queue.exists():
        print(f"error: queue not found at {args.queue}", file=sys.stderr)
        return 2

    with args.queue.open("r", encoding="utf-8") as handle:
        existing_rows = list(csv.DictReader(handle))
    existing_tickers: set[str] = set()
    for row in existing_rows:
        for piece in (row.get("ticker") or "").split("/"):
            t = piece.strip().upper()
            if t:
                existing_tickers.add(t)
    max_rank = max((int(r["rank"]) for r in existing_rows if r.get("rank", "").isdigit()), default=0)

    new_rows: list[dict[str, str]] = []
    skipped: list[str] = []
    for entry in PANEL:
        ticker = entry["ticker"].upper()
        if ticker in existing_tickers:
            skipped.append(ticker)
            continue
        max_rank += 1
        new_rows.append(_row_from_panel_entry(entry, max_rank))
        existing_tickers.add(ticker)

    print(f"panel size: {len(PANEL)} | already in queue: {len(skipped)} | to append: {len(new_rows)}")
    if args.dry_run or not new_rows:
        for row in new_rows[:5]:
            print(f"  preview rank={row['rank']} ticker={row['ticker']} tier={row['priority_tier']} pool={row['current_pool']}")
        if not args.dry_run:
            print("nothing to add")
        return 0

    if not args.no_backup:
        backup = args.queue.with_suffix(args.queue.suffix + ".bak")
        shutil.copy2(args.queue, backup)
        print(f"backup: {backup}")

    with args.queue.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        for row in new_rows:
            writer.writerow(row)
    print(f"appended {len(new_rows)} rows; skipped {len(skipped)}")
    by_tier: dict[str, int] = {}
    for row in new_rows:
        by_tier[row["priority_tier"]] = by_tier.get(row["priority_tier"], 0) + 1
    for tier, count in sorted(by_tier.items()):
        print(f"  {tier}: {count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
