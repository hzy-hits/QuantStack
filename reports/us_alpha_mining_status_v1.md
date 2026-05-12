# US Alpha Mining Status v1

日期：2026-05-12  
状态：first executable mining pass, pending original-source verification  
边界：这是研究进展，不是投资建议、买卖建议、目标价或仓位建议。

## 当前完成

| 项目 | 状态 | 文件 |
| --- | --- | --- |
| 美股 alpha mining 队列 | 已生成 30 条，含 P0/P1/P2 | `reports/us_alpha_mining_queue_v1.md` |
| 可筛选 CSV | 已生成 | `reports/us_alpha_mining_queue_v1.csv` |
| 缺失 evidence card 草稿 | 已补到 `evidence/us_alpha`；已有 batch1 的不重复生成 | `evidence/us_alpha/README.md` |
| 生成脚本 | 已加入可复跑脚本 | `scripts/generate_us_alpha_mining_queue.py` |
| COHR 样板卡 | 已补 Q3 FY2026 release、10-Q、investor presentation 三类原文 | `evidence/batch1/014-COHR-Coherent.md` |

## P0_us_alpha

| ticker | cluster | 动作 |
| --- | --- | --- |
| COHR | optics_connectivity | 已完成第一轮原文核验，保持候选/核心候选 |
| FN | optics_connectivity | 下一张建议卡，验证 datacom mix 与客户集中 |
| MOD | power_thermal | 验证 data center thermal revenue / margin / backlog |
| RMBS | ip_storage_eda | 验证 HBM / DDR / CXL IP royalty 与客户 attach |
| ALAB | optics_connectivity | 验证 PCIe/CXL retimer attach 和客户集中 |
| CRDO | optics_connectivity | 验证 AEC/SerDes hyperscaler demand 与毛利 |
| FORM | test_metrology | 验证 HBM/GPU probe card mix |
| LITE | optics_connectivity | 验证 datacom laser / AI customer qualification |
| MTSI | optics_connectivity | 验证 photonics component AI exposure |
| ONTO | test_metrology | 验证 advanced packaging metrology / inspection |
| PSTG | ip_storage_eda | 验证 AI storage 是否真有收入传导 |
| TER | test_metrology | 验证 AI tester demand 和 segment mix |

## 第一张样板卡结论：COHR

COHR 的原文目前支持继续放在美股 alpha 核心候选里：

- Q3 FY2026 revenue / Datacenter & Communications revenue 增长有公司原文支持。
- 10-Q 将 Datacenter growth 与 AI datacenter demand 直接连接。
- Investor presentation 给出 InP、CPO / silicon photonics、400G/lane、1.6T/3.2T/6.4T transceivers 等 next-gen optics 路线。
- NVIDIA strategic agreement / investment 是客户和产能侧强线索。

同时反证也很清楚：

- Datacenter & Communications 不是 AI-only 口径。
- 800G / 1.6T / CPO 分产品收入未拆。
- 客户集中和 purchase commitment 条款仍待核验。
- 9M FY2026 operating cash flow 被 inventory / working capital 压低，需要继续看 FCF 转化。

## 下一步

建议继续按这个顺序挖：

1. `FN`：光模块制造，和 COHR 同属 optics cluster，可比较客户集中和毛利。
2. `MOD`：thermal management，验证 AI data center cooling 是否真的带来收入/毛利。
3. `ALAB` 或 `CRDO`：连接芯片 / AEC / retimer，验证 rack-scale connectivity 的 attach rate 和客户集中。
4. `FORM`：测试/探针，验证 HBM/GPU complexity 是否进入订单和收入。

每完成 3-5 张卡，再回头调整 SQLite score 和 paper portfolio watchlist。
