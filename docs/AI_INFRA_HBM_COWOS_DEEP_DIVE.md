# HBM And CoWoS Deep Dive

This document is a source-review gated research memo for the first two AI infra
mainlines:

1. `hbm_structural_supercycle`
2. `cowos_advanced_packaging_bottleneck`

The conclusions here are research priors. They can guide universe construction,
screening, source-review queues and deep-dive prompts. They cannot be promoted
to `source_linked_supply_evidence` until the referenced company or standards
body source is reviewed and stored in the relationship/evidence ledger.

## 1. HBM Structural Supercycle

### Core Judgment

HBM has clearly moved beyond a simple 2-3 year supply mismatch. It is becoming a
long-term structural memory standard for high-end AI accelerators.

This does not mean HBM has no cycle, no oversupply risk, or no ASP downside.
The correct framing is:

> HBM is becoming a structural platform interface for high-end AI accelerators.
> The cycle now depends on HBM revenue and margin, 12-high and 16-high yield,
> HBM4/HBM4E customer qualification, equipment orders, test consumables, and
> substrate/material lead times.

### Evidence Logic

The judgment rests on three layers:

1. NVIDIA and AMD accelerator roadmaps use HBM capacity and bandwidth as core
   generation specifications. Examples to source-review include H200,
   Blackwell Ultra, AMD MI325X, AMD MI355X and NVIDIA Rubin / Vera Rubin.
2. JEDEC HBM4 standardization makes HBM4 a standardized interface direction,
   not a one-customer temporary customization.
3. SK hynix, Samsung and Micron are all moving from HBM3E 8-high/12-high toward
   HBM4/HBM4E. Source review should focus on official product briefs, earnings
   decks and call transcripts.

### Source-Review Checklist

| Category | Company / Body | Source To Review | Purpose | Status |
|---|---|---|---|---|
| Standard | JEDEC | JESD270-4 / HBM4 standard release | Confirm 2048-bit interface, 4/8/12/16-high stacks, capacity and bandwidth boundary | found, source review required |
| Accelerator customer | NVIDIA | H200, Blackwell/Blackwell Ultra, Rubin/Vera Rubin official specs and investor materials | Confirm HBM as generation-defining accelerator spec and HBM3E to HBM4 transition | found, source review required |
| Accelerator customer | AMD | Instinct MI325X and MI355X official product pages | Confirm non-NVIDIA accelerator dependence on HBM3E | found, source review required |
| Memory vendor | SK hynix | HBM3E 12-high, HBM4 development/sample/production, quarterly report, IR transcript | Confirm HBM3E/HBM4 route, 12H/16H, customer qualification, revenue contribution | partly found, financial-source review required |
| Memory vendor | Samsung | HBM product page, HBM4 newsroom, HBM3E/HBM4 tech brief, quarterly report | Confirm HBM4 production, 1c DRAM, 4nm base die, 12H/16H, customer qualification | found, source review required |
| Memory vendor | Micron | FY2025 Q4, FY2026 Q2 deck, HBM4 press release | Confirm HBM revenue, HBM4 production, 16H sampling, HBM4E, base logic die path | found, source review required |
| Bonding equipment | Hanmi | TC BONDER DRAGON, TC BONDER CW, annual report, order releases | Confirm HBM TSV stack bonding equipment exposure | found, annual/order split required |
| Bonding equipment | ASMPT | Annual results, TCB product/IR deck | Confirm TCB revenue, HBM/advanced logic use, TAM change | found, source review required |
| Bonding equipment | BESI | Quarterly results, Investor Day, hybrid bonding/TCB deck | Confirm HBM4/16H impact on TCB/hybrid bonding and orders | found, source review required |
| Temporary bonding | SUSS | Investor presentation, annual report | Confirm temporary bonding/debonding/cleaning orders from HBM manufacturers | found, source review required |
| Molding | TOWA | FY presentation, quarterly result | Confirm HBM molding and AI server/OSAT investment exposure | found, source review required |
| Wafer processing | DISCO | Annual/quarterly materials, presentation | Confirm thinning/dicing/grinding exposure to AI/HBM/OSAT demand | found, direct HBM specificity weaker |
| ATE | Advantest | HBM memory test product, IR/annual report | Confirm end-to-end HBM test solution and revenue elasticity | found, source review required |
| ATE | Teradyne | Magnum 7H HBM platform, IR | Confirm HBM3/3E/4/4E test coverage | found, source review required |
| Test/interface | Chroma | Annual report, AI/HPC test product, customer/application notes | Confirm HBM-specific test exposure | pending source review |
| Probe card/socket | MPI | Probe card product, annual report | Confirm HBM probe card or DRAM high-pin-count exposure | pending HBM-specific source review |
| Probe card/socket | WinWay | Probe card product, annual report | Confirm AI/HBM probe card orders and capacity | pending HBM-specific source review |
| Probe pin/socket | Leeno | Fine-pitch probe, memory socket, annual report | Confirm HBM socket/probe pin exposure | pending HBM-specific source review |
| Socket/interface | ISC | IR deck, quarterly result | Confirm HBM socket/equipment/material parts exposure | found, source review required |
| Probe card | TSE | Probe card product, IR | Confirm HBM probe card qualification/revenue | pending HBM-specific source review |
| Substrate | Ibiden | Quarterly/annual result, substrate capex | Confirm AI accelerator substrate and HBM/CoWoS spillover | pending HBM-specific source review |
| Substrate | Shinko | Annual/financial highlights, substrate capex | Confirm AI/HPC substrate demand and profit elasticity | pending HBM-specific source review |
| Substrate | Unimicron | Annual report, ABF/IC carrier substrate IR | Confirm AI/HPC substrate demand, capacity and pricing | pending source review |
| Material | Ajinomoto | ABF official tech page, annual report | Confirm ABF link to AI/HPC substrate | found, source review required |
| Material | Resonac | Advanced packaging materials, NCF/TIM, annual result | Confirm HBM/advanced package material spillover | pending HBM-specific source review |

### Already-Proven Research Claims After Source Review

These claims are the highest-priority source-review targets because they define
whether HBM is structural:

- HBM is part of AI accelerator generation specs: H200, Blackwell Ultra,
  AMD MI325X/MI355X, Rubin/Vera Rubin.
- HBM4 is standardized and entering production/sample paths.
- 8-high -> 12-high -> 16-high is a real route, not a market slogan.
- The bottleneck expands beyond DRAM bit capacity into stack, bonding,
  temporary bonding, molding, test, substrate and materials.

### Reasonable Inferences

- HBM is shifting from a shortage item into an AI accelerator architecture
  constraint because memory bandwidth and capacity are first-order platform
  specs.
- HBM4/HBM4E likely shifts value from memory die alone into base die, advanced
  packaging, custom base logic and test interface.
- 16-high can push elasticity toward bonding, thin-wafer handling, molding,
  thermal control and test complexity.

### Pending Source Review

Do not treat these as confirmed until original company sources are reviewed:

- Samsung and SK hynix HBM4E base die / custom logic / 16H route and customer
  qualification.
- Chroma, MPI, WinWay, Leeno and TSE HBM-specific revenue or qualification.
- Ibiden, Shinko, Unimicron and Resonac HBM-specific or CoWoS-specific revenue,
  customer qualification and gross-margin contribution.

### HBM Supply-Chain Map

```text
AI accelerator customer
NVIDIA / AMD / custom ASIC
        |
        v
GPU / ASIC + HBM package architecture
CoWoS / 2.5D interposer / advanced substrate / power & thermal design
        |
        v
HBM stack
DRAM die + logic base die + TSV + micro-bump / hybrid bonding + molding
        |
        +-- Memory vendors: SK hynix / Samsung / Micron
        +-- Bonding equipment: Hanmi / ASMPT / BESI
        +-- Temporary bonding and wafer handling: SUSS
        +-- Molding / encapsulation: TOWA
        +-- Wafer thinning / dicing / grinding: DISCO
        +-- ATE: Advantest / Teradyne / Chroma pending
        +-- Probe card / socket / pins: MPI / WinWay / Leeno / ISC / TSE
        +-- Substrate and materials: Ibiden / Shinko / Unimicron / Ajinomoto / Resonac
```

### Technical Route

| Stack | Current State | Technical Meaning | Investment Verification |
|---|---|---|---|
| 8-high | Common HBM3E 24GB baseline | Yield and packaging complexity are more controllable | No longer the main scarcity proof |
| 12-high | HBM3E 36GB in production; HBM4 36GB current mainstream route | Die thinning, bonding, thermal, molding and test complexity rise | Current structural bottleneck battlefield |
| 16-high | JEDEC HBM4 support; vendor sampling/planning needs source review | Yield, warpage, heat, test time and stack height are core | Next-round elasticity and disconfirmation focus |

12-high proves HBM has become production-standardized. 16-high determines
whether the complexity premium continues into the next phase.

### Financial Verification Metrics

#### Memory Vendors

| Metric | Why It Matters |
|---|---|
| HBM revenue / annualized run-rate | Confirms sample-to-revenue transition |
| HBM share in DRAM / data-center revenue | Confirms whether HBM becomes a structural revenue pool |
| HBM gross margin vs corporate gross margin | Confirms structural premium or rising price pressure |
| 12H / 16H yield learning curve | Confirms whether higher stack remains a bottleneck |
| Customer qualification count | Confirms industry standard vs single-customer cycle |
| HBM4/HBM4E shipment timing | Confirms alignment with accelerator platform ramp |

#### Equipment

| Company | Key Metrics | Logic |
|---|---|---|
| Hanmi | TC bonder orders, HBM customer count, DRAGON/CW shipments | TCB orders should lead memory-vendor revenue if 16H/stacking complexity rises |
| ASMPT | TCB revenue, advanced packaging backlog, HBM/logic mix | TCB should remain stronger than traditional assembly equipment if HBM/logic demand persists |
| BESI | Hybrid bonding / TCB orders, AI 2.5D shipment | Hybrid bonding may gain elasticity if HBM4/HBM5 migrates beyond classic TCB |
| SUSS | Temporary bonder/debonder/cleaner orders | Thin-wafer and temporary-bonding demand should grow with higher-stack HBM |
| TOWA | HBM molding equipment, AI server/OSAT orders | 12H/16H warpage and encapsulation needs should support molding demand |
| DISCO | Shipment value, consumables, OSAT utilization | Broader AI/OSAT wafer processing beneficiary, not pure HBM evidence by itself |

#### Testing And Interface

| Company | Key Metrics | Logic |
|---|---|---|
| Advantest | Memory test platform revenue, HBM test utilization | HBM3E/HBM4 speed and stack complexity can increase test time and value |
| Teradyne | Magnum 7H orders, HBM3/4 coverage, installed base | Official HBM3/3E/4/4E coverage should translate into order evidence |
| Chroma | AI/HPC reliability test revenue, HBM-specific customer | Pending source review |
| MPI / WinWay / TSE | HBM probe card revenue, qualification, replacement cycle | Higher pin count and parallelism may lift ASP and replacement frequency |
| Leeno / ISC | HBM socket, burn-in socket, fine-pitch probe pin revenue | ISC has HBM IR clues; Leeno remains source-review gated |

#### Substrate And Materials

| Company | Key Metrics | Logic |
|---|---|---|
| Ibiden / Shinko / Unimicron | AI/HPC substrate revenue, ABF capacity, lead time, capex | AI accelerator package expansion may create ABF spillover, but CPU/GPU/ASIC/HBM need separation |
| Ajinomoto | ABF sales, pricing, capacity additions | ABF is core high-performance package material; AI/HPC-specific wording matters |
| Resonac | NCF/TIM/advanced packaging materials revenue | 16H/HBM4 heat and material requirements may lift demand, pending source review |

### HBM Disconfirmation Dashboard

The strongest disconfirming signals are not generic "AI demand down" claims.
Watch for:

- HBM revenue grows but HBM ASP, gross margin or order visibility falls.
- HBM inventory rises and customer prepayment / long-term agreements weaken.
- 12H/16H yield matures quickly enough that HBM is no longer a bottleneck.
- HBM4/HBM4E delays or customer platforms reduce HBM attach/capacity per GPU.
- CXL, SOCAMM, LPDDR, compression or KV-cache offload meaningfully substitutes
  HBM intensity.
- TCB/hybrid bonding, temporary bonding, molding, ATE, probe card and socket
  orders roll over before memory-vendor revenue does.
- ABF substrate and advanced material lead times shorten, prices fall or capex
  is delayed.

## 2. CoWoS / Advanced Packaging Bottleneck

### Core Judgment

CoWoS / 2.5D / advanced packaging remains a bottleneck for AI accelerator
shipments. But the bottleneck is no longer a simple single-point TSMC CoWoS
capacity story.

The more accurate framing is:

> TSMC remains the gatekeeper for total assembly capability and leading-edge
> delivery. As TSMC expands CoWoS, OSAT participation rises and HBM4/HBM4E
> progresses, new constraints are more likely to show up in high-end substrate,
> T-glass / glass core, TCB / hybrid bonding equipment, temporary bonding,
> dicing / grinding, molding, inspection / metrology, and some test cells.

### Source-Review Checklist

| Company / Segment | Source To Review | Evidence Status | Use |
|---|---|---|---|
| TSMC | Earnings call transcript, annual report, advanced packaging / CoWoS tech page, capex commentary | source-review priority | Core source for whether CoWoS and advanced packaging remain tight |
| ASE | Earnings presentation, annual report / 20-F, advanced packaging / LEAP / test commentary | pending source review | Confirm advanced packaging, test, AI capex/backlog/utilization |
| Amkor | Q1 2026 results, earnings presentation, Form 10-K | source-review priority | Confirm OSAT advanced packaging/test in AI/HPC supply chain |
| Ibiden | FY2025/26 results, 2026-2028 investment plan, corporate profile | source-review priority | Confirm AI/high-performance server IC package substrate expansion |
| Shinko | FY2024 financial results, IR / delisting disclosures | source-review priority but latest visibility limited | Important substrate counter-evidence |
| Unimicron | Annual report, quarterly presentation, ABF/FC-BGA disclosures | pending source review | Confirm ABF substrate bottleneck exposure |
| Nan Ya PCB | Annual report, monthly sales, ABF/BT/CCL commentary | pending source review | Confirm substrate/material bottleneck exposure |
| Kinsus | Financial statement, monthly revenue, annual meeting materials | partial source clues | Confirm AI/ABF high-layer utilization and capacity |
| AT&S | Q3 FY2025/26 results, full-year outlook, glass core substrate release | source-review priority | Confirm T-glass / large-format complex substrate risk |
| BESI | Q1 2026 results, hybrid bonding / TCB disclosures | source-review priority | Confirm 2.5D AI computing and hybrid bonding order growth |
| SUSS | Q1 2026 results, 2026 investor presentation, TBDB / UV scanner / hybrid bonding disclosures | source-review priority | Confirm AI chip module value chain, CoWoS scanner, TBDB and hybrid bonding exposure |
| ASMPT | FY2025 results, Q1 2026 results, TCB product commentary | source-review priority | Confirm TCB, HBM4 12H/16H and fluxless TCB qualification |
| TOWA | FY2025 results, molding / compression / singulation product disclosure | source-review priority | Confirm AI-related logic / HBM molding equipment |
| DISCO | FY2025 Q4 results, shipment notes, FY2026 Q1 outlook | source-review priority | Confirm AI/OSAT demand for dicer/grinder/consumables |
| Camtek | Order releases, annual report / 20-F, advanced packaging inspection disclosures | source-review priority | Confirm CoWoS-like OSAT packaging, HBM, chiplet, hybrid bonding and micro-bump inspection demand |
| Nova | FY2025 results, 20-F, investor presentation | source-review priority | Confirm process-control intensity in advanced packaging, hybrid bonding, TSV/RDL and panel-level metrology |
| HBM vendors | Micron / Samsung / SK hynix HBM4/HBM4E launch, sampling, ramp and JEDEC HBM4 standard | source-review priority | Confirm HBM4/HBM4E package complexity and route |

### Already-Proven Research Claims After Source Review

- TSMC / CoWoS / advanced packaging remains an AI accelerator shipment
  bottleneck if official language continues to describe AI demand and advanced
  packaging capacity as tight.
- The bottleneck has spread from a single TSMC CoWoS capacity point into
  substrate, T-glass, TCB, hybrid bonding, temporary bonding/debonding, dicing,
  grinding, molding, inspection and metrology.
- Equipment-side bottleneck signals are strongest in TCB, hybrid bonding,
  temporary bonding/debonding, CoWoS lithography/scanner, molding, dicing,
  grinding, inspection and metrology.
- Test/inspection/metrology should not be ignored, but final test becoming the
  main bottleneck still requires more source evidence.

### Reasonable Inference

The correct statement is not "the bottleneck moved away from TSMC." It is:

> TSMC remains the primary gatekeeper, but the expansion of CoWoS, OSAT
> participation and HBM4/HBM4E complexity can create parallel bottlenecks in
> high-end substrate, T-glass/glass core, TCB/hybrid bonding, temporary bonding,
> dicing/grinding, molding, inspection/metrology and selected test capacity.

### Pending Source Review

Do not promote these as established facts yet:

- ASE as a confirmed incremental CoWoS / advanced packaging hard bottleneck.
- Unimicron, Nan Ya PCB and Kinsus as confirmed ABF substrate main bottlenecks.
- External interposer suppliers replacing TSMC as an independent bottleneck.
- Final test as an independent main bottleneck for AI accelerator shipments.

### Important Counter-Evidence

The strongest counter-evidence comes from any TSMC language that near-term
materials risk is mostly mitigated or that there is no near-term materials
impact. That conflicts with a claim that all materials are already the current
global bottleneck.

Shinko-style substrate counter-evidence also matters: AI demand can be strong
while certain flip-chip packages or server substrate categories remain weak.
That means substrate tightness may be concentrated in specific high-end body
size, layer-count, T-glass or large-format specs rather than all FC-BGA/ABF
capacity.

### CoWoS / 2.5D / Advanced Packaging Map

| Layer | Process / Material | Companies | Bottleneck Judgment |
|---|---|---|---|
| AI accelerator logic die | N3 / N2 / reticle-size die, HBM base die | TSMC | Still first-level bottleneck if AI demand and capacity remain tight |
| HBM stack | HBM3E -> HBM4 -> HBM4E, 12H/16H, higher I/O, thinner die | Micron, Samsung, SK hynix | Parallel bottleneck; HBM4/HBM4E route adds packaging complexity |
| Silicon interposer / RDL / TSV | CoWoS-S interposer, RDL, TIV, large interposer | TSMC, SUSS, Nova | TSMC-controlled 2.5D core still tight; external interposer bottleneck unproven |
| ABF / FC-BGA substrate | Large body, high layer count, fine circuit, T-glass, glass core | Ibiden, Shinko, Unimicron, Nan Ya PCB, Kinsus, AT&S | High-end substrate is a clear candidate bottleneck; tightness is not uniform |
| Assembly / OSAT | CoWoS assembly, HDFO, 2.5D integration, test | TSMC, ASE, Amkor | TSMC-led with OSAT expansion; ASE source review still required |
| TCB / hybrid bonding | Chip-to-substrate, chip-to-wafer, HBM stacking, logic/memory bonding | BESI, ASMPT, SUSS | Strong equipment bottleneck candidate |
| Temporary bonding / thinning | HBM DRAM thinning, wafer support, stacking prep | SUSS, DISCO | Higher HBM stack count should lift demand |
| Molding / compression / singulation | HBM / AI logic protection, warpage control, PLP | TOWA, ASMPT | Back-end packaging bottleneck candidate |
| Inspection / metrology | Micro-bump, hybrid bonding, TSV/RDL, panel-level, plating chemistry | Camtek, Nova | Hidden capacity/yield bottleneck candidate |

### HBM4 / HBM4E Effects On Equipment And Materials

| HBM4 / HBM4E Change | Packaging Impact | Affected Companies |
|---|---|---|
| Higher bandwidth, wider interface, higher I/O | Bump/interconnect density rises; RDL/TSV/interposer/substrate routing becomes harder | TSMC, SUSS, Nova, Camtek |
| 12H -> 16H, thinner die, higher stack count | Die thinning, temporary bonding/debonding, warpage control and stack yield become more important | SUSS, DISCO, ASMPT, TOWA |
| TCB remains important; hybrid bonding gradually enters HBM4E/HBM5 route | TCB and hybrid bonding coexist; hybrid bonding may gain mid-term elasticity | ASMPT, BESI, SUSS |
| Larger package / larger interposer / more HBM cubes | CoWoS size, substrate size, T-glass/glass core and thermal/mechanical stress become more important | TSMC, Ibiden, AT&S, Unimicron, Nan Ya PCB, Kinsus |
| Process-control intensity rises | Hybrid bonding, micro-bump, TSV/RDL, plating chemistry and panel-level metrology demand rises | Camtek, Nova |
| Thermal and warpage problems worsen | Underfill, mold compound, TIM, compression molding, flatness and CTE matching become yield variables | TOWA, ASMPT, AT&S, TSMC |

### Financial Verification Metrics

#### TSMC

Track:

- Whether advanced packaging capacity continues to be described as tight.
- CoWoS-S / CoWoS-L / SoIC / CoPoS or panel-level packaging ramp.
- HPC / AI accelerator revenue, AI customer pull-in and customer prepayment.
- Capex share for advanced packaging / specialty backend.
- N3 / N2 / HBM base die capacity tightness.
- Continued references to OSAT partners.

#### OSAT: ASE / Amkor

Track:

- Advanced packaging revenue mix.
- AI / HPC / datacenter computing program ramp.
- Packaging vs test revenue split.
- Capex guidance, equipment lead time, utilization.
- HDFO, 2.5D integration, CoWoS-like, LEAP and test capacity progress.

#### Substrate: Ibiden / Shinko / Unimicron / Nan Ya PCB / Kinsus / AT&S

Track:

- ABF / FC-BGA high-end substrate revenue mix.
- AI server / GPU / ASIC / CPU substrate shipments.
- Body size, layer count, fine circuit, embedded component and large-format specs.
- T-glass, ABF film, glass fabric, copper foil, resin availability.
- Capacity start-of-production, ramp yield and utilization.
- Substrate ASP and gross margin.
- Inventory and WIP.
- Customer specification upgrades that outpace material supply.

#### Equipment: BESI / SUSS / ASMPT / TOWA / DISCO

Track:

- Book-to-bill, order intake, backlog, shipment value.
- TCB, hybrid bonding, temporary bonding/debonding, UV projection scanner,
  molding, compression, dicing and grinding order mix.
- HBM3E vs HBM4 vs HBM4E project timing.
- Tool lead time, customer qualification, AOR/process-of-record.
- Gross margin impact from initial delivery cost and new-system ramp.
- Installed base, service and consumables revenue.

#### Inspection / Metrology: Camtek / Nova

Track:

- CoWoS-like / HBM / chiplet / hybrid bonding orders.
- Micro-bump inspection, wafer-level inspection, RDL/TSV/panel-level metrology.
- Chemical metrology for plating and advanced packaging.
- OSAT vs foundry vs IDM customer structure.
- Tool shipment timing and customer acceptance.

### CoWoS Disconfirmation Dashboard

Watch for:

- TSMC stops using tightness language and reports shorter CoWoS lead time.
- TSMC slows advanced packaging / OSAT partner expansion.
- AI accelerator customers stop complaining about CoWoS/HBM/packaging allocation
  and instead enter inventory correction.
- Amkor / ASE advanced packaging utilization, capex or backlog falls.
- Ibiden / AT&S / Kinsus / Unimicron / Nan Ya PCB high-end ABF substrate margin,
  ASP or utilization fails to improve.
- AT&S-style T-glass risk disappears because additional suppliers fully meet
  upgraded customer requirements.
- BESI / SUSS / ASMPT orders roll over, especially hybrid bonding, TCB, TBDB and
  CoWoS scanner orders.
- DISCO shipments / consumables stop being AI/OSAT utilization driven.
- Camtek / Nova advanced packaging / HBM / hybrid bonding orders slow.
- More Shinko-style counter-evidence appears: server or flip-chip package demand
  delays, inventory adjustment, or price pressure across substrate peers.

## 3. Final Research Conclusion

HBM should be upgraded from "2-3 year supply mismatch" to "AI accelerator
structural memory standard", but not to an unconditional no-cycle supercycle.

CoWoS / 2.5D / advanced packaging remains a bottleneck, but its shape has
evolved from "TSMC single-point CoWoS capacity" to "TSMC gatekeeper plus OSAT,
substrate, equipment, metrology and material parallel constraints."

The strongest source-review chain is:

```text
AI accelerator demand
-> TSMC advanced packaging tightness
-> large-format CoWoS remains main supply route
-> OSAT partners expand
-> HBM4/HBM4E raises I/O, stack height, die thinning, bonding and yield difficulty
-> high-end substrate / T-glass / TCB / hybrid bonding / TBDB / dicing / molding / inspection / metrology become expansion constraints
```

Avoid over-claiming:

- Near-term global material shortage is not proven.
- External interposer transfer is not proven.
- Final test as the main bottleneck is not yet proven.
- Many probe/socket/substrate/material names require HBM-specific source review
  before they can be called confirmed HBM beneficiaries.
