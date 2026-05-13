"""A-share tape-leadership sleeve and guardrails.

The sleeve treats news as a lagging label. Signal membership is decided only
from close-time tape: price leadership, volume expansion, money flow, and
sector/industry synchronization.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import duckdb


CN_TAPE_SLEEVE_ID = "cn_tape_leadership_continuation"


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return (
        con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema='main'
              AND table_name=?
            """,
            [table],
        ).fetchone()[0]
        > 0
    )


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    cur = con.execute(sql, params)
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]


def _round_or_none(value: Any, digits: int = 6) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:
        return None
    return round(parsed, digits)


def _as_iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)[:10]


def _fmt_num(value: Any, digits: int = 2) -> str:
    parsed = _round_or_none(value, digits)
    return "-" if parsed is None else f"{parsed:.{digits}f}"


def _fmt_pct(value: Any, digits: int = 2) -> str:
    parsed = _round_or_none(value, digits)
    return "-" if parsed is None else f"{parsed:+.{digits}f}%"

def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.max)
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        try:
            return datetime.combine(date.fromisoformat(text[:10]), time.max)
        except ValueError:
            return None


def is_causal_news(signal_close_ts: Any, news_publish_ts: Any) -> bool:
    signal_dt = _parse_dt(signal_close_ts)
    news_dt = _parse_dt(news_publish_ts)
    if signal_dt is None or news_dt is None:
        return False
    return news_dt <= signal_dt


def causal_news_score(news_items: list[dict[str, Any]], signal_close_ts: Any) -> float:
    """Score only news known by signal close.

    Positive lagging news is intentionally small; future news contributes zero.
    """
    score = 0.0
    for item in news_items:
        if not is_causal_news(signal_close_ts, item.get("published_at")):
            continue
        sentiment = str(item.get("sentiment") or "").lower()
        if sentiment in {"negative", "risk", "bearish"}:
            score -= 1.0
        elif sentiment in {"positive", "bullish"}:
            score += 0.1
    return score


def _has_cn_tape_tables(con: duckdb.DuckDBPyConnection) -> bool:
    required = {"prices", "stock_basic", "daily_basic", "moneyflow", "sector_fund_flow"}
    return all(table_exists(con, table) for table in required)


def _latest_cn_tape_trade_date(con: duckdb.DuckDBPyConnection, as_of: date) -> date | None:
    row = con.execute(
        """
        SELECT MAX(p.trade_date)
        FROM prices p
        JOIN daily_basic db ON db.ts_code = p.ts_code AND db.trade_date = p.trade_date
        LEFT JOIN moneyflow mf ON mf.ts_code = p.ts_code AND mf.trade_date = p.trade_date
        WHERE p.trade_date <= CAST(? AS DATE)
          AND p.close > 0
          AND p.amount > 0
        """,
        [as_of.isoformat()],
    ).fetchone()
    value = row[0] if row else None
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _cn_tape_feature_cte() -> str:
    return """
        WITH price_base AS (
            SELECT
                p.ts_code,
                p.trade_date,
                p.open,
                p.high,
                p.low,
                p.close,
                p.pre_close,
                p.pct_chg,
                p.amount,
                COALESCE(sb.name, '') AS name,
                COALESCE(sb.industry, '') AS industry,
                db.circ_mv,
                mf.net_mf_amount,
                (
                    COALESCE(mf.buy_lg_amount, 0) + COALESCE(mf.buy_elg_amount, 0)
                    - COALESCE(mf.sell_lg_amount, 0) - COALESCE(mf.sell_elg_amount, 0)
                ) AS large_net_amount,
                LAG(p.close, 5) OVER (PARTITION BY p.ts_code ORDER BY p.trade_date) AS close_5d_ago,
                LAG(p.close, 20) OVER (PARTITION BY p.ts_code ORDER BY p.trade_date) AS close_20d_ago,
                AVG(p.amount) OVER (
                    PARTITION BY p.ts_code
                    ORDER BY p.trade_date
                    ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS avg_amount_20
            FROM prices p
            LEFT JOIN stock_basic sb ON sb.ts_code = p.ts_code
            LEFT JOIN daily_basic db ON db.ts_code = p.ts_code AND db.trade_date = p.trade_date
            LEFT JOIN moneyflow mf ON mf.ts_code = p.ts_code AND mf.trade_date = p.trade_date
            WHERE p.trade_date >= CAST(? AS DATE)
              AND p.trade_date <= CAST(? AS DATE)
              AND p.close > 0
              AND p.amount > 0
        ),
        features AS (
            SELECT
                *,
                (close / NULLIF(close_5d_ago, 0) - 1.0) * 100.0 AS ret_5d_pct,
                (close / NULLIF(close_20d_ago, 0) - 1.0) * 100.0 AS ret_20d_pct,
                amount / NULLIF(avg_amount_20, 0) AS amount_ratio,
                net_mf_amount / NULLIF(amount, 0) AS flow_intensity,
                large_net_amount / NULLIF(amount, 0) AS large_flow_intensity
            FROM price_base
        ),
        narrative_features AS (
            SELECT
                *,
                CASE
                    WHEN industry IN (
                        '食品', '白酒', '乳制品', '红黄酒', '软饮料', '啤酒',
                        '家用电器', '家居用品', '服饰', '纺织', '日用化工',
                        '文教休闲', '百货', '超市连锁', '其他商业', '商贸代理',
                        '商品城', '电器连锁', '酒店餐饮', '旅游景点', '旅游服务',
                        '农业综合', '种植业', '饲料', '渔业', '林业'
                    )
                    THEN 'excluded_consumer'
                    WHEN industry IN (
                        '半导体', '元器件', '通信设备', 'IT设备',
                        '电气设备', '电器仪表', '新型电力', '火力发电', '水力发电', '供气供热',
                        '专用机械', '机床制造', '化工机械'
                    )
                    THEN 'ai_infra'
                    WHEN industry IN (
                        '煤炭开采', '石油开采', '石油加工', '石油贸易', '焦炭加工',
                        '小金属', '铜', '铝', '铅锌', '黄金',
                        '普钢', '特种钢', '钢加工', '矿物制品', '水泥', '玻璃',
                        '化工原料', '农药化肥',
                        '建筑工程', '工程机械',
                        '运输设备', '船舶', '航空', '港口', '水运', '铁路', '公路', '路桥'
                    )
                    THEN 'hard_assets_energy_heavy'
                    WHEN industry IN ('互联网', '软件服务')
                    THEN 'deprioritized_internet_software'
                    ELSE 'neutral'
                END AS narrative_group,
                CASE
                    WHEN industry IN (
                        '食品', '白酒', '乳制品', '红黄酒', '软饮料', '啤酒',
                        '家用电器', '家居用品', '服饰', '纺织', '日用化工',
                        '文教休闲', '百货', '超市连锁', '其他商业', '商贸代理',
                        '商品城', '电器连锁', '酒店餐饮', '旅游景点', '旅游服务',
                        '农业综合', '种植业', '饲料', '渔业', '林业'
                    )
                    THEN 'excluded_consumer'
                    WHEN industry = '通信设备'
                      OR (industry IN ('半导体', '元器件', 'IT设备') AND name LIKE '%光%')
                    THEN 'ai_networking_optical_cpo'
                    WHEN industry = '半导体'
                    THEN 'ai_chip_equipment_materials_packaging'
                    WHEN industry = 'IT设备'
                    THEN 'ai_datacenter_edge_infra'
                    WHEN industry = '元器件'
                    THEN 'ai_electronics_components'
                    WHEN industry IN ('电气设备', '电器仪表', '新型电力', '火力发电', '水力发电', '供气供热')
                    THEN 'ai_power_nuclear_grid'
                    WHEN industry IN ('专用机械', '机床制造', '化工机械')
                    THEN 'ai_industrial_capex'
                    WHEN industry IN (
                        '煤炭开采', '石油开采', '石油加工', '石油贸易', '焦炭加工',
                        '小金属', '铜', '铝', '铅锌', '黄金',
                        '普钢', '特种钢', '钢加工', '矿物制品', '水泥', '玻璃',
                        '化工原料', '农药化肥',
                        '建筑工程', '工程机械',
                        '运输设备', '船舶', '航空', '港口', '水运', '铁路', '公路', '路桥'
                    )
                    THEN 'hard_assets_energy_heavy'
                    WHEN industry IN ('互联网', '软件服务')
                    THEN 'deprioritized_internet_software'
                    ELSE 'neutral'
                END AS supercycle_layer,
                CASE
                    WHEN industry IN (
                        '食品', '白酒', '乳制品', '红黄酒', '软饮料', '啤酒',
                        '家用电器', '家居用品', '服饰', '纺织', '日用化工',
                        '文教休闲', '百货', '超市连锁', '其他商业', '商贸代理',
                        '商品城', '电器连锁', '酒店餐饮', '旅游景点', '旅游服务',
                        '农业综合', '种植业', '饲料', '渔业', '林业'
                    )
                    THEN 9
                    WHEN industry IN ('半导体', '元器件', '通信设备', 'IT设备')
                    THEN 1
                    WHEN industry IN ('电气设备', '电器仪表', '新型电力', '火力发电', '水力发电', '供气供热')
                    THEN 2
                    WHEN industry IN ('专用机械', '机床制造', '化工机械')
                    THEN 2
                    WHEN industry IN (
                        '煤炭开采', '石油开采', '石油加工', '石油贸易', '焦炭加工',
                        '小金属', '铜', '铝', '铅锌', '黄金',
                        '普钢', '特种钢', '钢加工', '矿物制品', '水泥', '玻璃',
                        '化工原料', '农药化肥',
                        '建筑工程', '工程机械',
                        '运输设备', '船舶', '航空', '港口', '水运', '铁路', '公路', '路桥'
                    )
                    THEN 3
                    WHEN industry IN ('互联网', '软件服务')
                    THEN 6
                    ELSE 5
                END AS supercycle_priority,
                CASE
                    WHEN industry = '通信设备'
                      OR (industry IN ('半导体', '元器件', 'IT设备') AND name LIKE '%光%')
                    THEN 'CPO/optical/fiber/datacenter communications candidate'
                    WHEN industry = '半导体'
                    THEN 'AI chip, equipment, packaging/test or semiconductor material candidate'
                    WHEN industry = 'IT设备'
                    THEN 'AI server/storage/network appliance candidate'
                    WHEN industry = '元器件'
                    THEN 'electronic component supplier candidate'
                    WHEN industry IN ('电气设备', '电器仪表', '新型电力', '火力发电', '水力发电', '供气供热')
                    THEN 'power/grid/electrification capacity candidate'
                    WHEN industry IN ('专用机械', '机床制造', '化工机械')
                    THEN 'industrial equipment and automation capacity candidate'
                    WHEN industry IN (
                        '煤炭开采', '石油开采', '石油加工', '石油贸易', '焦炭加工',
                        '小金属', '铜', '铝', '铅锌', '黄金',
                        '普钢', '特种钢', '钢加工', '矿物制品', '水泥', '玻璃',
                        '化工原料', '农药化肥',
                        '建筑工程', '工程机械',
                        '运输设备', '船舶', '航空', '港口', '水运', '铁路', '公路', '路桥'
                    )
                    THEN 'upstream material, energy or heavy-industry input candidate'
                    WHEN industry IN ('互联网', '软件服务')
                    THEN 'software/internet name without clear AI-infra bottleneck'
                    WHEN industry IN (
                        '食品', '白酒', '乳制品', '红黄酒', '软饮料', '啤酒',
                        '家用电器', '家居用品', '服饰', '纺织', '日用化工',
                        '文教休闲', '百货', '超市连锁', '其他商业', '商贸代理',
                        '商品城', '电器连锁', '酒店餐饮', '旅游景点', '旅游服务',
                        '农业综合', '种植业', '饲料', '渔业', '林业'
                    )
                    THEN 'excluded_daily_consumption'
                    ELSE 'not yet mapped to AI supercycle supply chain'
                END AS supply_chain_role,
                CASE
                    WHEN industry = '通信设备'
                      OR (industry IN ('半导体', '元器件', 'IT设备') AND name LIKE '%光%')
                    THEN 'optical bandwidth and AI datacenter interconnect'
                    WHEN industry = '半导体'
                    THEN 'chip supply chain capacity and advanced packaging/materials'
                    WHEN industry = 'IT设备'
                    THEN 'datacenter hardware and edge inference infrastructure'
                    WHEN industry = '元器件'
                    THEN 'AI hardware component availability'
                    WHEN industry IN ('电气设备', '电器仪表', '新型电力', '火力发电', '水力发电', '供气供热')
                    THEN 'firm power, grid equipment and AI datacenter electricity scarcity'
                    WHEN industry IN ('专用机械', '机床制造', '化工机械')
                    THEN 'AI supply-chain capex, precision equipment and manufacturing throughput'
                    WHEN industry IN (
                        '煤炭开采', '石油开采', '石油加工', '石油贸易', '焦炭加工',
                        '小金属', '铜', '铝', '铅锌', '黄金',
                        '普钢', '特种钢', '钢加工', '矿物制品', '水泥', '玻璃',
                        '化工原料', '农药化肥',
                        '建筑工程', '工程机械',
                        '运输设备', '船舶', '航空', '港口', '水运', '铁路', '公路', '路桥'
                    )
                    THEN 'resource/input cost and heavy-capex scarcity'
                    WHEN industry IN ('互联网', '软件服务')
                    THEN 'requires direct AI-lab/cloud/product evidence before priority upgrade'
                    ELSE 'no confirmed bottleneck tag'
                END AS bottleneck_focus,
                CASE
                    WHEN industry IN ('半导体', '元器件', '通信设备', 'IT设备', '电气设备', '电器仪表')
                    THEN 2.30
                    WHEN industry IN ('新型电力', '火力发电', '水力发电', '供气供热', '专用机械', '机床制造', '化工机械')
                    THEN 2.05
                    WHEN industry IN (
                        '煤炭开采', '石油开采', '石油加工', '石油贸易', '焦炭加工',
                        '小金属', '铜', '铝', '铅锌', '黄金',
                        '普钢', '特种钢', '钢加工', '矿物制品', '水泥', '玻璃',
                        '化工原料', '农药化肥',
                        '建筑工程', '工程机械',
                        '运输设备', '船舶', '航空', '港口', '水运', '铁路', '公路', '路桥'
                    )
                    THEN 1.80
                    WHEN industry IN ('互联网', '软件服务')
                    THEN -1.20
                    ELSE 0.0
                END AS narrative_score_adjust,
                CASE
                    WHEN industry = '通信设备'
                      OR (industry IN ('半导体', '元器件', 'IT设备') AND name LIKE '%光%')
                    THEN 'AI infra: CPO/optical/fiber/datacenter networking'
                    WHEN industry = '半导体'
                    THEN 'AI infra: chips/equipment/packaging/test/materials'
                    WHEN industry = 'IT设备'
                    THEN 'AI infra: servers/storage/network appliances'
                    WHEN industry = '元器件'
                    THEN 'AI infra: electronics components'
                    WHEN industry IN ('电气设备', '电器仪表', '新型电力', '火力发电', '水力发电', '供气供热')
                    THEN 'AI infra: firm power/grid/electrification'
                    WHEN industry IN ('专用机械', '机床制造', '化工机械')
                    THEN 'AI infra: industrial capex/equipment'
                    WHEN industry IN (
                        '煤炭开采', '石油开采', '石油加工', '石油贸易', '焦炭加工',
                        '小金属', '铜', '铝', '铅锌', '黄金',
                        '普钢', '特种钢', '钢加工', '矿物制品', '水泥', '玻璃',
                        '化工原料', '农药化肥',
                        '建筑工程', '工程机械',
                        '运输设备', '船舶', '航空', '港口', '水运', '铁路', '公路', '路桥'
                    )
                    THEN 'hard assets / energy / heavy industry'
                    WHEN industry IN ('互联网', '软件服务')
                    THEN 'internet/software deprioritized'
                    ELSE 'neutral narrative'
                END AS narrative_reason
            FROM features
        ),
        industry_day AS (
            SELECT
                trade_date,
                industry,
                COUNT(*) AS industry_n,
                AVG(pct_chg) AS industry_avg_pct,
                AVG(CASE WHEN pct_chg > 0 THEN 1.0 ELSE 0.0 END) AS industry_breadth
            FROM narrative_features
            WHERE industry <> ''
            GROUP BY trade_date, industry
        ),
        scored_signals AS (
            SELECT
                f.*,
                i.industry_n,
                i.industry_avg_pct,
                i.industry_breadth,
                sff.main_net_in AS sector_main_net_in,
                sff.main_net_pct AS sector_main_net_pct,
                CASE
                    WHEN sff.sector_name IS NOT NULL
                     AND (COALESCE(sff.main_net_in, 0) > 0 OR COALESCE(sff.main_net_pct, 0) > 0)
                    THEN 'full_confirm'
                    ELSE 'proxy_confirm'
                END AS confirm_quality,
                (
                    COALESCE(f.ret_5d_pct, 0) * 0.35
                    + COALESCE(f.pct_chg, 0) * 0.20
                    + COALESCE(f.amount_ratio, 0) * 1.60
                    + COALESCE(f.flow_intensity, 0) * 25.0
                    + COALESCE(i.industry_avg_pct, 0) * 0.20
                    + COALESCE(i.industry_breadth, 0) * 3.0
                    + COALESCE(f.narrative_score_adjust, 0)
                ) AS tape_score
            FROM narrative_features f
            LEFT JOIN industry_day i ON i.trade_date = f.trade_date AND i.industry = f.industry
            LEFT JOIN sector_fund_flow sff ON sff.trade_date = f.trade_date AND sff.sector_name = f.industry
            WHERE f.trade_date >= CAST(? AS DATE)
              AND f.trade_date <= CAST(? AS DATE)
              AND COALESCE(f.name, '') NOT LIKE '%ST%'
              AND f.narrative_group <> 'excluded_consumer'
              AND COALESCE(f.ret_5d_pct, -999) BETWEEN 6.0 AND 22.0
              AND COALESCE(f.ret_20d_pct, -999) <= 50.0
              AND COALESCE(f.pct_chg, -999) BETWEEN 1.5 AND 8.8
              AND COALESCE(f.amount_ratio, 0) BETWEEN 1.25 AND 6.0
              AND COALESCE(f.net_mf_amount, 0) > 0
              AND COALESCE(f.large_net_amount, 0) >= 0
              AND (
                    COALESCE(i.industry_avg_pct, -999) >= 0.60
                 OR COALESCE(i.industry_breadth, 0) >= 0.56
                 OR COALESCE(sff.main_net_in, 0) > 0
              )
        ),
        signals AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY tape_score DESC, ts_code) AS daily_rank
                FROM scored_signals
            )
            WHERE daily_rank <= 15
        )
    """


def query_cn_tape_leadership_returns(cn_db: Path, start: date, as_of: date) -> tuple[list[dict[str, Any]], str]:
    if not cn_db.exists():
        return [], "missing"
    con = duckdb.connect(str(cn_db), read_only=True)
    try:
        if not _has_cn_tape_tables(con):
            return [], "missing prices/stock_basic/moneyflow"
        start_pad = start - timedelta(days=80)
        rows = rows_as_dicts(
            con,
            _cn_tape_feature_cte()
            + """
            , joined AS (
                SELECT
                    s.*,
                    p.trade_date AS price_date,
                    p.close AS future_close,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.ts_code, s.trade_date
                        ORDER BY p.trade_date
                    ) AS rn
                FROM signals s
                JOIN prices p
                  ON p.ts_code = s.ts_code
                 AND p.trade_date > s.trade_date
                 AND p.close > 0
            ),
            entry AS (
                SELECT
                    ts_code, trade_date, name, industry, confirm_quality, tape_score,
                    ret_5d_pct, ret_20d_pct, pct_chg, amount_ratio,
                    flow_intensity, large_flow_intensity, industry_avg_pct,
                    industry_breadth, sector_main_net_in, narrative_group,
                    supercycle_layer, supercycle_priority, supply_chain_role,
                    bottleneck_focus, narrative_score_adjust, narrative_reason,
                    price_date AS entry_date,
                    future_close AS entry_close
                FROM joined
                WHERE rn = 1
            ),
            exit AS (
                SELECT ts_code, trade_date, price_date AS exit_date, future_close AS exit_close
                FROM joined
                WHERE rn = 6
            )
            SELECT
                e.trade_date AS report_date,
                e.ts_code AS symbol,
                e.name,
                e.industry,
                e.confirm_quality,
                e.tape_score,
                e.ret_5d_pct,
                e.ret_20d_pct,
                e.pct_chg,
                e.amount_ratio,
                e.flow_intensity,
                e.large_flow_intensity,
                e.industry_avg_pct,
                e.industry_breadth,
                e.sector_main_net_in,
                e.narrative_group,
                e.supercycle_layer,
                e.supercycle_priority,
                e.supply_chain_role,
                e.bottleneck_focus,
                e.narrative_score_adjust,
                e.narrative_reason,
                e.entry_date,
                x.exit_date,
                (x.exit_close / NULLIF(e.entry_close, 0) - 1.0) * 100.0 AS return_pct
            FROM entry e
            JOIN exit x ON x.ts_code = e.ts_code AND x.trade_date = e.trade_date
            ORDER BY e.trade_date, e.tape_score DESC, e.ts_code
            """,
            [start_pad.isoformat(), as_of.isoformat(), start.isoformat(), as_of.isoformat()],
        )
        full = sum(1 for row in rows if row.get("confirm_quality") == "full_confirm")
        proxy = sum(1 for row in rows if row.get("confirm_quality") == "proxy_confirm")
        return rows, f"ok full_confirm={full} proxy_confirm={proxy}"
    finally:
        con.close()


def query_cn_tape_current_candidates(cn_db: Path, as_of: date, *, top: int = 20) -> list[dict[str, Any]]:
    if not cn_db.exists():
        return []
    con = duckdb.connect(str(cn_db), read_only=True)
    try:
        if not _has_cn_tape_tables(con):
            return []
        effective_date = _latest_cn_tape_trade_date(con, as_of)
        if effective_date is None:
            return []
        start_pad = effective_date - timedelta(days=80)
        rows = rows_as_dicts(
            con,
            _cn_tape_feature_cte()
            + """
            SELECT
                trade_date AS report_date,
                ts_code AS symbol,
                name,
                industry,
                close,
                confirm_quality,
                tape_score,
                ret_5d_pct,
                ret_20d_pct,
                pct_chg,
                amount_ratio,
                flow_intensity,
                large_flow_intensity,
                industry_avg_pct,
                industry_breadth,
                sector_main_net_in,
                narrative_group,
                supercycle_layer,
                supercycle_priority,
                supply_chain_role,
                bottleneck_focus,
                narrative_score_adjust,
                narrative_reason
            FROM signals
            WHERE trade_date = CAST(? AS DATE)
            ORDER BY tape_score DESC, ts_code
            LIMIT ?
            """,
            [
                start_pad.isoformat(),
                effective_date.isoformat(),
                effective_date.isoformat(),
                effective_date.isoformat(),
                effective_date.isoformat(),
                top,
            ],
        )
        candidates: list[dict[str, Any]] = []
        for row in rows:
            close = _round_or_none(row.get("close")) or 0.0
            stop = close * 0.94 if close > 0 else None
            target = close * 1.10 if close > 0 else None
            candidates.append(
                {
                    "market": "cn",
                    "as_of": _as_iso(row.get("report_date")),
                    "symbol": row.get("symbol"),
                    "name": row.get("name") or "-",
                    "industry": row.get("industry") or "",
                    "state": "Execution Alpha",
                    "policy": "tape_leadership_continuation",
                    "action_intent": "TRADE",
                    "alpha_sleeve_id": CN_TAPE_SLEEVE_ID,
                    "alpha_factory_role": "execution_sleeve",
                    "execution_source": "alpha_factory_sleeve",
                    "observation_entry_zone": f"{_fmt_num(close)}-{_fmt_num(close * 1.015 if close else None)}",
                    "handling_line": _round_or_none(stop, 4),
                    "first_target": _round_or_none(target, 4),
                    "risk_unit_pct": 6.0,
                    "ev_pct": None,
                    "ev_lcb80_pct": None,
                    "time_exit": "T+1 review; T+5 hard exit unless trend extends",
                    "lifecycle_action": "buy_planned_entry_if_price_holds_leadership",
                    "execution_mode": "tape_leadership_no_chase_above_zone",
                    "alpha_state": "tape_leadership_continuation",
                    "gate_summary": (
                        f"price_first; confirm={row.get('confirm_quality')}; "
                        f"narrative={row.get('narrative_group')}/{row.get('supercycle_layer')}; "
                        f"ret5={_fmt_pct(row.get('ret_5d_pct'))}; "
                        f"amount_ratio={_fmt_num(row.get('amount_ratio'), 2)}; "
                        f"flow={_fmt_num(row.get('flow_intensity'), 3)}"
                    ),
                    "reason": (
                        "price/volume/flow/industry tape leadership sleeve; "
                        f"{row.get('narrative_reason') or 'neutral narrative'}; "
                        "news is lagging risk label only"
                    ),
                    "features_json": "{}",
                    "detail_json": "{}",
                    "tape_score": _round_or_none(row.get("tape_score"), 4),
                    "confirm_quality": row.get("confirm_quality"),
                    "narrative_group": row.get("narrative_group"),
                    "supercycle_layer": row.get("supercycle_layer"),
                    "supercycle_priority": row.get("supercycle_priority"),
                    "supply_chain_role": row.get("supply_chain_role"),
                    "bottleneck_focus": row.get("bottleneck_focus"),
                    "narrative_score_adjust": _round_or_none(row.get("narrative_score_adjust"), 4),
                    "narrative_reason": row.get("narrative_reason"),
                }
            )
        return candidates
    finally:
        con.close()


def query_cn_sector_narrative_screen(cn_db: Path, as_of: date, *, top: int = 20) -> list[dict[str, Any]]:
    """Rank A-share sectors before selecting names.

    This is intentionally price/volume/flow first. Consumer sectors are excluded
    from the tradable sector board for the current mandate.
    """
    if not cn_db.exists():
        return []
    con = duckdb.connect(str(cn_db), read_only=True)
    try:
        if not _has_cn_tape_tables(con):
            return []
        effective_date = _latest_cn_tape_trade_date(con, as_of)
        if effective_date is None:
            return []
        start_pad = effective_date - timedelta(days=80)
        return rows_as_dicts(
            con,
            _cn_tape_feature_cte()
            + """
            , sector_board AS (
                SELECT
                    trade_date AS as_of,
                    industry,
                    narrative_group,
                    MIN(supercycle_layer) AS supercycle_layer,
                    MIN(supercycle_priority) AS supercycle_priority,
                    narrative_reason,
                    COUNT(*) AS names,
                    AVG(pct_chg) AS sector_pct_chg,
                    AVG(ret_5d_pct) AS sector_ret_5d_pct,
                    AVG(ret_20d_pct) AS sector_ret_20d_pct,
                    AVG(amount_ratio) AS avg_amount_ratio,
                    AVG(flow_intensity) AS avg_flow_intensity,
                    AVG(large_flow_intensity) AS avg_large_flow_intensity,
                    AVG(CASE WHEN pct_chg > 0 THEN 1.0 ELSE 0.0 END) AS breadth,
                    SUM(CASE
                        WHEN ret_5d_pct >= 6.0
                         AND pct_chg >= 1.5
                         AND amount_ratio >= 1.25
                         AND COALESCE(net_mf_amount, 0) > 0
                        THEN 1 ELSE 0 END
                    ) AS leader_count,
                    MAX(COALESCE(sector_main_net_in, 0)) AS sector_main_net_in,
                    MAX(COALESCE(sector_main_net_pct, 0)) AS sector_main_net_pct,
                    MAX(COALESCE(narrative_score_adjust, 0)) AS narrative_score_adjust,
                    (
                        COALESCE(AVG(ret_5d_pct), 0) * 0.28
                        + COALESCE(AVG(pct_chg), 0) * 0.24
                        + COALESCE(AVG(amount_ratio), 0) * 1.35
                        + COALESCE(AVG(flow_intensity), 0) * 22.0
                        + COALESCE(AVG(CASE WHEN pct_chg > 0 THEN 1.0 ELSE 0.0 END), 0) * 4.0
                        + COALESCE(SUM(CASE
                            WHEN ret_5d_pct >= 6.0
                             AND pct_chg >= 1.5
                             AND amount_ratio >= 1.25
                             AND COALESCE(net_mf_amount, 0) > 0
                            THEN 1 ELSE 0 END), 0) * 0.18
                        + MAX(COALESCE(narrative_score_adjust, 0))
                    ) AS sector_score
                FROM signals
                WHERE trade_date = CAST(? AS DATE)
                  AND narrative_group <> 'excluded_consumer'
                GROUP BY trade_date, industry, narrative_group, narrative_reason
            )
            SELECT *
            FROM sector_board
            WHERE leader_count > 0
               OR sector_pct_chg >= 0.60
               OR breadth >= 0.56
               OR sector_main_net_in > 0
            ORDER BY sector_score DESC, leader_count DESC, industry
            LIMIT ?
            """,
            [
                start_pad.isoformat(),
                effective_date.isoformat(),
                effective_date.isoformat(),
                effective_date.isoformat(),
                effective_date.isoformat(),
                top,
            ],
        )
    finally:
        con.close()
