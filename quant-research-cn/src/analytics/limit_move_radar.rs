use anyhow::Result;
use chrono::{Duration, FixedOffset, NaiveDate, Utc};
use duckdb::Connection;
use serde_json::json;
use std::collections::HashMap;
use tracing::info;

use crate::analytics::rv::{infer_censor_side, price_limit_pct, CensorSide};

const MODULE: &str = "limit_move_radar";

#[derive(Debug, Clone)]
struct RawRow {
    ts_code: String,
    latest_trade_date: String,
    name: String,
    industry: String,
    close: f64,
    high: f64,
    low: f64,
    ret_1d: f64,
    ret_5d: f64,
    ret_20d: f64,
    volume_ratio: f64,
    turnover_rate: f64,
    total_mv: f64,
    circ_mv: f64,
    amount: f64,
    net_mf_amount: f64,
    net_mf_amount_raw: f64,
    elg_net_amount: f64,
    flow_conflict_flag: bool,
    information_score: f64,
    setup_score: f64,
    continuation_score: f64,
    fade_risk: f64,
    std20_ret: f64,
}

#[derive(Debug, Clone)]
struct IndustryHeat {
    avg_ret_1d: f64,
    hot_ratio: f64,
    count: usize,
}

#[derive(Debug, Clone)]
struct LimitMoveFeatures {
    ret_1d: f64,
    ret_5d: f64,
    ret_20d: f64,
    volume_ratio: f64,
    turnover_rate: f64,
    total_mv: f64,
    net_mf_amount: f64,
    flow_conflict_flag: bool,
    elg_net_amount: f64,
    information_score: f64,
    setup_score: f64,
    continuation_score: f64,
    fade_risk: f64,
    industry_heat: f64,
    industry_weakness: f64,
    is_limit_up_today: bool,
    is_limit_down_today: bool,
}

#[derive(Debug, Clone)]
struct NextDayLabelRow {
    ts_code: String,
    evaluation_date: String,
    name: String,
    pct_chg: f64,
    high: f64,
    low: f64,
    close: f64,
}

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    let date_str = as_of.to_string();
    let rows = load_rows(db, as_of)?;
    if rows.is_empty() {
        return Ok(0);
    }

    let industry_heat = compute_industry_heat(&rows);
    let next_day_labels = load_next_day_labels(db, as_of)?;

    db.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS limit_move_radar_stage (
            ts_code VARCHAR,
            as_of VARCHAR,
            module VARCHAR,
            metric VARCHAR,
            value DOUBLE,
            detail VARCHAR
        );
        DELETE FROM limit_move_radar_stage;",
    )?;

    let mut written = 0usize;
    {
        let mut appender = db.appender("limit_move_radar_stage")?;
        for row in rows {
            let heat = industry_heat
                .get(&row.industry)
                .cloned()
                .unwrap_or(IndustryHeat {
                    avg_ret_1d: 0.0,
                    hot_ratio: 0.0,
                    count: 0,
                });
            let board_limit = price_limit_pct(&row.ts_code, &row.name);
            let censor = infer_censor_side(
                &row.ts_code,
                &row.name,
                row.ret_1d,
                row.high,
                row.low,
                row.close,
            );
            let industry_heat_score = industry_heat_score(&heat);
            let industry_weakness_score = industry_weakness_score(&heat);
            let features = LimitMoveFeatures {
                ret_1d: row.ret_1d,
                ret_5d: row.ret_5d,
                ret_20d: row.ret_20d,
                volume_ratio: row.volume_ratio,
                turnover_rate: row.turnover_rate,
                total_mv: row.total_mv,
                net_mf_amount: row.net_mf_amount,
                flow_conflict_flag: row.flow_conflict_flag,
                elg_net_amount: row.elg_net_amount,
                information_score: row.information_score,
                setup_score: row.setup_score,
                continuation_score: row.continuation_score,
                fade_risk: row.fade_risk,
                industry_heat: industry_heat_score,
                industry_weakness: industry_weakness_score,
                is_limit_up_today: censor == CensorSide::Right,
                is_limit_down_today: censor == CensorSide::Left,
            };
            let up_score = score_limit_up_candidate(&features);
            let down_score = score_limit_down_risk(&features);
            let up_labels = limit_up_labels(&row, &features, board_limit);
            let down_labels = limit_down_labels(&row, &features, board_limit);

            let detail = json!({
                "data_scope": "as_of_or_earlier_only",
                "latest_trade_date": row.latest_trade_date,
                "name": row.name,
                "industry": row.industry,
                "board_scope": board_scope(&row.ts_code, &row.name),
                "price_limit_pct": round2(board_limit),
                "ret_1d": round2(row.ret_1d),
                "ret_5d": round2(row.ret_5d),
                "ret_20d": round2(row.ret_20d),
                "volume_ratio": round3(row.volume_ratio),
                "turnover_rate": round2(row.turnover_rate),
                "total_mv": round2(row.total_mv),
                "circ_mv": round2(row.circ_mv),
                "amount": round2(row.amount),
                "large_plus_extra_net": round2(row.net_mf_amount),
                "net_mf_amount": round2(row.net_mf_amount),
                "net_mf_amount_raw": round2(row.net_mf_amount_raw),
                "elg_net_amount": round2(row.elg_net_amount),
                "flow_conflict_flag": row.flow_conflict_flag,
                "information_score": round3(row.information_score),
                "setup_score": round3(row.setup_score),
                "continuation_score": round3(row.continuation_score),
                "fade_risk": round3(row.fade_risk),
                "std20_ret": round3(row.std20_ret),
                "industry_avg_ret_1d": round2(heat.avg_ret_1d),
                "industry_hot_ratio": round3(heat.hot_ratio),
                "industry_count": heat.count,
                "industry_heat_score": round3(industry_heat_score),
                "industry_weakness_score": round3(industry_weakness_score),
                "is_limit_up_today": features.is_limit_up_today,
                "is_limit_down_today": features.is_limit_down_today,
                "limit_up_radar_score": round3(up_score),
                "limit_down_risk_score": round3(down_score),
                "limit_up_labels": up_labels,
                "limit_down_labels": down_labels,
                "confirmation_required": "opening_auction_gap_volume_sector_breadth_and_board_seal",
            })
            .to_string();

            for (metric, value) in [
                ("limit_up_radar_score", up_score),
                ("limit_down_risk_score", down_score),
            ] {
                appender.append_row(duckdb::params![
                    &row.ts_code,
                    &date_str,
                    MODULE,
                    metric,
                    value,
                    &detail
                ])?;
            }
            written += 1;
        }
        for row in next_day_labels {
            append_next_day_label(&mut appender, &date_str, &row)?;
        }
    }

    db.execute(
        "DELETE FROM analytics WHERE as_of = ? AND module = ?",
        duckdb::params![&date_str, MODULE],
    )?;
    db.execute_batch(
        "INSERT INTO analytics (ts_code, as_of, module, metric, value, detail)
         SELECT ts_code, CAST(as_of AS DATE), module, metric, value, detail
         FROM limit_move_radar_stage",
    )?;

    info!(symbols = written, "limit_move_radar complete");
    Ok(written)
}

fn append_next_day_label(
    appender: &mut duckdb::Appender<'_>,
    date_str: &str,
    row: &NextDayLabelRow,
) -> Result<()> {
    let censor = infer_censor_side(
        &row.ts_code,
        &row.name,
        row.pct_chg,
        row.high,
        row.low,
        row.close,
    );
    let detail = json!({
        "data_scope": "post_close_label_only_not_for_signal_generation",
        "evaluation_date": row.evaluation_date,
        "name": row.name,
        "price_limit_pct": round2(price_limit_pct(&row.ts_code, &row.name)),
        "next_day_ret_pct": round2(row.pct_chg),
        "next_day_limit_up": censor == CensorSide::Right,
        "next_day_limit_down": censor == CensorSide::Left,
    })
    .to_string();

    for (metric, value) in [
        (
            "next_day_limit_up_label",
            if censor == CensorSide::Right {
                1.0
            } else {
                0.0
            },
        ),
        (
            "next_day_limit_down_label",
            if censor == CensorSide::Left { 1.0 } else { 0.0 },
        ),
        ("next_day_ret_pct", row.pct_chg),
    ] {
        appender.append_row(duckdb::params![
            &row.ts_code,
            date_str,
            MODULE,
            metric,
            value,
            &detail
        ])?;
    }
    Ok(())
}

fn load_rows(db: &Connection, as_of: NaiveDate) -> Result<Vec<RawRow>> {
    let date_str = as_of.to_string();
    let lower_bound = (as_of - Duration::days(90)).to_string();
    let sql = "
        WITH calendar AS (
            SELECT trade_date,
                   ROW_NUMBER() OVER (ORDER BY trade_date DESC) AS rn
            FROM (
                SELECT DISTINCT trade_date
                FROM prices
                WHERE trade_date <= CAST(? AS DATE)
                  AND trade_date >= CAST(? AS DATE)
            )
        ),
        marks AS (
            SELECT
                MAX(CASE WHEN rn = 1 THEN trade_date END) AS d0,
                MAX(CASE WHEN rn = 6 THEN trade_date END) AS d5,
                MAX(CASE WHEN rn = 21 THEN trade_date END) AS d20
            FROM calendar
        ),
        recent AS (
            SELECT
                p.ts_code,
                AVG(p.vol) AS avg_vol_base,
                STDDEV_POP(p.pct_chg) AS std20_ret
            FROM prices p
            JOIN calendar c
              ON c.trade_date = p.trade_date
             AND c.rn BETWEEN 2 AND 21
            GROUP BY p.ts_code
        ),
        an AS (
            SELECT
                ts_code,
                MAX(CASE WHEN module = 'flow' AND metric = 'information_score' THEN value END) AS information_score,
                MAX(CASE WHEN module = 'flow_audit' AND metric = 'large_plus_extra_net' THEN value END) AS large_plus_extra_net,
                MAX(CASE WHEN module = 'flow_audit' AND metric = 'net_mf_amount_raw' THEN value END) AS net_mf_amount_raw,
                MAX(CASE WHEN module = 'flow_audit' AND metric = 'flow_conflict_flag' THEN value END) AS flow_conflict_flag,
                MAX(CASE WHEN module = 'setup_alpha' AND metric = 'setup_score' THEN value END) AS setup_score,
                MAX(CASE WHEN module = 'continuation_vs_fade' AND metric = 'continuation_score' THEN value END) AS continuation_score,
                MAX(CASE WHEN module = 'continuation_vs_fade' AND metric = 'fade_risk' THEN value END) AS fade_risk
            FROM analytics
            WHERE as_of = CAST(? AS DATE)
              AND (
                  (module = 'flow' AND metric = 'information_score')
               OR (module = 'flow_audit' AND metric IN ('large_plus_extra_net', 'net_mf_amount_raw', 'flow_conflict_flag'))
               OR (module = 'setup_alpha' AND metric = 'setup_score')
               OR (module = 'continuation_vs_fade' AND metric IN ('continuation_score', 'fade_risk'))
              )
            GROUP BY ts_code
        )
        SELECT
            p0.ts_code,
            CAST(m.d0 AS VARCHAR) AS latest_trade_date,
            COALESCE(sb.name, '') AS name,
            COALESCE(sb.industry, '') AS industry,
            COALESCE(p0.close, 0) AS close_now,
            COALESCE(p0.high, 0) AS high_now,
            COALESCE(p0.low, 0) AS low_now,
            COALESCE(p0.pct_chg, 0) AS ret_1d,
            COALESCE(p0.vol, 0) AS vol_now,
            COALESCE(p0.amount, 0) AS amount_now,
            COALESCE(p5.close, 0) AS close_5d_ago,
            COALESCE(p20.close, 0) AS close_20d_ago,
            COALESCE(recent.avg_vol_base, 0) AS avg_vol_base,
            COALESCE(recent.std20_ret, 0) AS std20_ret,
            COALESCE(dbasic.turnover_rate, 0) AS turnover_rate,
            COALESCE(dbasic.volume_ratio, 0) AS daily_volume_ratio,
            COALESCE(dbasic.total_mv, 0) AS total_mv,
            COALESCE(dbasic.circ_mv, 0) AS circ_mv,
            COALESCE(
                an.large_plus_extra_net,
                COALESCE(mf.buy_lg_amount, 0) - COALESCE(mf.sell_lg_amount, 0)
              + COALESCE(mf.buy_elg_amount, 0) - COALESCE(mf.sell_elg_amount, 0),
                0
            ) AS large_plus_extra_net,
            COALESCE(an.net_mf_amount_raw, mf.net_mf_amount, 0) AS net_mf_amount_raw,
            COALESCE(mf.buy_elg_amount, 0) AS buy_elg_amount,
            COALESCE(mf.sell_elg_amount, 0) AS sell_elg_amount,
            COALESCE(an.flow_conflict_flag, 0) AS flow_conflict_flag,
            COALESCE(an.information_score, 0) AS information_score,
            COALESCE(an.setup_score, 0) AS setup_score,
            COALESCE(an.continuation_score, 0) AS continuation_score,
            COALESCE(an.fade_risk, 0) AS fade_risk
        FROM marks m
        JOIN prices p0
          ON p0.trade_date = m.d0
        LEFT JOIN prices p5
          ON p5.ts_code = p0.ts_code
         AND p5.trade_date = m.d5
        LEFT JOIN prices p20
          ON p20.ts_code = p0.ts_code
         AND p20.trade_date = m.d20
        LEFT JOIN recent
          ON recent.ts_code = p0.ts_code
        LEFT JOIN stock_basic sb
          ON sb.ts_code = p0.ts_code
        LEFT JOIN daily_basic dbasic
          ON dbasic.ts_code = p0.ts_code
         AND dbasic.trade_date = m.d0
        LEFT JOIN moneyflow mf
          ON mf.ts_code = p0.ts_code
         AND mf.trade_date = m.d0
        LEFT JOIN an
          ON an.ts_code = p0.ts_code
        WHERE p0.close > 0
    ";

    let mut stmt = db.prepare(sql)?;
    let rows = stmt.query_map(duckdb::params![&date_str, &lower_bound, &date_str], |row| {
        let ts_code = row.get::<_, String>(0)?;
        let close = row.get::<_, f64>(4).unwrap_or(0.0);
        let close_5d_ago = row.get::<_, f64>(10).unwrap_or(0.0);
        let close_20d_ago = row.get::<_, f64>(11).unwrap_or(0.0);
        let vol_now = row.get::<_, f64>(8).unwrap_or(0.0);
        let avg_vol_base = row.get::<_, f64>(12).unwrap_or(0.0);
        let daily_volume_ratio = row.get::<_, f64>(15).unwrap_or(0.0);
        let volume_ratio = if daily_volume_ratio > 0.0 {
            daily_volume_ratio
        } else if avg_vol_base > 0.0 {
            vol_now / avg_vol_base
        } else {
            0.0
        };
        Ok(RawRow {
            ts_code,
            latest_trade_date: row.get::<_, String>(1)?,
            name: row.get::<_, String>(2).unwrap_or_default(),
            industry: row.get::<_, String>(3).unwrap_or_default(),
            close,
            high: row.get::<_, f64>(5).unwrap_or(0.0),
            low: row.get::<_, f64>(6).unwrap_or(0.0),
            ret_1d: row.get::<_, f64>(7).unwrap_or(0.0),
            ret_5d: pct_return(close, close_5d_ago),
            ret_20d: pct_return(close, close_20d_ago),
            volume_ratio,
            turnover_rate: row.get::<_, f64>(14).unwrap_or(0.0),
            total_mv: row.get::<_, f64>(16).unwrap_or(0.0),
            circ_mv: row.get::<_, f64>(17).unwrap_or(0.0),
            amount: row.get::<_, f64>(9).unwrap_or(0.0),
            net_mf_amount: row.get::<_, f64>(18).unwrap_or(0.0),
            net_mf_amount_raw: row.get::<_, f64>(19).unwrap_or(0.0),
            elg_net_amount: row.get::<_, f64>(20).unwrap_or(0.0)
                - row.get::<_, f64>(21).unwrap_or(0.0),
            flow_conflict_flag: row.get::<_, f64>(22).unwrap_or(0.0) >= 0.5,
            information_score: row.get::<_, f64>(23).unwrap_or(0.0),
            setup_score: row.get::<_, f64>(24).unwrap_or(0.0),
            continuation_score: row.get::<_, f64>(25).unwrap_or(0.0),
            fade_risk: row.get::<_, f64>(26).unwrap_or(0.0),
            std20_ret: row.get::<_, f64>(13).unwrap_or(0.0),
        })
    })?;

    Ok(rows.filter_map(|row| row.ok()).collect())
}

fn load_next_day_labels(db: &Connection, as_of: NaiveDate) -> Result<Vec<NextDayLabelRow>> {
    let max_label_date = latest_closed_cst_date().to_string();
    let as_of_str = as_of.to_string();
    let mut next_stmt = db.prepare(
        "SELECT CAST(trade_date AS VARCHAR)
         FROM prices
         WHERE trade_date > CAST(? AS DATE)
           AND trade_date <= CAST(? AS DATE)
         ORDER BY trade_date
         LIMIT 1",
    )?;
    let mut next_rows = next_stmt.query(duckdb::params![&as_of_str, &max_label_date])?;
    let Some(next_row) = next_rows.next()? else {
        return Ok(Vec::new());
    };
    let next_date = next_row.get::<_, String>(0)?;

    let mut stmt = db.prepare(
        "SELECT p.ts_code,
                CAST(p.trade_date AS VARCHAR),
                COALESCE(sb.name, ''),
                COALESCE(p.pct_chg, 0),
                COALESCE(p.high, 0),
                COALESCE(p.low, 0),
                COALESCE(p.close, 0)
         FROM prices p
         LEFT JOIN stock_basic sb
           ON sb.ts_code = p.ts_code
         WHERE p.trade_date = CAST(? AS DATE)
           AND p.close > 0",
    )?;
    let rows = stmt.query_map(duckdb::params![&next_date], |row| {
        Ok(NextDayLabelRow {
            ts_code: row.get::<_, String>(0)?,
            evaluation_date: row.get::<_, String>(1)?,
            name: row.get::<_, String>(2).unwrap_or_default(),
            pct_chg: row.get::<_, f64>(3).unwrap_or(0.0),
            high: row.get::<_, f64>(4).unwrap_or(0.0),
            low: row.get::<_, f64>(5).unwrap_or(0.0),
            close: row.get::<_, f64>(6).unwrap_or(0.0),
        })
    })?;

    Ok(rows.filter_map(|row| row.ok()).collect())
}

fn latest_closed_cst_date() -> NaiveDate {
    let shanghai = FixedOffset::east_opt(8 * 3600).unwrap();
    Utc::now().with_timezone(&shanghai).date_naive() - Duration::days(1)
}

fn compute_industry_heat(rows: &[RawRow]) -> HashMap<String, IndustryHeat> {
    let mut buckets: HashMap<String, Vec<&RawRow>> = HashMap::new();
    for row in rows {
        let key = if row.industry.trim().is_empty() {
            "UNKNOWN".to_string()
        } else {
            row.industry.clone()
        };
        buckets.entry(key).or_default().push(row);
    }

    buckets
        .into_iter()
        .map(|(industry, members)| {
            let count = members.len();
            let avg_ret_1d = if count > 0 {
                members.iter().map(|row| row.ret_1d).sum::<f64>() / count as f64
            } else {
                0.0
            };
            let hot = members.iter().filter(|row| row.ret_1d >= 3.0).count();
            (
                industry,
                IndustryHeat {
                    avg_ret_1d,
                    hot_ratio: if count > 0 {
                        hot as f64 / count as f64
                    } else {
                        0.0
                    },
                    count,
                },
            )
        })
        .collect()
}

fn score_limit_up_candidate(f: &LimitMoveFeatures) -> f64 {
    let trend20 = sigmoid((f.ret_20d - 8.0) / 7.0);
    let momentum5 = sigmoid((f.ret_5d - 1.0) / 4.0);
    let turnover = sigmoid((f.turnover_rate - 4.0) / 3.0);
    let volume = sigmoid((f.volume_ratio - 1.10) / 0.35);
    let small_mid = small_mid_cap_score(f.total_mv);
    let flow = (0.45 * signed_flow_score(f.net_mf_amount)
        + 0.35 * signed_flow_score(f.elg_net_amount)
        + 0.20 * f.information_score.clamp(0.0, 1.0))
    .clamp(0.0, 1.0);
    let flow = if f.flow_conflict_flag {
        (flow * 0.72).clamp(0.0, 1.0)
    } else {
        flow
    };
    let structure = f
        .setup_score
        .max(f.continuation_score * 0.90)
        .clamp(0.0, 1.0);

    let mut score = 0.22 * trend20
        + 0.13 * momentum5
        + 0.14 * turnover
        + 0.13 * volume
        + 0.12 * small_mid
        + 0.12 * f.industry_heat
        + 0.08 * flow
        + 0.06 * structure;

    if f.is_limit_down_today {
        score *= 0.25;
    }
    if f.is_limit_up_today {
        // Still useful as a second-board radar, but not a pre-ignition candidate.
        score = score * 0.78 + 0.08;
    }
    if f.ret_20d > 80.0 && f.net_mf_amount <= 0.0 {
        score *= 0.85;
    }
    score.clamp(0.0, 1.0)
}

fn score_limit_down_risk(f: &LimitMoveFeatures) -> f64 {
    let short_weak = sigmoid((-f.ret_5d - 1.0) / 4.0);
    let current_down = sigmoid((-f.ret_1d - 1.0) / 2.0);
    let turnover = sigmoid((f.turnover_rate - 4.0) / 3.0);
    let volume = sigmoid((f.volume_ratio - 1.10) / 0.35);
    let fragile_spike = (sigmoid((f.ret_20d - 12.0) / 8.0) * current_down).clamp(0.0, 1.0);
    let flow_negative = (0.60 * signed_flow_score(-f.net_mf_amount)
        + 0.40 * signed_flow_score(-f.elg_net_amount))
    .clamp(0.0, 1.0);
    let flow_negative = if f.flow_conflict_flag {
        (flow_negative + 0.08).min(1.0)
    } else {
        flow_negative
    };
    let small_mid = small_mid_cap_score(f.total_mv);

    let mut score = 0.18 * short_weak
        + 0.16 * current_down
        + 0.14 * turnover
        + 0.12 * volume
        + 0.14 * flow_negative
        + 0.10 * f.industry_weakness
        + 0.08 * small_mid
        + 0.08 * fragile_spike;

    if f.is_limit_up_today {
        score *= 0.60;
    }
    if f.is_limit_down_today {
        score = score * 0.85 + 0.10;
    }
    if f.fade_risk > 0.55 {
        score = (score + 0.08).min(1.0);
    }
    score.clamp(0.0, 1.0)
}

fn limit_up_labels(row: &RawRow, f: &LimitMoveFeatures, board_limit: f64) -> Vec<&'static str> {
    let mut labels = Vec::new();
    if f.is_limit_up_today {
        labels.push("already_limit_up_second_board_only");
    } else if row.ret_1d >= board_limit * 0.55 {
        labels.push("near_limit_chase_risk");
    } else {
        labels.push("pre_ignition");
    }
    if row.ret_20d >= 15.0 {
        labels.push("strong_20d_trend");
    }
    if row.turnover_rate >= 5.0 {
        labels.push("high_turnover");
    }
    if row.volume_ratio >= 1.2 {
        labels.push("volume_expansion");
    }
    if f.industry_heat >= 0.55 {
        labels.push("hot_industry_cluster");
    }
    if row.net_mf_amount <= 0.0 && row.elg_net_amount <= 0.0 {
        labels.push("flow_not_confirmed");
    }
    if row.flow_conflict_flag {
        labels.push("flow_scope_conflict");
    }
    if row.ret_20d >= 60.0 {
        labels.push("overheated_20d");
    }
    if board_scope(&row.ts_code, &row.name) != "mainboard_10cm" {
        labels.push("outside_core_execution_scope");
    }
    labels
}

fn limit_down_labels(row: &RawRow, f: &LimitMoveFeatures, _board_limit: f64) -> Vec<&'static str> {
    let mut labels = Vec::new();
    if f.is_limit_down_today {
        labels.push("already_limit_down_contagion_risk");
    }
    if row.ret_5d <= -3.0 {
        labels.push("short_term_breakdown");
    }
    if row.ret_20d >= 12.0 && row.ret_1d < 0.0 {
        labels.push("fragile_spike_reversal");
    }
    if row.net_mf_amount < 0.0 {
        labels.push("moneyflow_out");
    }
    if row.flow_conflict_flag {
        labels.push("flow_scope_conflict");
    }
    if row.elg_net_amount < 0.0 {
        labels.push("extra_large_order_out");
    }
    if f.industry_weakness >= 0.55 {
        labels.push("weak_industry_cluster");
    }
    if row.turnover_rate >= 5.0 {
        labels.push("high_turnover_exit_risk");
    }
    if labels.is_empty() {
        labels.push("low_signal");
    }
    labels
}

fn industry_heat_score(heat: &IndustryHeat) -> f64 {
    if heat.count < 3 {
        return 0.0;
    }
    (0.55 * sigmoid((heat.avg_ret_1d - 0.6) / 1.1) + 0.45 * heat.hot_ratio).clamp(0.0, 1.0)
}

fn industry_weakness_score(heat: &IndustryHeat) -> f64 {
    if heat.count < 3 {
        return 0.0;
    }
    (0.55 * sigmoid((-heat.avg_ret_1d - 0.6) / 1.1) + 0.45 * (1.0 - heat.hot_ratio)).clamp(0.0, 1.0)
}

fn board_scope(ts_code: &str, name: &str) -> &'static str {
    let code = ts_code.split('.').next().unwrap_or(ts_code);
    let upper_name = name.to_uppercase();
    if upper_name.contains("ST") {
        "st_5cm"
    } else if ts_code.ends_with(".BJ") || code.starts_with('4') || code.starts_with('8') {
        "bj_30cm"
    } else if code.starts_with("300") || code.starts_with("301") || code.starts_with("688") {
        "growth_20cm"
    } else {
        "mainboard_10cm"
    }
}

fn pct_return(now: f64, then: f64) -> f64 {
    if now > 0.0 && then > 0.0 {
        (now / then - 1.0) * 100.0
    } else {
        0.0
    }
}

fn small_mid_cap_score(total_mv: f64) -> f64 {
    if total_mv <= 0.0 {
        return 0.35;
    }
    // Tushare total_mv is in 10k CNY. 500k = 50bn CNY, 5m = 500bn CNY.
    let low = 500_000.0_f64.ln();
    let high = 5_000_000.0_f64.ln();
    let x = total_mv.ln();
    ((high - x) / (high - low)).clamp(0.0, 1.0)
}

fn signed_flow_score(value: f64) -> f64 {
    sigmoid(value / 10_000.0)
}

fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_features() -> LimitMoveFeatures {
        LimitMoveFeatures {
            ret_1d: 1.2,
            ret_5d: 5.5,
            ret_20d: 22.0,
            volume_ratio: 1.45,
            turnover_rate: 7.0,
            total_mv: 800_000.0,
            net_mf_amount: 8_000.0,
            flow_conflict_flag: false,
            elg_net_amount: 3_000.0,
            information_score: 0.65,
            setup_score: 0.58,
            continuation_score: 0.62,
            fade_risk: 0.20,
            industry_heat: 0.70,
            industry_weakness: 0.20,
            is_limit_up_today: false,
            is_limit_down_today: false,
        }
    }

    #[test]
    fn hot_turnover_trend_scores_as_limit_up_radar() {
        let score = score_limit_up_candidate(&base_features());

        assert!(score >= 0.60, "score={score}");
    }

    #[test]
    fn limit_down_today_suppresses_limit_up_radar() {
        let mut features = base_features();
        features.is_limit_down_today = true;

        assert!(score_limit_up_candidate(&features) < 0.25);
    }

    #[test]
    fn weak_flow_and_reversal_scores_as_limit_down_risk() {
        let mut features = base_features();
        features.ret_1d = -4.0;
        features.ret_5d = -6.0;
        features.net_mf_amount = -20_000.0;
        features.elg_net_amount = -8_000.0;
        features.industry_weakness = 0.75;
        features.fade_risk = 0.70;

        assert!(score_limit_down_risk(&features) >= 0.55);
    }

    #[test]
    fn board_scope_keeps_growth_and_st_outside_core_scope() {
        assert_eq!(board_scope("000001.SZ", "平安银行"), "mainboard_10cm");
        assert_eq!(board_scope("300750.SZ", "宁德时代"), "growth_20cm");
        assert_eq!(board_scope("688001.SH", "华兴源创"), "growth_20cm");
        assert_eq!(board_scope("002000.SZ", "*ST测试"), "st_5cm");
    }
}
