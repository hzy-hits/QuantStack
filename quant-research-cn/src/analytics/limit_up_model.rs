use anyhow::Result;
use chrono::{Duration, NaiveDate};
use duckdb::Connection;
use rayon::prelude::*;
use serde_json::json;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use tracing::info;

use crate::analytics::rv::{infer_censor_side, CensorSide};

const MODULE: &str = "limit_up_model";
const TRAIN_LOOKBACK_DAYS: i64 = 540;
const MIN_TRAIN_SAMPLES: usize = 800;
const MIN_POSITIVES: usize = 20;
const NEGATIVE_SAMPLE_DENOM: u64 = 24;
const COST_PCT: f64 = 0.20;
const LCB_Z_80: f64 = 1.2816;

const FEATURE_NAMES: [&str; 20] = [
    "ret_1d",
    "ret_3d",
    "ret_5d",
    "ret_20d",
    "log_amount_ratio20",
    "amplitude_pct",
    "close_position",
    "turnover_rate",
    "large_plus_flow_ratio",
    "extra_large_flow_ratio",
    "flow_conflict",
    "industry_avg_ret",
    "industry_hot_ratio",
    "industry_limit_rate",
    "prior_limit_up_5",
    "prior_limit_up_20",
    "prior_touch_up_20",
    "setup_score",
    "continuation_score",
    "limit_up_radar_score",
];

#[derive(Clone, Debug)]
struct ModelRow {
    feature_date: NaiveDate,
    symbol: String,
    name: String,
    industry: String,
    board_scope: String,
    features: Vec<f64>,
    next_day_limit_up: Option<bool>,
    next_day_touch_limit: Option<bool>,
    next_day_failed_board: Option<bool>,
    next_day_ret_pct: Option<f64>,
    next_day_drawdown_pct: Option<f64>,
}

#[derive(Clone, Debug)]
struct Standardizer {
    mean: Vec<f64>,
    std: Vec<f64>,
}

#[derive(Clone, Debug)]
struct BinaryModel {
    weights: Vec<f64>,
}

#[derive(Clone, Debug, Default)]
struct BucketStats {
    samples: usize,
    hits: usize,
    touches: usize,
    failed_boards: usize,
    ret_sum: f64,
    ret_sq_sum: f64,
}

impl BucketStats {
    fn add(&mut self, row: &ModelRow) {
        self.samples += 1;
        if row.next_day_limit_up.unwrap_or(false) {
            self.hits += 1;
        }
        if row.next_day_touch_limit.unwrap_or(false) {
            self.touches += 1;
        }
        if row.next_day_failed_board.unwrap_or(false) {
            self.failed_boards += 1;
        }
        let ret = row.next_day_ret_pct.unwrap_or(0.0);
        self.ret_sum += ret;
        self.ret_sq_sum += ret * ret;
    }

    fn mean_ret(&self) -> f64 {
        if self.samples == 0 {
            0.0
        } else {
            self.ret_sum / self.samples as f64
        }
    }

    fn hit_rate(&self) -> f64 {
        if self.samples == 0 {
            0.0
        } else {
            self.hits as f64 / self.samples as f64
        }
    }

    fn touch_rate(&self) -> f64 {
        if self.samples == 0 {
            0.0
        } else {
            self.touches as f64 / self.samples as f64
        }
    }

    fn failed_board_rate(&self) -> f64 {
        if self.samples == 0 {
            0.0
        } else {
            self.failed_boards as f64 / self.samples as f64
        }
    }

    fn ret_std(&self) -> f64 {
        if self.samples <= 1 {
            return 0.0;
        }
        let mean = self.mean_ret();
        let var = (self.ret_sq_sum / self.samples as f64 - mean * mean).max(0.0);
        var.sqrt()
    }

    fn ev_after_cost(&self) -> f64 {
        self.mean_ret() - COST_PCT
    }

    fn ev_lcb_80(&self) -> f64 {
        if self.samples == 0 {
            -COST_PCT
        } else {
            self.mean_ret() - LCB_Z_80 * self.ret_std() / (self.samples as f64).sqrt() - COST_PCT
        }
    }
}

#[derive(Clone, Debug)]
struct ModelPack {
    limit_up: BinaryModel,
    touch_limit: BinaryModel,
    failed_board: BinaryModel,
    standardizer: Standardizer,
    thresholds: Vec<f64>,
    buckets: Vec<BucketStats>,
    auc: f64,
    brier: f64,
    train_start: NaiveDate,
    train_end: NaiveDate,
    train_samples: usize,
    train_positives: usize,
    validation_samples: usize,
    validation_positives: usize,
}

#[derive(Clone, Debug)]
struct Prediction {
    row: ModelRow,
    raw_p_limit_up: f64,
    raw_p_touch_limit: f64,
    raw_p_failed_board: f64,
    p_limit_up: f64,
    p_touch_limit: f64,
    p_failed_board: f64,
    ev_after_cost_pct: f64,
    ev_lcb_80_pct: f64,
    probability_decile: i32,
    model_state: String,
    decision_state: String,
}

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    let rows = load_rows(db, as_of)?;
    info!(rows = rows.len(), "limit_up_model feature rows loaded");
    if rows.is_empty() {
        store_empty_performance(db, as_of, "data_missing")?;
        return Ok(0);
    }

    let prediction_date = rows
        .iter()
        .map(|row| row.feature_date)
        .max()
        .unwrap_or(as_of);
    let training_rows: Vec<ModelRow> = rows
        .par_iter()
        .filter(|row| {
            row.feature_date < prediction_date
                && row.board_scope == "mainboard_10cm"
                && row.next_day_limit_up.is_some()
        })
        .cloned()
        .collect();
    let prediction_rows: Vec<ModelRow> = rows
        .par_iter()
        .filter(|row| row.feature_date == prediction_date)
        .cloned()
        .collect();

    let train_positives = training_rows
        .par_iter()
        .filter(|row| row.next_day_limit_up == Some(true))
        .count();
    let model_state =
        if training_rows.len() >= MIN_TRAIN_SAMPLES && train_positives >= MIN_POSITIVES {
            "trained"
        } else {
            "insufficient_history"
        };

    store_dataset(db, as_of, &prediction_rows)?;

    let predictions = if model_state == "trained" {
        let pack = train_model_pack(&training_rows)?;
        let predictions = prediction_rows
            .into_par_iter()
            .map(|row| predict_row(row, &pack))
            .collect::<Vec<_>>();
        store_performance(db, as_of, &pack)?;
        predictions
    } else {
        store_empty_performance(db, as_of, model_state)?;
        prediction_rows
            .into_par_iter()
            .map(|row| Prediction {
                row,
                raw_p_limit_up: 0.0,
                raw_p_touch_limit: 0.0,
                raw_p_failed_board: 0.0,
                p_limit_up: 0.0,
                p_touch_limit: 0.0,
                p_failed_board: 0.0,
                ev_after_cost_pct: 0.0,
                ev_lcb_80_pct: -COST_PCT,
                probability_decile: 0,
                model_state: model_state.to_string(),
                decision_state: "research_radar".to_string(),
            })
            .collect::<Vec<_>>()
    };

    store_predictions(db, as_of, &predictions)?;
    store_analytics(db, as_of, &predictions)?;
    info!(
        rows = predictions.len(),
        model_state,
        train_samples = training_rows.len(),
        train_positives,
        "limit_up_model complete"
    );
    Ok(predictions.len())
}

fn train_model_pack(rows: &[ModelRow]) -> Result<ModelPack> {
    let dates = rows
        .iter()
        .map(|row| row.feature_date)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let split_idx = (dates.len() * 4 / 5).clamp(1, dates.len().saturating_sub(1));
    let validation_start = dates[split_idx];
    let fit_rows_full = rows
        .par_iter()
        .filter(|row| row.feature_date < validation_start)
        .cloned()
        .collect::<Vec<_>>();
    let validation_rows = rows
        .par_iter()
        .filter(|row| row.feature_date >= validation_start)
        .cloned()
        .collect::<Vec<_>>();
    let fit_rows = sample_training_rows(&fit_rows_full);

    let lambda = select_lambda(&fit_rows, &validation_rows);
    let standardizer = Standardizer::fit(rows);
    let model_rows = sample_training_rows(rows);
    info!(
        full_rows = rows.len(),
        model_rows = model_rows.len(),
        "limit_up_model sampled rare-event training rows"
    );
    let (limit_up, (touch_limit, failed_board)) = rayon::join(
        || {
            train_binary_model(
                &model_rows,
                &standardizer,
                |row| row.next_day_limit_up.unwrap_or(false),
                lambda,
                70,
            )
        },
        || {
            rayon::join(
                || {
                    train_binary_model(
                        &model_rows,
                        &standardizer,
                        |row| row.next_day_touch_limit.unwrap_or(false),
                        lambda,
                        60,
                    )
                },
                || {
                    train_binary_model(
                        &model_rows,
                        &standardizer,
                        |row| row.next_day_failed_board.unwrap_or(false),
                        lambda,
                        60,
                    )
                },
            )
        },
    );

    let validation_probs = validation_rows
        .par_iter()
        .map(|row| limit_up.predict(&standardizer.transform(&row.features)))
        .collect::<Vec<_>>();
    let thresholds = decile_thresholds(validation_probs.clone());
    let mut buckets = vec![BucketStats::default(); 11];
    for (row, prob) in validation_rows.iter().zip(validation_probs.iter()) {
        let decile = probability_decile(*prob, &thresholds);
        buckets[decile as usize].add(row);
    }
    let labels = validation_rows
        .par_iter()
        .map(|row| row.next_day_limit_up.unwrap_or(false))
        .collect::<Vec<_>>();
    let brier = brier_score(&validation_probs, &labels);
    let auc = auc_score(&validation_probs, &labels);
    let train_start = rows
        .iter()
        .map(|row| row.feature_date)
        .min()
        .unwrap_or(validation_start);
    let train_end = rows
        .iter()
        .map(|row| row.feature_date)
        .max()
        .unwrap_or(validation_start);
    let train_positives = labels.iter().filter(|label| **label).count();
    let validation_samples = validation_rows.len();
    let validation_positives = train_positives;

    Ok(ModelPack {
        limit_up,
        touch_limit,
        failed_board,
        standardizer,
        thresholds,
        buckets,
        auc,
        brier,
        train_start,
        train_end,
        train_samples: rows.len(),
        train_positives: rows
            .iter()
            .filter(|row| row.next_day_limit_up.unwrap_or(false))
            .count(),
        validation_samples,
        validation_positives,
    })
}

fn sample_training_rows(rows: &[ModelRow]) -> Vec<ModelRow> {
    if rows.len() <= 50_000 {
        return rows.to_vec();
    }
    rows.par_iter()
        .filter(|row| {
            row.next_day_limit_up.unwrap_or(false)
                || row.next_day_touch_limit.unwrap_or(false)
                || row.next_day_failed_board.unwrap_or(false)
                || stable_sample_bucket(&row.symbol, row.feature_date) % NEGATIVE_SAMPLE_DENOM == 0
        })
        .cloned()
        .collect()
}

fn stable_sample_bucket(symbol: &str, date: NaiveDate) -> u64 {
    let mut hash = 14_695_981_039_346_656_037u64;
    let date_str = date.to_string();
    for b in symbol.as_bytes().iter().chain(date_str.as_bytes()) {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    hash
}

fn select_lambda(fit_rows: &[ModelRow], validation_rows: &[ModelRow]) -> f64 {
    if fit_rows.len() < MIN_TRAIN_SAMPLES / 2
        || validation_rows.len() < 100
        || validation_rows
            .iter()
            .filter(|row| row.next_day_limit_up == Some(true))
            .count()
            < 5
    {
        return 0.10;
    }

    let labels = validation_rows
        .iter()
        .map(|row| row.next_day_limit_up.unwrap_or(false))
        .collect::<Vec<_>>();
    [0.02, 0.10, 0.50]
        .into_iter()
        .map(|lambda| {
            let standardizer = Standardizer::fit(fit_rows);
            let model = train_binary_model(
                fit_rows,
                &standardizer,
                |row| row.next_day_limit_up.unwrap_or(false),
                lambda,
                45,
            );
            let probs = validation_rows
                .iter()
                .map(|row| model.predict(&standardizer.transform(&row.features)))
                .collect::<Vec<_>>();
            (lambda, brier_score(&probs, &labels))
        })
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal))
        .map(|(lambda, _)| lambda)
        .unwrap_or(0.10)
}

fn predict_row(row: ModelRow, pack: &ModelPack) -> Prediction {
    let features = pack.standardizer.transform(&row.features);
    let raw_p_limit_up = pack.limit_up.predict(&features);
    let raw_p_touch_limit = pack.touch_limit.predict(&features).max(raw_p_limit_up);
    let raw_p_failed_board = pack.failed_board.predict(&features);
    let decile = probability_decile(raw_p_limit_up, &pack.thresholds);
    let bucket = pack
        .buckets
        .get(decile as usize)
        .cloned()
        .unwrap_or_default();
    let p_limit_up = if bucket.samples > 0 {
        bucket.hit_rate()
    } else {
        raw_p_limit_up
    };
    let p_touch_limit = if bucket.samples > 0 {
        bucket.touch_rate().max(p_limit_up)
    } else {
        raw_p_touch_limit
    };
    let p_failed_board = if bucket.samples > 0 {
        bucket.failed_board_rate()
    } else {
        raw_p_failed_board
    };
    let ev_after_cost_pct = bucket.ev_after_cost();
    let ev_lcb_80_pct = bucket.ev_lcb_80();
    let decision_state = decision_state(
        &row,
        decile,
        p_limit_up,
        p_touch_limit,
        p_failed_board,
        ev_lcb_80_pct,
    );

    Prediction {
        row,
        raw_p_limit_up,
        raw_p_touch_limit,
        raw_p_failed_board,
        p_limit_up,
        p_touch_limit,
        p_failed_board,
        ev_after_cost_pct,
        ev_lcb_80_pct,
        probability_decile: decile,
        model_state: "trained".to_string(),
        decision_state,
    }
}

fn decision_state(
    row: &ModelRow,
    decile: i32,
    p_limit_up: f64,
    p_touch_limit: f64,
    p_failed_board: f64,
    ev_lcb_80_pct: f64,
) -> String {
    if row.board_scope != "mainboard_10cm" {
        return "heat_only".to_string();
    }
    if p_failed_board > p_limit_up && p_touch_limit >= 0.08 {
        return "blocked_tail".to_string();
    }
    if ev_lcb_80_pct > 0.0 && decile >= 9 {
        return "limit_up_candidate".to_string();
    }
    if decile >= 8 || p_touch_limit >= 0.08 {
        return "research_radar".to_string();
    }
    "heat_only".to_string()
}

fn train_binary_model<F>(
    rows: &[ModelRow],
    standardizer: &Standardizer,
    label_fn: F,
    l2: f64,
    iterations: usize,
) -> BinaryModel
where
    F: Fn(&ModelRow) -> bool + Sync,
{
    let width = FEATURE_NAMES.len() + 1;
    let mut weights = vec![0.0; width];
    let lr = 0.18;
    let n = rows.len().max(1) as f64;
    for _ in 0..iterations {
        let grad = rows
            .par_iter()
            .fold(
                || vec![0.0; width],
                |mut grad, row| {
                    let x = standardizer.transform(&row.features);
                    let y = if label_fn(row) { 1.0 } else { 0.0 };
                    let p = sigmoid(weights[0] + dot(&weights[1..], &x));
                    let err = p - y;
                    grad[0] += err;
                    for (idx, value) in x.iter().enumerate() {
                        grad[idx + 1] += err * value;
                    }
                    grad
                },
            )
            .reduce(
                || vec![0.0; width],
                |mut left, right| {
                    for idx in 0..width {
                        left[idx] += right[idx];
                    }
                    left
                },
            );
        weights[0] -= lr * grad[0] / n;
        for idx in 1..width {
            weights[idx] -= lr * (grad[idx] / n + l2 * weights[idx]);
        }
    }
    BinaryModel { weights }
}

impl BinaryModel {
    fn predict(&self, x: &[f64]) -> f64 {
        sigmoid(self.weights[0] + dot(&self.weights[1..], x))
    }
}

impl Standardizer {
    fn fit(rows: &[ModelRow]) -> Self {
        let width = FEATURE_NAMES.len();
        let n = rows.len().max(1) as f64;
        let mean = rows
            .par_iter()
            .fold(
                || vec![0.0; width],
                |mut acc, row| {
                    for (idx, value) in row.features.iter().enumerate() {
                        acc[idx] += *value;
                    }
                    acc
                },
            )
            .reduce(
                || vec![0.0; width],
                |mut left, right| {
                    for idx in 0..width {
                        left[idx] += right[idx];
                    }
                    left
                },
            )
            .into_iter()
            .map(|sum| sum / n)
            .collect::<Vec<_>>();
        let var = rows
            .par_iter()
            .fold(
                || vec![0.0; width],
                |mut acc, row| {
                    for (idx, value) in row.features.iter().enumerate() {
                        let diff = *value - mean[idx];
                        acc[idx] += diff * diff;
                    }
                    acc
                },
            )
            .reduce(
                || vec![0.0; width],
                |mut left, right| {
                    for idx in 0..width {
                        left[idx] += right[idx];
                    }
                    left
                },
            );
        let std = var
            .into_iter()
            .map(|v| (v / n).sqrt().max(1e-6))
            .collect::<Vec<_>>();
        Self { mean, std }
    }

    fn transform(&self, features: &[f64]) -> Vec<f64> {
        features
            .iter()
            .enumerate()
            .map(|(idx, value)| ((*value - self.mean[idx]) / self.std[idx]).clamp(-6.0, 6.0))
            .collect()
    }
}

fn load_rows(db: &Connection, as_of: NaiveDate) -> Result<Vec<ModelRow>> {
    let date_str = as_of.to_string();
    let lower_bound = (as_of - Duration::days(TRAIN_LOOKBACK_DAYS)).to_string();
    let sql = "
        WITH base AS (
            SELECT
                p.ts_code,
                p.trade_date,
                COALESCE(sb.name, '') AS name,
                COALESCE(sb.industry, '') AS industry,
                COALESCE(p.open, 0) AS open,
                COALESCE(p.high, 0) AS high,
                COALESCE(p.low, 0) AS low,
                COALESCE(p.close, 0) AS close,
                COALESCE(p.pre_close, 0) AS pre_close,
                COALESCE(p.pct_chg, 0) AS ret_1d,
                COALESCE(p.amount, 0) AS amount,
                COALESCE(dbasic.turnover_rate, 0) AS turnover_rate,
                COALESCE(dbasic.volume_ratio, 0) AS daily_volume_ratio,
                COALESCE(mf.buy_lg_amount, 0) + COALESCE(mf.buy_elg_amount, 0)
                  - COALESCE(mf.sell_lg_amount, 0) - COALESCE(mf.sell_elg_amount, 0) AS large_plus_net,
                COALESCE(mf.buy_elg_amount, 0) - COALESCE(mf.sell_elg_amount, 0) AS extra_large_net,
                COALESCE(mf.net_mf_amount, 0) AS net_mf_amount,
                CASE
                    WHEN COALESCE(sb.name, '') LIKE '%ST%' THEN 4.7
                    WHEN p.ts_code LIKE '300%' OR p.ts_code LIKE '301%' OR p.ts_code LIKE '688%' THEN 19.7
                    ELSE 9.7
                END AS limit_threshold,
                COALESCE(setup.value, 0) AS setup_score,
                COALESCE(cont.value, 0) AS continuation_score,
                COALESCE(fade.value, 0) AS fade_risk,
                COALESCE(radar.value, 0) AS limit_up_radar_score,
                COALESCE(flow_conflict.value, 0) AS flow_conflict_flag
            FROM prices p
            LEFT JOIN stock_basic sb ON sb.ts_code = p.ts_code
            LEFT JOIN daily_basic dbasic ON dbasic.ts_code = p.ts_code AND dbasic.trade_date = p.trade_date
            LEFT JOIN moneyflow mf ON mf.ts_code = p.ts_code AND mf.trade_date = p.trade_date
            LEFT JOIN analytics setup ON setup.ts_code = p.ts_code AND setup.as_of = p.trade_date
                AND setup.module = 'setup_alpha' AND setup.metric = 'setup_score'
            LEFT JOIN analytics cont ON cont.ts_code = p.ts_code AND cont.as_of = p.trade_date
                AND cont.module = 'continuation_vs_fade' AND cont.metric = 'continuation_score'
            LEFT JOIN analytics fade ON fade.ts_code = p.ts_code AND fade.as_of = p.trade_date
                AND fade.module = 'continuation_vs_fade' AND fade.metric = 'fade_risk'
            LEFT JOIN analytics radar ON radar.ts_code = p.ts_code AND radar.as_of = p.trade_date
                AND radar.module = 'limit_move_radar' AND radar.metric = 'limit_up_radar_score'
            LEFT JOIN analytics flow_conflict ON flow_conflict.ts_code = p.ts_code AND flow_conflict.as_of = p.trade_date
                AND flow_conflict.module = 'flow_audit' AND flow_conflict.metric = 'flow_conflict_flag'
            WHERE p.trade_date >= CAST(? AS DATE)
              AND p.trade_date <= CAST(? AS DATE)
              AND p.close > 0
              AND p.pre_close > 0
        ),
        feat AS (
            SELECT
                *,
                close / NULLIF(LAG(close, 3) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) - 1 AS ret_3d_raw,
                close / NULLIF(LAG(close, 5) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) - 1 AS ret_5d_raw,
                close / NULLIF(LAG(close, 20) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) - 1 AS ret_20d_raw,
                AVG(amount) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS avg_amount20,
                SUM(CASE WHEN ret_1d >= limit_threshold THEN 1 ELSE 0 END)
                    OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS prior_limit_up_5,
                SUM(CASE WHEN ret_1d >= limit_threshold THEN 1 ELSE 0 END)
                    OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior_limit_up_20,
                SUM(CASE WHEN high / NULLIF(pre_close, 0) - 1 >= (limit_threshold - 0.2) / 100.0 THEN 1 ELSE 0 END)
                    OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior_touch_up_20,
                LEAD(trade_date) OVER (PARTITION BY ts_code ORDER BY trade_date) AS next_trade_date,
                LEAD(ret_1d) OVER (PARTITION BY ts_code ORDER BY trade_date) AS next_ret_pct,
                LEAD(high) OVER (PARTITION BY ts_code ORDER BY trade_date) AS next_high,
                LEAD(low) OVER (PARTITION BY ts_code ORDER BY trade_date) AS next_low,
                LEAD(close) OVER (PARTITION BY ts_code ORDER BY trade_date) AS next_close,
                LEAD(pre_close) OVER (PARTITION BY ts_code ORDER BY trade_date) AS next_pre_close
            FROM base
        ),
        industry AS (
            SELECT
                trade_date,
                industry,
                AVG(ret_1d) AS industry_avg_ret,
                AVG(CASE WHEN ret_1d > 0 THEN 1.0 ELSE 0.0 END) AS industry_hot_ratio,
                AVG(CASE WHEN ret_1d >= limit_threshold THEN 1.0 ELSE 0.0 END) AS industry_limit_rate
            FROM base
            GROUP BY trade_date, industry
        )
        SELECT
            CAST(f.trade_date AS VARCHAR),
            f.ts_code,
            f.name,
            f.industry,
            f.ret_1d,
            COALESCE(f.ret_3d_raw, 0) * 100,
            COALESCE(f.ret_5d_raw, 0) * 100,
            COALESCE(f.ret_20d_raw, 0) * 100,
            CASE WHEN COALESCE(f.avg_amount20, 0) > 0 THEN f.amount / f.avg_amount20 ELSE COALESCE(f.daily_volume_ratio, 0) END AS amount_ratio20,
            CASE WHEN f.pre_close > 0 THEN (f.high - f.low) / f.pre_close * 100 ELSE 0 END AS amplitude_pct,
            CASE WHEN f.high > f.low THEN (f.close - f.low) / (f.high - f.low) ELSE 0.5 END AS close_position,
            f.turnover_rate,
            CASE WHEN f.amount > 0 THEN f.large_plus_net / f.amount ELSE 0 END AS large_plus_flow_ratio,
            CASE WHEN f.amount > 0 THEN f.extra_large_net / f.amount ELSE 0 END AS extra_large_flow_ratio,
            f.flow_conflict_flag,
            COALESCE(i.industry_avg_ret, 0),
            COALESCE(i.industry_hot_ratio, 0),
            COALESCE(i.industry_limit_rate, 0),
            COALESCE(f.prior_limit_up_5, 0),
            COALESCE(f.prior_limit_up_20, 0),
            COALESCE(f.prior_touch_up_20, 0),
            f.setup_score,
            f.continuation_score,
            f.fade_risk,
            f.limit_up_radar_score,
            f.limit_threshold,
            f.next_trade_date IS NOT NULL AS has_next,
            f.next_ret_pct,
            CASE WHEN COALESCE(f.next_pre_close, 0) > 0 THEN (f.next_high / f.next_pre_close - 1) * 100 ELSE NULL END AS next_high_ret_pct,
            CASE WHEN COALESCE(f.next_pre_close, 0) > 0 THEN (f.next_low / f.next_pre_close - 1) * 100 ELSE NULL END AS next_drawdown_pct,
            f.next_high,
            f.next_low,
            f.next_close
        FROM feat f
        LEFT JOIN industry i ON i.trade_date = f.trade_date AND i.industry = f.industry
        WHERE f.trade_date <= CAST(? AS DATE)
    ";
    let mut stmt = db.prepare(sql)?;
    let rows = stmt.query_map(duckdb::params![lower_bound, date_str, date_str], |row| {
        let feature_date_raw = row.get::<_, String>(0)?;
        let feature_date =
            NaiveDate::parse_from_str(&feature_date_raw[0..10], "%Y-%m-%d").unwrap_or(as_of);
        let symbol = row.get::<_, String>(1)?;
        let name = row.get::<_, String>(2).unwrap_or_default();
        let board_scope = board_scope(&symbol, &name).to_string();
        let ret_1d = row.get::<_, f64>(4).unwrap_or(0.0);
        let ret_3d = row.get::<_, f64>(5).unwrap_or(0.0);
        let ret_5d = row.get::<_, f64>(6).unwrap_or(0.0);
        let ret_20d = row.get::<_, f64>(7).unwrap_or(0.0);
        let amount_ratio20 = row.get::<_, f64>(8).unwrap_or(0.0);
        let amplitude_pct = row.get::<_, f64>(9).unwrap_or(0.0);
        let close_position = row.get::<_, f64>(10).unwrap_or(0.5);
        let turnover_rate = row.get::<_, f64>(11).unwrap_or(0.0);
        let large_plus_flow_ratio = row.get::<_, f64>(12).unwrap_or(0.0);
        let extra_large_flow_ratio = row.get::<_, f64>(13).unwrap_or(0.0);
        let flow_conflict = row.get::<_, f64>(14).unwrap_or(0.0);
        let industry_avg_ret = row.get::<_, f64>(15).unwrap_or(0.0);
        let industry_hot_ratio = row.get::<_, f64>(16).unwrap_or(0.0);
        let industry_limit_rate = row.get::<_, f64>(17).unwrap_or(0.0);
        let prior_limit_up_5 = row.get::<_, f64>(18).unwrap_or(0.0);
        let prior_limit_up_20 = row.get::<_, f64>(19).unwrap_or(0.0);
        let prior_touch_up_20 = row.get::<_, f64>(20).unwrap_or(0.0);
        let setup_score = row.get::<_, f64>(21).unwrap_or(0.0);
        let continuation_score = row.get::<_, f64>(22).unwrap_or(0.0);
        let _fade_risk = row.get::<_, f64>(23).unwrap_or(0.0);
        let limit_up_radar_score = row.get::<_, f64>(24).unwrap_or(0.0);
        let limit_threshold = row.get::<_, f64>(25).unwrap_or(9.7);
        let has_next = row.get::<_, bool>(26).unwrap_or(false);
        let next_ret = row.get::<_, Option<f64>>(27).ok().flatten();
        let next_high_ret = row.get::<_, Option<f64>>(28).ok().flatten();
        let next_drawdown = row.get::<_, Option<f64>>(29).ok().flatten();
        let next_high = row.get::<_, Option<f64>>(30).ok().flatten().unwrap_or(0.0);
        let next_low = row.get::<_, Option<f64>>(31).ok().flatten().unwrap_or(0.0);
        let next_close = row.get::<_, Option<f64>>(32).ok().flatten().unwrap_or(0.0);
        let next_day_limit_up = next_ret.map(|ret| {
            infer_censor_side(&symbol, &name, ret, next_high, next_low, next_close)
                == CensorSide::Right
        });
        let next_day_touch_limit =
            next_high_ret.map(|ret| ret >= limit_threshold - 0.20 && has_next);
        let next_day_failed_board = match (next_day_touch_limit, next_day_limit_up) {
            (Some(true), Some(false)) => Some(true),
            (Some(_), Some(_)) => Some(false),
            _ => None,
        };
        let features = vec![
            clamp(ret_1d, -20.0, 20.0),
            clamp(ret_3d, -30.0, 40.0),
            clamp(ret_5d, -35.0, 60.0),
            clamp(ret_20d, -50.0, 120.0),
            ln_pos(amount_ratio20),
            clamp(amplitude_pct, 0.0, 25.0),
            clamp(close_position, 0.0, 1.0),
            clamp(turnover_rate, 0.0, 60.0),
            clamp(large_plus_flow_ratio, -1.0, 1.0),
            clamp(extra_large_flow_ratio, -1.0, 1.0),
            clamp(flow_conflict, 0.0, 1.0),
            clamp(industry_avg_ret, -10.0, 10.0),
            clamp(industry_hot_ratio, 0.0, 1.0),
            clamp(industry_limit_rate, 0.0, 1.0),
            clamp(prior_limit_up_5, 0.0, 5.0),
            clamp(prior_limit_up_20, 0.0, 20.0),
            clamp(prior_touch_up_20, 0.0, 20.0),
            clamp(setup_score, 0.0, 1.0),
            clamp(continuation_score, 0.0, 1.0),
            clamp(limit_up_radar_score, 0.0, 1.0),
        ];
        Ok(ModelRow {
            feature_date,
            symbol,
            name,
            industry: row.get::<_, String>(3).unwrap_or_default(),
            board_scope,
            features,
            next_day_limit_up,
            next_day_touch_limit,
            next_day_failed_board,
            next_day_ret_pct: next_ret,
            next_day_drawdown_pct: next_drawdown,
        })
    })?;
    Ok(rows.filter_map(|row| row.ok()).collect())
}

fn store_dataset(db: &Connection, as_of: NaiveDate, rows: &[ModelRow]) -> Result<()> {
    db.execute(
        "DELETE FROM limit_up_model_dataset WHERE model_as_of = CAST(? AS DATE)",
        duckdb::params![as_of.to_string()],
    )?;
    let mut stmt = db.prepare(
        "INSERT OR REPLACE INTO limit_up_model_dataset (
            model_as_of, feature_date, symbol, board_scope, dataset_split,
            features_json, next_day_limit_up, next_day_touch_limit,
            next_day_failed_board, next_day_ret_pct, next_day_drawdown_pct
        ) VALUES (?, ?, ?, ?, 'prediction', ?, ?, ?, ?, ?, ?)",
    )?;
    for row in rows {
        stmt.execute(duckdb::params![
            as_of.to_string(),
            row.feature_date.to_string(),
            &row.symbol,
            &row.board_scope,
            feature_json(row).to_string(),
            row.next_day_limit_up,
            row.next_day_touch_limit,
            row.next_day_failed_board,
            row.next_day_ret_pct,
            row.next_day_drawdown_pct,
        ])?;
    }
    Ok(())
}

fn store_predictions(db: &Connection, as_of: NaiveDate, predictions: &[Prediction]) -> Result<()> {
    db.execute(
        "DELETE FROM limit_up_model_predictions WHERE as_of = CAST(? AS DATE)",
        duckdb::params![as_of.to_string()],
    )?;
    let mut stmt = db.prepare(
        "INSERT OR REPLACE INTO limit_up_model_predictions (
            as_of, symbol, board_scope, p_limit_up, p_touch_limit, p_failed_board,
            ev_after_cost_pct, ev_lcb_80_pct, probability_decile, model_state,
            decision_state, detail_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?;
    for pred in predictions {
        let detail = json!({
            "data_scope": "as_of_or_earlier_daily_features_only",
            "name": pred.row.name,
            "industry": pred.row.industry,
            "feature_date": pred.row.feature_date.to_string(),
            "auction_state": "auction_pending",
            "raw_model_score": {
                "p_limit_up": pred.raw_p_limit_up,
                "p_touch_limit": pred.raw_p_touch_limit,
                "p_failed_board": pred.raw_p_failed_board,
            },
            "feature_names": FEATURE_NAMES,
            "features": pred.row.features,
            "labels_if_available": {
                "next_day_limit_up": pred.row.next_day_limit_up,
                "next_day_touch_limit": pred.row.next_day_touch_limit,
                "next_day_failed_board": pred.row.next_day_failed_board,
                "next_day_ret_pct": pred.row.next_day_ret_pct,
                "next_day_drawdown_pct": pred.row.next_day_drawdown_pct,
            }
        })
        .to_string();
        stmt.execute(duckdb::params![
            as_of.to_string(),
            &pred.row.symbol,
            &pred.row.board_scope,
            pred.p_limit_up,
            pred.p_touch_limit,
            pred.p_failed_board,
            pred.ev_after_cost_pct,
            pred.ev_lcb_80_pct,
            pred.probability_decile,
            &pred.model_state,
            &pred.decision_state,
            detail,
        ])?;
    }
    Ok(())
}

fn store_analytics(db: &Connection, as_of: NaiveDate, predictions: &[Prediction]) -> Result<()> {
    db.execute(
        "DELETE FROM analytics WHERE as_of = CAST(? AS DATE) AND module = ?",
        duckdb::params![as_of.to_string(), MODULE],
    )?;
    db.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS limit_up_model_stage (
            ts_code VARCHAR,
            as_of VARCHAR,
            module VARCHAR,
            metric VARCHAR,
            value DOUBLE,
            detail VARCHAR
        );
        DELETE FROM limit_up_model_stage;",
    )?;
    {
        let mut appender = db.appender("limit_up_model_stage")?;
        for pred in predictions {
            let detail = json!({
                "name": pred.row.name,
                "industry": pred.row.industry,
                "board_scope": pred.row.board_scope,
                "model_state": pred.model_state,
                "decision_state": pred.decision_state,
                "probability_decile": pred.probability_decile,
                "ev_after_cost_pct": round3(pred.ev_after_cost_pct),
                "ev_lcb_80_pct": round3(pred.ev_lcb_80_pct),
                "auction_state": "auction_pending",
            })
            .to_string();
            for (metric, value) in [
                ("p_limit_up", pred.p_limit_up),
                ("p_touch_limit", pred.p_touch_limit),
                ("p_failed_board", pred.p_failed_board),
                ("ev_after_cost_pct", pred.ev_after_cost_pct),
                ("ev_lcb_80_pct", pred.ev_lcb_80_pct),
                ("probability_decile", pred.probability_decile as f64),
            ] {
                appender.append_row(duckdb::params![
                    &pred.row.symbol,
                    as_of.to_string(),
                    MODULE,
                    metric,
                    value,
                    &detail,
                ])?;
            }
        }
    }
    db.execute_batch(
        "INSERT INTO analytics (ts_code, as_of, module, metric, value, detail)
         SELECT ts_code, CAST(as_of AS DATE), module, metric, value, detail
         FROM limit_up_model_stage",
    )?;
    Ok(())
}

fn store_performance(db: &Connection, as_of: NaiveDate, pack: &ModelPack) -> Result<()> {
    db.execute(
        "DELETE FROM limit_up_model_performance WHERE as_of = CAST(? AS DATE)",
        duckdb::params![as_of.to_string()],
    )?;
    let top = pack.buckets.get(10).cloned().unwrap_or_default();
    let base_rate = if pack.validation_samples > 0 {
        pack.validation_positives as f64 / pack.validation_samples as f64
    } else {
        0.0
    };
    let top_hit_rate = if top.samples > 0 {
        top.hits as f64 / top.samples as f64
    } else {
        0.0
    };
    let decile_table = (1..=10)
        .map(|idx| {
            let bucket = pack.buckets.get(idx).cloned().unwrap_or_default();
            json!({
                "decile": idx,
                "samples": bucket.samples,
                "limit_up_hits": bucket.hits,
                "hit_rate": if bucket.samples > 0 { bucket.hits as f64 / bucket.samples as f64 } else { 0.0 },
                "touch_rate": if bucket.samples > 0 { bucket.touches as f64 / bucket.samples as f64 } else { 0.0 },
                "failed_board_rate": if bucket.samples > 0 { bucket.failed_boards as f64 / bucket.samples as f64 } else { 0.0 },
                "avg_next_ret_pct": round3(bucket.mean_ret()),
                "ev_after_cost_pct": round3(bucket.ev_after_cost()),
                "ev_lcb_80_pct": round3(bucket.ev_lcb_80()),
            })
        })
        .collect::<Vec<_>>();
    let detail = json!({
        "model_state": "trained",
        "train_samples": pack.train_samples,
        "train_positives": pack.train_positives,
        "validation_samples": pack.validation_samples,
        "validation_positives": pack.validation_positives,
        "validation_base_limit_up_rate": round3(base_rate),
        "performance_window": "time_split_holdout",
        "feature_names": FEATURE_NAMES,
        "cost_pct": COST_PCT,
        "deciles": decile_table,
    })
    .to_string();
    db.execute(
        "INSERT OR REPLACE INTO limit_up_model_performance (
            as_of, train_start, train_end, model_state, train_samples, train_positives,
            auc, brier, top_decile_hit_rate, top_decile_lift, failed_board_rate,
            avg_next_ret_pct, decile_table_json, detail_json
        ) VALUES (?, ?, ?, 'trained', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            as_of.to_string(),
            pack.train_start.to_string(),
            pack.train_end.to_string(),
            pack.train_samples as i64,
            pack.train_positives as i64,
            pack.auc,
            pack.brier,
            top_hit_rate,
            if base_rate > 0.0 {
                top_hit_rate / base_rate
            } else {
                0.0
            },
            if top.samples > 0 {
                top.failed_boards as f64 / top.samples as f64
            } else {
                0.0
            },
            top.mean_ret(),
            json!(decile_table).to_string(),
            detail,
        ],
    )?;
    Ok(())
}

fn store_empty_performance(db: &Connection, as_of: NaiveDate, state: &str) -> Result<()> {
    db.execute(
        "DELETE FROM limit_up_model_performance WHERE as_of = CAST(? AS DATE)",
        duckdb::params![as_of.to_string()],
    )?;
    db.execute(
        "INSERT OR REPLACE INTO limit_up_model_performance (
            as_of, train_start, train_end, model_state, train_samples, train_positives,
            auc, brier, top_decile_hit_rate, top_decile_lift, failed_board_rate,
            avg_next_ret_pct, decile_table_json, detail_json
        ) VALUES (?, NULL, NULL, ?, 0, 0, 0, 0, 0, 0, 0, 0, '[]', ?)",
        duckdb::params![
            as_of.to_string(),
            state,
            json!({"model_state": state, "reason": state}).to_string(),
        ],
    )?;
    Ok(())
}

fn feature_json(row: &ModelRow) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("name".to_string(), json!(row.name));
    obj.insert("industry".to_string(), json!(row.industry));
    obj.insert("board_scope".to_string(), json!(row.board_scope));
    for (name, value) in FEATURE_NAMES.iter().zip(row.features.iter()) {
        obj.insert((*name).to_string(), json!(round3(*value)));
    }
    serde_json::Value::Object(obj)
}

fn decile_thresholds(mut probs: Vec<f64>) -> Vec<f64> {
    if probs.is_empty() {
        return vec![0.0; 9];
    }
    probs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    (1..10)
        .map(|bucket| {
            let idx = ((probs.len() * bucket) / 10)
                .saturating_sub(1)
                .min(probs.len() - 1);
            probs[idx]
        })
        .collect()
}

fn probability_decile(prob: f64, thresholds: &[f64]) -> i32 {
    let mut decile = 1;
    for threshold in thresholds {
        if prob > *threshold {
            decile += 1;
        }
    }
    decile.clamp(1, 10)
}

fn brier_score(probs: &[f64], labels: &[bool]) -> f64 {
    if probs.is_empty() {
        return 0.0;
    }
    probs
        .iter()
        .zip(labels.iter())
        .map(|(p, y)| {
            let target = if *y { 1.0 } else { 0.0 };
            let diff = p - target;
            diff * diff
        })
        .sum::<f64>()
        / probs.len() as f64
}

fn auc_score(probs: &[f64], labels: &[bool]) -> f64 {
    let pos = labels.iter().filter(|v| **v).count();
    let neg = labels.len().saturating_sub(pos);
    if pos == 0 || neg == 0 {
        return 0.5;
    }
    let mut pairs = probs
        .iter()
        .copied()
        .zip(labels.iter().copied())
        .collect::<Vec<_>>();
    pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
    let mut rank_sum = 0.0;
    for (idx, (_, label)) in pairs.iter().enumerate() {
        if *label {
            rank_sum += (idx + 1) as f64;
        }
    }
    (rank_sum - (pos * (pos + 1) / 2) as f64) / (pos * neg) as f64
}

fn sigmoid(v: f64) -> f64 {
    if v >= 0.0 {
        1.0 / (1.0 + (-v).exp())
    } else {
        let e = v.exp();
        e / (1.0 + e)
    }
}

fn dot(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn ln_pos(v: f64) -> f64 {
    if v > 0.0 {
        v.ln().clamp(-4.0, 4.0)
    } else {
        0.0
    }
}

fn clamp(v: f64, lo: f64, hi: f64) -> f64 {
    if v.is_finite() {
        v.clamp(lo, hi)
    } else {
        0.0
    }
}

fn board_scope(ts_code: &str, name: &str) -> &'static str {
    if name.contains("ST") {
        "st_5cm"
    } else if ts_code.starts_with("300") || ts_code.starts_with("301") {
        "chinext_20cm"
    } else if ts_code.starts_with("688") {
        "star_20cm"
    } else if ts_code.starts_with('8') || ts_code.starts_with('4') || ts_code.starts_with('9') {
        "bse_30cm"
    } else {
        "mainboard_10cm"
    }
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logistic_model_learns_separable_signal() {
        let rows = (0..80)
            .map(|idx| {
                let x = idx as f64 / 10.0 - 4.0;
                ModelRow {
                    feature_date: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
                    symbol: format!("{idx:06}.SZ"),
                    name: String::new(),
                    industry: String::new(),
                    board_scope: "mainboard_10cm".to_string(),
                    features: vec![x; FEATURE_NAMES.len()],
                    next_day_limit_up: Some(x > 0.0),
                    next_day_touch_limit: Some(x > 0.0),
                    next_day_failed_board: Some(false),
                    next_day_ret_pct: Some(x),
                    next_day_drawdown_pct: Some(0.0),
                }
            })
            .collect::<Vec<_>>();
        let standardizer = Standardizer::fit(&rows);
        let model = train_binary_model(
            &rows,
            &standardizer,
            |row| row.next_day_limit_up.unwrap_or(false),
            0.01,
            100,
        );
        let low = model.predict(&standardizer.transform(&rows[5].features));
        let high = model.predict(&standardizer.transform(&rows[75].features));
        assert!(high > low + 0.35);
    }

    #[test]
    fn out_of_scope_prediction_stays_heat_only() {
        let row = ModelRow {
            feature_date: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            symbol: "300001.SZ".to_string(),
            name: String::new(),
            industry: String::new(),
            board_scope: "chinext_20cm".to_string(),
            features: vec![0.0; FEATURE_NAMES.len()],
            next_day_limit_up: None,
            next_day_touch_limit: None,
            next_day_failed_board: None,
            next_day_ret_pct: None,
            next_day_drawdown_pct: None,
        };
        assert_eq!(decision_state(&row, 10, 0.30, 0.40, 0.01, 1.0), "heat_only");
    }
}
