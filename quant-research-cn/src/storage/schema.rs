/// DuckDB table definitions for A-share pipeline.
/// All tables use INSERT OR REPLACE for idempotent upserts.
pub const CREATE_TABLES: &str = "
    CREATE TABLE IF NOT EXISTS prices (
        ts_code     VARCHAR NOT NULL,
        trade_date  DATE NOT NULL,
        open        DOUBLE,
        high        DOUBLE,
        low         DOUBLE,
        close       DOUBLE,
        pre_close   DOUBLE,
        change      DOUBLE,
        pct_chg     DOUBLE,
        vol         DOUBLE,
        amount      DOUBLE,
        adj_factor  DOUBLE,
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS daily_basic (
        ts_code       VARCHAR NOT NULL,
        trade_date    DATE NOT NULL,
        turnover_rate DOUBLE,
        volume_ratio  DOUBLE,
        pe            DOUBLE,
        pe_ttm        DOUBLE,
        pb            DOUBLE,
        ps_ttm        DOUBLE,
        total_mv      DOUBLE,
        circ_mv       DOUBLE,
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS forecast (
        ts_code        VARCHAR NOT NULL,
        ann_date       DATE NOT NULL,
        end_date       DATE NOT NULL,
        forecast_type  VARCHAR NOT NULL,
        p_change_min   DOUBLE,
        p_change_max   DOUBLE,
        net_profit_min DOUBLE,
        net_profit_max DOUBLE,
        summary        VARCHAR,
        PRIMARY KEY (ts_code, ann_date, end_date)
    );

    CREATE TABLE IF NOT EXISTS margin_detail (
        ts_code    VARCHAR NOT NULL,
        trade_date DATE NOT NULL,
        rzye       DOUBLE,
        rzmre      DOUBLE,
        rzche      DOUBLE,
        rqye       DOUBLE,
        rqmcl      DOUBLE,
        rqchl      DOUBLE,
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS northbound_flow (
        trade_date  DATE NOT NULL,
        buy_amount  DOUBLE,
        sell_amount DOUBLE,
        net_amount  DOUBLE,
        source      VARCHAR,
        PRIMARY KEY (trade_date, source)
    );

    CREATE TABLE IF NOT EXISTS block_trade (
        ts_code    VARCHAR NOT NULL,
        trade_date DATE NOT NULL,
        price      DOUBLE,
        vol        DOUBLE,
        amount     DOUBLE,
        buyer      VARCHAR,
        seller     VARCHAR,
        premium    DOUBLE,
        PRIMARY KEY (ts_code, trade_date, buyer, seller)
    );

    CREATE TABLE IF NOT EXISTS top_list (
        ts_code      VARCHAR NOT NULL,
        trade_date   DATE NOT NULL,
        reason       VARCHAR,
        buy_amount   DOUBLE,
        sell_amount  DOUBLE,
        net_amount   DOUBLE,
        broker_name  VARCHAR,
        PRIMARY KEY (ts_code, trade_date, broker_name)
    );

    CREATE TABLE IF NOT EXISTS share_unlock (
        ts_code      VARCHAR NOT NULL,
        ann_date     DATE,
        float_date   DATE NOT NULL,
        float_share  DOUBLE,
        float_ratio  DOUBLE,
        holder_name  VARCHAR,
        share_type   VARCHAR,
        PRIMARY KEY (ts_code, float_date, holder_name)
    );

    CREATE TABLE IF NOT EXISTS macro_cn (
        date        DATE NOT NULL,
        series_id   VARCHAR NOT NULL,
        series_name VARCHAR,
        value       DOUBLE,
        PRIMARY KEY (date, series_id)
    );

    CREATE TABLE IF NOT EXISTS analytics (
        ts_code     VARCHAR NOT NULL,
        as_of       DATE NOT NULL,
        module      VARCHAR NOT NULL,
        metric      VARCHAR NOT NULL,
        value       DOUBLE,
        detail      VARCHAR,
        PRIMARY KEY (ts_code, as_of, module, metric)
    );

    CREATE TABLE IF NOT EXISTS hmm_forecasts (
        forecast_id VARCHAR NOT NULL PRIMARY KEY,
        as_of       DATE NOT NULL,
        horizon     VARCHAR NOT NULL,
        p_predicted DOUBLE NOT NULL,
        actual      INTEGER,
        resolved    BOOLEAN DEFAULT FALSE
    );

    CREATE TABLE IF NOT EXISTS index_weight (
        index_code  VARCHAR NOT NULL,
        con_code    VARCHAR NOT NULL,
        trade_date  DATE NOT NULL,
        weight      DOUBLE,
        PRIMARY KEY (index_code, con_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS opt_daily (
        ts_code      VARCHAR NOT NULL,
        trade_date   DATE NOT NULL,
        exchange     VARCHAR,
        pre_settle   DOUBLE,
        pre_close    DOUBLE,
        open         DOUBLE,
        high         DOUBLE,
        low          DOUBLE,
        close        DOUBLE,
        settle       DOUBLE,
        vol          DOUBLE,
        amount       DOUBLE,
        oi           DOUBLE,
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS opt_basic (
        ts_code        VARCHAR NOT NULL PRIMARY KEY,
        name           VARCHAR,
        call_put       VARCHAR,
        exercise_price DOUBLE,
        maturity_date  DATE,
        list_date      DATE,
        delist_date    DATE,
        opt_code       VARCHAR,
        per_unit       DOUBLE,
        exercise_type  VARCHAR
    );

    CREATE TABLE IF NOT EXISTS sge_daily (
        ts_code      VARCHAR NOT NULL,
        trade_date   DATE NOT NULL,
        close        DOUBLE,
        open         DOUBLE,
        high         DOUBLE,
        low          DOUBLE,
        price_avg    DOUBLE,
        change       DOUBLE,
        pct_change   DOUBLE,
        vol          DOUBLE,
        amount       DOUBLE,
        oi           DOUBLE,
        PRIMARY KEY (ts_code, trade_date)
    );

    -- ── Financial statements ──────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS income (
        ts_code      VARCHAR NOT NULL,
        ann_date     DATE,
        end_date     DATE NOT NULL,
        revenue      DOUBLE,
        n_income     DOUBLE,
        basic_eps    DOUBLE,
        diluted_eps  DOUBLE,
        PRIMARY KEY (ts_code, end_date)
    );

    CREATE TABLE IF NOT EXISTS balancesheet (
        ts_code                     VARCHAR NOT NULL,
        ann_date                    DATE,
        end_date                    DATE NOT NULL,
        total_assets                DOUBLE,
        total_liab                  DOUBLE,
        total_hldr_eqy_exc_min_int  DOUBLE,
        PRIMARY KEY (ts_code, end_date)
    );

    CREATE TABLE IF NOT EXISTS cashflow (
        ts_code              VARCHAR NOT NULL,
        ann_date             DATE,
        end_date             DATE NOT NULL,
        n_cashflow_act       DOUBLE,
        n_cashflow_inv_act   DOUBLE,
        n_cash_flows_fnc_act DOUBLE,
        PRIMARY KEY (ts_code, end_date)
    );

    CREATE TABLE IF NOT EXISTS fina_indicator (
        ts_code         VARCHAR NOT NULL,
        ann_date        DATE,
        end_date        DATE NOT NULL,
        roe             DOUBLE,
        roa             DOUBLE,
        debt_to_assets  DOUBLE,
        current_ratio   DOUBLE,
        quick_ratio     DOUBLE,
        eps             DOUBLE,
        bps             DOUBLE,
        cfps            DOUBLE,
        netprofit_yoy   DOUBLE,
        or_yoy          DOUBLE,
        PRIMARY KEY (ts_code, end_date)
    );

    CREATE TABLE IF NOT EXISTS dividend (
        ts_code        VARCHAR NOT NULL,
        end_date       DATE NOT NULL,
        ann_date       DATE,
        div_proc       VARCHAR,
        stk_div        DOUBLE,
        cash_div       DOUBLE,
        cash_div_tax   DOUBLE,
        record_date    VARCHAR,
        ex_date        VARCHAR,
        pay_date       VARCHAR,
        PRIMARY KEY (ts_code, end_date)
    );

    -- ── Flow & positioning ───────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS moneyflow (
        ts_code          VARCHAR NOT NULL,
        trade_date       DATE NOT NULL,
        buy_sm_vol       DOUBLE,
        buy_sm_amount    DOUBLE,
        sell_sm_vol      DOUBLE,
        sell_sm_amount   DOUBLE,
        buy_md_vol       DOUBLE,
        buy_md_amount    DOUBLE,
        sell_md_vol      DOUBLE,
        sell_md_amount   DOUBLE,
        buy_lg_vol       DOUBLE,
        buy_lg_amount    DOUBLE,
        sell_lg_vol      DOUBLE,
        sell_lg_amount   DOUBLE,
        buy_elg_vol      DOUBLE,
        buy_elg_amount   DOUBLE,
        sell_elg_vol     DOUBLE,
        sell_elg_amount  DOUBLE,
        net_mf_vol       DOUBLE,
        net_mf_amount    DOUBLE,
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS hsgt_top10 (
        trade_date   DATE NOT NULL,
        ts_code      VARCHAR NOT NULL,
        name         VARCHAR,
        close        DOUBLE,
        rank         INTEGER,
        market_type  VARCHAR NOT NULL,
        amount       DOUBLE,
        net_amount   DOUBLE,
        buy          DOUBLE,
        sell         DOUBLE,
        PRIMARY KEY (trade_date, market_type, ts_code)
    );

    CREATE TABLE IF NOT EXISTS hk_hold (
        trade_date   DATE NOT NULL,
        ts_code      VARCHAR NOT NULL,
        name         VARCHAR,
        vol          DOUBLE,
        ratio        DOUBLE,
        exchange     VARCHAR NOT NULL,
        PRIMARY KEY (trade_date, exchange, ts_code)
    );

    -- ── Events & corporate actions ──────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS disclosure_date (
        ts_code      VARCHAR NOT NULL,
        ann_date     VARCHAR,
        end_date     DATE NOT NULL,
        pre_date     VARCHAR,
        actual_date  VARCHAR,
        modify_date  VARCHAR,
        PRIMARY KEY (ts_code, end_date)
    );

    CREATE TABLE IF NOT EXISTS stk_holdertrade (
        ts_code      VARCHAR NOT NULL,
        ann_date     DATE NOT NULL,
        holder_name  VARCHAR NOT NULL,
        holder_type  VARCHAR,
        in_de        VARCHAR,
        change_vol   DOUBLE,
        change_ratio DOUBLE,
        after_share  DOUBLE,
        after_ratio  DOUBLE,
        PRIMARY KEY (ts_code, ann_date, holder_name)
    );

    CREATE TABLE IF NOT EXISTS pledge_detail (
        ts_code        VARCHAR NOT NULL,
        ann_date       VARCHAR,
        holder_name    VARCHAR NOT NULL,
        pledge_amount  DOUBLE,
        start_date     VARCHAR,
        end_date       VARCHAR,
        is_release     VARCHAR,
        PRIMARY KEY (ts_code, holder_name)
    );

    CREATE TABLE IF NOT EXISTS repurchase (
        ts_code   VARCHAR NOT NULL,
        ann_date  DATE NOT NULL,
        end_date  VARCHAR,
        proc      VARCHAR,
        exp_date  VARCHAR,
        vol       DOUBLE,
        amount    DOUBLE,
        PRIMARY KEY (ts_code, ann_date)
    );

    CREATE TABLE IF NOT EXISTS stk_holdernumber (
        ts_code      VARCHAR NOT NULL,
        ann_date     DATE,
        end_date     DATE NOT NULL,
        holder_num   BIGINT,
        PRIMARY KEY (ts_code, end_date)
    );

    -- ── Stock names & industry mapping ────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS stock_basic (
        ts_code     VARCHAR NOT NULL PRIMARY KEY,
        symbol      VARCHAR,
        name        VARCHAR,
        area        VARCHAR,
        industry    VARCHAR,
        market      VARCHAR,
        list_date   VARCHAR,
        list_status VARCHAR
    );

    -- ── Industry & universe ─────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS industry_classify (
        index_code     VARCHAR NOT NULL PRIMARY KEY,
        industry_name  VARCHAR,
        level          VARCHAR,
        is_pub         VARCHAR
    );

    CREATE TABLE IF NOT EXISTS fund_portfolio (
        ts_code          VARCHAR NOT NULL,
        ann_date         DATE NOT NULL,
        end_date         DATE NOT NULL,
        symbol           VARCHAR NOT NULL,
        mkv              DOUBLE,
        amount           DOUBLE,
        stk_mkv_ratio    DOUBLE,
        PRIMARY KEY (ts_code, end_date, symbol)
    );

    -- ── Derivatives & futures ───────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS cb_daily (
        ts_code      VARCHAR NOT NULL,
        trade_date   DATE NOT NULL,
        close        DOUBLE,
        open         DOUBLE,
        high         DOUBLE,
        low          DOUBLE,
        vol          DOUBLE,
        amount       DOUBLE,
        cb_value     DOUBLE,
        cb_over_rate DOUBLE,
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS fut_daily (
        ts_code      VARCHAR NOT NULL,
        trade_date   DATE NOT NULL,
        open         DOUBLE,
        high         DOUBLE,
        low          DOUBLE,
        close        DOUBLE,
        settle       DOUBLE,
        vol          DOUBLE,
        amount       DOUBLE,
        oi           DOUBLE,
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS fut_holding (
        ts_code      VARCHAR NOT NULL,
        trade_date   DATE NOT NULL,
        broker       VARCHAR NOT NULL,
        vol          DOUBLE,
        vol_chg      DOUBLE,
        long_hld     DOUBLE,
        long_chg     DOUBLE,
        short_hld    DOUBLE,
        short_chg    DOUBLE,
        PRIMARY KEY (ts_code, trade_date, broker)
    );

    -- ── AKShare exclusive (概念板块, 行业资金流向, 个股新闻) ──────────────────
    CREATE TABLE IF NOT EXISTS concept_board (
        trade_date    DATE NOT NULL,
        board_name    VARCHAR NOT NULL,
        board_code    VARCHAR,
        pct_chg       DOUBLE,
        turnover_rate DOUBLE,
        total_mv      DOUBLE,
        amount        DOUBLE,
        up_count      INTEGER,
        down_count    INTEGER,
        lead_stock    VARCHAR,
        lead_pct      DOUBLE,
        PRIMARY KEY (trade_date, board_name)
    );

    CREATE TABLE IF NOT EXISTS sector_fund_flow (
        trade_date    DATE NOT NULL,
        sector_name   VARCHAR NOT NULL,
        pct_chg       DOUBLE,
        main_net_in   DOUBLE,
        main_net_pct  DOUBLE,
        super_net_in  DOUBLE,
        big_net_in    DOUBLE,
        mid_net_in    DOUBLE,
        small_net_in  DOUBLE,
        PRIMARY KEY (trade_date, sector_name)
    );

    CREATE TABLE IF NOT EXISTS stock_news (
        ts_code       VARCHAR NOT NULL,
        publish_time  VARCHAR NOT NULL,
        title         VARCHAR NOT NULL,
        content       VARCHAR,
        source        VARCHAR,
        url           VARCHAR,
        PRIMARY KEY (ts_code, publish_time, title)
    );

    -- ── Meta ────────────────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS run_log (
        run_id    VARCHAR NOT NULL,
        step      VARCHAR NOT NULL,
        status    VARCHAR NOT NULL,
        rows      INTEGER DEFAULT 0,
        detail    VARCHAR,
        ts        TIMESTAMP DEFAULT current_timestamp
    );

    -- ── Report review ledger ────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS report_decisions (
        report_date            DATE NOT NULL,
        session                VARCHAR NOT NULL,
        symbol                 VARCHAR NOT NULL,
        selection_status       VARCHAR NOT NULL,
        rank_order             INTEGER,
        report_bucket          VARCHAR,
        signal_direction       VARCHAR,
        signal_confidence      VARCHAR,
        composite_score        DOUBLE,
        execution_mode         VARCHAR,
        execution_score        DOUBLE,
        max_chase_gap_pct      DOUBLE,
        pullback_trigger_pct   DOUBLE,
        setup_score            DOUBLE,
        continuation_score     DOUBLE,
        fade_risk              DOUBLE,
        reference_close        DOUBLE,
        details_json           VARCHAR,
        created_at             TIMESTAMP DEFAULT current_timestamp,
        PRIMARY KEY (report_date, session, symbol, selection_status)
    );

    CREATE TABLE IF NOT EXISTS report_outcomes (
        report_date            DATE NOT NULL,
        session                VARCHAR NOT NULL,
        symbol                 VARCHAR NOT NULL,
        selection_status       VARCHAR NOT NULL,
        evaluation_date        DATE NOT NULL,
        next_trade_date        DATE,
        second_trade_date      DATE,
        reference_close        DOUBLE,
        next_open              DOUBLE,
        next_close             DOUBLE,
        best_high_2d           DOUBLE,
        worst_low_2d           DOUBLE,
        next_open_ret_pct      DOUBLE,
        next_close_ret_pct     DOUBLE,
        best_up_2d_pct         DOUBLE,
        best_down_2d_pct       DOUBLE,
        gap_vs_chase_limit     DOUBLE,
        data_ready             BOOLEAN DEFAULT FALSE,
        PRIMARY KEY (report_date, session, symbol, selection_status)
    );

    CREATE TABLE IF NOT EXISTS alpha_postmortem (
        report_date            DATE NOT NULL,
        session                VARCHAR NOT NULL,
        symbol                 VARCHAR NOT NULL,
        selection_status       VARCHAR NOT NULL,
        evaluation_date        DATE NOT NULL,
        label                  VARCHAR NOT NULL,
        review_note            VARCHAR,
        factor_feedback_action VARCHAR,
        factor_feedback_weight DOUBLE,
        best_ret_pct           DOUBLE,
        next_open_ret_pct      DOUBLE,
        next_close_ret_pct     DOUBLE,
        gap_vs_chase_limit     DOUBLE,
        PRIMARY KEY (report_date, session, symbol, selection_status)
    );

    CREATE TABLE IF NOT EXISTS algorithm_postmortem (
        report_date            DATE NOT NULL,
        session                VARCHAR NOT NULL,
        symbol                 VARCHAR NOT NULL,
        selection_status       VARCHAR NOT NULL,
        evaluation_date        DATE NOT NULL,
        action_label           VARCHAR NOT NULL,
        action_source          VARCHAR,
        direction              VARCHAR,
        direction_right        BOOLEAN,
        executable             BOOLEAN,
        fill_price             DOUBLE,
        exit_price             DOUBLE,
        realized_pnl_pct       DOUBLE,
        best_possible_ret_pct  DOUBLE,
        stale_chase            BOOLEAN,
        no_fill_reason         VARCHAR,
        label                  VARCHAR NOT NULL,
        feedback_action        VARCHAR,
        feedback_weight        DOUBLE,
        action_intent          VARCHAR,
        calibration_bucket     VARCHAR,
        regime_bucket          VARCHAR,
        fill_quality           VARCHAR,
        detail_json            VARCHAR,
        PRIMARY KEY (report_date, session, symbol, selection_status)
    );

    CREATE TABLE IF NOT EXISTS paper_trades (
        report_date            DATE NOT NULL,
        session                VARCHAR NOT NULL,
        symbol                 VARCHAR NOT NULL,
        selection_status       VARCHAR NOT NULL,
        strategy_family        VARCHAR NOT NULL,
        strategy_key           VARCHAR NOT NULL,
        execution_rule         VARCHAR NOT NULL,
        action_intent          VARCHAR NOT NULL,
        evaluation_date        DATE NOT NULL,
        reference_close        DOUBLE,
        planned_entry          DOUBLE,
        fill_date              DATE,
        fill_price             DOUBLE,
        exit_date              DATE,
        exit_price             DOUBLE,
        fill_status            VARCHAR NOT NULL,
        realized_ret_pct       DOUBLE,
        max_favorable_pct      DOUBLE,
        max_adverse_pct        DOUBLE,
        shadow_alpha_prob      DOUBLE,
        downside_stress        DOUBLE,
        stale_chase_risk       DOUBLE,
        flow_conflict_flag     BOOLEAN DEFAULT FALSE,
        label                  VARCHAR NOT NULL,
        detail_json            VARCHAR,
        updated_at             TIMESTAMP DEFAULT current_timestamp,
        PRIMARY KEY (report_date, session, symbol, selection_status, execution_rule)
    );

    CREATE TABLE IF NOT EXISTS strategy_ev (
        as_of                  DATE NOT NULL,
        strategy_key           VARCHAR NOT NULL,
        strategy_family        VARCHAR NOT NULL,
        samples                INTEGER,
        planned_trades         INTEGER,
        fills                  INTEGER,
        wins                   INTEGER,
        losses                 INTEGER,
        fill_rate              DOUBLE,
        win_rate_raw           DOUBLE,
        win_rate_bayes         DOUBLE,
        avg_win_pct            DOUBLE,
        avg_loss_pct           DOUBLE,
        avg_tail_loss_pct      DOUBLE,
        avg_downside_stress    DOUBLE,
        ev_pct                 DOUBLE,
        risk_unit_pct          DOUBLE,
        ev_per_risk            DOUBLE,
        ev_norm_score          DOUBLE,
        eligible              BOOLEAN DEFAULT FALSE,
        fail_reasons           VARCHAR,
        detail_json            VARCHAR,
        updated_at             TIMESTAMP DEFAULT current_timestamp,
        PRIMARY KEY (as_of, strategy_key)
    );

    ALTER TABLE strategy_ev ADD COLUMN IF NOT EXISTS risk_unit_pct DOUBLE;
    ALTER TABLE strategy_ev ADD COLUMN IF NOT EXISTS ev_per_risk DOUBLE;
    ALTER TABLE strategy_ev ADD COLUMN IF NOT EXISTS ev_norm_score DOUBLE;
";
