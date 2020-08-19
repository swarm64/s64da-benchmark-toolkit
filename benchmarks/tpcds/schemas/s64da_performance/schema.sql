-- round(double precision, int) does not exist by default
CREATE FUNCTION round(double precision, integer) RETURNS NUMERIC AS $$
    SELECT round($1::numeric, $2)
$$ LANGUAGE sql;

CREATE TABLE customer_address
(
    ca_address_sk             INTEGER               NOT NULL,
    ca_address_id             VARCHAR(16)           NOT NULL,
    ca_street_number          VARCHAR(10),
    ca_street_name            VARCHAR(60),
    ca_street_type            VARCHAR(15),
    ca_suite_number           VARCHAR(10),
    ca_city                   VARCHAR(60),
    ca_county                 VARCHAR(30),
    ca_state                  VARCHAR(2),
    ca_zip                    VARCHAR(10),
    ca_country                VARCHAR(20),
    ca_gmt_offset             DOUBLE PRECISION,
    ca_location_type          VARCHAR(20)
);

CREATE TABLE customer_demographics
(
    cd_demo_sk                INTEGER               NOT NULL,
    cd_gender                 "char",
    cd_marital_status         "char",
    cd_education_status       VARCHAR(20),
    cd_purchase_estimate      BIGINT,
    cd_credit_rating          VARCHAR(10),
    cd_dep_count              BIGINT,
    cd_dep_employed_count     BIGINT,
    cd_dep_college_count      BIGINT
);

CREATE TABLE date_dim
(
    d_date_sk                 INTEGER               NOT NULL,
    d_date_id                 VARCHAR(16)           NOT NULL,
    d_date                    DATE,
    d_month_seq               BIGINT,
    d_week_seq                BIGINT,
    d_quarter_seq             BIGINT,
    d_year                    BIGINT,
    d_dow                     BIGINT,
    d_moy                     BIGINT,
    d_dom                     BIGINT,
    d_qoy                     BIGINT,
    d_fy_year                 BIGINT,
    d_fy_quarter_seq          BIGINT,
    d_fy_week_seq             BIGINT,
    d_day_name                VARCHAR(9),
    d_quarter_name            VARCHAR(6),
    d_holiday                 "char",
    d_weekend                 "char",
    d_following_holiday       "char",
    d_first_dom               BIGINT,
    d_last_dom                BIGINT,
    d_same_day_ly             BIGINT,
    d_same_day_lq             BIGINT,
    d_current_day             "char",
    d_current_week            "char",
    d_current_month           "char",
    d_current_quarter         "char",
    d_current_year            "char"
);

CREATE TABLE warehouse
(
    w_warehouse_sk            INTEGER               NOT NULL,
    w_warehouse_id            VARCHAR(16)           NOT NULL,
    w_warehouse_name          VARCHAR(20),
    w_warehouse_sq_ft         BIGINT,
    w_street_number           VARCHAR(10),
    w_street_name             VARCHAR(60),
    w_street_type             VARCHAR(15),
    w_suite_number            VARCHAR(10),
    w_city                    VARCHAR(60),
    w_county                  VARCHAR(30),
    w_state                   VARCHAR(2),
    w_zip                     VARCHAR(10),
    w_country                 VARCHAR(20),
    w_gmt_offset              DOUBLE PRECISION
);

CREATE TABLE ship_mode
(
    sm_ship_mode_sk           INTEGER               NOT NULL,
    sm_ship_mode_id           VARCHAR(16)           NOT NULL,
    sm_type                   VARCHAR(30),
    sm_code                   VARCHAR(10),
    sm_carrier                VARCHAR(20),
    sm_contract               VARCHAR(20)
);

CREATE TABLE time_dim
(
    t_time_sk                 INTEGER               NOT NULL,
    t_time_id                 VARCHAR(16)           NOT NULL,
    t_time                    BIGINT,
    t_hour                    BIGINT,
    t_minute                  BIGINT,
    t_second                  BIGINT,
    t_am_pm                   VARCHAR(2),
    t_shift                   VARCHAR(20),
    t_sub_shift               VARCHAR(20),
    t_meal_time               VARCHAR(20)
);

CREATE TABLE reason
(
    r_reason_sk               INTEGER               NOT NULL,
    r_reason_id               VARCHAR(16)           NOT NULL,
    r_reason_desc             VARCHAR(100)
);

CREATE TABLE income_band
(
    ib_income_band_sk         INTEGER               NOT NULL,
    ib_lower_bound            BIGINT,
    ib_upper_bound            BIGINT
);

CREATE TABLE item
(
    i_item_sk                 INTEGER               NOT NULL,
    i_item_id                 VARCHAR(16)           NOT NULL,
    i_rec_start_date          DATE,
    i_rec_end_date            DATE,
    i_item_desc               VARCHAR(200),
    i_current_price           DOUBLE PRECISION,
    i_wholesale_cost          DOUBLE PRECISION,
    i_brand_id                BIGINT,
    i_brand                   VARCHAR(50),
    i_class_id                BIGINT,
    i_class                   VARCHAR(50),
    i_category_id             BIGINT,
    i_category                VARCHAR(50),
    i_manufact_id             BIGINT,
    i_manufact                VARCHAR(50),
    i_size                    VARCHAR(20),
    i_formulation             VARCHAR(20),
    i_color                   VARCHAR(20),
    i_units                   VARCHAR(10),
    i_container               VARCHAR(10),
    i_manager_id              BIGINT,
    i_product_name            VARCHAR(50)
);

CREATE TABLE store
(
    s_store_sk                INTEGER               NOT NULL,
    s_store_id                VARCHAR(16)           NOT NULL,
    s_rec_start_date          DATE,
    s_rec_end_date            DATE,
    s_closed_date_sk          INTEGER,
    s_store_name              VARCHAR(50),
    s_number_employees        BIGINT,
    s_floor_space             BIGINT,
    s_hours                   VARCHAR(20),
    s_manager                 VARCHAR(40),
    s_market_id               BIGINT,
    s_geography_class         VARCHAR(100),
    s_market_desc             VARCHAR(100),
    s_market_manager          VARCHAR(40),
    s_division_id             BIGINT,
    s_division_name           VARCHAR(50),
    s_company_id              BIGINT,
    s_company_name            VARCHAR(50),
    s_street_number           VARCHAR(10),
    s_street_name             VARCHAR(60),
    s_street_type             VARCHAR(15),
    s_suite_number            VARCHAR(10),
    s_city                    VARCHAR(60),
    s_county                  VARCHAR(30),
    s_state                   VARCHAR(2),
    s_zip                     VARCHAR(10),
    s_country                 VARCHAR(20),
    s_gmt_offset              DOUBLE PRECISION,
    s_tax_precentage          DOUBLE PRECISION
);

CREATE TABLE call_center
(
    cc_call_center_sk         INTEGER               NOT NULL,
    cc_call_center_id         VARCHAR(16)           NOT NULL,
    cc_rec_start_date         DATE,
    cc_rec_end_date           DATE,
    cc_closed_date_sk         INTEGER,
    cc_open_date_sk           INTEGER,
    cc_name                   VARCHAR(50),
    cc_class                  VARCHAR(50),
    cc_employees              BIGINT,
    cc_sq_ft                  BIGINT,
    cc_hours                  VARCHAR(20),
    cc_manager                VARCHAR(40),
    cc_mkt_id                 BIGINT,
    cc_mkt_class              VARCHAR(50),
    cc_mkt_desc               VARCHAR(100),
    cc_market_manager         VARCHAR(40),
    cc_division               BIGINT,
    cc_division_name          VARCHAR(50),
    cc_company                BIGINT,
    cc_company_name           VARCHAR(50),
    cc_street_number          VARCHAR(10),
    cc_street_name            VARCHAR(60),
    cc_street_type            VARCHAR(15),
    cc_suite_number           VARCHAR(10),
    cc_city                   VARCHAR(60),
    cc_county                 VARCHAR(30),
    cc_state                  VARCHAR(2),
    cc_zip                    VARCHAR(10),
    cc_country                VARCHAR(20),
    cc_gmt_offset             DOUBLE PRECISION,
    cc_tax_percentage         DOUBLE PRECISION
);

CREATE TABLE customer
(
    c_customer_sk             INTEGER               NOT NULL,
    c_customer_id             VARCHAR(16)           NOT NULL,
    c_current_cdemo_sk        INTEGER,
    c_current_hdemo_sk        INTEGER,
    c_current_addr_sk         INTEGER,
    c_first_shipto_date_sk    INTEGER,
    c_first_sales_date_sk     INTEGER,
    c_salutation              VARCHAR(10),
    c_first_name              VARCHAR(20),
    c_last_name               VARCHAR(30),
    c_preferred_cust_flag     "char",
    c_birth_day               BIGINT,
    c_birth_month             BIGINT,
    c_birth_year              BIGINT,
    c_birth_country           VARCHAR(20),
    c_login                   VARCHAR(13),
    c_email_address           VARCHAR(50),
    c_last_review_date_sk     INTEGER
);

CREATE TABLE web_site
(
    web_site_sk               INTEGER               NOT NULL,
    web_site_id               VARCHAR(16)           NOT NULL,
    web_rec_start_date        DATE,
    web_rec_end_date          DATE,
    web_name                  VARCHAR(50),
    web_open_date_sk          INTEGER,
    web_close_date_sk         INTEGER,
    web_class                 VARCHAR(50),
    web_manager               VARCHAR(40),
    web_mkt_id                BIGINT,
    web_mkt_class             VARCHAR(50),
    web_mkt_desc              VARCHAR(100),
    web_market_manager        VARCHAR(40),
    web_company_id            BIGINT,
    web_company_name          VARCHAR(50),
    web_street_number         VARCHAR(10),
    web_street_name           VARCHAR(60),
    web_street_type           VARCHAR(15),
    web_suite_number          VARCHAR(10),
    web_city                  VARCHAR(60),
    web_county                VARCHAR(30),
    web_state                 VARCHAR(2),
    web_zip                   VARCHAR(10),
    web_country               VARCHAR(20),
    web_gmt_offset            DOUBLE PRECISION,
    web_tax_percentage        DOUBLE PRECISION
);

CREATE TABLE store_returns
(
    sr_returned_date_sk       INTEGER,
    sr_return_time_sk         INTEGER,
    sr_item_sk                INTEGER               NOT NULL,
    sr_customer_sk            INTEGER,
    sr_cdemo_sk               INTEGER,
    sr_hdemo_sk               INTEGER,
    sr_addr_sk                INTEGER,
    sr_store_sk               INTEGER,
    sr_reason_sk              INTEGER,
    sr_ticket_number          INTEGER               NOT NULL,
    sr_return_quantity        BIGINT,
    sr_return_amt             DOUBLE PRECISION,
    sr_return_tax             DOUBLE PRECISION,
    sr_return_amt_inc_tax     DOUBLE PRECISION,
    sr_fee                    DOUBLE PRECISION,
    sr_return_ship_cost       DOUBLE PRECISION,
    sr_refunded_cash          DOUBLE PRECISION,
    sr_reversed_charge        DOUBLE PRECISION,
    sr_store_credit           DOUBLE PRECISION,
    sr_net_loss               DOUBLE PRECISION
);

CREATE TABLE household_demographics
(
    hd_demo_sk                INTEGER               NOT NULL,
    hd_income_band_sk         INTEGER,
    hd_buy_potential          VARCHAR(15),
    hd_dep_count              BIGINT,
    hd_vehicle_count          BIGINT
);

CREATE TABLE web_page
(
    wp_web_page_sk            INTEGER               NOT NULL,
    wp_web_page_id            VARCHAR(16)           NOT NULL,
    wp_rec_start_date         DATE,
    wp_rec_end_date           DATE,
    wp_creation_date_sk       INTEGER,
    wp_access_date_sk         INTEGER,
    wp_autogen_flag           "char",
    wp_customer_sk            INTEGER,
    wp_url                    VARCHAR(100),
    wp_type                   VARCHAR(50),
    wp_char_count             BIGINT,
    wp_link_count             BIGINT,
    wp_image_count            BIGINT,
    wp_max_ad_count           BIGINT
);

CREATE TABLE promotion
(
    p_promo_sk                INTEGER               NOT NULL,
    p_promo_id                VARCHAR(16)           NOT NULL,
    p_start_date_sk           INTEGER,
    p_end_date_sk             INTEGER,
    p_item_sk                 INTEGER,
    p_cost                    DECIMAL(15,2),
    p_response_target         BIGINT,
    p_promo_name              VARCHAR(50),
    p_channel_dmail           "char",
    p_channel_email           "char",
    p_channel_catalog         "char",
    p_channel_tv              "char",
    p_channel_radio           "char",
    p_channel_press           "char",
    p_channel_event           "char",
    p_channel_demo            "char",
    p_channel_details         VARCHAR(100),
    p_purpose                 VARCHAR(15),
    p_discount_active         "char"
);

CREATE TABLE catalog_page
(
    cp_catalog_page_sk        INTEGER               NOT NULL,
    cp_catalog_page_id        VARCHAR(16)           NOT NULL,
    cp_start_date_sk          INTEGER,
    cp_end_date_sk            INTEGER,
    cp_department             VARCHAR(50),
    cp_catalog_number         BIGINT,
    cp_catalog_page_number    BIGINT,
    cp_description            VARCHAR(100),
    cp_type                   VARCHAR(100)
);

CREATE TABLE inventory
(
    inv_date_sk               INTEGER               NOT NULL,
    inv_item_sk               INTEGER               NOT NULL,
    inv_warehouse_sk          INTEGER               NOT NULL,
    inv_quantity_on_hand      BIGINT
);

CREATE TABLE catalog_returns
(
    cr_returned_date_sk       INTEGER,
    cr_returned_time_sk       INTEGER,
    cr_item_sk                INTEGER               NOT NULL,
    cr_refunded_customer_sk   INTEGER,
    cr_refunded_cdemo_sk      INTEGER,
    cr_refunded_hdemo_sk      INTEGER,
    cr_refunded_addr_sk       INTEGER,
    cr_returning_customer_sk  INTEGER,
    cr_returning_cdemo_sk     INTEGER,
    cr_returning_hdemo_sk     INTEGER,
    cr_returning_addr_sk      INTEGER,
    cr_call_center_sk         INTEGER,
    cr_catalog_page_sk        INTEGER,
    cr_ship_mode_sk           INTEGER,
    cr_warehouse_sk           INTEGER,
    cr_reason_sk              INTEGER,
    cr_order_number           INTEGER               NOT NULL,
    cr_return_quantity        BIGINT,
    cr_return_amount          DOUBLE PRECISION,
    cr_return_tax             DOUBLE PRECISION,
    cr_return_amt_inc_tax     DOUBLE PRECISION,
    cr_fee                    DOUBLE PRECISION,
    cr_return_ship_cost       DOUBLE PRECISION,
    cr_refunded_cash          DOUBLE PRECISION,
    cr_reversed_charge        DOUBLE PRECISION,
    cr_store_credit           DOUBLE PRECISION,
    cr_net_loss               DOUBLE PRECISION
);

CREATE TABLE web_returns
(
    wr_returned_date_sk       INTEGER,
    wr_returned_time_sk       INTEGER,
    wr_item_sk                INTEGER               NOT NULL,
    wr_refunded_customer_sk   INTEGER,
    wr_refunded_cdemo_sk      INTEGER,
    wr_refunded_hdemo_sk      INTEGER,
    wr_refunded_addr_sk       INTEGER,
    wr_returning_customer_sk  INTEGER,
    wr_returning_cdemo_sk     INTEGER,
    wr_returning_hdemo_sk     INTEGER,
    wr_returning_addr_sk      INTEGER,
    wr_web_page_sk            INTEGER,
    wr_reason_sk              INTEGER,
    wr_order_number           INTEGER               NOT NULL,
    wr_return_quantity        BIGINT,
    wr_return_amt             DOUBLE PRECISION,
    wr_return_tax             DOUBLE PRECISION,
    wr_return_amt_inc_tax     DOUBLE PRECISION,
    wr_fee                    DOUBLE PRECISION,
    wr_return_ship_cost       DOUBLE PRECISION,
    wr_refunded_cash          DOUBLE PRECISION,
    wr_reversed_charge        DOUBLE PRECISION,
    wr_account_credit         DOUBLE PRECISION,
    wr_net_loss               DOUBLE PRECISION
);

CREATE TABLE web_sales
(
    ws_sold_date_sk           INTEGER,
    ws_sold_time_sk           INTEGER,
    ws_ship_date_sk           INTEGER,
    ws_item_sk                INTEGER               NOT NULL,
    ws_bill_customer_sk       INTEGER,
    ws_bill_cdemo_sk          INTEGER,
    ws_bill_hdemo_sk          INTEGER,
    ws_bill_addr_sk           INTEGER,
    ws_ship_customer_sk       INTEGER,
    ws_ship_cdemo_sk          INTEGER,
    ws_ship_hdemo_sk          INTEGER,
    ws_ship_addr_sk           INTEGER,
    ws_web_page_sk            INTEGER,
    ws_web_site_sk            INTEGER,
    ws_ship_mode_sk           INTEGER,
    ws_warehouse_sk           INTEGER,
    ws_promo_sk               INTEGER,
    ws_order_number           INTEGER               NOT NULL,
    ws_quantity               BIGINT,
    ws_wholesale_cost         DOUBLE PRECISION,
    ws_list_price             DOUBLE PRECISION,
    ws_sales_price            DOUBLE PRECISION,
    ws_ext_discount_amt       DOUBLE PRECISION,
    ws_ext_sales_price        DOUBLE PRECISION,
    ws_ext_wholesale_cost     DOUBLE PRECISION,
    ws_ext_list_price         DOUBLE PRECISION,
    ws_ext_tax                DOUBLE PRECISION,
    ws_coupon_amt             DOUBLE PRECISION,
    ws_ext_ship_cost          DOUBLE PRECISION,
    ws_net_paid               DOUBLE PRECISION,
    ws_net_paid_inc_tax       DOUBLE PRECISION,
    ws_net_paid_inc_ship      DOUBLE PRECISION,
    ws_net_paid_inc_ship_tax  DOUBLE PRECISION,
    ws_net_profit             DOUBLE PRECISION
);

CREATE TABLE catalog_sales
(
    cs_sold_date_sk           INTEGER,
    cs_sold_time_sk           INTEGER,
    cs_ship_date_sk           INTEGER,
    cs_bill_customer_sk       INTEGER,
    cs_bill_cdemo_sk          INTEGER,
    cs_bill_hdemo_sk          INTEGER,
    cs_bill_addr_sk           INTEGER,
    cs_ship_customer_sk       INTEGER,
    cs_ship_cdemo_sk          INTEGER,
    cs_ship_hdemo_sk          INTEGER,
    cs_ship_addr_sk           INTEGER,
    cs_call_center_sk         INTEGER,
    cs_catalog_page_sk        INTEGER,
    cs_ship_mode_sk           INTEGER,
    cs_warehouse_sk           INTEGER,
    cs_item_sk                INTEGER               NOT NULL,
    cs_promo_sk               INTEGER,
    cs_order_number           INTEGER               NOT NULL,
    cs_quantity               BIGINT,
    cs_wholesale_cost         DOUBLE PRECISION,
    cs_list_price             DOUBLE PRECISION,
    cs_sales_price            DOUBLE PRECISION,
    cs_ext_discount_amt       DOUBLE PRECISION,
    cs_ext_sales_price        DOUBLE PRECISION,
    cs_ext_wholesale_cost     DOUBLE PRECISION,
    cs_ext_list_price         DOUBLE PRECISION,
    cs_ext_tax                DOUBLE PRECISION,
    cs_coupon_amt             DOUBLE PRECISION,
    cs_ext_ship_cost          DOUBLE PRECISION,
    cs_net_paid               DOUBLE PRECISION,
    cs_net_paid_inc_tax       DOUBLE PRECISION,
    cs_net_paid_inc_ship      DOUBLE PRECISION,
    cs_net_paid_inc_ship_tax  DOUBLE PRECISION,
    cs_net_profit             DOUBLE PRECISION
);

CREATE TABLE store_sales
(
    ss_sold_date_sk           INTEGER,
    ss_sold_time_sk           INTEGER,
    ss_item_sk                INTEGER               NOT NULL,
    ss_customer_sk            INTEGER,
    ss_cdemo_sk               INTEGER,
    ss_hdemo_sk               INTEGER,
    ss_addr_sk                INTEGER,
    ss_store_sk               INTEGER,
    ss_promo_sk               INTEGER,
    ss_ticket_number          INTEGER               NOT NULL,
    ss_quantity               BIGINT,
    ss_wholesale_cost         DOUBLE PRECISION,
    ss_list_price             DOUBLE PRECISION,
    ss_sales_price            DOUBLE PRECISION,
    ss_ext_discount_amt       DOUBLE PRECISION,
    ss_ext_sales_price        DOUBLE PRECISION,
    ss_ext_wholesale_cost     DOUBLE PRECISION,
    ss_ext_list_price         DOUBLE PRECISION,
    ss_ext_tax                DOUBLE PRECISION,
    ss_coupon_amt             DOUBLE PRECISION,
    ss_net_paid               DOUBLE PRECISION,
    ss_net_paid_inc_tax       DOUBLE PRECISION,
    ss_net_profit             DOUBLE PRECISION
);

CREATE INDEX customer_address_cache ON customer_address USING columnstore (
    ca_address_sk,
    ca_address_id,
    ca_street_number,
    ca_street_name,
    ca_street_type,
    ca_suite_number,
    ca_city,
    ca_county,
    ca_state,
    ca_zip,
    ca_country,
    ca_gmt_offset,
    ca_location_type
);

CREATE INDEX customer_demographics_cache ON customer_demographics USING columnstore (
    cd_demo_sk,
    cd_gender,
    cd_marital_status,
    cd_education_status,
    cd_purchase_estimate,
    cd_credit_rating,
    cd_dep_count,
    cd_dep_employed_count,
    cd_dep_college_count
);

CREATE INDEX date_dim_cache ON date_dim USING columnstore (
    d_date_sk,
    d_date_id,
    d_date,
    d_month_seq,
    d_week_seq,
    d_quarter_seq,
    d_year,
    d_dow,
    d_moy,
    d_dom,
    d_qoy,
    d_fy_year,
    d_fy_quarter_seq,
    d_fy_week_seq,
    d_day_name,
    d_quarter_name,
    d_holiday,
    d_weekend,
    d_following_holiday,
    d_first_dom,
    d_last_dom,
    d_same_day_ly,
    d_same_day_lq,
    d_current_day,
    d_current_week,
    d_current_month,
    d_current_quarter,
    d_current_year
);

CREATE INDEX warehouse_cache ON warehouse USING columnstore (
    w_warehouse_sk,
    w_warehouse_id,
    w_warehouse_name,
    w_warehouse_sq_ft,
    w_street_number,
    w_street_name,
    w_street_type,
    w_suite_number,
    w_city,
    w_county,
    w_state,
    w_zip,
    w_country,
    w_gmt_offset
);

CREATE INDEX ship_mode_cache ON ship_mode USING columnstore (
    sm_ship_mode_sk,
    sm_ship_mode_id,
    sm_type,
    sm_code,
    sm_carrier,
    sm_contract
);

CREATE INDEX time_dim_cache ON time_dim USING columnstore (
    t_time_sk,
    t_time_id,
    t_time,
    t_hour,
    t_minute,
    t_second,
    t_am_pm,
    t_shift,
    t_sub_shift,
    t_meal_time
);

CREATE INDEX reason_cache ON reason USING columnstore (
    r_reason_sk,
    r_reason_id,
    r_reason_desc
);

CREATE INDEX income_band_cache ON income_band USING columnstore (
    ib_income_band_sk,
    ib_lower_bound,
    ib_upper_bound
);

CREATE INDEX item_cache ON item USING columnstore (
    i_item_sk,
    i_item_id,
    i_rec_start_date,
    i_rec_end_date,
    i_item_desc,
    i_current_price,
    i_wholesale_cost,
    i_brand_id,
    i_brand,
    i_class_id,
    i_class,
    i_category_id,
    i_category,
    i_manufact_id,
    i_manufact,
    i_size,
    i_formulation,
    i_color,
    i_units,
    i_container,
    i_manager_id,
    i_product_name
);

CREATE INDEX store_cache ON store USING columnstore (
    s_store_sk,
    s_store_id,
    s_rec_start_date,
    s_rec_end_date,
    s_closed_date_sk,
    s_store_name,
    s_number_employees,
    s_floor_space,
    s_hours,
    s_manager,
    s_market_id,
    s_geography_class,
    s_market_desc,
    s_market_manager,
    s_division_id,
    s_division_name,
    s_company_id,
    s_company_name,
    s_street_number,
    s_street_name,
    s_street_type,
    s_suite_number,
    s_city,
    s_county,
    s_state,
    s_zip,
    s_country,
    s_gmt_offset,
    s_tax_precentage
);

CREATE INDEX call_center_cache ON call_center USING columnstore (
    cc_call_center_sk,
    cc_call_center_id,
    cc_rec_start_date,
    cc_rec_end_date,
    cc_closed_date_sk,
    cc_open_date_sk,
    cc_name,
    cc_class,
    cc_employees,
    cc_sq_ft,
    cc_hours,
    cc_manager,
    cc_mkt_id,
    cc_mkt_class,
    cc_mkt_desc,
    cc_market_manager,
    cc_division,
    cc_division_name,
    cc_company,
    cc_company_name,
    cc_street_number,
    cc_street_name,
    cc_street_type,
    cc_suite_number,
    cc_city,
    cc_county,
    cc_state,
    cc_zip,
    cc_country,
    cc_gmt_offset,
    cc_tax_percentage
);

CREATE INDEX customer_cache ON customer USING columnstore (
    c_customer_sk,
    c_customer_id,
    c_current_cdemo_sk,
    c_current_hdemo_sk,
    c_current_addr_sk,
    c_first_shipto_date_sk,
    c_first_sales_date_sk,
    c_salutation,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_day,
    c_birth_month,
    c_birth_year,
    c_birth_country,
    c_login,
    c_email_address,
    c_last_review_date_sk
);

CREATE INDEX web_site_cache ON web_site USING columnstore (
    web_site_sk,
    web_site_id,
    web_rec_start_date,
    web_rec_end_date,
    web_name,
    web_open_date_sk,
    web_close_date_sk,
    web_class,
    web_manager,
    web_mkt_id,
    web_mkt_class,
    web_mkt_desc,
    web_market_manager,
    web_company_id,
    web_company_name,
    web_street_number,
    web_street_name,
    web_street_type,
    web_suite_number,
    web_city,
    web_county,
    web_state,
    web_zip,
    web_country,
    web_gmt_offset,
    web_tax_percentage
);


CREATE INDEX household_demographics_cache ON household_demographics USING columnstore (
    hd_demo_sk,
    hd_income_band_sk,
    hd_buy_potential,
    hd_dep_count,
    hd_vehicle_count
);

CREATE INDEX web_page_cache ON web_page USING columnstore (
    wp_web_page_sk,
    wp_web_page_id,
    wp_rec_start_date,
    wp_rec_end_date,
    wp_creation_date_sk,
    wp_access_date_sk,
    wp_autogen_flag,
    wp_customer_sk,
    wp_url,
    wp_type,
    wp_char_count,
    wp_link_count,
    wp_image_count,
    wp_max_ad_count
);

CREATE INDEX promotion_cache ON promotion USING columnstore (
    p_promo_sk,
    p_promo_id,
    p_start_date_sk,
    p_end_date_sk,
    p_item_sk,
    p_cost,
    p_response_target,
    p_promo_name,
    p_channel_dmail,
    p_channel_email,
    p_channel_catalog,
    p_channel_tv,
    p_channel_radio,
    p_channel_press,
    p_channel_event,
    p_channel_demo,
    p_channel_details,
    p_purpose,
    p_discount_active
);

CREATE INDEX catalog_page_cache ON catalog_page USING columnstore (
    cp_catalog_page_sk,
    cp_catalog_page_id,
    cp_start_date_sk,
    cp_end_date_sk,
    cp_department,
    cp_catalog_number,
    cp_catalog_page_number,
    cp_description,
    cp_type
);

CREATE INDEX inventory_cache ON inventory USING columnstore (
    inv_date_sk,
    inv_item_sk,
    inv_warehouse_sk,
    inv_quantity_on_hand
);

CREATE INDEX store_returns_cache ON store_returns USING columnstore (
    sr_returned_date_sk,
    sr_return_time_sk,
    sr_item_sk,
    sr_customer_sk,
    sr_cdemo_sk,
    sr_hdemo_sk,
    sr_addr_sk,
    sr_store_sk,
    sr_reason_sk,
    sr_ticket_number,
    sr_return_quantity,
    sr_return_amt,
    sr_return_tax,
    sr_return_amt_inc_tax,
    sr_fee,
    sr_return_ship_cost,
    sr_refunded_cash,
    sr_reversed_charge,
    sr_store_credit,
    sr_net_loss
);

CREATE INDEX catalog_returns_cache ON catalog_returns USING columnstore (
    cr_returned_date_sk,
    cr_returned_time_sk,
    cr_item_sk,
    cr_refunded_customer_sk,
    cr_refunded_cdemo_sk,
    cr_refunded_hdemo_sk,
    cr_refunded_addr_sk,
    cr_returning_customer_sk,
    cr_returning_cdemo_sk,
    cr_returning_hdemo_sk,
    cr_returning_addr_sk,
    cr_call_center_sk,
    cr_catalog_page_sk,
    cr_ship_mode_sk,
    cr_warehouse_sk,
    cr_reason_sk,
    cr_order_number,
    cr_return_quantity,
    cr_return_amount,
    cr_return_tax,
    cr_return_amt_inc_tax,
    cr_fee,
    cr_return_ship_cost,
    cr_refunded_cash,
    cr_reversed_charge,
    cr_store_credit,
    cr_net_loss
);

CREATE INDEX web_returns_cache ON web_returns USING columnstore (
    wr_returned_date_sk,
    wr_returned_time_sk,
    wr_item_sk,
    wr_refunded_customer_sk,
    wr_refunded_cdemo_sk,
    wr_refunded_hdemo_sk,
    wr_refunded_addr_sk,
    wr_returning_customer_sk,
    wr_returning_cdemo_sk,
    wr_returning_hdemo_sk,
    wr_returning_addr_sk,
    wr_web_page_sk,
    wr_reason_sk,
    wr_order_number,
    wr_return_quantity,
    wr_return_amt,
    wr_return_tax,
    wr_return_amt_inc_tax,
    wr_fee,
    wr_return_ship_cost,
    wr_refunded_cash,
    wr_reversed_charge,
    wr_account_credit,
    wr_net_loss
);

-- columns removed from the sales tables
CREATE INDEX web_sales_cache ON web_sales USING columnstore (
    ws_sold_date_sk,
    ws_sold_time_sk,
    ws_ship_date_sk,
    ws_item_sk,
    ws_bill_customer_sk,
    ws_bill_addr_sk,
    ws_ship_hdemo_sk,
    ws_ship_addr_sk,
    ws_web_page_sk,
    ws_web_site_sk,
    ws_ship_mode_sk,
    ws_warehouse_sk,
    ws_promo_sk,
    ws_order_number,
    ws_quantity,
    ws_wholesale_cost,
    ws_list_price,
    ws_sales_price,
    ws_ext_discount_amt,
    ws_ext_sales_price,
    ws_ext_wholesale_cost,
    ws_ext_list_price,
    ws_ext_ship_cost,
    ws_net_paid,
    ws_net_paid_inc_ship_tax,
    ws_net_profit
);

CREATE INDEX catalog_sales_cache ON catalog_sales USING columnstore (
    cs_sold_date_sk,
    cs_sold_time_sk,
    cs_ship_date_sk,
    cs_bill_customer_sk,
    cs_bill_cdemo_sk,
    cs_bill_hdemo_sk,
    cs_bill_addr_sk,
    cs_ship_customer_sk,
    cs_ship_addr_sk,
    cs_call_center_sk,
    cs_catalog_page_sk,
    cs_ship_mode_sk,
    cs_warehouse_sk,
    cs_item_sk,
    cs_promo_sk,
    cs_order_number,
    cs_quantity,
    cs_wholesale_cost,
    cs_list_price,
    cs_sales_price,
    cs_ext_discount_amt,
    cs_ext_sales_price,
    cs_ext_wholesale_cost,
    cs_ext_list_price,
    cs_coupon_amt,
    cs_ext_ship_cost,
    cs_net_paid,
    cs_net_paid_inc_ship_tax,
    cs_net_profit
);

CREATE INDEX store_sales_cache ON store_sales USING columnstore (
    ss_sold_date_sk,
    ss_sold_time_sk,
    ss_item_sk,
    ss_customer_sk,
    ss_cdemo_sk,
    ss_hdemo_sk,
    ss_addr_sk,
    ss_store_sk,
    ss_promo_sk,
    ss_ticket_number,
    ss_quantity,
    ss_wholesale_cost,
    ss_list_price,
    ss_sales_price,
    ss_ext_discount_amt,
    ss_ext_sales_price,
    ss_ext_wholesale_cost,
    ss_ext_list_price,
    ss_ext_tax,
    ss_coupon_amt,
    ss_net_paid,
    ss_net_profit
);
