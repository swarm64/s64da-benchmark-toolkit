{% set num_partitions = num_partitions|int %}

-- round(double precision, int) does not exist by default
CREATE FUNCTION round(double precision, integer) RETURNS NUMERIC AS $$
    SELECT round($1::numeric, $2)
$$ LANGUAGE sql;

CREATE EXTENSION swarm64da;

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
    ca_gmt_offset             DECIMAL(5,2),
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
    w_gmt_offset              DECIMAL(5,2)
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
    i_current_price           DECIMAL(7,2),
    i_wholesale_cost          DECIMAL(7,2),
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
    s_gmt_offset              DECIMAL(5,2),
    s_tax_precentage          DECIMAL(5,2)
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
    cc_gmt_offset             DECIMAL(5,2),
    cc_tax_percentage         DECIMAL(5,2)
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
    web_gmt_offset            DECIMAL(5,2),
    web_tax_percentage        DECIMAL(5,2)
);

CREATE TABLE store_returns
(
    sr_returned_date_sk       INTEGER,                        -- ~16x JOIN
    sr_return_time_sk         INTEGER,                        --
    sr_item_sk                INTEGER               NOT NULL, -- ~16x JOIN
    sr_customer_sk            INTEGER,                        -- ~9x JOIN
    sr_cdemo_sk               INTEGER,                        -- ~1x JOIN
    sr_hdemo_sk               INTEGER,                        -- 
    sr_addr_sk                INTEGER,                        --
    sr_store_sk               INTEGER,                        -- ~4x JOIN
    sr_reason_sk              INTEGER,                        -- ~1x JOIN
    sr_ticket_number          INTEGER               NOT NULL, --
    sr_return_quantity        BIGINT,                         --
    sr_return_amt             DECIMAL(7,2),                   --
    sr_return_tax             DECIMAL(7,2),                   --
    sr_return_amt_inc_tax     DECIMAL(7,2),                   --
    sr_fee                    DECIMAL(7,2),                   --
    sr_return_ship_cost       DECIMAL(7,2),                   --
    sr_refunded_cash          DECIMAL(7,2),                   --
    sr_reversed_charge        DECIMAL(7,2),                   --
    sr_store_credit           DECIMAL(7,2),                   --
    sr_net_loss               DECIMAL(7,2)                    --
) PARTITION BY HASH(sr_item_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    store_returns_prt_{{ partition_idx }}
PARTITION OF
    store_returns FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server options(optimized_columns 'sr_returned_date_sk, sr_item_sk', optimization_level_target '900');
{% endfor %}

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
    inv_date_sk               INTEGER               NOT NULL, -- 7x JOIN
    inv_item_sk               INTEGER               NOT NULL, -- 7x JOIN
    inv_warehouse_sk          INTEGER               NOT NULL, -- 4x JOIN
    inv_quantity_on_hand      BIGINT                          -- 3x WHERE
) PARTITION BY HASH(inv_date_sk, inv_item_sk, inv_warehouse_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    inventory_prt_{{ partition_idx }}
PARTITION OF
    inventory FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server options(optimized_columns 'inv_date_sk, inv_item_sk, inv_warehouse_sk', optimization_level_target '900');
{% endfor %}

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
    cr_return_amount          DECIMAL(7,2),
    cr_return_tax             DECIMAL(7,2),
    cr_return_amt_inc_tax     DECIMAL(7,2),
    cr_fee                    DECIMAL(7,2),
    cr_return_ship_cost       DECIMAL(7,2),
    cr_refunded_cash          DECIMAL(7,2),
    cr_reversed_charge        DECIMAL(7,2),
    cr_store_credit           DECIMAL(7,2),
    cr_net_loss               DECIMAL(7,2)
) PARTITION BY HASH(cr_item_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    catalog_returns_prt_{{ partition_idx }}
PARTITION OF
    catalog_returns FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server options(optimized_columns 'cr_returned_date_sk, cr_item_sk', optimization_level_target '900');
{% endfor %}

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
    wr_return_amt             DECIMAL(7,2),
    wr_return_tax             DECIMAL(7,2),
    wr_return_amt_inc_tax     DECIMAL(7,2),
    wr_fee                    DECIMAL(7,2),
    wr_return_ship_cost       DECIMAL(7,2),
    wr_refunded_cash          DECIMAL(7,2),
    wr_reversed_charge        DECIMAL(7,2),
    wr_account_credit         DECIMAL(7,2),
    wr_net_loss               DECIMAL(7,2)
) PARTITION BY HASH(wr_item_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    web_returns_prt_{{ partition_idx }}
PARTITION OF
    web_returns FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server options(optimized_columns 'wr_returned_date_sk, wr_item_sk', optimization_level_target '900');
{% endfor %}

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
    ws_wholesale_cost         DECIMAL(7,2),
    ws_list_price             DECIMAL(7,2),
    ws_sales_price            DECIMAL(7,2),
    ws_ext_discount_amt       DECIMAL(7,2),
    ws_ext_sales_price        DECIMAL(7,2),
    ws_ext_wholesale_cost     DECIMAL(7,2),
    ws_ext_list_price         DECIMAL(7,2),
    ws_ext_tax                DECIMAL(7,2),
    ws_coupon_amt             DECIMAL(7,2),
    ws_ext_ship_cost          DECIMAL(7,2),
    ws_net_paid               DECIMAL(7,2),
    ws_net_paid_inc_tax       DECIMAL(7,2),
    ws_net_paid_inc_ship      DECIMAL(7,2),
    ws_net_paid_inc_ship_tax  DECIMAL(7,2),
    ws_net_profit             DECIMAL(7,2)
)
PARTITION BY HASH(ws_item_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    web_sales_prt_{{ partition_idx }}
PARTITION OF
    web_sales FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server options(optimized_columns 'ws_sold_date_sk, ws_item_sk', optimization_level_target '900');
{% endfor %}

CREATE TABLE catalog_sales
(
    cs_sold_date_sk           INTEGER,                         -- ~50x JOIN
    cs_sold_time_sk           INTEGER,                         -- ~2x JOIN
    cs_ship_date_sk           INTEGER,                         -- ~11x JOIN
    cs_bill_customer_sk       INTEGER,                         -- ~16x JOIN
    cs_bill_cdemo_sk          INTEGER,                         -- ~3x JOIN
    cs_bill_hdemo_sk          INTEGER,                         -- ~1x JOIN
    cs_bill_addr_sk           INTEGER,                         -- ~3x JOIN
    cs_ship_customer_sk       INTEGER,                         -- ~5x JOIN
    cs_ship_cdemo_sk          INTEGER,                         -- ~0x JOIN
    cs_ship_hdemo_sk          INTEGER,                         -- ~0x JOIN
    cs_ship_addr_sk           INTEGER,                         -- ~1x JOIN
    cs_call_center_sk         INTEGER,                         -- ~6x JOIN
    cs_catalog_page_sk        INTEGER,                         -- ~2x JOIN
    cs_ship_mode_sk           INTEGER,                         -- ~2x JOIN
    cs_warehouse_sk           INTEGER,                         -- ~4x JOIN
    cs_item_sk                INTEGER               NOT NULL,  -- ~45x JOIN
    cs_promo_sk               INTEGER,                         -- ~3x JOIN
    cs_order_number           INTEGER               NOT NULL,
    cs_quantity               BIGINT,
    cs_wholesale_cost         DECIMAL(7,2),
    cs_list_price             DECIMAL(7,2),
    cs_sales_price            DECIMAL(7,2),
    cs_ext_discount_amt       DECIMAL(7,2),
    cs_ext_sales_price        DECIMAL(7,2),
    cs_ext_wholesale_cost     DECIMAL(7,2),
    cs_ext_list_price         DECIMAL(7,2),
    cs_ext_tax                DECIMAL(7,2),
    cs_coupon_amt             DECIMAL(7,2),
    cs_ext_ship_cost          DECIMAL(7,2),
    cs_net_paid               DECIMAL(7,2),
    cs_net_paid_inc_tax       DECIMAL(7,2),
    cs_net_paid_inc_ship      DECIMAL(7,2),
    cs_net_paid_inc_ship_tax  DECIMAL(7,2),
    cs_net_profit             DECIMAL(7,2)
) PARTITION BY HASH(cs_item_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    catalog_sales_prt_{{ partition_idx }}
PARTITION OF
    catalog_sales FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server options(optimized_columns 'cs_sold_date_sk, cs_item_sk', optimization_level_target '900');
{% endfor %}

CREATE TABLE store_sales
(
    ss_sold_date_sk           INTEGER,                        -- ~80x JOIN
    ss_sold_time_sk           INTEGER,                        -- ~10x JOIN
    ss_item_sk                INTEGER               NOT NULL, -- ~83x JOIN
    ss_customer_sk            INTEGER,                        -- ~51x JOIN
    ss_cdemo_sk               INTEGER,                        -- ~11x JOIN
    ss_hdemo_sk               INTEGER,                        -- ~18x JOIN
    ss_addr_sk                INTEGER,                        -- ~16x JOIN
    ss_store_sk               INTEGER,                        -- ~58x JOIN
    ss_promo_sk               INTEGER,                        --  ~6x JOIN
    ss_ticket_number          INTEGER               NOT NULL,
    ss_quantity               BIGINT,
    ss_wholesale_cost         DECIMAL(7,2),
    ss_list_price             DECIMAL(7,2),
    ss_sales_price            DECIMAL(7,2),
    ss_ext_discount_amt       DECIMAL(7,2),
    ss_ext_sales_price        DECIMAL(7,2),
    ss_ext_wholesale_cost     DECIMAL(7,2),
    ss_ext_list_price         DECIMAL(7,2),
    ss_ext_tax                DECIMAL(7,2),
    ss_coupon_amt             DECIMAL(7,2),
    ss_net_paid               DECIMAL(7,2),
    ss_net_paid_inc_tax       DECIMAL(7,2),
    ss_net_profit             DECIMAL(7,2)
)
PARTITION BY HASH(ss_item_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    store_sales_prt_{{ partition_idx }}
PARTITION OF
    store_sales FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server options(optimized_columns 'ss_item_sk, ss_sold_date_sk', optimization_level_target '900');
{% endfor %}
