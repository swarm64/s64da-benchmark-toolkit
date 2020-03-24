-- All statistics on usage are per query, so 1x WHERE means 1 query had a WHERE clause on that column
-- For WHERE clauses the complex join queries (ANTI join between two results) were not analyzed.
-- Whenever a "~" is used it was counted using a simple grep instead of manually inspecting all queries

-- Statistics:
-- - Date is almost always filtered on
-- - Item is often filtered on
-- - Store is often joined but not filtered
-- - Customer is filtered via address quite often (22x)

-- TODO:
-- - Better unique constraints that allow group-by clauses to be simplified

{% set num_partitions = num_partitions|int %}

-- round(double precision, int) does not exist by default
CREATE FUNCTION round(double precision, integer) RETURNS NUMERIC AS $$
    SELECT round($1::numeric, $2)
$$ LANGUAGE sql;

CREATE EXTENSION swarm64da;

-- 1TB = 6M records
CREATE TABLE customer_address
(
    ca_address_sk             INTEGER               NOT NULL, -- JOIN column
    ca_address_id             VARCHAR(16)           NOT NULL, -- not used
    ca_street_number          VARCHAR(10),                    -- 
    ca_street_name            VARCHAR(60),                    --
    ca_street_type            VARCHAR(15),                    --
    ca_suite_number           VARCHAR(10),                    --
    ca_city                   VARCHAR(60),                    -- 1x WHERE
    ca_county                 VARCHAR(30),                    -- 1x WHERE IN, 3x AGG
    ca_state                  VARCHAR(2),                     -- 11x WHERE (4x IN), 3x AGG
    ca_zip                    VARCHAR(10),                    -- 3x WHERE SUBSTR
    ca_country                VARCHAR(20),                    -- 2x WHERE, 1x AGG
    ca_gmt_offset             DECIMAL(5,2),                   -- 5x WHERE
    ca_location_type          VARCHAR(20)                     --
) PARTITION BY HASH(ca_address_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    customer_address_prt_{{ partition_idx }}
PARTITION OF
   customer_address FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'ca_address_sk, ca_state');
{% endfor %}

-- 1TB = 2M records
CREATE TABLE customer_demographics
(
    cd_demo_sk                INTEGER               NOT NULL, -- JOIN
    cd_gender                 "char",                         -- 4x WHERE, 3x AGG
    cd_marital_status         "char",                         -- 8x WHERE, 4x AGG
    cd_education_status       VARCHAR(20),                    -- 8x WHERE, 3x AGG
    cd_purchase_estimate      BIGINT,                         -- 2x AGG
    cd_credit_rating          VARCHAR(10),                    -- 2x AGG
    cd_dep_count              BIGINT,                         -- 2x AGG
    cd_dep_employed_count     BIGINT,                         -- 2x AGG
    cd_dep_college_count      BIGINT                          -- 2x AGG
) PARTITION BY HASH(cd_demo_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    customer_demographics_prt_{{ partition_idx }}
PARTITION OF
   customer_demographics FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'cd_demo_sk');
{% endfor %}

-- contains 200 YEARS of data (~73k records)
CREATE TABLE date_dim
(
    d_date_sk                 INTEGER               NOT NULL, --
    d_date_id                 VARCHAR(16)           NOT NULL, --
    d_date                    DATE,                           -- 12x WHERE, 1x AGG
    d_month_seq               BIGINT,                         -- 9x WHERE
    d_week_seq                BIGINT,                         --
    d_quarter_seq             BIGINT,                         --
    d_year                    BIGINT,                         -- 45x WHERE (1x IN), 2x AGG
    d_dow                     BIGINT,                         -- 2x WHERE
    d_moy                     BIGINT,                         -- 21x WHERE
    d_dom                     BIGINT,                         -- 3x WHERE 
    d_qoy                     BIGINT,                         -- 3x WHERE, 2x AGG
    d_fy_year                 BIGINT,                         --
    d_fy_quarter_seq          BIGINT,                         --
    d_fy_week_seq             BIGINT,                         --
    d_day_name                VARCHAR(9),                     --
    d_quarter_name            VARCHAR(6),                     -- 1x WHERE
    d_holiday                 "char",                         --
    d_weekend                 "char",                         --
    d_following_holiday       "char",                         --
    d_first_dom               BIGINT,                         --
    d_last_dom                BIGINT,                         --
    d_same_day_ly             BIGINT,                         --
    d_same_day_lq             BIGINT,                         --
    d_current_day             "char",                         --
    d_current_week            "char",                         --
    d_current_month           "char",                         --
    d_current_quarter         "char",                         --
    d_current_year            "char"                          --
) PARTITION BY HASH(d_date_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    date_dim_prt_{{ partition_idx }}
PARTITION OF
   date_dim FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'd_date_sk');
{% endfor %}

-- 20 records
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
) PARTITION BY HASH(w_warehouse_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    warehouse_prt_{{ partition_idx }}
PARTITION OF
   warehouse FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'w_warehouse_sk');
{% endfor %}

-- 20 records
CREATE TABLE ship_mode
(
    sm_ship_mode_sk           INTEGER               NOT NULL,
    sm_ship_mode_id           VARCHAR(16)           NOT NULL,
    sm_type                   VARCHAR(30),
    sm_code                   VARCHAR(10),
    sm_carrier                VARCHAR(20),
    sm_contract               VARCHAR(20)
) PARTITION BY HASH(sm_ship_mode_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    ship_mode_prt_{{ partition_idx }}
PARTITION OF
   ship_mode FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'sm_ship_mode_sk');
{% endfor %}


-- 86k records, one per sec
CREATE TABLE time_dim
(
    t_time_sk                 INTEGER               NOT NULL, -- 
    t_time_id                 VARCHAR(16)           NOT NULL, -- 
    t_time                    BIGINT,                         -- 1x WHERE
    t_hour                    BIGINT,                         -- 3x WHERE
    t_minute                  BIGINT,                         -- 2x WHERE
    t_second                  BIGINT,                         -- 
    t_am_pm                   VARCHAR(2),                     -- 
    t_shift                   VARCHAR(20),                    -- 
    t_sub_shift               VARCHAR(20),                    -- 
    t_meal_time               VARCHAR(20)                     -- 
) PARTITION BY HASH(t_time_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    time_dim_prt_{{ partition_idx }}
PARTITION OF
   time_dim FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 't_time_sk');
{% endfor %}

-- 65 records
CREATE TABLE reason
(
    r_reason_sk               INTEGER               NOT NULL,
    r_reason_id               VARCHAR(16)           NOT NULL,
    r_reason_desc             VARCHAR(100)
) PARTITION BY HASH(r_reason_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    reason_prt_{{ partition_idx }}
PARTITION OF
   reason FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'r_reason_sk');
{% endfor %}

-- 20 records
CREATE TABLE income_band
(
    ib_income_band_sk         INTEGER               NOT NULL,
    ib_lower_bound            BIGINT,
    ib_upper_bound            BIGINT
) PARTITION BY HASH(ib_income_band_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    income_band_prt_{{ partition_idx }}
PARTITION OF
   income_band FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'ib_lower_bound, ib_upper_bound');
{% endfor %}

-- 1TB = 300k records
CREATE TABLE item
(
    i_item_sk                 INTEGER               NOT NULL, -- JOIN, 1x WHERE IN
    i_item_id                 VARCHAR(16)           NOT NULL, -- 15x AGG
    i_rec_start_date          DATE,                           -- 
    i_rec_end_date            DATE,                           -- 
    i_item_desc               VARCHAR(200),                   -- 8x AGG
    i_current_price           DECIMAL(7,2),                   -- 4x WHERE, 4x AGG
    i_wholesale_cost          DECIMAL(7,2),                   -- 
    i_brand_id                BIGINT,                         -- 7x AGG
    i_brand                   VARCHAR(50),                    -- 1x WHERE (1x IN) 9x AGG
    i_class_id                BIGINT,                         -- 2x AGG
    i_class                   VARCHAR(50),                    -- 3x WHERE (2x IN), 8x AGG
    i_category_id             BIGINT,                         -- 3x AGG
    i_category                VARCHAR(50),                    -- 13x WHERE (5x IN), 11x AGG
    i_manufact_id             BIGINT,                         -- 5x WHERE (1x IN), 4x AGG
    i_manufact                VARCHAR(50),                    -- 1x AGG
    i_size                    VARCHAR(20),                    -- 1x WHERE
    i_formulation             VARCHAR(20),                    -- 
    i_color                   VARCHAR(20),                    -- 5x WHERE (1x IN)
    i_units                   VARCHAR(10),                    -- 1x WHERE
    i_container               VARCHAR(10),                    -- 
    i_manager_id              BIGINT,                         -- 3x WHERE, 1x AGG
    i_product_name            VARCHAR(50)                     -- 2x AGG
) PARTITION BY HASH(i_item_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    item_prt_{{ partition_idx }}
PARTITION OF
   item FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'i_item_sk');
{% endfor %}

-- 1k records
CREATE TABLE store
(
    s_store_sk                INTEGER               NOT NULL, -- JOIN
    s_store_id                VARCHAR(16)           NOT NULL, --
    s_rec_start_date          DATE,                           -- 
    s_rec_end_date            DATE,                           -- 
    s_closed_date_sk          INTEGER,                        -- 
    s_store_name              VARCHAR(50),                    -- 2x WHERE
    s_number_employees        BIGINT,                         -- 
    s_floor_space             BIGINT,                         -- 
    s_hours                   VARCHAR(20),                    -- 
    s_manager                 VARCHAR(40),                    -- 
    s_market_id               BIGINT,                         -- 1x WHERE
    s_geography_class         VARCHAR(100),                   -- 
    s_market_desc             VARCHAR(100),                   -- 
    s_market_manager          VARCHAR(40),                    -- 
    s_division_id             BIGINT,                         -- 
    s_division_name           VARCHAR(50),                    -- 
    s_company_id              BIGINT,                         -- 
    s_company_name            VARCHAR(50),                    -- 
    s_street_number           VARCHAR(10),                    -- 
    s_street_name             VARCHAR(60),                    -- 
    s_street_type             VARCHAR(15),                    -- 
    s_suite_number            VARCHAR(10),                    -- 
    s_city                    VARCHAR(60),                    -- 1x WHERE
    s_county                  VARCHAR(30),                    -- 1x WHERE
    s_state                   VARCHAR(2),                     -- 3x WHERE
    s_zip                     VARCHAR(10),                    -- 
    s_country                 VARCHAR(20),                    -- 
    s_gmt_offset              DECIMAL(5,2),                   -- 2x WHERE
    s_tax_precentage          DECIMAL(5,2)                    -- 
) PARTITION BY HASH(s_store_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    store_prt_{{ partition_idx }}
PARTITION OF
    store FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 's_store_sk, s_market_id');
{% endfor %}

-- 42 records
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
) PARTITION BY HASH(cc_call_center_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    call_center_prt_{{ partition_idx }}
PARTITION OF
    call_center FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'cc_call_center_sk');
{% endfor %}

-- 1TB = 12M records
CREATE TABLE customer
(
    c_customer_sk             INTEGER               NOT NULL, -- 64x JOIN
    c_customer_id             VARCHAR(16)           NOT NULL, -- 
    c_current_cdemo_sk        INTEGER,                        -- 7x JOIN
    c_current_hdemo_sk        INTEGER,                        -- 3x JOIN
    c_current_addr_sk         INTEGER,                        -- 22x JOIN
    c_first_shipto_date_sk    INTEGER,                        -- 1x JOIN
    c_first_sales_date_sk     INTEGER,                        -- 1x JOIN
    c_salutation              VARCHAR(10),                    -- 
    c_first_name              VARCHAR(20),                    -- 
    c_last_name               VARCHAR(30),                    -- 
    c_preferred_cust_flag     "char",                         -- 1x WHERE
    c_birth_day               BIGINT,                         -- 
    c_birth_month             BIGINT,                         -- 1x WHERE (1x IN)
    c_birth_year              BIGINT,                         -- 
    c_birth_country           VARCHAR(20),                    -- 
    c_login                   VARCHAR(13),                    -- 
    c_email_address           VARCHAR(50),                    -- 
    c_last_review_date_sk     INTEGER                         -- 
) PARTITION BY HASH(c_customer_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    customer_prt_{{ partition_idx }}
PARTITION OF
    customer FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'c_customer_sk');
{% endfor %}

-- 54 records
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
) PARTITION BY HASH(web_site_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    web_site_prt_{{ partition_idx }}
PARTITION OF
    web_site FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'web_site_sk');
{% endfor %}

-- 1TB = 289M records
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

-- 7200 records
CREATE TABLE household_demographics
(
    hd_demo_sk                INTEGER               NOT NULL,
    hd_income_band_sk         INTEGER,
    hd_buy_potential          VARCHAR(15),
    hd_dep_count              BIGINT,
    hd_vehicle_count          BIGINT
) PARTITION BY HASH(hd_demo_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    household_demographics_prt_{{ partition_idx }}
PARTITION OF
    household_demographics FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'hd_vehicle_count');
{% endfor %}

-- 3000 records
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
) PARTITION BY HASH(wp_web_page_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    web_page_prt_{{ partition_idx }}
PARTITION OF
   web_page FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'wp_web_page_sk');
{% endfor %}

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
) PARTITION BY HASH(p_promo_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    promotion_prt_{{ partition_idx }}
PARTITION OF
    promotion FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'p_promo_sk');
{% endfor %}

-- 1TB = 30k records
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
) PARTITION BY HASH(cp_catalog_page_sk);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    catalog_page_prt_{{ partition_idx }}
PARTITION OF
    catalog_page FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER  {{ partition_idx }})
SERVER
   swarm64da_server OPTIONS (optimized_columns 'cp_catalog_page_sk');
{% endfor %}

-- 1TB = 783M records
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

-- 1TB = 143M records
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

-- 1TB = 71M records
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

-- 1TB = 720M records
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

-- 1TB = 1.4B records
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

-- 1TB = 2.8B records
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
