create index idx_s_store_id             on store (s_store_id);
create index idx_s_rec_start_date       on store (s_rec_start_date);
create index idx_s_rec_end_date         on store (s_rec_end_date);
create index idx_s_closed_date_sk       on store (s_closed_date_sk);

create index idx_cc_rec_start_date      on call_center (cc_rec_start_date);
create index idx_cc_call_center_id      on call_center (cc_call_center_id);
create index idx_cc_rec_end_date        on call_center (cc_rec_end_date);
create index idx_cc_closed_date_sk      on call_center (cc_closed_date_sk);
create index idx_cc_open_date_sk        on call_center (cc_open_date_sk);

create index idx_cp_catalog_page_id     on catalog_page (cp_catalog_page_id);
create index idx_cp_start_date_sk       on catalog_page (cp_start_date_sk);
create index idx_cp_end_date_sk         on catalog_page (cp_end_date_sk);

create index idx_web_site_id            on web_site (web_site_id);
create index idx_web_rec_start_date     on web_site (web_rec_start_date);
create index idx_web_rec_end_date       on web_site (web_rec_end_date);
create index idx_web_open_date_sk       on web_site (web_open_date_sk);
create index idx_web_close_date_sk      on web_site (web_close_date_sk);

create index idx_wp_web_page_id         on web_page (wp_web_page_id);
create index idx_wp_rec_start_date      on web_page (wp_rec_start_date);
create index idx_wp_rec_end_date        on web_page (wp_rec_end_date);
create index idx_wp_creation_date_sk    on web_page (wp_creation_date_sk);
create index idx_wp_access_date_sk      on web_page (wp_access_date_sk);
create index idx_wp_customer_sk         on web_page (wp_customer_sk);

create index idx_w_warehouse_id         on warehouse (w_warehouse_id);

create index idx_c_customer_id          on customer (c_customer_id);
create index idx_c_current_cdemo_sk     on customer (c_current_cdemo_sk);
create index idx_c_current_hdemo_sk     on customer (c_current_hdemo_sk);
create index idx_c_current_addr_sk      on customer (c_current_addr_sk);
create index idx_c_first_shipto_date_sk on customer (c_first_shipto_date_sk);
create index idx_c_first_sales_date_sk  on customer (c_first_sales_date_sk);
create index idx_c_last_review_date_sk  on customer (c_last_review_date_sk);

create index idx_ca_address_id          on customer_address (ca_address_id);

create index idx_d_date_id              on date_dim (d_date_id);
create index idx_d_date                 on date_dim (d_date);

create index idx_hd_income_band_sk      on household_demographics (hd_income_band_sk)   ;

create index idx_i_item_id              on item (i_item_id);
create index idx_i_rec_start_date       on item (i_rec_start_date);
create index idx_i_rec_end_date         on item (i_rec_end_date);

create index idx_p_promo_id             on promotion (p_promo_id);
create index idx_p_start_date_sk        on promotion (p_start_date_sk);
create index idx_p_end_date_sk          on promotion (p_end_date_sk);
create index idx_p_item_sk              on promotion (p_item_sk);

create index idx_r_reason_id            on reason (r_reason_id);

create index idx_sm_ship_mode_id        on ship_mode (sm_ship_mode_id);

create index idx_t_time_id              on time_dim (t_time_id);
