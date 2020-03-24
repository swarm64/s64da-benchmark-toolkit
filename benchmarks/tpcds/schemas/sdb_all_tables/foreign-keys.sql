ALTER FOREIGN TABLE call_center OPTIONS (ADD foreign_keys
    'foreign key (cc_open_date_sk)          references date_dim (d_date_sk),
    foreign key (cc_closed_date_sk)         references date_dim (d_date_sk)'
);

ALTER FOREIGN TABLE catalog_page OPTIONS (ADD foreign_keys
    'foreign key (cp_start_date_sk)         references date_dim (d_date_sk),
    foreign key (cp_end_date_sk)            references date_dim (d_date_sk)'
);

ALTER FOREIGN TABLE customer OPTIONS (ADD foreign_keys
    'foreign key (c_current_cdemo_sk)       references customer_demographics (cd_demo_sk),
    foreign key (c_current_hdemo_sk)        references household_demographics (hd_demo_sk),
    foreign key (c_current_addr_sk)         references customer_address (ca_address_sk),
    foreign key (c_first_shipto_date_sk)    references date_dim (d_date_sk),
    foreign key (c_first_sales_date_sk)     references date_dim (d_date_sk),
    foreign key (c_last_review_date_sk)     references date_dim (d_date_sk)'
);

ALTER FOREIGN TABLE household_demographics OPTIONS (ADD foreign_keys
    'foreign key (hd_income_band_sk)        references income_band (ib_income_band_sk)'
);

ALTER FOREIGN TABLE promotion OPTIONS (ADD foreign_keys
    'foreign key (p_start_date_sk)          references date_dim (d_date_sk),
    foreign key (p_end_date_sk)             references date_dim (d_date_sk),
    foreign key (p_item_sk)                 references item (i_item_sk)'
);

ALTER FOREIGN TABLE store OPTIONS (ADD foreign_keys
    'foreign key (s_closed_date_sk)         references date_dim (d_date_sk)'
);

ALTER FOREIGN TABLE web_page OPTIONS (ADD foreign_keys
    'foreign key (wp_creation_date_sk)      references date_dim (d_date_sk),
    foreign key (wp_access_date_sk)         references date_dim (d_date_sk),
    foreign key (wp_customer_sk)            references customer (c_customer_sk)'
);

ALTER FOREIGN TABLE web_site OPTIONS (ADD foreign_keys
    'foreign key (web_open_date_sk)         references date_dim (d_date_sk),
    foreign key (web_close_date_sk)         references date_dim (d_date_sk)'
);

ALTER FOREIGN TABLE catalog_returns OPTIONS (ADD foreign_keys
   'foreign key (cr_returned_date_sk)       references date_dim (d_date_sk),
    foreign key (cr_returned_time_sk)       references time_dim (t_time_sk),
    foreign key (cr_returned_date_sk)       references date_dim (d_date_sk),
    foreign key (cr_returned_time_sk)       references time_dim (t_time_sk),
    foreign key (cr_item_sk)                references item (i_item_sk),
    foreign key (cr_item_sk, cr_order_number) references catalog_sales (cs_item_sk, cs_order_number),
    foreign key (cr_refunded_customer_sk)   references customer (c_customer_sk),
    foreign key (cr_refunded_cdemo_sk)      references customer_demographics (cd_demo_sk),
    foreign key (cr_refunded_hdemo_sk)      references household_demographics (hd_demo_sk),
    foreign key (cr_refunded_addr_sk)       references customer_address (ca_address_sk),
    foreign key (cr_returning_customer_sk)  references customer (c_customer_sk),
    foreign key (cr_returning_cdemo_sk)     references customer_demographics (cd_demo_sk),
    foreign key (cr_returning_hdemo_sk)     references household_demographics (hd_demo_sk),
    foreign key (cr_returning_addr_sk)      references customer_address (ca_address_sk),
    foreign key (cr_call_center_sk)         references call_center (cc_call_center_sk),
    foreign key (cr_catalog_page_sk)        references catalog_page (cp_catalog_page_sk),
    foreign key (cr_ship_mode_sk)           references ship_mode (sm_ship_mode_sk),
    foreign key (cr_warehouse_sk)           references warehouse (w_warehouse_sk),
    foreign key (cr_reason_sk)              references reason (r_reason_sk)'
);

ALTER FOREIGN TABLE catalog_sales OPTIONS(ADD foreign_keys
    'foreign key (cs_sold_date_sk)          references date_dim (d_date_sk),
    foreign key (cs_sold_time_sk)           references time_dim (t_time_sk),
    foreign key (cs_ship_date_sk)           references date_dim (d_date_sk),
    foreign key (cs_bill_customer_sk)       references customer (c_customer_sk),
    foreign key (cs_bill_cdemo_sk)          references customer_demographics (cd_demo_sk),
    foreign key (cs_bill_hdemo_sk)          references household_demographics (hd_demo_sk),
    foreign key (cs_bill_addr_sk)           references customer_address (ca_address_sk),
    foreign key (cs_ship_customer_sk)       references customer (c_customer_sk),
    foreign key (cs_ship_cdemo_sk)          references customer_demographics (cd_demo_sk),
    foreign key (cs_ship_hdemo_sk)          references household_demographics (hd_demo_sk),
    foreign key (cs_ship_addr_sk)           references customer_address (ca_address_sk),
    foreign key (cs_call_center_sk)         references call_center (cc_call_center_sk) ,
    foreign key (cs_catalog_page_sk)        references catalog_page (cp_catalog_page_sk),
    foreign key (cs_ship_mode_sk)           references ship_mode (sm_ship_mode_sk),
    foreign key (cs_warehouse_sk)           references warehouse (w_warehouse_sk),
    foreign key (cs_item_sk)                references item (i_item_sk),
    foreign key (cs_promo_sk)               references promotion (p_promo_sk)'
);

ALTER FOREIGN TABLE inventory OPTIONS(ADD foreign_keys
    'foreign key (inv_date_sk)              references date_dim (d_date_sk),
    foreign key (inv_item_sk)               references item (i_item_sk),
    foreign key (inv_warehouse_sk)          references warehouse (w_warehouse_sk)'
);

ALTER FOREIGN TABLE store_returns OPTIONS(ADD foreign_keys
    'foreign key (sr_returned_date_sk)      references date_dim (d_date_sk),
    foreign key (sr_return_time_sk)         references time_dim (t_time_sk),
    foreign key (sr_item_sk)                references item (i_item_sk),
    foreign key (sr_item_sk, sr_ticket_number) references store_sales (ss_item_sk, ss_ticket_number),
    foreign key (sr_customer_sk)            references customer (c_customer_sk),
    foreign key (sr_cdemo_sk)               references customer_demographics (cd_demo_sk),
    foreign key (sr_hdemo_sk)               references household_demographics (hd_demo_sk),
    foreign key (sr_addr_sk)                references customer_address (ca_address_sk),
    foreign key (sr_store_sk)               references store (s_store_sk),
    foreign key (sr_reason_sk)              references reason (r_reason_sk)'
);

ALTER FOREIGN TABLE store_sales OPTIONS(ADD foreign_keys
    'foreign key (ss_sold_date_sk)          references date_dim (d_date_sk),
    foreign key (ss_sold_time_sk)           references time_dim (t_time_sk),
    foreign key (ss_item_sk)                references item (i_item_sk),
    foreign key (ss_customer_sk)            references customer (c_customer_sk),
    foreign key (ss_cdemo_sk)               references customer_demographics (cd_demo_sk),
    foreign key (ss_hdemo_sk)               references household_demographics (hd_demo_sk),
    foreign key (ss_addr_sk)                references customer_address (ca_address_sk),
    foreign key (ss_store_sk)               references store (s_store_sk),
    foreign key (ss_promo_sk)               references promotion (p_promo_sk)'
);

ALTER FOREIGN TABLE web_returns OPTIONS(ADD foreign_keys
    'foreign key (wr_returned_date_sk)      references date_dim (d_date_sk),
    foreign key (wr_returned_time_sk)       references time_dim (t_time_sk),
    foreign key (wr_item_sk)                references item (i_item_sk),
    foreign key (wr_item_sk, wr_order_number) references web_sales (ws_item_sk, ws_order_number),
    foreign key (wr_refunded_customer_sk)   references customer (c_customer_sk),
    foreign key (wr_refunded_cdemo_sk)      references customer_demographics (cd_demo_sk),
    foreign key (wr_refunded_hdemo_sk)      references household_demographics (hd_demo_sk),
    foreign key (wr_refunded_addr_sk)       references customer_address (ca_address_sk),
    foreign key (wr_returning_customer_sk)  references customer (c_customer_sk),
    foreign key (wr_returning_cdemo_sk)     references customer_demographics (cd_demo_sk),
    foreign key (wr_returning_hdemo_sk)     references household_demographics (hd_demo_sk),
    foreign key (wr_returning_addr_sk)      references customer_address (ca_address_sk),
    foreign key (wr_web_page_sk)            references web_page (wp_web_page_sk),
    foreign key (wr_reason_sk)              references reason (r_reason_sk)'
);

ALTER FOREIGN TABLE web_sales OPTIONS(ADD foreign_keys
    'foreign key (ws_sold_date_sk)          references date_dim (d_date_sk),
    foreign key (ws_sold_time_sk)           references time_dim (t_time_sk),
    foreign key (ws_ship_date_sk)           references date_dim (d_date_sk),
    foreign key (ws_item_sk)                references item (i_item_sk),
    foreign key (ws_bill_customer_sk)       references customer (c_customer_sk),
    foreign key (ws_bill_cdemo_sk)          references customer_demographics (cd_demo_sk),
    foreign key (ws_bill_hdemo_sk)          references household_demographics (hd_demo_sk),
    foreign key (ws_bill_addr_sk)           references customer_address (ca_address_sk),
    foreign key (ws_ship_customer_sk)       references customer (c_customer_sk),
    foreign key (ws_ship_cdemo_sk)          references customer_demographics (cd_demo_sk),
    foreign key (ws_ship_hdemo_sk)          references household_demographics (hd_demo_sk),
    foreign key (ws_ship_addr_sk)           references customer_address (ca_address_sk),
    foreign key (ws_web_page_sk)            references web_page (wp_web_page_sk),
    foreign key (ws_web_site_sk)            references web_site (web_site_sk),
    foreign key (ws_ship_mode_sk)           references ship_mode (sm_ship_mode_sk),
    foreign key (ws_warehouse_sk)           references warehouse (w_warehouse_sk),
    foreign key (ws_promo_sk)               references promotion (p_promo_sk)'
);
