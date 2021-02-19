CREATE INDEX idx_customer_cache ON customer USING columnstore (
    c_id
  , c_d_id
  , c_w_id
  , c_first
  , c_middle
  , c_last
  , c_street_1
  , c_street_2
  , c_city
  , c_state
  , c_zip
  , c_phone
  , c_since
  , c_credit
  , c_credit_lim
  , c_discount
  , c_balance
  , c_ytd_payment
  , c_payment_cnt
  , c_delivery_cnt
  , c_data
);

CREATE INDEX idx_orders_cache ON orders USING columnstore (
    o_id
  , o_d_id
  , o_w_id
  , o_c_id
  , o_entry_d
  , o_carrier_id
  , o_ol_cnt
  , o_all_local
);

CREATE INDEX idx_order_line_cache ON order_line USING columnstore (
    ol_o_id
  , ol_d_id
  , ol_w_id
  , ol_number
  , ol_i_id
  , ol_supply_w_id
  , ol_delivery_d
  , ol_quantity
  , ol_amount
  , ol_dist_info
);

CREATE INDEX idx_stock_cache ON stock USING columnstore (
    s_i_id
  , s_w_id
  , s_quantity
  , s_dist_01
  , s_dist_02
  , s_dist_03
  , s_dist_04
  , s_dist_05
  , s_dist_06
  , s_dist_07
  , s_dist_08
  , s_dist_09
  , s_dist_10
  , s_ytd
  , s_order_cnt
  , s_remote_cnt
  , s_data
);

CREATE INDEX idx_history_cache ON history USING columnstore (
    id
  , h_c_id
  , h_c_d_id
  , h_c_w_id
  , h_d_id
  , h_w_id
  , h_date
  , h_amount
  , h_data
);