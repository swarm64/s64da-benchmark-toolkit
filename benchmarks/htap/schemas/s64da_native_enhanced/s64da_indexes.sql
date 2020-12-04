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

CREATE INDEX idx_item_cache ON item USING columnstore (
    i_id
  , i_im_id
  , i_name
  , i_price
  , i_data
);

CREATE INDEX idx_region_cache ON region USING columnstore (
    r_regionkey
  , r_name
  , r_comment
);

CREATE INDEX idx_nation_cache ON nation USING columnstore (
    n_nationkey
  , n_name
  , n_regionkey
  , n_comment
);

CREATE INDEX idx_supplier_cache ON supplier USING columnstore (
    su_suppkey
  , su_name
  , su_address
  , su_nationkey
  , su_phone
  , su_acctbal
  , su_comment
);
