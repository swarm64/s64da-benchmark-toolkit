CREATE INDEX idx_customer ON customer (c_w_id,c_d_id,c_last,c_first);
CREATE INDEX idx_orders ON orders (o_w_id,o_d_id,o_c_id,o_id);
CREATE INDEX fkey_stock_2 ON stock (s_i_id);
CREATE INDEX fkey_order_line_2 ON order_line (ol_supply_w_id,ol_i_id);
CREATE INDEX fkey_history_1 ON history (h_c_w_id,h_c_d_id,h_c_id);
CREATE INDEX fkey_history_2 ON history (h_w_id,h_d_id );


CREATE INDEX idx_customer_s64da ON customer USING columnstore(
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
) WITH(compression_type='lz4');

CREATE INDEX idx_district_tuple ON district(d_id, d_w_id);

CREATE INDEX idx_orders_s64da ON orders USING columnstore(
    o_id
  , o_d_id
  , o_w_id
  , o_c_id
  , o_entry_d
  , o_carrier_id
  , o_ol_cnt
  , o_all_local
) WITH(compression_type='lz4');

CREATE INDEX idx_order_line_s64da ON order_line USING columnstore(
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
) WITH(compression_type='lz4');
