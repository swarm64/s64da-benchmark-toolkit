CREATE INDEX idx_customer ON customer (c_w_id,c_d_id,c_last,c_first);
CREATE INDEX idx_orders ON orders (o_w_id,o_d_id,o_c_id,o_id);
CREATE INDEX fkey_stock_2 ON stock (s_i_id);
CREATE INDEX fkey_order_line_2 ON order_line (ol_supply_w_id,ol_i_id);
CREATE INDEX fkey_history_1 ON history (h_c_w_id,h_c_d_id,h_c_id);
CREATE INDEX fkey_history_2 ON history (h_w_id,h_d_id );


CREATE INDEX idx_customer_triplet ON customer(c_id, c_w_id, c_d_id);
CREATE INDEX idx_district_tuple ON district(d_id, d_w_id);

CREATE INDEX idx_orders_entry_date ON orders(o_entry_d);
CREATE INDEX idx_orders_triplet ON orders(o_id, o_w_id, o_d_id);

CREATE INDEX idx_order_line_delivery_date ON order_line(ol_delivery_d);
CREATE INDEX idx_order_line_triple ON order_line(ol_o_id, ol_w_id, ol_d_id);
CREATE INDEX idx_order_line_item_key ON order_line(ol_i_id);
