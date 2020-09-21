ALTER TABLE warehouse ADD CONSTRAINT warehouse_pk PRIMARY KEY (w_id);

ALTER TABLE district ADD CONSTRAINT district_pk PRIMARY KEY (d_w_id, d_id);

ALTER TABLE customer ADD CONSTRAINT customer_pk PRIMARY KEY (c_w_id, c_d_id, c_id);

ALTER TABLE history ADD CONSTRAINT history_pk PRIMARY KEY (id);

ALTER TABLE orders ADD CONSTRAINT orders_pk PRIMARY KEY (o_w_id, o_d_id, o_id);

ALTER TABLE new_orders ADD CONSTRAINT new_orders_pk PRIMARY KEY (no_w_id, no_d_id, no_o_id);

ALTER TABLE order_line ADD CONSTRAINT order_line_pk PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number);

ALTER TABLE stock ADD CONSTRAINT stock_pk PRIMARY KEY (s_w_id, s_i_id);

ALTER TABLE item ADD CONSTRAINT item_pk PRIMARY KEY (i_id);

ALTER TABLE region ADD CONSTRAINT region_pk PRIMARY KEY (r_regionkey);

ALTER TABLE nation ADD CONSTRAINT nation_pk PRIMARY KEY (n_nationkey);

ALTER TABLE supplier ADD CONSTRAINT supplier_pk PRIMARY KEY (su_suppkey);
