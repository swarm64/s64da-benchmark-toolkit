ALTER TABLE customer
    ADD CONSTRAINT customer_nation_fk FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey);
ALTER TABLE lineitem
    ADD CONSTRAINT lineitem_order_fk FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey);
ALTER TABLE ONLY nation
    ADD CONSTRAINT nation_region_fk FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey);
ALTER TABLE orders
    ADD CONSTRAINT order_customer_fk FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey);
ALTER TABLE supplier
    ADD CONSTRAINT supplier_nation_fk FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey);
