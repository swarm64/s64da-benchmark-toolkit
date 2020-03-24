ALTER TABLE ONLY customer
    ADD CONSTRAINT customer_nation_fk FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey);
ALTER TABLE ONLY nation
    ADD CONSTRAINT nation_region_fk FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey);
ALTER TABLE ONLY partsupp
    ADD CONSTRAINT partsupp_part_fk FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey);
ALTER TABLE ONLY partsupp
    ADD CONSTRAINT partsupp_supplier_fk FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey);
ALTER TABLE ONLY supplier
    ADD CONSTRAINT supplier_nation_fk FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey);
