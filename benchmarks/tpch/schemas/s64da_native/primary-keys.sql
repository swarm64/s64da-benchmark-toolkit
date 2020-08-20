ALTER TABLE ONLY customer
    ADD CONSTRAINT customer_pk PRIMARY KEY (c_custkey);
ALTER TABLE ONLY lineitem
    ADD CONSTRAINT lineitem_pk PRIMARY KEY (l_linenumber, l_orderkey);
ALTER TABLE ONLY nation
    ADD CONSTRAINT nation_pk PRIMARY KEY (n_nationkey);
ALTER TABLE ONLY orders
    ADD CONSTRAINT orders_pk PRIMARY KEY (o_orderkey);
ALTER TABLE ONLY part
    ADD CONSTRAINT part_pk PRIMARY KEY (p_partkey);
ALTER TABLE ONLY partsupp
    ADD CONSTRAINT partsupp_pk PRIMARY KEY (ps_partkey, ps_suppkey);
ALTER TABLE ONLY region
    ADD CONSTRAINT region_pk PRIMARY KEY (r_regionkey);
ALTER TABLE ONLY supplier
    ADD CONSTRAINT supplier_pk PRIMARY KEY (s_suppkey);
