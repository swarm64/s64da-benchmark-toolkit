ALTER FOREIGN TABLE customer OPTIONS(ADD foreign_keys
    'FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey)');
ALTER FOREIGN TABLE nation OPTIONS(ADD foreign_keys
    'FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey)');
ALTER FOREIGN TABLE partsupp OPTIONS(ADD foreign_keys
    'FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey),'
    'FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey)');
ALTER FOREIGN TABLE supplier OPTIONS(ADD foreign_keys
    'FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey)');
ALTER FOREIGN TABLE lineitem OPTIONS(ADD foreign_keys
    'FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey),'
    'FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp(ps_partkey, ps_suppkey),'
    'FOREIGN KEY (l_partkey) REFERENCES part(p_partkey),'
    'FOREIGN KEY (l_suppkey) REFERENCES supplier(s_suppkey)');
ALTER FOREIGN TABLE orders OPTIONS(ADD foreign_keys
    'FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey)');
