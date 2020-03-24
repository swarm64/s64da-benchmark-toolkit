ALTER FOREIGN TABLE lineorder OPTIONS(ADD foreign_keys'
    FOREIGN KEY(lo_custkey) REFERENCES customer(c_custkey),
    FOREIGN KEY(lo_partkey) REFERENCES part(p_partkey),
    FOREIGN KEY(lo_suppkey) REFERENCES supplier(s_suppkey),
    FOREIGN KEY(lo_orderdate) REFERENCES date(d_datekey)
');
