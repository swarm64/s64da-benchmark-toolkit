ALTER TABLE lineorder ADD FOREIGN KEY(lo_custkey) REFERENCES customer(c_custkey);
ALTER TABLE lineorder ADD FOREIGN KEY(lo_partkey) REFERENCES part(p_partkey);
ALTER TABLE lineorder ADD FOREIGN KEY(lo_suppkey) REFERENCES supplier(s_suppkey);
ALTER TABLE lineorder ADD FOREIGN KEY(lo_orderdate) REFERENCES date(d_datekey);
