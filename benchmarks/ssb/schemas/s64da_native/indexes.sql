CREATE INDEX idx_customer  ON customer(c_custkey);
CREATE INDEX idx_part      ON part(p_partkey);
CREATE INDEX idx_supplier  ON supplier(s_suppkey);
CREATE INDEX idx_date      ON date(d_datekey);
CREATE INDEX idx_lineorder ON lineorder(lo_orderkey, lo_linenumber);
