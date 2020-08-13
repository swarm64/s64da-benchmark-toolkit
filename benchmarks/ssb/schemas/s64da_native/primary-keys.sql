ALTER TABLE customer  ADD PRIMARY KEY(c_custkey);
ALTER TABLE part      ADD PRIMARY KEY(p_partkey);
ALTER TABLE supplier  ADD PRIMARY KEY(s_suppkey);
ALTER TABLE date      ADD PRIMARY KEY(d_datekey);
ALTER TABLE lineorder ADD PRIMARY KEY(lo_orderkey, lo_linenumber);
