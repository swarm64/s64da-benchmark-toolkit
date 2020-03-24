ALTER TABLE part      ADD PRIMARY KEY(p_partkey);
ALTER TABLE supplier  ADD PRIMARY KEY(s_suppkey);
ALTER TABLE date      ADD PRIMARY KEY(d_datekey);

ALTER FOREIGN TABLE customer  OPTIONS(ADD primary_key 'PRIMARY KEY(c_custkey)');
ALTER FOREIGN TABLE lineorder OPTIONS(ADD primary_key 'PRIMARY KEY(lo_orderkey, lo_linenumber)');
