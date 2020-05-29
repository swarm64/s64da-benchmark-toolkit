ALTER FOREIGN TABLE customer OPTIONS(ADD primary_key_hint 'PRIMARY KEY (c_custkey)');
ALTER FOREIGN TABLE nation OPTIONS(ADD primary_key_hint 'PRIMARY KEY (n_nationkey)');
ALTER FOREIGN TABLE part OPTIONS(ADD primary_key_hint 'PRIMARY KEY (p_partkey)');
ALTER FOREIGN TABLE partsupp OPTIONS(ADD primary_key_hint 'PRIMARY KEY (ps_partkey, ps_suppkey)');
ALTER FOREIGN TABLE region OPTIONS(ADD primary_key_hint 'PRIMARY KEY (r_regionkey)');
ALTER FOREIGN TABLE supplier OPTIONS(ADD primary_key_hint 'PRIMARY KEY (s_suppkey)');
ALTER FOREIGN TABLE lineitem OPTIONS(ADD primary_key_hint 'PRIMARY KEY (l_linenumber, l_orderkey)');
ALTER FOREIGN TABLE orders OPTIONS(ADD primary_key_hint 'PRIMARY KEY (o_orderkey)');
