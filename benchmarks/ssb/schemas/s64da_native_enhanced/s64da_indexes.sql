CREATE INDEX customer_cache ON customer USING columnstore (
  c_custkey, c_name, c_address, c_city, c_nation, c_region, c_phone, c_mktsegment
);
CREATE INDEX part_cache ON part USING columnstore (
  p_partkey, p_name, p_mfgr, p_category, p_brand1, p_color, p_type, p_size, p_container
);
CREATE INDEX supplier_cache ON supplier USING columnstore (
  s_suppkey, s_name, s_address, s_city, s_nation, s_region, s_phone
);
CREATE INDEX lineorder_cache ON lineorder USING columnstore (
  lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey,
  lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity,
  lo_extendedprice, lo_ordertotalprice, lo_discount, lo_revenue,
  lo_supplycost, lo_tax, lo_commitdate, lo_shipmode
);
