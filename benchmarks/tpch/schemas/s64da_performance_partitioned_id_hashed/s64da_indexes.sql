CREATE INDEX part_cache ON part USING columnstore (
    p_partkey,
    p_name,
    p_mfgr,
    p_brand,
    p_type,
    p_size,
    p_container,
    p_retailprice,  -- unused
    p_comment       -- unused
);

CREATE INDEX supplier_cache ON supplier USING columnstore (
    s_suppkey,
    s_name,
    s_address,
    s_nationkey,
    s_phone,
    s_acctbal,
    s_comment
);

CREATE INDEX partsupp_cache ON partsupp USING columnstore (
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    ps_comment     -- unused
);

CREATE INDEX customer_cache ON customer USING columnstore (
    c_custkey,
    c_name,
    c_address,
    c_nationkey,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment
);

CREATE INDEX orders_cache ON orders USING columnstore (
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,         -- unused
    o_shippriority,
    o_comment
);

CREATE INDEX lineitem_cache ON lineitem USING columnstore (
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber, -- unused
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode,
    l_comment --unused
);
