{% set num_partitions = num_partitions|int %}

CREATE EXTENSION swarm64da;

CREATE TABLE nation (
    n_nationkey int NOT NULL,
    n_name character varying(25) NOT NULL,
    n_regionkey int NOT NULL,
    n_comment character varying(152) NOT NULL
) PARTITION BY HASH (n_nationkey);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    nation_prt_{{ partition_idx }}
PARTITION OF
    nation FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER {{ partition_idx }})
SERVER swarm64da_server OPTIONS (optimized_columns 'n_nationkey', optimization_level_target '900');
{% endfor %}

CREATE TABLE region (
    r_regionkey int NOT NULL,
    r_name character varying(25) NOT NULL,
    r_comment character varying(152) NOT NULL
) PARTITION BY HASH (r_regionkey);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    region_prt_{{ partition_idx }}
PARTITION OF
    region FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER {{ partition_idx }})
SERVER swarm64da_server OPTIONS (optimized_columns 'r_regionkey', optimization_level_target '900');
{% endfor %}

CREATE TABLE part (
    p_partkey int NOT NULL,
    p_name character varying(55) NOT NULL,
    p_mfgr character varying(25) NOT NULL,
    p_brand character varying(10) NOT NULL,
    p_type character varying(25) NOT NULL,
    p_size int NOT NULL,
    p_container character varying(10) NOT NULL,
    p_retailprice double precision NOT NULL,
    p_comment character varying(23) NOT NULL
) PARTITION BY HASH (p_partkey);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    part_prt_{{ partition_idx }}
PARTITION OF
    part FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER {{ partition_idx }})
SERVER
   swarm64da_server options(optimized_columns 'p_partkey', optimization_level_target '900');
{% endfor %}


CREATE TABLE supplier (
    s_suppkey int NOT NULL,
    s_name character varying(25) NOT NULL,
    s_address character varying(40) NOT NULL,
    s_nationkey int NOT NULL,
    s_phone character varying(15) NOT NULL,
    s_acctbal double precision NOT NULL,
    s_comment character varying(101) NOT NULL
) PARTITION BY HASH (s_suppkey);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    supplier_prt_{{ partition_idx }}
PARTITION OF
    supplier FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER {{ partition_idx }})
SERVER
   swarm64da_server options(optimized_columns 's_nationkey', optimization_level_target '900');
{% endfor %}


CREATE TABLE partsupp (
    ps_partkey int NOT NULL,
    ps_suppkey int NOT NULL,
    ps_availqty int NOT NULL,
    ps_supplycost double precision NOT NULL,
    ps_comment character varying(199) NOT NULL
) PARTITION BY HASH (ps_partkey);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    partsupp_prt_{{ partition_idx }}
PARTITION OF
    partsupp FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER {{ partition_idx }})
SERVER
   swarm64da_server options(optimized_columns 'ps_suppkey', optimization_level_target '900');
{% endfor %}


CREATE TABLE customer (
    c_custkey int NOT NULL,
    c_name character varying(25) NOT NULL,
    c_address character varying(40) NOT NULL,
    c_nationkey int NOT NULL,
    c_phone character varying(15) NOT NULL,
    c_acctbal double precision NOT NULL,
    c_mktsegment character varying(10) NOT NULL,
    c_comment character varying(117) NOT NULL
) PARTITION BY HASH (c_custkey);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    customer_prt_{{ partition_idx }}
PARTITION OF
    customer FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER {{ partition_idx }})
SERVER
   swarm64da_server options(optimized_columns 'c_nationkey', optimization_level_target '900');
{% endfor %}


CREATE TABLE orders (
    o_orderkey bigint NOT NULL,
    o_custkey int NOT NULL,
    o_orderstatus character varying(1) NOT NULL,
    o_totalprice double precision NOT NULL,
    o_orderdate date NOT NULL,
    o_orderpriority character varying(15) NOT NULL,
    o_clerk character varying(15) NOT NULL,
    o_shippriority int NOT NULL,
    o_comment character varying(79) NOT NULL
) PARTITION BY HASH (o_orderkey);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    orders_prt_{{ partition_idx }}
PARTITION OF
    orders FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER {{ partition_idx }})
SERVER
   swarm64da_server options(optimized_columns 'o_orderdate, o_custkey', optimization_level_target '900');
{% endfor %}


CREATE TABLE lineitem (
    l_orderkey bigint NOT NULL,
    l_partkey int NOT NULL,
    l_suppkey int NOT NULL,
    l_linenumber int NOT NULL,
    l_quantity double precision NOT NULL,
    l_extendedprice double precision NOT NULL,
    l_discount double precision NOT NULL,
    l_tax double precision NOT NULL,
    l_returnflag character varying(1) NOT NULL,
    l_linestatus character varying(1) NOT NULL,
    l_shipdate date NOT NULL,
    l_commitdate date NOT NULL,
    l_receiptdate date NOT NULL,
    l_shipinstruct character varying(25) NOT NULL,
    l_shipmode character varying(10) NOT NULL,
    l_comment character varying(44) NOT NULL
) PARTITION BY HASH (l_orderkey);

{% for partition_idx in range(num_partitions) %}
CREATE FOREIGN TABLE
    lineitem_prt_{{ partition_idx }}
PARTITION OF
    lineitem FOR VALUES WITH (MODULUS {{ num_partitions }}, REMAINDER {{ partition_idx }})
SERVER
   swarm64da_server options(optimized_columns 'l_shipdate, l_partkey, l_receiptdate', optimization_level_target '900');
{% endfor %}
