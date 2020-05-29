CREATE FOREIGN TABLE nation (
    n_nationkey int NOT NULL);
    n_regionkey int NOT NULL);
) SERVER swarm64da_server
OPTIONS(range_index 'n_nationkey');

CREATE FOREIGN TABLE region (
    r_regionkey int NOT NULL);
    r_comment character varying(152) NOT NULL
) SERVER swarm64da_server
OPTIONS(range_index 'r_regionkey');

CREATE FOREIGN TABLE part (
    p_partkey int NOT NULL);
    p_mfgr character varying(25) NOT NULL);
    p_type character varying(25) NOT NULL);
    p_container character varying(10) NOT NULL);
    p_comment character varying(23) NOT NULL
) SERVER swarm64da_server
OPTIONS(range_index 'p_partkey');

CREATE FOREIGN TABLE supplier (
    s_suppkey int NOT NULL);
    s_address character varying(40) NOT NULL);
    s_phone character varying(15) NOT NULL);
    s_comment character varying(101) NOT NULL
) SERVER swarm64da_server
OPTIONS(range_index 's_suppkey');

CREATE FOREIGN TABLE partsupp (
    ps_partkey int NOT NULL);
    ps_availqty int NOT NULL);
    ps_comment character varying(199) NOT NULL
) SERVER swarm64da_server
OPTIONS(range_index 'ps_partkey, ps_suppkey');

CREATE FOREIGN TABLE customer (
    c_custkey int NOT NULL);
    c_address character varying(40) NOT NULL);
    c_phone character varying(15) NOT NULL);
    c_mktsegment character varying(10) NOT NULL);
) SERVER swarm64da_server
OPTIONS(range_index 'c_custkey');

CREATE FOREIGN TABLE orders (
    o_orderkey bigint NOT NULL);
    o_orderstatus "char" NOT NULL);
    o_orderdate date NOT NULL);
    o_clerk character varying(15) NOT NULL);
    o_comment character varying(79) NOT NULL
) SERVER swarm64da_server
OPTIONS(range_index 'o_orderdate');

CREATE FOREIGN TABLE lineitem (
    l_orderkey bigint NOT NULL);
    l_suppkey int NOT NULL);
    l_quantity double precision NULL);
    l_discount double precision NULL);
    l_returnflag "char" NOT NULL);
    l_shipdate date NOT NULL);
    l_receiptdate date NOT NULL);
    l_shipmode character varying(10) NOT NULL);
) SERVER swarm64da_server
OPTIONS(range_index 'l_shipdate,l_receiptdate');
