DROP DATABASE IF EXISTS htap_stats;
CREATE DATABASE htap_stats;
\c htap_stats

CREATE TABLE oltp_stats(
    id UUID
  , ts TIMESTAMP NOT NULL
  , current_ok_rate INT NOT NULL
  , current_err_rate INT NOT NULL
);

ALTER TABLE oltp_stats ADD PRIMARY KEY(id, ts);
CREATE INDEX oltp_stats_ts_idx ON oltp_stats USING brin(ts);

CREATE TABLE olap_stats(
    id UUID
  , ts TIMESTAMP NOT NULL
  , worker_id SMALLINT NOT NULL
  , query_id SMALLINT NOT NULL
  , runtime DOUBLE PRECISION NOT NULL
);

ALTER TABLE olap_stats ADD PRIMARY KEY(id, ts, worker_id, query_id);
CREATE INDEX olap_stats_ts_idx ON olap_stats USING brin(ts);
