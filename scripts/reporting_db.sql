CREATE TYPE t_db_types AS ENUM(
    'postgresql'
  , 'swarm64da'
);

CREATE TABLE meta(
    id UUID PRIMARY KEY
  , timestamp_start TIMESTAMP
  , timestamp_stop TIMESTAMP
  , db_type t_db_types
  , db_version VARCHAR(25)
  , benchmark JSONB
  , db_config JSONB
  , machine_config JSONB
);

CREATE TYPE t_query_types AS ENUM(
    'ingest'
  , 'maintenance'
  , 'query'
);

CREATE TYPE t_query_status AS ENUM(
    'ok'
  , 'error'
  , 'timeout'
);

CREATE TABLE query_data(
    run_id UUID
  , query_type t_query_types
  , stream_id INT
  , query_identifier VARCHAR(25)
  , timestamp_start TIMESTAMP
  , timestamp_stop TIMESTAMP
  , status t_query_status
  , status_detail TEXT
);

CREATE TABLE system_data(
    run_id UUID
  , timestamp_snapshot TIMESTAMP
  , data JSONB
);
