-- The Top Supplier Query finds the supplier who contributed the most to the
-- overall revenue for parts shipped during a given quarter of a given year. In
-- case of a tie, the query lists all suppliers whose contribution was equal to
-- the maximum, presented in supplier number order.
--
-- Notes:
--   * supplier -> warehouse

DROP FUNCTION IF EXISTS top_warehouse(DATE);
CREATE FUNCTION top_warehouse(in_date DATE) RETURNS TABLE (
    w_id SMALLINT
  , w_name VARCHAR(10)
  , w_street_1 VARCHAR(20)
  , w_street_2 VARCHAR(20)
  , w_city VARCHAR(20)
  , w_state CHAR(2)
  , w_zip CHAR(9)
  , total_revenue NUMERIC
) AS $$
BEGIN
  CREATE TEMP TABLE top_warehouse AS
  WITH revenue0 AS(
    SELECT
        w.w_id AS warehouse_no
      , SUM(i_price * ol_quantity * (1 - c_discount)) AS total_revenue
    FROM order_line
    JOIN item ON ol_i_id = i_id
    JOIN orders ON ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id
    JOIN customer ON o_c_id = c_id AND o_w_id = c_w_id AND o_d_id = c_d_id
    JOIN warehouse w ON o_w_id = w.w_id
    WHERE ol_delivery_d >= date_trunc('month', in_date)
      AND ol_delivery_d <  date_trunc('month', in_date) + INTERVAL '3 month'
    GROUP BY w.w_id
  ) SELECT
      w.w_id
    , w.w_name
    , w.w_street_1
    , w.w_street_2
    , w.w_city
    , w.w_state
    , w.w_zip
    , r0.total_revenue
  FROM warehouse w
  JOIN revenue0 r0 ON w.w_id = r0.warehouse_no
  WHERE r0.total_revenue = (
    SELECT max(r0_inner.total_revenue)
    FROM revenue0 r0_inner
  )
  ORDER BY w.w_id;

  RETURN QUERY SELECT * FROM top_warehouse;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM top_warehouse('2001-01-01'::date);
