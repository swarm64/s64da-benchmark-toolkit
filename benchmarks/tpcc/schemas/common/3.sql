-- The Shipping Priority Query retrieves the shipping priority and potential
-- revenue, defined as the sum of l_extendedprice * (1-l_discount), of the orders
-- having the largest revenue among those that had not been shipped as of a given
-- date. Orders are listed in decreasing order of revenue. If more than 10
-- unshipped orders exist, only the 10 orders with the largest revenue are listed.

-- Notes:
--   * Removed the market segment, as it does not apply for TPC-C
--   * Removed the o_shippriority, as it does not apply for TPC-C

DROP FUNCTION IF EXISTS shipping_priority(DATE);
CREATE FUNCTION shipping_priority(in_key_date DATE) RETURNS TABLE(
    orderkey BIGINT
  , revenue NUMERIC
  , orderdate TIMESTAMP
) AS $$
BEGIN
  CREATE TEMP TABLE shipping_priority AS
  SELECT
      ol_o_id AS orderkey
    , SUM(i_price * ol_quantity * (1 - c_discount)) AS revenue
    , o_entry_d AS orderdate
  FROM order_line
  JOIN orders ON ol_o_id = o_id AND ol_d_id = o_d_id AND ol_w_id = o_w_id
  JOIN customer ON o_c_id = c_id AND o_d_id = c_d_id AND o_w_id = c_w_id
  JOIN item ON ol_i_id = i_id
  WHERE o_entry_d < in_key_date::date
    AND ol_delivery_d > in_key_date::date
  GROUP BY ol_o_id, o_entry_d
  ORDER BY revenue DESC, o_entry_d ASC
  LIMIT 10;

  RETURN QUERY SELECT * FROM shipping_priority;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM shipping_priority('2001-01-01'::date);
