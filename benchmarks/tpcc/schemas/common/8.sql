-- The market share for a given nation within a given region is defined as the
-- fraction of the revenue, the sum of [l_extendedprice * (1-l_discount)], from
-- the products of a specified type in that region that was supplied by
-- suppliers from the given nation. The query determines this for the years
-- 1995 and 1996 presented in this order.
--
-- Notes:
--  * nation/region replaced by district
--  * year is an input variable
--  * product of specific type becomes specific item

DROP FUNCTION IF EXISTS market_share(DATE, VARCHAR(10), VARCHAR(24));
CREATE FUNCTION market_share(
    in_date DATE
  , in_district VARCHAR(10)
  , in_item_name VARCHAR(24)
) RETURNS TABLE(
    order_year INT
  , market_share NUMERIC
) AS $$
BEGIN
  CREATE TEMP TABLE market_share AS
  SELECT
      tmp.order_year
    , SUM(CASE WHEN d_name = in_district THEN volume ELSE 0 END) / SUM(volume) AS market_share
  FROM (
    SELECT
        extract(year FROM o_entry_d)::INT AS order_year
      , i_price * ol_quantity * (1 - c_discount) AS volume
      , d_name
    FROM order_line
    JOIN item ON ol_i_id = i_id
    JOIN orders ON ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id
    JOIN customer ON o_c_id = c_id AND o_w_id = c_w_id AND o_d_id = c_d_id
    JOIN district ON o_d_id = d_id AND o_w_id = d_w_id
    WHERE i_name = in_item_name
      AND o_entry_d >  in_date - INTERVAL '2 year'
      AND o_entry_d <= in_date
  ) tmp
  GROUP BY tmp.order_year
  ORDER BY tmp.order_year;

  RETURN QUERY SELECT * FROM market_share;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM market_share('2001-01-01'::date, 'name-DTrzQ', 'item-5796-37.87251706609');

