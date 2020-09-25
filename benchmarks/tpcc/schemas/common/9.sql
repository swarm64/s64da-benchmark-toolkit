-- The Product Type Profit Measure Query finds, for each nation and each year,
-- the profit for all parts ordered in that year that contain a specified
-- substring in their names and that were filled by a supplier in that nation.
-- The profit is defined as the sum of [(l_extendedprice*(1-l_discount)) -
-- (ps_supplycost * l_quantity)] for all lineitems describing parts in the
-- specified line. The query lists the nations in ascending alphabetical order
-- and, for each nation, the year and profit in descending order by year (most
-- recent first).
--
-- Notes:
--   * nation -> district
--   * part -> item, name as input variable
--   * assumes that supply costs are 85% of price

DROP FUNCTION IF EXISTS product_type_profit_measure(VARCHAR(24));
CREATE FUNCTION product_type_profit_measure(in_item_name VARCHAR(24)) RETURNS TABLE(
    district_name VARCHAR(10)
  , order_year INT
  , sum_profit NUMERIC
) AS $$
BEGIN
  CREATE TEMP TABLE product_type_profit_measure AS
  SELECT
      profit.district_name
    , profit.order_year
    , SUM(amount) AS sum_profit
  FROM(
    SELECT
        d_name AS district_name
      , extract(year FROM o_entry_d)::INT AS order_year
      , i_price * ol_quantity * (1 - c_discount) - (i_price * 0.15) * ol_quantity AS amount
    FROM order_line
    JOIN item ON ol_i_id = i_id
    JOIN orders ON ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id
    JOIN customer ON o_c_id = c_id AND o_w_id = c_w_id AND o_d_id = c_d_id
    JOIN district ON o_d_id = d_id AND o_w_id = d_w_id
    WHERE i_name LIKE '%' || in_item_name || '%'
  ) AS profit
  GROUP BY profit.district_name, profit.order_year
  ORDER BY profit.district_name, profit.order_year DESC;

  RETURN QUERY SELECT * FROM product_type_profit_measure;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM product_type_profit_measure('5796');

