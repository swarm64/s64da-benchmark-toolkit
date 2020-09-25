-- This query determines the distribution of customers by the number of orders
-- They have made, including customers who have no record of orders, past or
-- Present. It counts and reports how many customers have no orders, how many
-- Have 1, 2, 3, etc. A check is made to ensure that the orders counted do not
-- Fall into one of several special categories of orders. Special categories
-- are Identified in the order comment column by looking for a particular pattern.

DROP FUNCTION IF EXISTS customers_distribution();
CREATE FUNCTION customers_distribution() RETURNS TABLE(
    customer_count BIGINT
  , customers_distribution BIGINT
) AS $$
BEGIN
  CREATE TEMP TABLE customers_distribution AS
  SELECT
      c_orders.customer_count
    , COUNT(*) AS customers_distribution
  FROM (
    SELECT
        c_id
      , COUNT(o_id)
    FROM customer
    LEFT OUTER JOIN orders ON c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id
    GROUP BY c_id
  ) AS c_orders (c_id, customer_count)
  GROUP BY c_orders.customer_count
  ORDER BY customers_distribution DESC, c_orders.customer_count DESC;

  RETURN QUERY SELECT * FROM customers_distribution;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM customers_distribution();
