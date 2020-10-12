-- The Small-Quantity-Order Revenue Query considers parts of a given brand and
-- with a given container type and determines the average lineitem quantity of
-- such parts ordered for all orders (past and pending) in the 7-year data-
-- base. What would be the average yearly gross (undiscounted) loss in revenue
-- if orders for these parts with a quantity of less than 20% of this average
-- were no longer taken?
--
-- Notes:
--   * Brand -> Item name
--   * Container not used

DROP FUNCTION IF EXISTS small_quantity_order_revenue(VARCHAR(24));
CREATE FUNCTION small_quantity_order_revenue(in_item_name VARCHAR(24)) RETURNS NUMERIC AS $$
DECLARE
  avg_yearly NUMERIC;
BEGIN
  SELECT SUM(i_price * ol_quantity) / 7.0
  FROM order_line
  JOIN item ON ol_i_id = i_id
  WHERE i_name = in_item_name
    AND ol_quantity < (
      SELECT 0.2 * AVG(ol_quantity)
      FROM order_line
      WHERE ol_i_id = i_id
    )
  INTO avg_yearly;

  RETURN avg_yearly;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM small_quantity_order_revenue('item-1208-22.65503544243');
