-- The Potential Part Promotion query identifies suppliers who have an excess
-- of a given part available; an excess is defined to be more than 50% of the
-- parts like the given part that the supplier shipped in a given year for a
-- given nation. Only parts whose names share a certain naming convention are
-- considered.
--
-- Notes:
--   * Part -> Item
--   * Name becomes input variable
--   * Date also input variable
--   * Nation -> Removed, go over all warehouses

DROP FUNCTION IF EXISTS potential_part_promotion(in_date DATE, in_item_name VARCHAR(24));
CREATE FUNCTION potential_part_promotion(in_date DATE, in_item_name VARCHAR(24)) RETURNS TABLE(
    w_name VARCHAR(10)
  , w_street_1 VARCHAR(20)
  , w_street_2 VARCHAR(20)
  , w_city VARCHAR(20)
  , w_state CHAR(2)
  , w_zip CHAR(9)
) AS $$
BEGIN
  CREATE TEMP TABLE potential_part_promotion AS
  SELECT
      w.w_name
    , w.w_street_1
    , w.w_street_2
    , w.w_city
    , w.w_state
    , w.w_zip
  FROM warehouse w
  WHERE w_id IN (
    SELECT s_w_id
    FROM stock
    WHERE s_i_id IN (
      SELECT i_id
      FROM item
      WHERE i_name LIKE '%' || in_item_name || '%'
    ) AND s_quantity > (
      SELECT 0.5 * SUM(ol_quantity)
      FROM order_line
      JOIN item ON ol_i_id = i_id
      WHERE ol_delivery_d >= date_trunc('year', in_date)
        AND ol_delivery_d <  date_trunc('year', in_date) + INTERVAL '1 year'
        AND ol_w_id = w_id
    )
  )
  ORDER BY w.w_name;

  RETURN QUERY SELECT * FROM potential_part_promotion;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM potential_part_promotion('2001-01-01'::date, 'item-4985-91.23485831739');
