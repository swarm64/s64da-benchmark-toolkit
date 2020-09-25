-- The Suppliers Who Kept Orders Waiting query identifies suppliers, for a
-- given nation, whose product was part of a multi-supplier order (with current
-- status of 'F') where they were the only supplier who failed to meet the
-- committed delivery date.
--
-- Notes:
--   * o_orderstatus not applicable
--   * nation/region -> warehouse

DROP FUNCTION IF EXISTS waiting_orders();
CREATE FUNCTION waiting_orders() RETURNS TABLE(
    w_name VARCHAR(10)
  , numwait BIGINT
) AS $$
BEGIN
  CREATE TEMP TABLE waiting_orders AS
  SELECT
      w.w_name
    , COUNT(*) AS numwait
  FROM order_line ol1
  JOIN warehouse w ON ol1.ol_w_id = w_id
  JOIN orders o ON ol1.ol_o_id = o.o_id AND ol1.ol_w_id = o.o_w_id AND ol1.ol_d_id = o.o_d_id
  WHERE ol1.ol_delivery_d > o.o_entry_d
    AND EXISTS(
      SELECT *
      FROM order_line ol2
      WHERE ol2.ol_o_id = ol1.ol_o_id AND ol2.ol_d_id = ol1.ol_d_id
        AND ol2.ol_w_id <> ol1.ol_w_id
    )
    AND NOT EXISTS (
      SELECT *
      FROM order_line ol3
      JOIN orders o3 ON ol3.ol_o_id = o3.o_id
                    AND ol3.ol_d_id = o3.o_d_id
                    AND ol3.ol_w_id = o3.o_w_id
      WHERE ol3.ol_o_id =  ol1.ol_o_id AND ol3.ol_d_id = ol1.ol_d_id
        AND ol3.ol_w_id <> ol1.ol_w_id
        AND ol3.ol_delivery_d > o3.o_entry_d
    )
  GROUP BY w.w_name
  ORDER BY numwait DESC, w.w_name
  LIMIT 100;

  RETURN QUERY SELECT * FROM waiting_orders;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM waiting_orders();
