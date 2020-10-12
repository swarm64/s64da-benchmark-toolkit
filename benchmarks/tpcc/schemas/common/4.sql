-- The Order Priority Checking Query counts the number of orders ordered in a
-- given quarter of a given year in which at least one lineitem was received by
-- the customer later than its committed date. The query lists the count of
-- such orders for each order priority sorted in ascending priority order.

DROP FUNCTION IF EXISTS priority_checking(DATE);
CREATE FUNCTION priority_checking(in_date DATE) RETURNS TABLE(
    quarter_begin DATE
  , quarter_end DATE
  , order_count BIGINT
) AS $$
BEGIN
  CREATE TEMP TABLE priority_checking AS
  SELECT
      date_trunc('quarter', in_date)::DATE AS quarter_begin
    , (date_trunc('quarter', in_date) + INTERVAL '3 month' - INTERVAL '1 day')::DATE AS quarter_end
    , count(*) AS order_count
  FROM orders
  WHERE o_entry_d >= date_trunc('quarter', in_date)
    AND o_entry_d <  date_trunc('quarter', in_date) + INTERVAL '3 month'
    AND EXISTS (
      SELECT *
      FROM order_line
      WHERE ol_o_id = o_id AND ol_d_id = o_d_id AND ol_w_id = o_w_id
        AND o_entry_d < ol_delivery_d
    )
  GROUP BY 1, 2;

  RETURN QUERY SELECT * FROM priority_checking;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM priority_checking('2001-01-01'::date);
