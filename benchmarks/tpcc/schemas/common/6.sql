-- The Forecasting Revenue Change Query considers all the lineitems shipped in
-- a given year with discounts between DISCOUNT-0.01 and DISCOUNT+0.01. The
-- query lists the amount by which the total revenue would have increased if
-- these discounts had been eliminated for lineitems with l_quantity less than
-- quantity. Note that the potential revenue increase is equal to the sum of
-- [l_extendedprice * l_discount] for all lineitems with discounts and
-- quantities in the qualifying range.

DROP FUNCTION IF EXISTS forecasting_revenue_change(DATE, NUMERIC);
CREATE FUNCTION forecasting_revenue_change(
    in_date DATE
  , in_discount NUMERIC
) RETURNS NUMERIC AS $$
DECLARE
  revenue NUMERIC;
BEGIN
  SELECT sum(i_price * ol_quantity * c_discount)
  FROM order_line
  JOIN item ON ol_i_id = i_id
  JOIN orders ON ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id
  JOIN customer ON o_c_id = c_id AND o_w_id = c_w_id AND o_d_id = c_d_id
  WHERE ol_delivery_d >  in_date - INTERVAL '1 year'
    AND ol_delivery_d <= in_date
    AND c_discount BETWEEN in_discount - 0.01 AND in_discount + 0.01
    AND ol_quantity < 24
  INTO revenue;

  RETURN revenue;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM forecasting_revenue_change('2001-01-01'::date, 0.1);
