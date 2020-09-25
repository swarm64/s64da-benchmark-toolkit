-- The Promotion Effect Query determines what percentage of the revenue in a
-- given year and month was derived from promotional parts. The query considers
-- only parts actually shipped in that month and gives the percentage. Revenue
-- is defined as (l_extendedprice * (1-l_discount)).

DROP FUNCTION IF EXISTS promotion_effect(DATE, VARCHAR(24));
CREATE FUNCTION promotion_effect(in_date DATE, in_item_name VARCHAR(24)) RETURNS NUMERIC AS $$
DECLARE
  promo_revenue NUMERIC;
BEGIN
  SELECT 100.00 * SUM(
    CASE
      WHEN i_name = in_item_name THEN i_price * ol_quantity * (1 - c_discount)
      ELSE 0
    END
  ) / SUM(i_price * ol_quantity * (1 - c_discount)) AS promo_revenue
  FROM order_line
  JOIN item ON ol_i_id = i_id
  JOIN orders ON ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id
  JOIN customer ON o_c_id = c_id AND o_w_id = c_w_id AND o_d_id = c_d_id
  WHERE ol_delivery_d >= date_trunc('month', in_date)
    AND ol_delivery_d <  date_trunc('month', in_date) + INTERVAL '1 month'
  INTO promo_revenue;

  RETURN promo_revenue;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM promotion_effect('2001-01-01'::date, 'item-8718-29.79910043632');
