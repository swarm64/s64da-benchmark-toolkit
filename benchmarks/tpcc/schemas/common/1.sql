-- Business question:
-- The Pricing Summary Report Query provides a summary pricing report for all
-- lineitems shipped as of a given date. The date is within 60 - 120 days of the
-- greatest ship date contained in the database. The query lists totals for
-- extended price, discounted extended price, discounted extended price plus tax,
-- average quantity, average extended price, and average discount. These aggregates
-- are grouped by RETURNFLAG and LINESTATUS, and listed in ascending order of
-- RETURNFLAG and LINESTATUS. A count of the number of lineitems in each group is
-- included.

DROP FUNCTION IF EXISTS pricing_summary(DATE);
CREATE FUNCTION pricing_summary(in_date DATE) RETURNS TABLE (
    sum_qty BIGINT
  , sum_base_price NUMERIC
  , sum_discounted_price NUMERIC
  , sum_charge NUMERIC
  , avg_qty NUMERIC
  , avg_price NUMERIC
  , avg_discount NUMERIC
  , count_order BIGINT
) AS $$
BEGIN
  CREATE TEMP TABLE rettable AS
  SELECT
      SUM(ol_quantity) AS sum_qty
    , SUM(i_price * ol_quantity) AS sum_base_price
    , SUM(i_price * ol_quantity * (1 - c_discount)) AS sum_discounted_price
    , SUM(i_price * ol_quantity * (1 - c_discount) * (1 + w_tax)) AS sum_charge
    , AVG(ol_quantity) AS avg_qty
    , AVG(i_price) AS avg_price
    , AVG(c_discount) AS avg_discount
    , COUNT(*) AS count_order
  FROM order_line
  JOIN orders ON ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id
  JOIN customer ON o_c_id = c_id AND o_d_id = c_d_id AND o_w_id = c_w_id
  JOIN item ON ol_i_id = i_id
  JOIN warehouse ON ol_w_id = w_id
  WHERE ol_delivery_d <= in_date - INTERVAL '90 day';

  RETURN QUERY SELECT * FROM rettable;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM pricing_summary('2020-01-01');
