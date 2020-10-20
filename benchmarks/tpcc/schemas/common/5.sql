-- The Local Supplier Volume Query lists for each nation in a region the
-- revenue volume that resulted from lineitem transactions in which the
-- customer ordering parts and the supplier filling them were both within that
-- nation. The query is run in order to determine whether to institute local
-- distribution centers in a given region. The query considers only parts
-- ordered in a given year. The query displays the nations and revenue volume
-- in descending order by revenue. Revenue volume for all qualifying lineitems
-- in a particular nation is defined as sum(l_extendedprice * (1 - l_discount)).
--
-- Notes:
--  * District instead of nation/region

DROP FUNCTION IF EXISTS local_supplier_volume(DATE);
CREATE FUNCTION local_supplier_volume(in_date DATE) RETURNS TABLE(
    district_name VARCHAR(10)
  , revenue NUMERIC
) AS $$
BEGIN
  CREATE TEMP TABLE local_supplier_volume AS
  SELECT
      d_name AS district_name
    , SUM(i_price * ol_quantity * (1 - c_discount)) AS revenue
  FROM order_line
  JOIN orders ON ol_o_id = o_id AND ol_d_id = o_d_id AND ol_w_id = o_w_id
  JOIN item ON ol_i_id = i_id
  JOIN customer ON o_c_id = c_id AND o_d_id = c_d_id AND o_w_id = c_w_id
  JOIN district ON o_d_id = d_id AND o_w_id = d_w_id
  WHERE o_entry_d >   in_date - INTERVAL '1 year'
    AND o_entry_d <=  in_date
  GROUP BY d_name
  ORDER BY revenue DESC;

  RETURN QUERY SELECT * FROM local_supplier_volume;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM local_supplier_volume('2001-01-01'::date);
