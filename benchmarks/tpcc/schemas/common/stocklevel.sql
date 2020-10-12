DROP FUNCTION IF EXISTS stocklevel(INT, INT, INT);
CREATE FUNCTION stocklevel(
    in_w_id INT
  , in_d_id INT
  , in_threshold INT
) RETURNS INT AS $$
DECLARE
  this_d_next_o_id INT;
  low_stock_count INT;
BEGIN
  SELECT d_next_o_id
  FROM district
  WHERE d_id = in_d_id
    AND d_w_id = in_w_id
  INTO this_d_next_o_id;

  SELECT COUNT(DISTINCT(s_i_id))
  FROM order_line, stock
  WHERE ol_w_id = in_w_id
    AND ol_d_id = in_d_id
    AND ol_o_id <  this_d_next_o_id
    AND ol_o_id >= this_d_next_o_id
    AND s_w_id = in_w_id
    AND s_i_id = ol_i_id
    AND s_quantity < in_threshold
  INTO low_stock_count;

  RETURN low_stock_count;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;
