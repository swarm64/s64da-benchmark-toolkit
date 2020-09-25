-- The Large Volume Customer Query finds a list of the top 100 customers who
-- have ever placed large quantity orders. The query lists the customer name,
-- customer key, the order key, date and total price and the quantity for the
-- order.

DROP FUNCTION IF EXISTS large_volume_customer();
CREATE FUNCTION large_volume_customer() RETURNS TABLE(
    c_id INTEGER
  , c_d_id SMALLINT
  , c_w_id SMALLINT
  , c_last VARCHAR(16)
  , o_id BIGINT
  , o_entry_d TIMESTAMP
  , total_price NUMERIC
  , quantity BIGINT
) AS $$
BEGIN
  CREATE TEMP TABLE large_volume_customer AS
  SELECT
      c.c_id
    , c.c_w_id
    , c.c_d_id
    , c.c_last
    , o.o_id
    , o.o_entry_d
    , SUM(i_price * ol_quantity) AS total_price
    , SUM(ol_quantity) AS quantity
  FROM order_line
  JOIN item ON ol_i_id = i_id
  JOIN orders o ON ol_o_id = o.o_id AND ol_w_id = o.o_w_id AND ol_d_id = o.o_d_id
  JOIN customer c ON o_c_id = c.c_id AND o_w_id = c.c_w_id AND o_d_id = c.c_d_id
  WHERE '{o.o_id, o.o_w_id, o.o_d_id}' IN (
    SELECT '{ol_o_id, ol_w_id, ol_d_id}'
    FROM order_line
    GROUP BY ol_o_id, ol_w_id, ol_d_id
    HAVING SUM(ol_quantity) > 314
  )
  GROUP BY c.c_id, c.c_w_id, c.c_d_id, c.c_last, o.o_id, o.o_entry_d
  ORDER BY total_price DESC, o.o_entry_d
  LIMIT 100;

  RETURN QUERY SELECT * FROM large_volume_customer;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM large_volume_customer();
