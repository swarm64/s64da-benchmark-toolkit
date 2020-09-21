DROP FUNCTION IF EXISTS delivery(INT, INT, INT);
CREATE FUNCTION delivery(
    in_w_id INT
  , in_o_carrier_id INT
  , in_dist_per_ware INT
) RETURNS VOID AS $$
DECLARE
  d_id INT;
  this_no_o_id INT;
  this_o_c_id INT;
  ol_total NUMERIC(12,2);
BEGIN
  FOR d_id IN 1..in_dist_per_ware LOOP
    SELECT COALESCE(MIN(no_o_id), 0)
    FROM new_orders
    WHERE no_d_id = d_id
      AND no_w_id = in_w_id
    INTO this_no_o_id;

    DELETE FROM new_orders
    WHERE no_o_id = this_no_o_id
      AND no_d_id = d_id
      AND no_w_id = in_w_id;

    SELECT o_c_id
    FROM orders
    WHERE o_id = this_no_o_id
      AND o_d_id = d_id
      AND o_w_id = in_w_id
    INTO this_o_c_id;

    UPDATE orders
    SET o_carrier_id = in_o_carrier_id
    WHERE o_id = this_no_o_id
      AND o_d_id = d_id
      AND o_w_id = in_w_id;

    UPDATE order_line
    SET ol_delivery_d = NOW()
    WHERE ol_o_id = this_no_o_id
      AND ol_d_id = d_id
      AND ol_w_id = in_w_id;

    SELECT SUM(ol_amount)
    FROM order_line
    WHERE ol_o_id = this_no_o_id
      AND ol_d_id = d_id
      AND ol_w_id = in_w_id
    INTO ol_total;

    UPDATE customer
    SET c_balance = c_balance + ol_total
      , c_delivery_cnt = c_delivery_cnt + 1
    WHERE c_id = this_o_c_id
      AND c_d_id = d_id
      AND c_w_id = in_w_id;
  END LOOP;
END;
$$ LANGUAGE plpgsql;
