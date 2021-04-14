DROP FUNCTION IF EXISTS new_order(INT, INT, INT, INT, INT, INT[], INT[], INT[], TIMESTAMPTZ);
CREATE FUNCTION new_order(
    in_w_id INT
  , in_c_id INT
  , in_d_id INT
  , in_ol_cnt INT
  , in_all_local INT
  , in_itemids INT[]
  , in_supware INT[]
  , in_qty INT[]
  , in_timestamp TIMESTAMPTZ
) RETURNS TABLE(
    out_w_id INT
  , out_d_id INT
  , out_c_id INT
  , out_c_last CHARACTER VARYING(16)
  , out_c_credit CHARACTER(2)
  , out_c_discount NUMERIC(4,2)
  , out_w_tax NUMERIC(4,2)
  , out_d_tax NUMERIC(4,2)
  , out_o_ol_cnt INT
  , out_o_id INT
  , out_o_entry_d TIMESTAMPTZ
  , out_total_amount NUMERIC(13,2)
  , out_ol_supply_w_id INT
  , out_ol_i_id INT
  , out_i_name CHARACTER VARYING(24)
  , out_ol_quantity INT
  , out_s_quantity SMALLINT
  , out_brand_generic CHARACTER
  , out_i_price NUMERIC(5,2)
  , out_ol_amount NUMERIC(6,2)
) AS $$
DECLARE
  a RECORD;
  b RECORD;
  item_record RECORD;
  stock_record RECORD;

  return_value RECORD;

  ol_number INT;
  ol_supply_w_id INT;
  ol_i_id INT;
  ol_quantity INT;

  ol_amount NUMERIC(6,2);
BEGIN
  SELECT
      c_discount
    , c_last
    , c_credit
    , w_tax
  INTO a
  FROM customer, warehouse
  WHERE w_id = in_w_id
    AND c_w_id = w_id
    AND c_d_id = in_d_id
    AND c_id = in_c_id;

  UPDATE district
  SET d_next_o_id = d_next_o_id + 1
  WHERE d_id = in_d_id
    AND d_w_id = in_w_id
  RETURNING d_next_o_id, d_tax INTO b;

  INSERT INTO orders(
      o_id
    , o_d_id
    , o_w_id
    , o_c_id
    , o_entry_d
    , o_ol_cnt
    , o_all_local
  ) VALUES (
      b.d_next_o_id
    , in_d_id
    , in_w_id
    , in_c_id
    , in_timestamp
    , in_ol_cnt
    , in_all_local
  );

  INSERT INTO new_orders(no_o_id, no_d_id, no_w_id)
  VALUES (b.d_next_o_id, in_d_id, in_w_id);

  FOR ol_number IN 1 .. in_ol_cnt LOOP
    ol_i_id = in_itemids[ol_number];

    SELECT i_price, i_name, i_data
    INTO item_record
    FROM item
    WHERE i_id = ol_i_id;

    IF item_record IS NULL THEN
      RAISE EXCEPTION 'Item record is null';
    END IF;

    ol_supply_w_id = in_supware[ol_number];
    ol_quantity = in_qty[ol_number];

    UPDATE stock
    SET s_quantity = CASE
          WHEN s_quantity > ol_quantity THEN s_quantity - ol_quantity
          ELSE s_quantity - ol_quantity + 91
        END
      , s_order_cnt = s_order_cnt + 1
      , s_remote_cnt = CASE
                         WHEN ol_supply_w_id <> in_w_id THEN s_remote_cnt + 1
                         ELSE s_remote_cnt
                       END
    WHERE s_i_id = ol_i_id
      AND s_w_id = ol_supply_w_id
    RETURNING
        s_data, s_quantity,
        CASE
          WHEN in_d_id = 1 THEN s_dist_01
          WHEN in_d_id = 2 THEN s_dist_02
          WHEN in_d_id = 3 THEN s_dist_03
          WHEN in_d_id = 4 THEN s_dist_04
          WHEN in_d_id = 5 THEN s_dist_05
          WHEN in_d_id = 6 THEN s_dist_06
          WHEN in_d_id = 7 THEN s_dist_07
          WHEN in_d_id = 8 THEN s_dist_08
          WHEN in_d_id = 9 THEN s_dist_09
          WHEN in_d_id = 10 THEN s_dist_10
        END AS ol_dist_info
    INTO stock_record;

    ol_amount = ol_quantity * item_record.i_price * (1 + a.w_tax + b.d_tax) * (1 - a.c_discount);

    INSERT INTO order_line(
        ol_o_id
      , ol_d_id
      , ol_w_id
      , ol_number
      , ol_i_id
      , ol_supply_w_id
      , ol_quantity
      , ol_amount
      , ol_dist_info
    ) VALUES (
        b.d_next_o_id
      , in_d_id
      , in_w_id
      , ol_number
      , ol_i_id
      , ol_supply_w_id
      , ol_quantity
      , ol_amount
      , stock_record.ol_dist_info
    );

    RETURN QUERY SELECT
        in_w_id
      , in_d_id
      , in_c_id
      , a.c_last
      , a.c_credit
      , a.c_discount
      , a.w_tax
      , b.d_tax
      , in_ol_cnt
      , b.d_next_o_id
      , in_timestamp
      , ROUND(
          SUM(ol_amount) * (1 - a.c_discount) * (1 + a.w_tax + b.d_tax), 2
        ) AS amount
      , ol_supply_w_id
      , ol_i_id
      , item_record.i_name
      , ol_quantity
      , stock_record.s_quantity
      , CASE WHEN item_record.i_data LIKE '%ORIGINAL%'
              AND stock_record.s_data LIKE '%ORIGINAL%' THEN 'B'::CHAR ELSE 'G'::CHAR END
      , item_record.i_price
      , ol_amount
    ;
  END LOOP;

  RETURN;
END
$$ LANGUAGE PLPGSQL PARALLEL SAFE;
