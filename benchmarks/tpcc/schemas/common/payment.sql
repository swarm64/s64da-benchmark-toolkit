DROP FUNCTION IF EXISTS payment(INT, INT, INT, INT, INT, NUMERIC(12,2), BOOL, CHARACTER VARYING(16), TIMESTAMPTZ);
CREATE FUNCTION payment(
    in_w_id INT
  , in_d_id INT
  , in_c_d_id INT
  , in_c_id INT
  , in_c_w_id INT
  , in_h_amount NUMERIC(12,2)
  , in_byname BOOL
  , in_c_last CHARACTER VARYING(16)
  , in_timestamp TIMESTAMPTZ
) RETURNS TABLE(
    out_w_id INT
  , out_d_id INT
  , out_c_id INT
  , out_c_d_id INT
  , out_c_w_id INT
  , out_h_amount NUMERIC(12,2)
  , out_h_date TIMESTAMP WITH TIME ZONE
  , out_w_street1 CHARACTER VARYING(20)
  , out_w_street2 CHARACTER VARYING(20)
  , out_w_city CHARACTER VARYING(20)
  , out_w_state CHARACTER(2)
  , out_w_zip CHARACTER(9)
  , out_d_street1 CHARACTER VARYING(20)
  , out_d_street2 CHARACTER VARYING(20)
  , out_d_city CHARACTER VARYING(20)
  , out_d_state CHARACTER(2)
  , out_d_zip CHARACTER(9)
  , out_c_first character varying(16)
  , out_c_middle character(2)
  , out_c_last character varying(16)
  , out_c_street_1 character varying(20)
  , out_c_street_2 character varying(20)
  , out_c_city character varying(20)
  , out_c_state character(2)
  , out_c_zip character(9)
  , out_c_phone character(16)
  , out_c_since timestamp without time zone
  , out_c_credit character(2)
  , out_c_credit_lim bigint
  , out_c_discount numeric(4,2)
  , out_c_balance numeric(12,2)
  , out_c_data text
) AS $$
DECLARE
  w_record RECORD;
  d_record RECORD;
  c_record RECORD;
  namecount BIGINT;
BEGIN

  UPDATE warehouse
  SET w_ytd = w_ytd + in_h_amount
  WHERE w_id = in_w_id;

  SELECT
      w_street_1
    , w_street_2
    , w_city
    , w_state
    , w_zip
    , w_name
  INTO w_record
  FROM warehouse
  WHERE w_id = in_w_id;

  UPDATE district
  SET d_ytd = d_ytd + in_h_amount
  WHERE d_w_id = in_w_id
    AND d_id = in_d_id;

  SELECT
      d_street_1
    , d_street_2
    , d_city
    , d_state
    , d_zip
    , d_name
  INTO d_record
  FROM district
  WHERE d_w_id = in_w_id
    AND d_id = in_d_id;

  IF in_byname = true THEN
    SELECT count(c_id)
    FROM customer
    INTO namecount
    WHERE c_w_id = in_c_w_id
      AND c_d_id = in_c_d_id
      AND c_last = in_c_last;

    IF namecount % 2 = 0 THEN
      namecount = namecount + 1;
    END IF;

    SELECT c_id
    INTO in_c_id
    FROM customer
    WHERE c_w_id = in_c_w_id
      AND c_d_id = in_c_d_id
      AND c_last = in_c_last
    ORDER BY c_first
    OFFSET namecount / 2
    LIMIT 1;
  END IF;

  SELECT
      c_first
    , c_middle
    , c_last
    , c_street_1
    , c_street_2
    , c_city
    , c_state
    , c_zip
    , c_phone
    , c_credit
    , c_credit_lim
    , c_discount
    , c_balance
    , c_since
    , c_ytd_payment
  FROM customer
  INTO c_record
  WHERE c_w_id = in_c_w_id
    AND c_d_id = in_c_d_id
    AND c_id = in_c_id
  FOR UPDATE;

  IF c_record.c_credit = 'BC' THEN
    UPDATE customer
    SET c_balance = c_record.c_balance - in_h_amount
      , c_ytd_payment = c_record.c_ytd_payment + in_h_amount
      , c_data = (
          SELECT substr(
            format('| %4s %2s %4s %2s %4s $%s %12s %24s',
                c_id
              , c_d_id
              , c_w_id
              , in_d_id
              , in_w_id
              , to_char(in_h_amount, '9999999.99')
              , extract(epoch from clock_timestamp())
              , c_data
            ), 1, 500
          )
          FROM customer
          WHERE c_w_id = in_c_w_id
            AND c_d_id = in_c_d_id
            AND c_id = in_c_id
        )
    WHERE c_w_id = in_c_w_id
      AND c_d_id = in_c_d_id
      AND c_id = in_c_id;
  ELSE
    UPDATE customer
    SET c_balance = c_record.c_balance - in_h_amount
      , c_ytd_payment = c_record.c_ytd_payment + in_h_amount
    WHERE c_w_id = in_c_w_id
      AND c_d_id = in_c_d_id
      AND c_id = in_c_id;
  END IF;

  INSERT INTO history(
      h_c_d_id
    , h_c_w_id
    , h_c_id
    , h_d_id
    , h_w_id
    , h_date
    , h_amount
    , h_data
  ) VALUES(
      in_c_d_id
    , in_c_w_id
    , in_c_id
    , in_d_id
    , in_w_id
    , in_timestamp
    , in_h_amount
    , format('%10s %10s    ', w_record.w_name, d_record.d_name)
  );

  RETURN QUERY SELECT
      in_w_id
    , in_d_id
    , in_c_id
    , in_c_d_id
    , in_c_w_id
    , in_h_amount
    , in_timestamp
    , w_record.w_street_1
    , w_record.w_street_2
    , w_record.w_city
    , w_record.w_state
    , w_record.w_zip
    , d_record.d_street_1
    , d_record.d_street_2
    , d_record.d_city
    , d_record.d_state
    , d_record.d_zip
    , c_first
    , c_middle
    , c_last
    , c_street_1
    , c_street_2
    , c_city
    , c_state
    , c_zip
    , c_phone
    , c_since
    , c_credit
    , c_credit_lim
    , c_discount
    , c_balance
    , CASE WHEN c_credit = 'BC' THEN c_data ELSE NULL END
  FROM customer
  WHERE c_w_id = in_c_w_id
    AND c_d_id = in_c_d_id
    AND c_id = in_c_id
  ;
END
$$
LANGUAGE plpgsql PARALLEL SAFE;
