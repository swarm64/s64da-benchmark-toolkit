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
) RETURNS VOID AS $$
DECLARE
  w_record RECORD;
  d_record RECORD;
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

  UPDATE customer
  SET c_balance = c_balance - in_h_amount
    , c_ytd_payment = c_ytd_payment + in_h_amount
    , c_data =
      CASE
        WHEN c_credit = 'BC' THEN
          substr(
            format('| %4s %2s %4s %2s %4s $%s %12s %24s',
                c_id
              , c_d_id
              , c_w_id
              , in_d_id
              , in_w_id
              , to_char(in_h_amount, '9999999.99')
              , extract(epoch from in_timestamp)
              , c_data
            ), 1, 500
          )
        ELSE c_data
      END
  WHERE c_w_id = in_c_w_id
    AND c_d_id = in_c_d_id
    AND c_id = in_c_id;

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
END
$$
LANGUAGE plpgsql PARALLEL SAFE;
