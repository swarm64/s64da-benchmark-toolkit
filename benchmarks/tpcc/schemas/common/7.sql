-- The Volume Shipping Query finds, for two given nations, the gross discounted
-- revenues derived from lineitems in which parts were shipped from a supplier
-- in either nation to a customer in the other nation during 1995 and 1996. The
-- query lists the supplier nation, the customer nation, the year, and the
-- revenue from shipments that took place in that year. The query orders the
-- answer by Supplier nation, Customer nation, and year (all ascending).
--
-- Notes:
--  * Replaced nation by district
--  * Supplier district -> order district
--  * Date is now a variable

DROP FUNCTION IF EXISTS volume_shipping(DATE, VARCHAR(10), VARCHAR(10));
CREATE FUNCTION volume_shipping(
    in_date DATE
  , in_district_1 VARCHAR(10)
  , in_district_2 VARCHAR(10)
) RETURNS TABLE (
    order_district_name VARCHAR(10)
  , customer_district_name VARCHAR(10)
  , delivery_year INT
  , revenue NUMERIC
) AS $$
BEGIN
  CREATE TEMP TABLE volume_shipping AS
  SELECT
      shipping.order_district_name
    , shipping.customer_district_name
    , shipping.delivery_year
    , SUM(shipping.volume) AS revenue
  FROM (
    SELECT
        ds.d_name AS order_district_name
      , dc.d_name AS customer_district_name
      , EXTRACT(year from ol_delivery_d)::INT AS delivery_year
      , i_price * ol_quantity * (1 - c_discount) AS volume
    FROM order_line
    JOIN item ON ol_i_id = i_id
    JOIN orders ON ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id
    JOIN customer ON o_c_id = c_id AND o_w_id = c_w_id AND o_d_id = c_d_id
    JOIN warehouse ON o_w_id = w_id
    JOIN district ds ON o_d_id = ds.d_id AND o_w_id = ds.d_w_id
    JOIN district dc ON c_d_id = dc.d_id AND c_w_id = dc.d_w_id
    WHERE (
        (ds.d_name = in_district_1 AND dc.d_name = in_district_2) OR
        (ds.d_name = in_district_2 AND dc.d_name = in_district_1)
      ) AND ol_delivery_d >= date_trunc('year', in_date)
        AND ol_delivery_d <  date_trunc('year', in_date) + INTERVAL '2 year'
  ) AS shipping
  GROUP BY shipping.order_district_name, shipping.customer_district_name, shipping.delivery_year
  ORDER BY shipping.order_district_name, shipping.customer_district_name, shipping.delivery_year;

  RETURN QUERY SELECT * FROM volume_shipping;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM volume_shipping('2001-01-01'::date, 'name-WLPHO', 'name-umTno');
