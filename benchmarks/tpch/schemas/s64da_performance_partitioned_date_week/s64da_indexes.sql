CREATE INDEX part_cache ON part USING columnstore ((0));
CREATE INDEX supplier_cache ON supplier USING columnstore ((0));
CREATE INDEX partsupp_cache ON partsupp USING columnstore ((0));
CREATE INDEX customer_cache ON customer USING columnstore ((0));
CREATE INDEX orders_cache ON orders USING columnstore ((0));
CREATE INDEX lineitem_cache ON lineitem USING columnstore ((0));
