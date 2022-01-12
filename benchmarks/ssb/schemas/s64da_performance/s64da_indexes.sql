CREATE INDEX customer_cache ON customer USING columnstore ((0));
CREATE INDEX part_cache ON part USING columnstore ((0));
CREATE INDEX supplier_cache ON supplier USING columnstore ((0));
CREATE INDEX lineorder_cache ON lineorder USING columnstore ((0));
