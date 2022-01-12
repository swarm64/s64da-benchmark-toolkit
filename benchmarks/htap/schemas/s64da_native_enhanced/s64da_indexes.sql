CREATE INDEX idx_customer_cache ON customer USING columnstore ((0));
CREATE INDEX idx_orders_cache ON orders USING columnstore ((0));
CREATE INDEX idx_order_line_cache ON order_line USING columnstore ((0));
CREATE INDEX idx_stock_cache ON stock USING columnstore ((0));
