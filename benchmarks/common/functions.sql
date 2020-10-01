DROP FUNCTION IF EXISTS partition_by_hash(VARCHAR, INT);
CREATE FUNCTION partition_by_hash(table_name VARCHAR, num_partitions INT) RETURNS VOID AS $$
DECLARE
  n INT;
BEGIN
    FOR n IN 1..num_partitions LOOP
        RAISE NOTICE 'Creating partition % of %', n, num_partitions;
        EXECUTE '
            CREATE TABLE ' || table_name || '__part_' || n || '
            PARTITION OF ' || table_name || '
            FOR VALUES WITH (MODULUS ' || num_partitions || ', REMAINDER ' || n - 1  || ')';
    END LOOP;
END;
$$ LANGUAGE plpgsql;
