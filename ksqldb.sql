
--Create a stream from a topic
CREATE STREAM orders_stream WITH(
    KAFKA_TOPIC='orders', 
    VALUE_FORMAT='AVRO',
    PARTITIONS=6,
    TIMESTAMP='ordertime'
)



-- make data transformations from a stream
SELECT
    TIMESTAMPTOSTRING(ordertime, 'yyyy-MM-dd HH:mm:ss.SSS') AS ordertime_formatted,
    orderid,
    itemid,
    orderunits,
    address->city, 
    address->state,
    address->zipcode
    from ORDERS_STREAM;



--save transformation as another stream
CREATE STREAM orders_transformed AS
    SELECT
        TIMESTAMPTOSTRING(ordertime, 'yyyy-MM-dd HH:mm:ss.SSS') AS ordertime_formatted,
        orderid,
        itemid,
        orderunits,
        address->city, 
        address->state,
        address->zipcode
        from ORDERS_STREAM;


--make data aggregations from a stream
SELECT 
    address->state, count(address->state) as no_of_orders_per_state
FROM ORDERS_STREAM
WINDOW TUMBLING (SIZE 7 DAYS) 
GROUP BY address->state
EMIT CHANGES;


--save aggregation as a table
CREATE TABLE ORDERS_BY_STATE AS
SELECT address->state, count(address->state) as no_of_orders_per_state
FROM  ORDERS_STREAM
WINDOW TUMBLING (SIZE 7 DAYS) 
GROUP BY address->state
EMIT CHANGES;







