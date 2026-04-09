CREATE TABLE datagen (
    event_type int,
    person ROW<
        id  BIGINT,
        name  VARCHAR,
        emailAddress  VARCHAR,
        creditCard  VARCHAR,
        city  VARCHAR,
        state  VARCHAR,
        `dateTime` TIMESTAMP(3),
        extra  VARCHAR>,
    auction ROW<
        id  BIGINT,
        itemName  VARCHAR,
        description  VARCHAR,
        initialBid  BIGINT,
        reserve  BIGINT,
        `dateTime`  TIMESTAMP(3),
        expires  TIMESTAMP(3),
        seller  BIGINT,
        category  BIGINT,
        extra  VARCHAR>,
    bid ROW<
        auction  BIGINT,
        bidder  BIGINT,
        price  BIGINT,
        channel  VARCHAR,
        url  VARCHAR,
        `dateTime`  TIMESTAMP(3),
        extra  VARCHAR>,
    `dateTime` AS
        CASE
            WHEN event_type = 0 THEN person.`dateTime`
            WHEN event_type = 1 THEN auction.`dateTime`
            ELSE bid.`dateTime`
        END,
    WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND
    -- To enable a partition key column (BIGINT epoch-ms, start of day UTC), add the line below
    -- and set 'partition.key.field' = '<column-name>' in the WITH clause:
    -- , `timestamp` BIGINT
) WITH (
    'connector' = 'nexmark',
    'first-event.rate' = '${TPS}',
    'next-event.rate' = '${TPS}',
    'events.num' = '${EVENTS_NUM}',
    'person.proportion' = '${PERSON_PROPORTION}',
    'auction.proportion' = '${AUCTION_PROPORTION}',
    'bid.proportion' = '${BID_PROPORTION}'
    -- Partition key options (all optional):
    -- , 'partition.key.field' = 'timestamp'         -- enables partition column (default: disabled)
    -- , 'partition.number' = '7'                    -- generate values: today, today-1, ..., today-6
    -- , 'partition.distribution.mode' = 'UNIFORM'   -- UNIFORM (hash, deterministic) | LATEST | RANDOM (non-deterministic) | SKEWED | CUSTOM
);