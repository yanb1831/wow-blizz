 CREATE TABLE IF NOT EXISTS stage.auction (
    created_at timestamp DEFAULT CURRENT_TIMESTAMP,
    id bigint NULL,
    buyout bigint NULL,
    quantity int NULL,
    time_left varchar(30) NULL,
    item_id bigint NULL
    );
 CREATE TABLE IF NOT EXISTS prod.auction (
    created_at timestamp NOT NULL,
    id bigint NOT NULL,
    buyout bigint NULL,
    quantity int NULL,
    time_left varchar(30) NULL,
    item_id bigint NOT NULL,
    PRIMARY KEY (id)
 );

CREATE TABLE IF NOT EXISTS stage.token_prices (
    last_updated_timestamp bigint NULL,
    price bigint NULL,
    region varchar(2)
    );

CREATE TABLE IF NOT EXISTS prod.token_prices (
    last_updated_timestamp timestamp NULL,
    price decimal(9, 0) NULL,
    region varchar(2) NULL
    );

CREATE TABLE IF NOT EXISTS stage.items (
    created_at timestamp DEFAULT CURRENT_TIMESTAMP,
    id float NULL,
    name varchar(255) NULL,
    level float NULL,
    required_level float NULL
);

CREATE TABLE IF NOT EXISTS prod.items (
    created_at timestamp NOT NULL,
    id int NOT NULL,
    name varchar(255) NOT NULL,
    level int NOT NULL,
    required_level int NOT NULL,
    PRIMARY KEY (id)
);