INSERT INTO prod.token_prices (last_updated_timestamp, price, region)
SELECT to_timestamp(last_updated_timestamp / 1000) AT time ZONE 'Europe/Moscow' AS last_updated_timestamp,
	   price / 10000 AS price,
	   region
FROM stage.token_prices
WHERE to_timestamp(last_updated_timestamp / 1000)::date = '{{ ds }}'
ORDER BY 1;