INSERT INTO prod.auction (created_at, id, buyout, quantity, time_left, item_id)
SELECT created_at, id, buyout / 10000.0 AS buyout, quantity, time_left, item_id
FROM stage.auction
WHERE created_at::date = '{{ ds }}'
ORDER BY 1;